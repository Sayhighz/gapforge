"""PostgreSQL boundary for durable evidence-pipeline stages."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.analysis.deduplication import minhash_signature, minhash_similarity
from gapforge.analysis.identity import pseudonymize_author
from gapforge.analysis.normalization import content_hash, normalize_text, normalize_url
from gapforge.domain.contracts import (
    CollectedItem,
    CollectResult,
    RunMode,
    Source,
)
from gapforge.integration.mappers import (
    checkpoint_from_storage,
    checkpoint_to_storage,
    mission_revision_from_storage,
    storage_uuid_for_identifier,
)
from gapforge.runtime.evidence_pipeline import (
    CapturedEvidence,
    CollectionBudgetExhaustedError,
    ExistingCandidate,
    ExistingIntelligence,
    PipelineContext,
    PipelineStageCommit,
)
from gapforge.runtime.evidence_stages import CompetitorResearchCheckpoint
from gapforge.storage import models


class StageConflictError(RuntimeError):
    """A stage key was reused with different durable content."""


class StageArtifactWriter(Protocol):
    async def persist_stage(
        self,
        session: AsyncSession,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None: ...


class SqlAlchemyEvidencePipelineStore:
    """Commit stage checkpoints, budgets, and raw lineage in one transaction."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        author_hmac_secret: bytes | None,
        clock: Callable[[], datetime],
        artifact_writer: StageArtifactWriter | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.author_hmac_secret = author_hmac_secret
        self.clock = clock
        self.artifact_writer = artifact_writer

    async def load_context(self, task: models.ResearchTask) -> PipelineContext:
        async with self.session_factory() as session:
            fresh = await session.get(models.ResearchTask, task.id)
            if fresh is None:
                raise LookupError("research task no longer exists")
            if fresh.lease_owner is None:
                raise PermissionError("research task has no active worker lease")
            run = await session.get(models.ResearchRun, fresh.run_id)
            if run is None:
                raise LookupError("research run no longer exists")
            revision = await session.get(models.MissionRevision, run.mission_revision_id)
            if revision is None:
                raise LookupError("mission revision no longer exists")
            checkpoints = (
                await session.scalars(
                    select(models.SourceCheckpoint).where(
                        models.SourceCheckpoint.mission_revision_id == revision.id
                    )
                )
            ).all()
            source_checkpoints = {
                Source(row.source): checkpoint_from_storage(
                    Source(row.source), row.cursor, row.watermark_at
                )
                for row in checkpoints
            }
            limits = {key: int(value) for key, value in run.budget_limits.items()}
            limits["max_collector_requests_per_run"] = max(
                0,
                limits["max_collector_requests_per_run"]
                - int(run.budget_used.get("collector_requests", 0)),
            )
            limits["max_raw_signals_per_run"] = max(
                0,
                limits["max_raw_signals_per_run"] - int(run.budget_used.get("raw_signals", 0)),
            )
            limits["max_search_calls_per_run"] = max(
                0,
                limits.get("max_search_calls_per_run", 0)
                - int(run.budget_used.get("search_calls", 0)),
            )
            limits["max_agent_calls_per_run"] = max(
                0,
                limits.get("max_agent_calls_per_run", 0)
                - int(run.budget_used.get("agent_calls", 0)),
            )
            return PipelineContext(
                run_id=run.id,
                task_id=fresh.id,
                task_attempt=fresh.attempt_count,
                worker_id=fresh.lease_owner,
                mission_revision=mission_revision_from_storage(revision),
                mode=RunMode(run.mode),
                budget_limits=limits,
                collection_until=run.started_at or self.clock(),
                source_checkpoints=source_checkpoints,
            )

    async def query_existing(self, context: PipelineContext) -> ExistingIntelligence:
        async with self.session_factory() as session:
            query = func.plainto_tsquery("simple", context.mission_revision.prompt)
            evidence_rows = (
                await session.execute(
                    select(models.RawSignalRevision)
                    .where(models.RawSignalRevision.search_document.bool_op("@@")(query))
                    .order_by(
                        func.ts_rank(models.RawSignalRevision.search_document, query).desc(),
                        models.RawSignalRevision.domain_revision_id,
                    )
                    .limit(12)
                )
            ).scalars()
            opportunity_rows = (
                await session.execute(
                    select(models.Opportunity)
                    .where(
                        func.similarity(models.Opportunity.title, context.mission_revision.prompt)
                        >= 0.2
                    )
                    .order_by(
                        func.similarity(
                            models.Opportunity.title, context.mission_revision.prompt
                        ).desc(),
                        models.Opportunity.id,
                    )
                    .limit(8)
                )
            ).scalars()
            candidates = tuple(
                [
                    ExistingCandidate(
                        "EVIDENCE",
                        row.domain_revision_id,
                        _summary(row.title, row.body),
                    )
                    for row in evidence_rows
                ]
                + [
                    ExistingCandidate("OPPORTUNITY", str(row.id), row.title[:500])
                    for row in opportunity_rows
                ]
            )
        return ExistingIntelligence(candidates[:20])

    async def load_stage(
        self,
        context: PipelineContext,
        stage: str,
    ) -> dict[str, object] | None:
        async with self.session_factory() as session:
            task = await session.get(models.ResearchTask, context.task_id)
            if task is None:
                raise LookupError("research task no longer exists")
            run = await session.scalar(
                select(models.ResearchRun)
                .where(models.ResearchRun.id == context.run_id)
                .with_for_update()
            )
            if run is None:
                raise LookupError("research run no longer exists")
            _assert_stage_lease(task, run, context, self.clock())
            stages = _pipeline_stages(task.checkpoint)
            record = stages.get(stage)
            if not isinstance(record, dict):
                return None
            payload = record.get("payload")
            if not isinstance(payload, dict):
                raise StageConflictError("durable stage payload is invalid")
            return cast(dict[str, object], payload)

    async def load_evidence(
        self,
        context: PipelineContext,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CapturedEvidence, ...]:
        async with self.session_factory() as session:
            rows = (
                await session.execute(
                    select(models.RawSignalRevision, models.RawSignal)
                    .join(
                        models.RawSignal,
                        models.RawSignal.id == models.RawSignalRevision.raw_signal_id,
                    )
                    .where(models.RawSignalRevision.domain_revision_id.in_(evidence_ids))
                )
            ).all()
        by_id = {
            revision.domain_revision_id: CapturedEvidence(
                evidence_id=revision.domain_revision_id,
                source=Source(raw.source),
                url=raw.canonical_url,
                text="\n".join(value for value in (revision.title, revision.body) if value),
                observed_at=revision.observed_at,
                duplicate_group=revision.duplicate_group_key,
                author_id=raw.author_pseudonym,
                thread_id=raw.parent_external_id or raw.external_id,
            )
            for revision, raw in rows
        }
        if set(by_id) != set(evidence_ids):
            raise LookupError("checkpoint references missing evidence revisions")
        return tuple(by_id[evidence_id] for evidence_id in evidence_ids)

    async def commit_stage(
        self,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None:
        async with self.session_factory() as session:
            task = await session.scalar(
                select(models.ResearchTask)
                .where(models.ResearchTask.id == context.task_id)
                .with_for_update()
            )
            if task is None:
                raise LookupError("research task no longer exists")
            run = await session.scalar(
                select(models.ResearchRun)
                .where(models.ResearchRun.id == context.run_id)
                .with_for_update()
            )
            if run is None:
                raise LookupError("research run no longer exists")
            _assert_stage_lease(task, run, context, self.clock())
            stages = _pipeline_stages(task.checkpoint)
            existing = stages.get(commit.stage)
            input_sha256 = _payload_sha256(commit.payload)
            expected_identity = {
                "idempotency_key": commit.idempotency_key,
                "input_sha256": input_sha256,
            }
            if existing is not None:
                if not isinstance(existing, dict) or any(
                    existing.get(key) != value for key, value in expected_identity.items()
                ):
                    raise StageConflictError("stage idempotency key or input changed")
                await session.rollback()
                return
            durable_payload = commit.payload
            if commit.stage in {"COLLECT", "COLLECT_R2"}:
                try:
                    durable_payload = await self._persist_collection(
                        session,
                        context,
                        commit.payload,
                    )
                except CollectionBudgetExhaustedError:
                    await session.rollback()
                    raise
            elif commit.stage == "COMPETITOR_RESEARCH":
                try:
                    checkpoint = CompetitorResearchCheckpoint.model_validate(commit.payload)
                    await self._admit_budget(
                        session,
                        context.run_id,
                        {
                            "search_calls": (
                                sum(item.request_count for item in checkpoint.searches),
                                "max_search_calls_per_run",
                            )
                        },
                    )
                except CollectionBudgetExhaustedError:
                    await session.rollback()
                    raise
            if self.artifact_writer is not None:
                await self.artifact_writer.persist_stage(
                    session,
                    context,
                    PipelineStageCommit(
                        stage=commit.stage,
                        idempotency_key=commit.idempotency_key,
                        payload=durable_payload,
                    ),
                )
            stages[commit.stage] = {
                **expected_identity,
                "payload": durable_payload,
            }
            pipeline = dict(cast(dict[str, Any], task.checkpoint.get("pipeline", {})))
            pipeline["stages"] = stages
            task.checkpoint = {**task.checkpoint, "pipeline": pipeline}
            run.last_checkpoint = {
                "task_id": str(task.id),
                "stage": commit.stage,
                "idempotency_key": commit.idempotency_key,
            }
            await session.commit()

    async def remaining_agent_calls(self, context: PipelineContext) -> int:
        async with self.session_factory() as session:
            task = await session.scalar(
                select(models.ResearchTask)
                .where(models.ResearchTask.id == context.task_id)
                .with_for_update()
            )
            run = await session.scalar(
                select(models.ResearchRun)
                .where(models.ResearchRun.id == context.run_id)
                .with_for_update()
            )
            if task is None or run is None:
                raise LookupError("research task or run no longer exists")
            _assert_stage_lease(task, run, context, self.clock())
            return max(
                0,
                int(run.budget_limits.get("max_agent_calls_per_run", 0))
                - int(run.budget_used.get("agent_calls", 0)),
            )

    async def remaining_collection_budget(self, context: PipelineContext) -> tuple[int, int]:
        async with self.session_factory() as session:
            task = await session.scalar(
                select(models.ResearchTask)
                .where(models.ResearchTask.id == context.task_id)
                .with_for_update()
            )
            run = await session.scalar(
                select(models.ResearchRun)
                .where(models.ResearchRun.id == context.run_id)
                .with_for_update()
            )
            if task is None or run is None:
                raise LookupError("research task or run no longer exists")
            _assert_stage_lease(task, run, context, self.clock())
            return (
                max(
                    0,
                    int(run.budget_limits.get("max_collector_requests_per_run", 0))
                    - int(run.budget_used.get("collector_requests", 0)),
                ),
                max(
                    0,
                    int(run.budget_limits.get("max_raw_signals_per_run", 0))
                    - int(run.budget_used.get("raw_signals", 0)),
                ),
            )

    async def _persist_collection(
        self,
        session: AsyncSession,
        context: PipelineContext,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        results = tuple(CollectResult.model_validate(value) for value in payload["results"])
        request_count = sum(result.request_count for result in results)
        items = tuple(item for result in results for item in result.items)
        await self._admit_budget(
            session,
            context.run_id,
            {
                "collector_requests": (
                    request_count,
                    "max_collector_requests_per_run",
                ),
                "raw_signals": (len(items), "max_raw_signals_per_run"),
            },
        )

        evidence_ids: list[str] = []
        for item in items:
            evidence_id = await self._persist_item(session, item)
            evidence_ids.append(evidence_id)
        for result in results:
            if result.checkpoint is None:
                continue
            stored = checkpoint_to_storage(result.checkpoint)
            row = await session.scalar(
                select(models.SourceCheckpoint).where(
                    models.SourceCheckpoint.mission_revision_id == context.mission_revision.id,
                    models.SourceCheckpoint.source == result.source.value,
                )
            )
            if row is None:
                row = models.SourceCheckpoint(
                    mission_revision_id=context.mission_revision.id,
                    source=result.source.value,
                )
                session.add(row)
            row.cursor = stored.cursor
            row.watermark_at = stored.watermark_at
            row.last_successful_run_id = context.run_id
        return {
            "results": [
                {
                    **result.model_dump(mode="json"),
                    "items": [],
                }
                for result in results
            ],
            "item_count": len(items),
            "evidence_ids": evidence_ids,
            "examined_volume": payload.get("examined_volume", {}),
            "examined_items": [
                {
                    "source": item.source.value,
                    "external_id": item.external_id,
                    "source_created_at": item.source_created_at.isoformat(),
                }
                for item in sorted(
                    {(item.source.value, item.external_id): item for item in items}.values(),
                    key=lambda value: (value.source.value, value.external_id),
                )
            ],
        }

    async def _admit_budget(
        self,
        session: AsyncSession,
        run_id: Any,
        requested: dict[str, tuple[int, str]],
    ) -> None:
        run = await session.scalar(
            select(models.ResearchRun).where(models.ResearchRun.id == run_id).with_for_update()
        )
        if run is None:
            raise LookupError("research run no longer exists")
        now = self.clock()
        exhausted = [
            (
                counter,
                int(run.budget_used.get(counter, 0)),
                int(run.budget_limits.get(limit_key, 0)),
            )
            for counter, (amount, limit_key) in requested.items()
            if int(run.budget_used.get(counter, 0)) + amount
            > int(run.budget_limits.get(limit_key, 0))
        ]
        if run.deadline_at <= now or exhausted:
            raise CollectionBudgetExhaustedError("collection budget exhausted")
        run.budget_used = {
            **run.budget_used,
            **{
                counter: int(run.budget_used.get(counter, 0)) + amount
                for counter, (amount, _) in requested.items()
                if amount
            },
        }
        await session.flush()

    async def _persist_item(
        self,
        session: AsyncSession,
        item: CollectedItem,
    ) -> str:
        raw_identifier = f"{item.source.value}:{item.external_id}"
        raw = await session.scalar(
            select(models.RawSignal).where(
                models.RawSignal.source == item.source.value,
                models.RawSignal.external_id == item.external_id,
            )
        )
        latest_before = None
        if raw is not None:
            latest_before = await session.scalar(
                select(models.RawSignalRevision)
                .where(models.RawSignalRevision.raw_signal_id == raw.id)
                .order_by(models.RawSignalRevision.revision_number.desc())
                .limit(1)
            )
        author = (
            pseudonymize_author(item.source, item.author_identity, self.author_hmac_secret)
            if self.author_hmac_secret is not None
            else None
        )
        now = self.clock()
        if raw is None:
            raw = models.RawSignal(
                id=storage_uuid_for_identifier("raw-signal", raw_identifier),
                source=item.source.value,
                external_id=item.external_id,
                canonical_url=normalize_url(str(item.canonical_url)),
                parent_external_id=item.parent_thread_id,
                author_pseudonym=author.pseudonym if author else None,
                author_kind=author.reason if author else "UNKNOWN",
                source_created_at=item.source_created_at,
                source_edited_at=item.source_edited_at,
                source_deleted_at=item.source_deleted_at,
                collected_at=now,
                is_tombstone=item.source_deleted_at is not None,
            )
            session.add(raw)
            await session.flush()
        else:
            raw.canonical_url = normalize_url(str(item.canonical_url))
            raw.parent_external_id = item.parent_thread_id
            raw.author_pseudonym = author.pseudonym if author else None
            raw.author_kind = author.reason if author else "UNKNOWN"
            raw.source_edited_at = item.source_edited_at
            raw.source_deleted_at = item.source_deleted_at
            raw.collected_at = now
            raw.is_tombstone = item.source_deleted_at is not None

        digest = (
            content_hash(None, "[deleted]")
            if item.source_deleted_at is not None
            else content_hash(item.title, item.body)
        )
        latest = latest_before
        if latest is not None and latest.content_hash.hex() == digest:
            return latest.domain_revision_id
        duplicate_group = await self._resolve_duplicate_group(
            session,
            item,
            normalized_url=raw.canonical_url,
            digest=digest,
            source_group=latest.duplicate_group_key if latest is not None else None,
        )
        revision_number = 1 if latest is None else latest.revision_number + 1
        domain_revision_id = f"{raw_identifier}:r{revision_number}"
        session.add(
            models.RawSignalRevision(
                id=storage_uuid_for_identifier("raw-signal-revision", domain_revision_id),
                raw_signal_id=raw.id,
                revision_number=revision_number,
                domain_revision_id=domain_revision_id,
                title=None if item.source_deleted_at else item.title,
                body=None if item.source_deleted_at else item.body,
                original_language="und",
                engagement=item.engagement.model_dump(mode="json"),
                source_metadata=item.metadata,
                content_hash=bytes.fromhex(digest),
                normalization_version="1",
                observed_at=now,
                is_tombstone=item.source_deleted_at is not None,
                duplicate_group_key=duplicate_group,
            )
        )
        await session.flush()
        return domain_revision_id

    async def _resolve_duplicate_group(
        self,
        session: AsyncSession,
        item: CollectedItem,
        *,
        normalized_url: str,
        digest: str,
        source_group: str | None,
    ) -> str:
        if source_group is not None:
            return source_group
        url_match = await session.scalar(
            select(models.RawSignalRevision)
            .join(models.RawSignal, models.RawSignal.id == models.RawSignalRevision.raw_signal_id)
            .where(models.RawSignal.canonical_url == normalized_url)
            .order_by(models.RawSignalRevision.domain_revision_id)
            .limit(1)
        )
        if url_match is not None:
            return url_match.duplicate_group_key
        hash_match = await session.scalar(
            select(models.RawSignalRevision)
            .where(models.RawSignalRevision.content_hash == bytes.fromhex(digest))
            .order_by(models.RawSignalRevision.domain_revision_id)
            .limit(1)
        )
        if hash_match is not None:
            return hash_match.duplicate_group_key

        text = normalize_text(f"{item.title or ''} {item.body or ''}")
        if text:
            signature = minhash_signature(text)
            near_rows = (
                await session.scalars(
                    select(models.RawSignalRevision)
                    .where(models.RawSignalRevision.is_tombstone.is_(False))
                    .order_by(models.RawSignalRevision.domain_revision_id)
                )
            ).all()
            near_matches = [
                row
                for row in near_rows
                if minhash_similarity(
                    signature,
                    minhash_signature(normalize_text(f"{row.title or ''} {row.body or ''}")),
                )
                >= 0.9
            ]
            if near_matches:
                return min(near_matches, key=lambda row: row.domain_revision_id).duplicate_group_key

            lexical = await session.scalar(
                select(models.RawSignalRevision)
                .where(func.similarity(models.RawSignalRevision.search_text, text) >= 0.92)
                .order_by(
                    func.similarity(models.RawSignalRevision.search_text, text).desc(),
                    models.RawSignalRevision.domain_revision_id,
                )
                .limit(1)
            )
            if lexical is not None:
                return lexical.duplicate_group_key
        seed = f"SOURCE_EXTERNAL_ID\0{item.source.value}:{item.external_id}"
        return hashlib.sha256(seed.encode()).hexdigest()


def _pipeline_stages(checkpoint: dict[str, Any]) -> dict[str, Any]:
    pipeline = checkpoint.get("pipeline", {})
    if not isinstance(pipeline, dict):
        raise StageConflictError("pipeline checkpoint must be an object")
    stages = pipeline.get("stages", {})
    if not isinstance(stages, dict):
        raise StageConflictError("pipeline stage checkpoint must be an object")
    return dict(stages)


def _payload_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _summary(title: str | None, body: str | None) -> str:
    return " ".join(part for part in (title, body) if part)[:500] or "unavailable"


def _assert_stage_lease(
    task: models.ResearchTask,
    run: models.ResearchRun,
    context: PipelineContext,
    now: datetime,
) -> None:
    if (
        task.status != "LEASED"
        or task.lease_owner != context.worker_id
        or task.attempt_count != context.task_attempt
        or task.lease_expires_at is None
        or task.lease_expires_at <= now
    ):
        raise PermissionError("research task lease was lost")
    if run.status != "RUNNING" or run.deadline_at <= now:
        raise PermissionError("research run is no longer writable")
