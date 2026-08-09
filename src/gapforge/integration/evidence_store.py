"""PostgreSQL boundary for durable evidence-pipeline stages."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.analysis.identity import pseudonymize_author
from gapforge.analysis.normalization import content_hash, normalize_url
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
from gapforge.queue.control import RunController
from gapforge.runtime.evidence_pipeline import (
    ExistingIntelligence,
    PipelineContext,
    PipelineStageCommit,
)
from gapforge.storage import models


class StageConflictError(RuntimeError):
    """A stage key was reused with different durable content."""


class SqlAlchemyEvidencePipelineStore:
    """Commit stage checkpoints, budgets, and raw lineage in one transaction."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        author_hmac_secret: bytes | None,
        clock: Callable[[], datetime],
    ) -> None:
        self.session_factory = session_factory
        self.author_hmac_secret = author_hmac_secret
        self.clock = clock

    async def load_context(self, task: models.ResearchTask) -> PipelineContext:
        async with self.session_factory() as session:
            fresh = await session.get(models.ResearchTask, task.id)
            if fresh is None:
                raise LookupError("research task no longer exists")
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
            return PipelineContext(
                run_id=run.id,
                task_id=fresh.id,
                task_attempt=fresh.attempt_count,
                mission_revision=mission_revision_from_storage(revision),
                mode=RunMode(run.mode),
                budget_limits=limits,
                collection_until=run.started_at or self.clock(),
                source_checkpoints=source_checkpoints,
            )

    async def query_existing(self, context: PipelineContext) -> ExistingIntelligence:
        async with self.session_factory() as session:
            opportunity_count = int(
                await session.scalar(select(func.count(models.Opportunity.id))) or 0
            )
            evidence_count = int(
                await session.scalar(select(func.count(models.RawSignalRevision.id))) or 0
            )
        return ExistingIntelligence(opportunity_count, evidence_count)

    async def load_stage(
        self,
        context: PipelineContext,
        stage: str,
    ) -> dict[str, object] | None:
        async with self.session_factory() as session:
            task = await session.get(models.ResearchTask, context.task_id)
            if task is None:
                raise LookupError("research task no longer exists")
            stages = _pipeline_stages(task.checkpoint)
            record = stages.get(stage)
            if not isinstance(record, dict):
                return None
            payload = record.get("payload")
            if not isinstance(payload, dict):
                raise StageConflictError("durable stage payload is invalid")
            return cast(dict[str, object], payload)

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
            stages = _pipeline_stages(task.checkpoint)
            existing = stages.get(commit.stage)
            durable_payload = commit.payload
            if commit.stage == "COLLECT":
                durable_payload = await self._persist_collection(
                    session,
                    context,
                    commit.payload,
                )
            if existing is not None:
                expected = {
                    "idempotency_key": commit.idempotency_key,
                    "payload": durable_payload,
                }
                if existing != expected:
                    raise StageConflictError("stage idempotency key or payload changed")
                await session.rollback()
                return
            stages[commit.stage] = {
                "idempotency_key": commit.idempotency_key,
                "payload": durable_payload,
            }
            pipeline = dict(cast(dict[str, Any], task.checkpoint.get("pipeline", {})))
            pipeline["stages"] = stages
            task.checkpoint = {**task.checkpoint, "pipeline": pipeline}
            run = await session.get(models.ResearchRun, context.run_id)
            if run is None:
                raise LookupError("research run no longer exists")
            run.last_checkpoint = {
                "task_id": str(task.id),
                "stage": commit.stage,
                "idempotency_key": commit.idempotency_key,
            }
            await session.commit()

    async def _persist_collection(
        self,
        session: AsyncSession,
        context: PipelineContext,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        results = tuple(CollectResult.model_validate(value) for value in payload["results"])
        request_count = sum(result.request_count for result in results)
        items = tuple(item for result in results for item in result.items)
        controller = RunController(session)
        if request_count:
            decision = await controller.consume_budget(
                context.run_id,
                counter="collector_requests",
                limit_key="max_collector_requests_per_run",
                amount=request_count,
                now=self.clock(),
            )
            if not decision.allowed:
                raise RuntimeError("collector request budget exhausted")
        if items:
            decision = await controller.consume_budget(
                context.run_id,
                counter="raw_signals",
                limit_key="max_raw_signals_per_run",
                amount=len(items),
                now=self.clock(),
            )
            if not decision.allowed:
                raise RuntimeError("raw signal budget exhausted")

        duplicate_keys = _duplicate_keys(items)
        evidence_ids: list[str] = []
        for item, duplicate_group in zip(items, duplicate_keys, strict=True):
            evidence_id = await self._persist_item(session, item, duplicate_group)
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
        }

    async def _persist_item(
        self,
        session: AsyncSession,
        item: CollectedItem,
        duplicate_group: str,
    ) -> str:
        raw_identifier = f"{item.source.value}:{item.external_id}"
        raw = await session.scalar(
            select(models.RawSignal).where(
                models.RawSignal.source == item.source.value,
                models.RawSignal.external_id == item.external_id,
            )
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
        latest = await session.scalar(
            select(models.RawSignalRevision)
            .where(models.RawSignalRevision.raw_signal_id == raw.id)
            .order_by(models.RawSignalRevision.revision_number.desc())
            .limit(1)
        )
        if latest is not None and latest.content_hash.hex() == digest:
            return latest.domain_revision_id
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
        return domain_revision_id


def _pipeline_stages(checkpoint: dict[str, Any]) -> dict[str, Any]:
    pipeline = checkpoint.get("pipeline", {})
    if not isinstance(pipeline, dict):
        raise StageConflictError("pipeline checkpoint must be an object")
    stages = pipeline.get("stages", {})
    if not isinstance(stages, dict):
        raise StageConflictError("pipeline stage checkpoint must be an object")
    return dict(stages)


def _duplicate_keys(items: tuple[CollectedItem, ...]) -> tuple[str, ...]:
    identities = tuple(f"{item.source.value}:{item.external_id}" for item in items)
    urls = tuple(normalize_url(str(item.canonical_url)) for item in items)
    hashes = tuple(content_hash(item.title, item.body) for item in items)
    identity_counts, url_counts, hash_counts = map(Counter, (identities, urls, hashes))
    values = []
    for identity, url, digest in zip(identities, urls, hashes, strict=True):
        if identity_counts[identity] > 1:
            seed = f"SOURCE_EXTERNAL_ID\0{identity}"
        elif url_counts[url] > 1:
            seed = f"NORMALIZED_URL\0{url}"
        elif hash_counts[digest] > 1:
            seed = f"CONTENT_HASH\0{digest}"
        else:
            seed = f"SOURCE_EXTERNAL_ID\0{identity}"
        values.append(hashlib.sha256(seed.encode()).hexdigest())
    return tuple(values)
