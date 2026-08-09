"""Typer command-line interface for durable GapForge operations."""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import typer
from pydantic import BaseModel, ValidationError
from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError

from gapforge.backup import BackupError, BackupService
from gapforge.config import AgentProviderName, Settings
from gapforge.health import HealthService
from gapforge.integration.persistence import (
    MergeAction,
    MergeDecisionConflictError,
    MergeDecisionService,
)
from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.storage.admin import execute_read_only_sql
from gapforge.storage.database import Database
from gapforge.storage.models import (
    AtomicClaim,
    EvidenceCard,
    LifecycleEvent,
    MergeCandidate,
    MissionOpportunityAssessment,
    MissionRevision,
    Opportunity,
    OpportunityScoreSnapshot,
    ResearchMission,
    ResearchRun,
    ResearchTask,
)
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerRegistry, Worker

app = typer.Typer(name="gap", no_args_is_help=True, pretty_exceptions_enable=False)
mission_app = typer.Typer(no_args_is_help=True)
run_app = typer.Typer(no_args_is_help=True)
opportunity_app = typer.Typer(no_args_is_help=True)
evidence_app = typer.Typer(no_args_is_help=True)
merge_app = typer.Typer(no_args_is_help=True)
report_app = typer.Typer(no_args_is_help=True)
backup_app = typer.Typer(no_args_is_help=True)
admin_app = typer.Typer(no_args_is_help=True)

app.add_typer(mission_app, name="mission")
app.add_typer(run_app, name="run")
app.add_typer(opportunity_app, name="opportunity")
app.add_typer(evidence_app, name="evidence")
app.add_typer(merge_app, name="merge-candidate")
app.add_typer(report_app, name="report")
app.add_typer(backup_app, name="backup")
app.add_typer(admin_app, name="admin")

JsonOption = Annotated[bool, typer.Option("--json", help="Emit the stable JSON envelope.")]


class CliError(Exception):
    def __init__(self, code: str, message: str, *, exit_code: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.exit_code = exit_code


@dataclass(frozen=True, slots=True)
class CommandOutcome[T]:
    data: T
    warnings: tuple[str, ...] = ()


def _settings() -> Settings:
    return Settings()


def _database(settings: Settings) -> Database:
    return Database.from_url(settings.database_url.get_secret_value())


def _build_task_handler_registry() -> TaskHandlerRegistry:
    """Integration seam for I3's concrete research orchestrator handler."""

    return TaskHandlerRegistry()


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, UUID, Path)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _emit(
    command: str,
    data: Any,
    *,
    json_output: bool,
    warnings: Sequence[str] = (),
) -> None:
    envelope = {
        "schema_version": "1.0",
        "command": command,
        "data": _jsonable(data),
        "warnings": list(warnings),
        "error": None,
    }
    if json_output:
        typer.echo(json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), default=str))
    else:
        typer.echo(json.dumps(envelope["data"], ensure_ascii=False, indent=2, default=str))
        for warning in warnings:
            typer.echo(f"warning: {warning}", err=True)


def _fail(command: str, error: CliError, *, json_output: bool) -> None:
    if json_output:
        envelope: dict[str, Any] = {
            "schema_version": "1.0",
            "command": command,
            "data": None,
            "warnings": [],
            "error": {"code": error.code, "message": error.message},
        }
        typer.echo(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")))
    else:
        typer.echo(f"{error.code}: {error.message}", err=True)
    raise typer.Exit(error.exit_code)


def _execute[T](
    command: str,
    operation: Callable[[], Coroutine[Any, Any, T]],
    *,
    json_output: bool,
    warnings: Sequence[str] = (),
) -> None:
    try:
        data: T = asyncio.run(operation())
    except CliError as error:
        _fail(command, error, json_output=json_output)
    except ValidationError as error:
        _fail(
            command,
            CliError("INVALID_ARGUMENT", str(error.errors()[0]["msg"]), exit_code=2),
            json_output=json_output,
        )
    except IntegrityError:
        _fail(
            command,
            CliError("CONFLICT", "operation conflicts with persisted state", exit_code=4),
            json_output=json_output,
        )
    except Exception as error:
        _fail(
            command,
            CliError("INTERNAL_ERROR", type(error).__name__, exit_code=1),
            json_output=json_output,
        )
    else:
        if isinstance(data, CommandOutcome):
            _emit(
                command,
                data.data,
                json_output=json_output,
                warnings=(*warnings, *data.warnings),
            )
        else:
            _emit(command, data, json_output=json_output, warnings=warnings)


def _parse_uuid(value: str, kind: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise CliError("INVALID_ARGUMENT", f"invalid {kind} ID", exit_code=2) from error


def _mission_data(mission: ResearchMission, revision: MissionRevision | None) -> dict[str, Any]:
    return {
        "id": mission.id,
        "title": mission.title,
        "status": mission.status,
        "created_at": mission.created_at,
        "updated_at": mission.updated_at,
        "revision": _revision_data(revision) if revision else None,
    }


def _revision_data(revision: MissionRevision) -> dict[str, Any]:
    return {
        "id": revision.id,
        "mission_id": revision.mission_id,
        "revision_number": revision.revision_number,
        "parent_revision_id": revision.parent_revision_id,
        "change_reason": revision.change_reason,
        "mission_text": revision.mission_text,
        "original_language": revision.original_language,
        "output_locale": revision.output_locale,
        "interpretation": revision.interpretation,
        "created_at": revision.created_at,
    }


def _run_data(run: ResearchRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "mission_revision_id": run.mission_revision_id,
        "mode": run.mode,
        "status": run.status,
        "priority": run.priority,
        "deadline_at": run.deadline_at,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "budget_limits": run.budget_limits,
        "budget_used": run.budget_used,
        "warnings": run.warnings,
        "last_checkpoint": run.last_checkpoint,
        "created_at": run.created_at,
    }


def _task_data(task: ResearchTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "run_id": task.run_id,
        "task_type": task.task_type,
        "status": task.status,
        "attempt_count": task.attempt_count,
        "max_attempts": task.max_attempts,
        "checkpoint": task.checkpoint,
        "retry_class": task.retry_class,
        "lease_owner": task.lease_owner,
        "lease_expires_at": task.lease_expires_at,
        "created_at": task.created_at,
    }


@mission_app.command("create")
def mission_create(
    text_value: Annotated[str, typer.Argument(help="Natural-language research mission.")],
    title: Annotated[str | None, typer.Option("--title")] = None,
    output_locale: Annotated[str | None, typer.Option("--output-locale")] = None,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> dict[str, Any]:
        settings = _settings()
        database = _database(settings)
        try:
            thai = any("\u0e00" <= character <= "\u0e7f" for character in text_value)
            locale = output_locale or ("th" if thai else "en")
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                mission, revision = await uow.missions.create_with_revision(
                    title=title or text_value.strip()[:120],
                    mission_text=text_value,
                    original_language="th" if thai else "en",
                    output_locale=locale,
                )
                await uow.commit()
                return _mission_data(mission, revision)
        finally:
            await database.dispose()

    _execute("mission.create", operation, json_output=json_output)


@mission_app.command("list")
def mission_list(
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                missions = await uow.missions.list(limit=limit)
                return [
                    _mission_data(mission, await uow.missions.latest_revision(mission.id))
                    for mission in missions
                ]
        finally:
            await database.dispose()

    _execute("mission.list", operation, json_output=json_output)


@mission_app.command("show")
def mission_show(mission_id: str, json_output: JsonOption = False) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(mission_id, "mission")
        database = _database(_settings())
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                mission = await uow.missions.get(identifier)
                if mission is None:
                    raise CliError("NOT_FOUND", "mission not found", exit_code=3)
                return _mission_data(mission, await uow.missions.latest_revision(identifier))
        finally:
            await database.dispose()

    _execute("mission.show", operation, json_output=json_output)


@mission_app.command("revise")
def mission_revise(
    mission_id: str,
    text_value: Annotated[str, typer.Argument(help="Revised natural-language mission.")],
    reason: Annotated[str, typer.Option("--reason", help="Immutable change reason.")],
    output_locale: Annotated[str | None, typer.Option("--output-locale")] = None,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(mission_id, "mission")
        thai = any("\u0e00" <= character <= "\u0e7f" for character in text_value)
        database = _database(_settings())
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                try:
                    revision = await uow.missions.revise(
                        identifier,
                        mission_text=text_value,
                        change_reason=reason,
                        original_language="th" if thai else "en",
                        output_locale=output_locale or ("th" if thai else "en"),
                    )
                except LookupError as error:
                    raise CliError("NOT_FOUND", "mission not found", exit_code=3) from error
                await uow.commit()
                return _revision_data(revision)
        finally:
            await database.dispose()

    _execute("mission.revise", operation, json_output=json_output)


def _mission_status_command(command: str, mission_id: str, status: str, json_output: bool) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(mission_id, "mission")
        database = _database(_settings())
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                try:
                    mission = await uow.missions.set_status(identifier, status)
                except LookupError as error:
                    raise CliError("NOT_FOUND", "mission not found", exit_code=3) from error
                except ValueError as error:
                    raise CliError("INVALID_STATE", str(error), exit_code=4) from error
                revision = await uow.missions.latest_revision(identifier)
                data = _mission_data(mission, revision)
                await uow.commit()
                return data
        finally:
            await database.dispose()

    _execute(command, operation, json_output=json_output)


@mission_app.command("activate")
def mission_activate(mission_id: str, json_output: JsonOption = False) -> None:
    _mission_status_command("mission.activate", mission_id, "ACTIVE", json_output)


@mission_app.command("pause")
def mission_pause(mission_id: str, json_output: JsonOption = False) -> None:
    _mission_status_command("mission.pause", mission_id, "PAUSED", json_output)


@mission_app.command("archive")
def mission_archive(mission_id: str, json_output: JsonOption = False) -> None:
    _mission_status_command("mission.archive", mission_id, "ARCHIVED", json_output)


@app.command("hunt")
def hunt(
    mission_id: Annotated[str, typer.Option("--mission")], json_output: JsonOption = False
) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(mission_id, "mission")
        settings = _settings()
        database = _database(settings)
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                mission = await uow.missions.get(identifier)
                revision = await uow.missions.latest_revision(identifier)
                if mission is None or revision is None:
                    raise CliError("NOT_FOUND", "mission not found", exit_code=3)
                if mission.status == "ARCHIVED":
                    raise CliError("INVALID_STATE", "archived mission cannot hunt", exit_code=4)
                if uow.session is None:
                    raise RuntimeError("unit of work session unavailable")
                scheduled = await RunScheduler(uow.session).schedule(
                    request=RunScheduleRequest(
                        mission_revision_id=revision.id,
                        mode="HUNT",
                        priority=0,
                        budget_limits=settings.budget_snapshot(),
                    ),
                )
                await uow.commit()
                return _run_data(scheduled.run)
        finally:
            await database.dispose()

    _execute("hunt", operation, json_output=json_output)


@app.command("monitor")
def monitor(
    once: Annotated[bool, typer.Option("--once", help="Schedule one monitor cycle.")] = False,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> CommandOutcome[dict[str, Any]]:
        if not once:
            raise CliError("INVALID_ARGUMENT", "v0.1 monitor requires --once", exit_code=2)
        settings = _settings()
        database = _database(settings)
        queued: list[dict[str, Any]] = []
        warnings: list[str] = []
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                if uow.session is None:
                    raise RuntimeError("unit of work session unavailable")
                session = uow.session
                missions = await uow.missions.list(limit=500)
                for mission in missions:
                    if mission.status != "ACTIVE":
                        continue
                    revision = await uow.missions.latest_revision(mission.id)
                    if revision is None:
                        continue
                    scheduled = await RunScheduler(session).schedule(
                        request=RunScheduleRequest(
                            mission_revision_id=revision.id,
                            mode="MONITOR",
                            priority=100,
                            budget_limits=settings.budget_snapshot(),
                        ),
                    )
                    if not scheduled.created:
                        warnings.append(f"mission {mission.id} already has an active revision run")
                        continue
                    queued.append(_run_data(scheduled.run))
                await uow.commit()
            return CommandOutcome({"queued": queued}, tuple(warnings))
        finally:
            await database.dispose()

    _execute("monitor", operation, json_output=json_output)


@run_app.command("list")
def run_list(
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                return [_run_data(run) for run in await uow.runs.list(limit=limit)]
        finally:
            await database.dispose()

    _execute("run.list", operation, json_output=json_output)


@run_app.command("show")
def run_show(run_id: str, json_output: JsonOption = False) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(run_id, "run")
        database = _database(_settings())
        try:
            async with database.session() as session:
                run = await session.get(ResearchRun, identifier)
                if run is None:
                    raise CliError("NOT_FOUND", "run not found", exit_code=3)
                tasks = (
                    await session.scalars(
                        select(ResearchTask)
                        .where(ResearchTask.run_id == identifier)
                        .order_by(ResearchTask.created_at)
                    )
                ).all()
                return {**_run_data(run), "tasks": [_task_data(task) for task in tasks]}
        finally:
            await database.dispose()

    _execute("run.show", operation, json_output=json_output)


@app.command("worker")
def worker(
    once: Annotated[
        bool,
        typer.Option("--once/--continuous", help="Process once or poll continuously."),
    ] = True,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> dict[str, Any]:
        registry = _build_task_handler_registry()
        registered_task_types = sorted(registry.task_types)
        if not registered_task_types and once:
            raise CliError(
                "WORKER_NOT_CONFIGURED",
                "no research task handlers are registered",
                exit_code=4,
            )
        if not registered_task_types:
            typer.echo(
                "warning: worker is idle because no research task handlers are registered",
                err=True,
            )
        database = _database(_settings())
        try:
            runtime = Worker(
                database,
                worker_id=f"{socket.gethostname()}:{os_getpid()}",
                handlers=registry,
            )
            if not once:
                await runtime.run_forever()
                return {"processed": False, "registered_task_types": registered_task_types}
            return {
                "processed": await runtime.run_once(),
                "registered_task_types": registered_task_types,
            }
        finally:
            await database.dispose()

    _execute(
        "worker",
        operation,
        json_output=json_output,
        warnings=("research task handlers are registered during cross-lane integration",),
    )


def os_getpid() -> int:
    import os

    return os.getpid()


@opportunity_app.command("list")
def opportunity_list(json_output: JsonOption = False) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with database.session() as session:
                rows = (
                    await session.scalars(select(Opportunity).order_by(Opportunity.created_at))
                ).all()
                return [
                    {"id": row.id, "canonical_key": row.canonical_key, "title": row.title}
                    for row in rows
                ]
        finally:
            await database.dispose()

    _execute("opportunity.list", operation, json_output=json_output)


async def _opportunity_detail(database: Database, identifier: UUID) -> dict[str, Any]:
    async with database.session() as session:
        opportunity = await session.get(Opportunity, identifier)
        if opportunity is None:
            raise CliError("NOT_FOUND", "opportunity not found", exit_code=3)
        assessments = (
            await session.scalars(
                select(MissionOpportunityAssessment).where(
                    MissionOpportunityAssessment.opportunity_id == identifier
                )
            )
        ).all()
        assessment_data: list[dict[str, Any]] = []
        for assessment in assessments:
            score = await session.scalar(
                select(OpportunityScoreSnapshot)
                .where(OpportunityScoreSnapshot.assessment_id == assessment.id)
                .order_by(desc(OpportunityScoreSnapshot.created_at))
                .limit(1)
            )
            assessment_data.append(
                {
                    "id": assessment.id,
                    "mission_revision_id": assessment.mission_revision_id,
                    "status": assessment.lifecycle_status,
                    "verdict": assessment.verdict,
                    "score": score.final_score if score else None,
                }
            )
        return {
            "id": opportunity.id,
            "canonical_key": opportunity.canonical_key,
            "title": opportunity.title,
            "canonical_problem_id": opportunity.canonical_problem_id,
            "gap_hypothesis_id": opportunity.gap_hypothesis_id,
            "assessments": assessment_data,
        }


@opportunity_app.command("show")
def opportunity_show(opportunity_id: str, json_output: JsonOption = False) -> None:
    async def operation() -> dict[str, Any]:
        database = _database(_settings())
        try:
            return await _opportunity_detail(database, _parse_uuid(opportunity_id, "opportunity"))
        finally:
            await database.dispose()

    _execute("opportunity.show", operation, json_output=json_output)


@opportunity_app.command("compare")
def opportunity_compare(
    opportunity_ids: Annotated[list[str], typer.Argument(min=2, max=10)],
    json_output: JsonOption = False,
) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            return [
                await _opportunity_detail(database, _parse_uuid(value, "opportunity"))
                for value in opportunity_ids
            ]
        finally:
            await database.dispose()

    _execute("opportunity.compare", operation, json_output=json_output)


@evidence_app.command("list")
def evidence_list(
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with database.session() as session:
                cards = (
                    await session.scalars(
                        select(EvidenceCard).order_by(desc(EvidenceCard.created_at)).limit(limit)
                    )
                ).all()
                return [
                    {
                        "id": card.id,
                        "canonical_problem_id": card.canonical_problem_id,
                        "run_id": card.run_id,
                        "confidence": card.confidence,
                        "created_at": card.created_at,
                    }
                    for card in cards
                ]
        finally:
            await database.dispose()

    _execute("evidence.list", operation, json_output=json_output)


@evidence_app.command("show")
def evidence_show(evidence_id: str, json_output: JsonOption = False) -> None:
    async def operation() -> dict[str, Any]:
        identifier = _parse_uuid(evidence_id, "evidence")
        database = _database(_settings())
        try:
            async with database.session() as session:
                card = await session.get(EvidenceCard, identifier)
                claim = await session.get(AtomicClaim, identifier) if card is None else None
                if card is None and claim is None:
                    raise CliError("NOT_FOUND", "evidence not found", exit_code=3)
                if card is not None:
                    return {
                        "kind": "evidence_card",
                        "id": card.id,
                        "metrics": card.metrics,
                        "confidence": card.confidence,
                        "supporting_claim_ids": card.supporting_claim_ids,
                        "contradicting_claim_ids": card.contradicting_claim_ids,
                        "missing_evidence": card.missing_evidence,
                    }
                assert claim is not None
                return {
                    "kind": "atomic_claim",
                    "id": claim.id,
                    "status": claim.status,
                    "text": claim.text,
                    "evidence_ids": claim.evidence_ids,
                }
        finally:
            await database.dispose()

    _execute("evidence.show", operation, json_output=json_output)


@app.command("changes")
def changes(
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with database.session() as session:
                events = (
                    await session.scalars(
                        select(LifecycleEvent)
                        .order_by(desc(LifecycleEvent.created_at))
                        .limit(limit)
                    )
                ).all()
                return [
                    {
                        "id": event.id,
                        "assessment_id": event.assessment_id,
                        "from_status": event.from_status,
                        "to_status": event.to_status,
                        "reason": event.reason,
                        "created_at": event.created_at,
                    }
                    for event in events
                ]
        finally:
            await database.dispose()

    _execute("changes", operation, json_output=json_output)


@app.command("rejected")
def rejected(json_output: JsonOption = False) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with database.session() as session:
                rows = (
                    await session.scalars(
                        select(MissionOpportunityAssessment)
                        .where(MissionOpportunityAssessment.lifecycle_status == "REJECTED")
                        .order_by(desc(MissionOpportunityAssessment.rejected_at))
                    )
                ).all()
                return [
                    {
                        "id": row.id,
                        "opportunity_id": row.opportunity_id,
                        "mission_revision_id": row.mission_revision_id,
                        "verdict": row.verdict,
                        "rejected_at": row.rejected_at,
                    }
                    for row in rows
                ]
        finally:
            await database.dispose()

    _execute("rejected", operation, json_output=json_output)


@merge_app.command("list")
def merge_list(json_output: JsonOption = False) -> None:
    async def operation() -> list[dict[str, Any]]:
        database = _database(_settings())
        try:
            async with database.session() as session:
                rows = (
                    await session.scalars(
                        select(MergeCandidate).order_by(desc(MergeCandidate.created_at))
                    )
                ).all()
                return [
                    {
                        "id": row.id,
                        "left_problem_id": row.left_problem_id,
                        "right_problem_id": row.right_problem_id,
                        "similarity": row.similarity,
                        "status": row.status,
                        "rationale": row.rationale,
                    }
                    for row in rows
                ]
        finally:
            await database.dispose()

    _execute("merge-candidate.list", operation, json_output=json_output)


def _merge_decision(
    command: str,
    candidate_id: str,
    action: MergeAction,
    actor: str,
    reason: str,
    json_output: bool,
) -> None:
    async def operation() -> dict[str, Any]:
        database = _database(_settings())
        try:
            try:
                event = await MergeDecisionService(
                    database.session_factory,
                    clock=lambda: datetime.now(UTC),
                ).decide(
                    _parse_uuid(candidate_id, "merge candidate"),
                    action=action,
                    actor=actor,
                    reason=reason,
                )
            except LookupError as error:
                raise CliError("NOT_FOUND", "merge candidate not found", exit_code=3) from error
            except MergeDecisionConflictError as error:
                raise CliError("INVALID_STATE", str(error), exit_code=4) from error
            return {
                "id": event.candidate_id,
                "action": event.action,
                "from_status": event.from_status,
                "status": event.to_status,
                "actor": event.actor,
                "reason": event.reason,
                "decision_number": event.decision_number,
                "decided_at": event.created_at,
            }
        finally:
            await database.dispose()

    _execute(command, operation, json_output=json_output)


@merge_app.command("accept")
def merge_accept(
    candidate_id: str,
    actor: Annotated[str, typer.Option("--actor", help="Bounded local audit identity.")],
    reason: Annotated[str, typer.Option("--reason")],
    json_output: JsonOption = False,
) -> None:
    _merge_decision("merge-candidate.accept", candidate_id, "ACCEPT", actor, reason, json_output)


@merge_app.command("reject")
def merge_reject(
    candidate_id: str,
    actor: Annotated[
        str,
        typer.Option("--actor", help="Bounded local audit identity; rejection is final."),
    ],
    reason: Annotated[str, typer.Option("--reason")],
    json_output: JsonOption = False,
) -> None:
    _merge_decision("merge-candidate.reject", candidate_id, "REJECT", actor, reason, json_output)


@merge_app.command("reverse")
def merge_reverse(
    candidate_id: str,
    actor: Annotated[str, typer.Option("--actor", help="Bounded local audit identity.")],
    reason: Annotated[str, typer.Option("--reason")],
    json_output: JsonOption = False,
) -> None:
    """Reverse an accepted equivalence; rejected candidates remain final."""

    _merge_decision("merge-candidate.reverse", candidate_id, "REVERSE", actor, reason, json_output)


def _integration_placeholder(command: str, json_output: bool) -> None:
    _emit(
        command,
        {"available": False},
        json_output=json_output,
        warnings=("command is wired during cross-lane research integration",),
    )


@report_app.command("run")
def report_run(run_id: str, json_output: JsonOption = False) -> None:
    _parse_uuid(run_id, "run")
    _integration_placeholder("report.run", json_output)


@report_app.command("opportunity")
def report_opportunity(opportunity_id: str, json_output: JsonOption = False) -> None:
    _parse_uuid(opportunity_id, "opportunity")
    _integration_placeholder("report.opportunity", json_output)


@app.command("health")
def health(json_output: JsonOption = False) -> None:
    async def operation() -> Any:
        settings = _settings()
        database = _database(settings)
        provider = (
            CodexCliProvider(
                binary=settings.codex_binary,
                codex_home=settings.codex_home,
            )
            if settings.agent_provider is AgentProviderName.CODEX_CLI
            else None
        )
        try:
            return await HealthService(settings, database, codex_provider=provider).check()
        finally:
            await database.dispose()

    _execute("health", operation, json_output=json_output)


@admin_app.command("sql")
def admin_sql(
    query: Annotated[str, typer.Argument(help="One bounded SELECT statement.")],
    read_only: Annotated[bool, typer.Option("--read-only")] = False,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 100,
    timeout_ms: Annotated[int, typer.Option(min=100, max=5000)] = 2000,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> dict[str, Any]:
        if not read_only:
            raise CliError("INVALID_ARGUMENT", "--read-only is required", exit_code=2)
        database = _database(_settings())
        try:
            async with database.session() as session:
                try:
                    result = await execute_read_only_sql(
                        session,
                        query,
                        row_limit=limit,
                        timeout_ms=timeout_ms,
                    )
                except ValueError as error:
                    raise CliError("UNSAFE_SQL", str(error), exit_code=2) from error
                return {
                    "columns": result.columns,
                    "rows": result.rows,
                    "truncated": result.truncated,
                }
        finally:
            await database.dispose()

    _execute("admin.sql", operation, json_output=json_output)


def _backup_service(settings: Settings) -> BackupService:
    return BackupService(
        database_url=settings.database_url.get_secret_value(),
        backups_dir=settings.backups_dir,
        maintenance_database=settings.backup_maintenance_database,
    )


def _backup_error(error: BackupError) -> CliError:
    return CliError("BACKUP_ERROR", str(error), exit_code=5)


@backup_app.command("create")
def backup_create(json_output: JsonOption = False) -> None:
    async def operation() -> Any:
        try:
            return await _backup_service(_settings()).create()
        except BackupError as error:
            raise _backup_error(error) from error

    _execute("backup.create", operation, json_output=json_output)


@backup_app.command("list")
def backup_list(json_output: JsonOption = False) -> None:
    async def operation() -> Any:
        try:
            return _backup_service(_settings()).list_backups()
        except BackupError as error:
            raise _backup_error(error) from error

    _execute("backup.list", operation, json_output=json_output)


@backup_app.command("verify")
def backup_verify(name: str, json_output: JsonOption = False) -> None:
    async def operation() -> Any:
        try:
            return await _backup_service(_settings()).verify(name)
        except BackupError as error:
            raise _backup_error(error) from error

    _execute("backup.verify", operation, json_output=json_output)


@backup_app.command("restore")
def backup_restore(
    name: str,
    target_database: Annotated[str, typer.Option("--target")],
    yes: Annotated[
        bool, typer.Option("--yes", help="Confirm creation of the restore target.")
    ] = False,
    json_output: JsonOption = False,
) -> None:
    async def operation() -> Any:
        try:
            return await _backup_service(_settings()).restore(
                name,
                target_database=target_database,
                confirmed=yes,
            )
        except BackupError as error:
            raise _backup_error(error) from error

    _execute("backup.restore", operation, json_output=json_output)


if __name__ == "__main__":
    app()
