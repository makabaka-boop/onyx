"""Scheduled task run cancellation (ext-dep).

Covers the per-run cancel contract against real Postgres and the
``StubSandboxManager``:

* QUEUED cancel settles CANCELLED and the executor never starts an agent.
* RUNNING cancel issues a confirmed, precisely-scoped abort and the executor
  settles CANCELLED without a failure notification.
* A cancel that races a clean completion keeps the real SUCCEEDED result.
* Abort failure surfaces with a reason, reverts to RUNNING, and retries.
* Terminal runs cannot be changed; the stuck sweeper reclaims a
  CANCEL_REQUESTED row as CANCELLED.
"""

from __future__ import annotations

import datetime
from collections.abc import Generator
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from onyx.background.celery.tasks.scheduled_tasks.tasks import (
    cleanup_stuck_scheduled_runs,
)
from onyx.configs.constants import NotificationType
from onyx.db.action_policy import get_active_scheduled_task_context
from onyx.db.engine.sql_engine import get_session_with_current_tenant
from onyx.db.enums import (
    BuildSessionStatus,
    SandboxStatus,
    ScheduledTaskRunStatus,
    ScheduledTaskStatus,
    ScheduledTaskTriggerSource,
    SessionOrigin,
)
from onyx.db.models import (
    BuildSession,
    Notification,
    Sandbox,
    ScheduledTask,
    ScheduledTaskRun,
    User,
)
from onyx.db.scheduled_task import request_run_cancellation
from onyx.error_handling.error_codes import OnyxErrorCode
from onyx.error_handling.exceptions import OnyxError
from onyx.server.features.build.db.build_session import get_session_messages
from onyx.server.features.build.sandbox.base import SandboxEvent
from onyx.server.features.build.sandbox.event_schema import (
    TURN_ERROR_CODE_TRANSPORT,
    AgentMessageChunk,
    Error,
    PromptResponse,
    RequestPermissionRequest,
)
from onyx.server.features.build.scheduled_tasks.cancel import cancel_scheduled_run
from onyx.server.features.build.scheduled_tasks.executor import run_scheduled_task_logic
from onyx.server.features.build.session.manager import SessionManager
from shared_configs.configs import POSTGRES_DEFAULT_SCHEMA_STANDARD_VALUE
from tests.common.craft.stubs import StubSandboxManager

OPENCODE_SESSION_ID = "opencode-cancel-session"


@pytest.fixture(autouse=True)
def _tenant_context(tenant_context: None) -> None:  # noqa: ARG001
    return None


def _seed_task(
    db_session: Session,
    user: User,
    *,
    name: str = "nightly-report",
) -> ScheduledTask:
    task = ScheduledTask(
        user_id=user.id,
        name=name,
        prompt="Summarise yesterday's events",
        cron_expression="0 9 * * *",
        editor_mode="advanced",
        status=ScheduledTaskStatus.ACTIVE,
        next_run_at=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(days=1),
    )
    db_session.add(task)
    db_session.flush()
    return task


def _seed_run(
    db_session: Session,
    task: ScheduledTask,
    *,
    status: ScheduledTaskRunStatus,
    trigger_source: ScheduledTaskTriggerSource = ScheduledTaskTriggerSource.SCHEDULED,
    session_id: UUID | None = None,
) -> ScheduledTaskRun:
    run = ScheduledTaskRun(
        task_id=task.id,
        status=status,
        trigger_source=trigger_source,
        started_at=datetime.datetime.now(datetime.timezone.utc),
        session_id=session_id,
    )
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run


def _failure_notification_count(db_session: Session, user: User) -> int:
    return int(
        db_session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.user_id == user.id,
                Notification.notif_type == NotificationType.SCHEDULED_TASK_FAILED,
            )
        )
    )


def _configure_stub(stub: StubSandboxManager) -> None:
    stub.health_check_returns = True
    stub.setup_session_workspace_silent = True
    stub.write_sandbox_file_silent = True
    stub.write_files_to_sandbox_silent = True
    # The executor resolves opencode session ids through the stub; give the
    # session a stable id so abort plumbing has something concrete to target.
    stub.ensure_opencode_session_returns = OPENCODE_SESSION_ID


def _flip_to_cancel_requested_when_send_starts(run_id: UUID) -> None:
    """Simulate the API process flipping RUNNING -> CANCEL_REQUESTED while the
    executor's turn is in flight (the cancel endpoint's DB half)."""
    with get_session_with_current_tenant() as db_session:
        assert request_run_cancellation(db_session=db_session, run_id=run_id)
        db_session.commit()


# ---------------------------------------------------------------------------
# QUEUED cancellation
# ---------------------------------------------------------------------------


def test_cancel_queued_run_settles_cancelled(
    db_session: Session,
    test_user: User,
) -> None:
    task = _seed_task(db_session, test_user)
    run = _seed_run(db_session, task, status=ScheduledTaskRunStatus.QUEUED)

    result = cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )

    assert result.status == ScheduledTaskRunStatus.CANCELLED
    assert result.finished_at is not None
    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.CANCELLED
    assert _failure_notification_count(db_session, test_user) == 0


def test_cancelled_queued_run_never_starts_agent(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "onyx.server.features.build.session.sandbox_lifecycle.build_user_skills_payload",
        lambda *_: ("", {}),
    )
    sandbox(user=test_user, status=SandboxStatus.RUNNING)
    _configure_stub(stub_sandbox_manager)
    stub_sandbox_manager.send_message_events = [
        PromptResponse.model_validate({"stopReason": "end_turn"}),
    ]

    task = _seed_task(db_session, test_user)
    run = _seed_run(db_session, task, status=ScheduledTaskRunStatus.QUEUED)
    cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )

    run_scheduled_task_logic(run.id)

    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.CANCELLED
    # No session was created and the agent was never prompted.
    assert refreshed.session_id is None
    assert stub_sandbox_manager.send_message_count == 0
    assert _failure_notification_count(db_session, test_user) == 0


def test_wake_failure_after_queued_cancel_does_not_fail_run(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox(user=test_user, status=SandboxStatus.RUNNING)
    task = _seed_task(db_session, test_user)
    run = _seed_run(db_session, task, status=ScheduledTaskRunStatus.QUEUED)
    cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )

    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("sandbox woke too late")

    monkeypatch.setattr(SessionManager, "ensure_sandbox_running", _raise)

    run_scheduled_task_logic(run.id)

    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.CANCELLED
    assert refreshed.error_class is None
    assert _failure_notification_count(db_session, test_user) == 0


# ---------------------------------------------------------------------------
# RUNNING cancellation: service half (abort delivery)
# ---------------------------------------------------------------------------


def _seed_running_run_with_session(
    db_session: Session,
    test_user: User,
    stub: StubSandboxManager,
    *,
    sandbox_row: Sandbox,
    trigger_source: ScheduledTaskTriggerSource = ScheduledTaskTriggerSource.SCHEDULED,
) -> tuple[ScheduledTask, ScheduledTaskRun, Any]:
    _configure_stub(stub)
    task = _seed_task(db_session, test_user)
    run = _seed_run(
        db_session,
        task,
        status=ScheduledTaskRunStatus.RUNNING,
        trigger_source=trigger_source,
    )
    manager = SessionManager(db_session)
    build_session = manager.create_session(
        user_id=test_user.id,
        origin=SessionOrigin.SCHEDULED,
        name="Scheduled: nightly-report",
    )
    build_session.opencode_session_id = OPENCODE_SESSION_ID
    run.session_id = build_session.id
    db_session.commit()
    db_session.refresh(run)
    _ = stub, sandbox_row
    return task, run, build_session


def test_cancel_running_run_requests_scoped_abort(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
) -> None:
    sandbox_row = sandbox(user=test_user, status=SandboxStatus.RUNNING)
    task, run, build_session = _seed_running_run_with_session(
        db_session, test_user, stub_sandbox_manager, sandbox_row=sandbox_row
    )

    # A second, unrelated session in the same sandbox must never be aborted.
    other_session = SessionManager(db_session).create_session(
        user_id=test_user.id,
        origin=SessionOrigin.INTERACTIVE,
        name="interactive",
    )
    other_session.opencode_session_id = "opencode-other"
    db_session.commit()

    result = cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )

    assert result.status == ScheduledTaskRunStatus.CANCEL_REQUESTED
    assert stub_sandbox_manager.abort_calls == [
        (sandbox_row.id, build_session.id, OPENCODE_SESSION_ID)
    ]
    assert other_session.opencode_session_id == "opencode-other"
    # Task schedule state is untouched.
    db_session.refresh(task)
    assert task.status == ScheduledTaskStatus.ACTIVE
    assert _failure_notification_count(db_session, test_user) == 0


def test_cancel_manual_run_now_run_is_supported(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
) -> None:
    sandbox_row = sandbox(user=test_user, status=SandboxStatus.RUNNING)
    task, run, _ = _seed_running_run_with_session(
        db_session,
        test_user,
        stub_sandbox_manager,
        sandbox_row=sandbox_row,
        trigger_source=ScheduledTaskTriggerSource.MANUAL_RUN_NOW,
    )

    result = cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )
    assert result.status == ScheduledTaskRunStatus.CANCEL_REQUESTED


def test_abort_failure_reverts_and_surfaces_reason(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
) -> None:
    sandbox_row = sandbox(user=test_user, status=SandboxStatus.RUNNING)
    task, run, _ = _seed_running_run_with_session(
        db_session, test_user, stub_sandbox_manager, sandbox_row=sandbox_row
    )
    stub_sandbox_manager.abort_strict_raises = RuntimeError("pod refused abort")

    with pytest.raises(OnyxError) as exc_info:
        cancel_scheduled_run(
            db_session=db_session,
            task_id=task.id,
            run_id=run.id,
            user_id=test_user.id,
        )
    assert exc_info.value.error_code == OnyxErrorCode.BAD_GATEWAY
    assert "pod refused abort" in exc_info.value.detail

    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.RUNNING

    # Retry with a healthy transport succeeds.
    stub_sandbox_manager.abort_strict_raises = None
    result = cancel_scheduled_run(
        db_session=db_session,
        task_id=task.id,
        run_id=run.id,
        user_id=test_user.id,
    )
    assert result.status == ScheduledTaskRunStatus.CANCEL_REQUESTED
    # Both attempts targeted the same single session.
    targets = {(call[1], call[2]) for call in stub_sandbox_manager.abort_calls}
    assert targets == {(run.session_id, OPENCODE_SESSION_ID)}


def test_terminal_run_cancel_conflicts_and_preserves_row(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
) -> None:
    sandbox_row = sandbox(user=test_user, status=SandboxStatus.RUNNING)
    for terminal_status in (
        ScheduledTaskRunStatus.SUCCEEDED,
        ScheduledTaskRunStatus.FAILED,
        ScheduledTaskRunStatus.SKIPPED,
        ScheduledTaskRunStatus.CANCELLED,
    ):
        task, run, _ = _seed_running_run_with_session(
            db_session,
            test_user,
            stub_sandbox_manager,
            sandbox_row=sandbox_row,
        )
        db_session.execute(
            ScheduledTaskRun.__table__.update()
            .where(ScheduledTaskRun.id == run.id)
            .values(
                status=terminal_status,
                finished_at=datetime.datetime.now(datetime.timezone.utc),
            )
        )
        db_session.commit()
        db_session.expire_all()
        before = db_session.get(ScheduledTaskRun, run.id)
        assert before is not None
        before_finished = before.finished_at

        with pytest.raises(OnyxError) as exc_info:
            cancel_scheduled_run(
                db_session=db_session,
                task_id=task.id,
                run_id=run.id,
                user_id=test_user.id,
            )
        assert exc_info.value.error_code == OnyxErrorCode.CONFLICT

        db_session.expire_all()
        after = db_session.get(ScheduledTaskRun, run.id)
        assert after is not None
        assert after.status == terminal_status
        assert after.finished_at == before_finished


# ---------------------------------------------------------------------------
# RUNNING cancellation: executor half (settlement)
# ---------------------------------------------------------------------------


def _executor_with_cancel_mid_turn(
    *,
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
    events: list[SandboxEvent],
) -> ScheduledTaskRun:
    monkeypatch.setattr(
        "onyx.server.features.build.session.sandbox_lifecycle.build_user_skills_payload",
        lambda *_: ("", {}),
    )
    sandbox(user=test_user, status=SandboxStatus.RUNNING)
    _configure_stub(stub_sandbox_manager)

    task = _seed_task(db_session, test_user)
    run = _seed_run(db_session, task, status=ScheduledTaskRunStatus.QUEUED)

    def produce(
        *_args: Any, **_kwargs: Any
    ) -> Generator[SandboxEvent, None, None]:
        _flip_to_cancel_requested_when_send_starts(run.id)
        yield from iter(events)

    stub_sandbox_manager.send_message_events = None
    monkeypatch.setattr(stub_sandbox_manager, "send_message", produce)

    run_scheduled_task_logic(run.id)
    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    return refreshed


def test_executor_settles_aborted_turn_cancelled(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _executor_with_cancel_mid_turn(
        db_session=db_session,
        test_user=test_user,
        sandbox=sandbox,
        session_manager_with_stub=session_manager_with_stub,
        stub_sandbox_manager=stub_sandbox_manager,
        monkeypatch=monkeypatch,
        events=[PromptResponse.model_validate({"stopReason": "cancelled"})],
    )
    assert run.status == ScheduledTaskRunStatus.CANCELLED
    assert run.finished_at is not None
    assert run.error_class is None
    # The executor never aborts; the cancel endpoint owns that RPC.
    assert stub_sandbox_manager.abort_calls == []
    assert run.session_id is not None
    assert _failure_notification_count(db_session, test_user) == 0


def test_cancelled_run_keeps_saved_session_content(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _executor_with_cancel_mid_turn(
        db_session=db_session,
        test_user=test_user,
        sandbox=sandbox,
        session_manager_with_stub=session_manager_with_stub,
        stub_sandbox_manager=stub_sandbox_manager,
        monkeypatch=monkeypatch,
        events=[
            AgentMessageChunk.model_validate(
                {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "Partial report"},
                }
            ),
            PromptResponse.model_validate({"stopReason": "cancelled"}),
        ],
    )
    assert run.status == ScheduledTaskRunStatus.CANCELLED
    messages = get_session_messages(
        session_id=run.session_id, db_session=db_session
    )
    assert len(messages) >= 2  # persisted user prompt + partial agent content


@pytest.mark.parametrize(
    "events,expected",
    [
        (
            [PromptResponse.model_validate({"stopReason": "end_turn"})],
            ScheduledTaskRunStatus.SUCCEEDED,
        ),
        (
            [
                Error.model_validate(
                    {"code": TURN_ERROR_CODE_TRANSPORT, "message": "bus closed"}
                )
            ],
            ScheduledTaskRunStatus.CANCELLED,
        ),
        (
            [
                RequestPermissionRequest.model_validate(
                    {
                        "sessionId": "s",
                        "options": [],
                        "toolCall": {"toolCallId": "a"},
                    }
                )
            ],
            ScheduledTaskRunStatus.CANCELLED,
        ),
    ],
)
def test_cancel_race_outcomes(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
    events: list[SandboxEvent],
    expected: ScheduledTaskRunStatus,
) -> None:
    run = _executor_with_cancel_mid_turn(
        db_session=db_session,
        test_user=test_user,
        sandbox=sandbox,
        session_manager_with_stub=session_manager_with_stub,
        stub_sandbox_manager=stub_sandbox_manager,
        monkeypatch=monkeypatch,
        events=events,
    )
    assert run.status == expected
    # User cancellation never notifies, even on the success race.
    assert _failure_notification_count(db_session, test_user) == 0


def test_spontaneous_cancelled_turn_without_request_still_fails(
    db_session: Session,
    test_user: User,
    sandbox: Any,
    session_manager_with_stub: SessionManager,  # noqa: ARG001
    stub_sandbox_manager: StubSandboxManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: an aborted turn with NO cancel request stays FAILED."""
    monkeypatch.setattr(
        "onyx.server.features.build.session.sandbox_lifecycle.build_user_skills_payload",
        lambda *_: ("", {}),
    )
    sandbox(user=test_user, status=SandboxStatus.RUNNING)
    _configure_stub(stub_sandbox_manager)
    task = _seed_task(db_session, test_user)
    run = _seed_run(db_session, task, status=ScheduledTaskRunStatus.QUEUED)
    stub_sandbox_manager.send_message_events = [
        PromptResponse.model_validate({"stopReason": "cancelled"}),
    ]

    run_scheduled_task_logic(run.id)

    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.FAILED
    assert _failure_notification_count(db_session, test_user) == 1


# ---------------------------------------------------------------------------
# Stuck sweeper
# ---------------------------------------------------------------------------


def test_sweeper_reclaims_cancel_requested_as_cancelled(
    db_session: Session,
    test_user: User,
    tenant_context: None,  # noqa: ARG001
) -> None:
    task = _seed_task(db_session, test_user)
    stale_started = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        minutes=80
    )
    run = ScheduledTaskRun(
        task_id=task.id,
        status=ScheduledTaskRunStatus.CANCEL_REQUESTED,
        trigger_source=ScheduledTaskTriggerSource.SCHEDULED,
        started_at=stale_started,
    )
    db_session.add(run)
    db_session.commit()

    marked = cleanup_stuck_scheduled_runs.run(
        tenant_id=POSTGRES_DEFAULT_SCHEMA_STANDARD_VALUE
    )
    assert marked >= 1

    db_session.expire_all()
    refreshed = db_session.get(ScheduledTaskRun, run.id)
    assert refreshed is not None
    assert refreshed.status == ScheduledTaskRunStatus.CANCELLED
    assert refreshed.error_class is None
    assert _failure_notification_count(db_session, test_user) == 0


# ---------------------------------------------------------------------------
# Action-policy gate: CANCEL_REQUESTED keeps task authority, CANCELLED loses it
# ---------------------------------------------------------------------------


def _scheduled_session_with_run(
    db_session: Session,
    user: User,
    status: ScheduledTaskRunStatus,
) -> tuple[ScheduledTaskRun, BuildSession]:
    task = _seed_task(db_session, user)
    session = BuildSession(
        user_id=user.id,
        name="Scheduled: nightly-report",
        status=BuildSessionStatus.ACTIVE,
        origin=SessionOrigin.SCHEDULED,
    )
    db_session.add(session)
    db_session.flush()
    run = _seed_run(
        db_session,
        task,
        status=status,
        session_id=session.id,
    )
    return run, session


def test_cancel_requested_keeps_task_action_authority(
    db_session: Session,
    test_user: User,
) -> None:
    run, session = _scheduled_session_with_run(
        db_session, test_user, ScheduledTaskRunStatus.CANCEL_REQUESTED
    )

    authority = get_active_scheduled_task_context(db_session, session.id)
    assert authority == (run.id, run.task_id)


def test_running_keeps_task_action_authority(
    db_session: Session,
    test_user: User,
) -> None:
    run, session = _scheduled_session_with_run(
        db_session, test_user, ScheduledTaskRunStatus.RUNNING
    )

    authority = get_active_scheduled_task_context(db_session, session.id)
    assert authority == (run.id, run.task_id)


def test_cancelled_run_loses_task_action_authority(
    db_session: Session,
    test_user: User,
) -> None:
    _run, session = _scheduled_session_with_run(
        db_session, test_user, ScheduledTaskRunStatus.CANCELLED
    )

    with pytest.raises(OnyxError) as exc_info:
        get_active_scheduled_task_context(db_session, session.id)
    assert exc_info.value.error_code == OnyxErrorCode.INSUFFICIENT_PERMISSIONS
