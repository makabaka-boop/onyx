"""Owner-scoped cancellation of a single scheduled-task run.

State contracts (see the feature spec):

- QUEUED -> CANCELLED in one conditional update. The executor only claims a
  run while it is still QUEUED, so a cancelled queued run can never start an
  agent.
- RUNNING -> CANCEL_REQUESTED, then a confirmed abort is sent to the run's own
  opencode session. The executor settles the row to CANCELLED only after the
  stopped turn is observed. If the abort RPC fails the row returns to RUNNING
  and the error reason propagates, so callers can retry and never show success
  early.
- CANCEL_REQUESTED is retried by re-sending the (idempotent) abort.
- Terminal runs are never modified and return CONFLICT.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from onyx.db.enums import ScheduledTaskRunStatus
from onyx.db.models import ScheduledTaskRun
from onyx.db.scheduled_task import (
    cancel_queued_run,
    get_owned_run,
    request_run_cancellation,
    revert_run_cancellation,
)
from onyx.error_handling.error_codes import OnyxErrorCode
from onyx.error_handling.exceptions import OnyxError
from onyx.server.features.build.db.build_session import get_build_session
from onyx.server.features.build.db.sandbox import get_sandbox_by_user_id
from onyx.server.features.build.sandbox.factory import get_sandbox_manager

# Keep the surfaced transport failure readable in API clients.
_MAX_REASON_CHARS = 1000


def _terminal_conflict(run: ScheduledTaskRun) -> OnyxError:
    return OnyxError(
        OnyxErrorCode.CONFLICT,
        f"Run has already ended with status {run.status.value}.",
    )


def _abort_run_session(
    *, db_session: Session, run: ScheduledTaskRun, user_id: UUID
) -> None:
    """Send a confirmed abort to the run's opencode session.

    Raises ``OnyxError(BAD_GATEWAY)`` (after restoring the run to RUNNING)
    when the stop cannot be delivered. Does nothing when the opencode session
    id is not assigned yet — the executor rechecks CANCEL_REQUESTED before it
    sends the first prompt.
    """
    session_id = run.session_id
    build_session = (
        get_build_session(session_id, user_id, db_session)
        if session_id is not None
        else None
    )
    opencode_session_id = (
        build_session.opencode_session_id if build_session is not None else None
    )
    sandbox = get_sandbox_by_user_id(db_session, user_id)

    if not opencode_session_id or sandbox is None or not sandbox.status.is_active():
        # Pre-prompt window or no reachable pod to prep against. Leaving the
        # row CANCEL_REQUESTED is sufficient: the executor either settles it
        # before sending the prompt or the stuck sweeper reclaims it.
        return

    # An opencode session id can only exist once the run row was linked.
    assert session_id is not None
    try:
        get_sandbox_manager().abort_opencode_session_strict(
            sandbox.id, session_id, opencode_session_id
        )
    except Exception as exc:
        # The executor may have settled the run while the RPC was in flight;
        # only revert when the row is still waiting on this abort.
        if revert_run_cancellation(db_session=db_session, run_id=run.id):
            reason = f"{type(exc).__name__}: {exc}"
            raise OnyxError(
                OnyxErrorCode.BAD_GATEWAY,
                f"Could not stop the running session: {reason}"[:_MAX_REASON_CHARS],
            ) from exc
        # Already settled — swallow the transport error and return the real
        # resulting row to the caller.


def cancel_scheduled_run(
    *,
    db_session: Session,
    task_id: UUID,
    run_id: UUID,
    user_id: UUID,
) -> ScheduledTaskRun:
    """Cancel one run owned by ``user_id``. Returns the resulting run row.

    Raises:
        OnyxError(NOT_FOUND): the run/task does not exist or belongs to
            another user.
        OnyxError(CONFLICT): the run already reached a terminal state.
        OnyxError(BAD_GATEWAY): the stop RPC for a running run failed; the
            run remains RUNNING (or CANCEL_REQUESTED) and can be retried.
    """
    run = get_owned_run(
        db_session=db_session, task_id=task_id, run_id=run_id, user_id=user_id
    )
    if run is None:
        raise OnyxError(OnyxErrorCode.NOT_FOUND, "Scheduled task run not found")

    if run.status.is_terminal():
        raise _terminal_conflict(run)

    if run.status == ScheduledTaskRunStatus.QUEUED:
        if cancel_queued_run(db_session=db_session, run_id=run_id):
            db_session.commit()
            db_session.refresh(run)
            return run
        # The executor claimed it concurrently (or it settled) — re-read and
        # continue down the running path / terminal conflict.
        db_session.refresh(run)
        if run.status.is_terminal():
            raise _terminal_conflict(run)

    if run.status == ScheduledTaskRunStatus.RUNNING:
        requested = request_run_cancellation(db_session=db_session, run_id=run_id)
        db_session.commit()
        db_session.refresh(run)
        if not requested:
            # A concurrent request moved it first, or the run settled.
            if run.status.is_terminal():
                raise _terminal_conflict(run)
            # CANCEL_REQUESTED from a duplicate request falls through to abort.

    # Status is CANCEL_REQUESTED: deliver (or redeliver) the confirmed abort.
    if run.status != ScheduledTaskRunStatus.CANCEL_REQUESTED:
        # Defensive: nothing else is cancellable.
        raise _terminal_conflict(run)

    _abort_run_session(db_session=db_session, run=run, user_id=user_id)
    db_session.commit()
    db_session.refresh(run)
    return run
