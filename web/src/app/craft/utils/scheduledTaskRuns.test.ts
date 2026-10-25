import { type RunReasonTranslate } from "@/app/craft/v1/tasks/utils";
import {
  getNonClickableReason,
  isScheduledRunContextInFlight,
} from "@/app/craft/v1/tasks/utils";
import type {
  ScheduledRunContextResponse,
  ScheduledRunSummary,
  ScheduledTaskRunStatus,
} from "@/app/craft/v1/tasks/types";

function run(
  status: ScheduledTaskRunStatus,
  sessionId: string | null
): ScheduledRunSummary {
  return {
    id: `run-${status}`,
    status,
    trigger_source: "MANUAL_RUN_NOW",
    started_at: "2026-01-01T00:00:00Z",
    finished_at: null,
    session_id: sessionId,
    summary: null,
    skip_reason: null,
    error_class: null,
  };
}

function context(status: ScheduledTaskRunStatus): ScheduledRunContextResponse {
  return {
    run_id: `run-${status}`,
    task_id: "task-1",
    task_name: "Task",
    status,
    started_at: "2026-01-01T00:00:00Z",
    finished_at: null,
  };
}

const tStub = ((key: string) => key) as RunReasonTranslate;

describe("scheduled task run utils", () => {
  it.each([
    "RUNNING",
    "CANCEL_REQUESTED",
    "AWAITING_APPROVAL",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
  ] as const)(
    "allows opening %s runs with linked sessions",
    (status) => {
      expect(getNonClickableReason(run(status, "session-1"), tStub)).toBeNull();
    }
  );

  it("keeps queued and skipped runs blocked", () => {
    expect(getNonClickableReason(run("QUEUED", null), tStub)).toBe("queued");
    expect(getNonClickableReason(run("SKIPPED", null), tStub)).toBe(
      "skipped"
    );
  });

  it("blocks openable statuses until a session exists", () => {
    expect(getNonClickableReason(run("RUNNING", null), tStub)).toBe(
      "noSession"
    );
    expect(getNonClickableReason(run("CANCEL_REQUESTED", null), tStub)).toBe(
      "noSession"
    );
  });

  it("blocks cancelled runs that never created a session with its own reason", () => {
    expect(getNonClickableReason(run("CANCELLED", null), tStub)).toBe(
      "cancelled"
    );
  });

  it("keeps cancelled runs with a session openable", () => {
    expect(
      getNonClickableReason(run("CANCELLED", "session-1"), tStub)
    ).toBeNull();
  });

  it("treats running, awaiting-approval and cancel-requested contexts as in-flight", () => {
    expect(isScheduledRunContextInFlight(context("RUNNING"))).toBe(true);
    expect(isScheduledRunContextInFlight(context("AWAITING_APPROVAL"))).toBe(
      true
    );
    expect(isScheduledRunContextInFlight(context("CANCEL_REQUESTED"))).toBe(
      true
    );
    expect(isScheduledRunContextInFlight(context("SUCCEEDED"))).toBe(false);
    expect(isScheduledRunContextInFlight(context("FAILED"))).toBe(false);
    expect(isScheduledRunContextInFlight(context("CANCELLED"))).toBe(false);
  });
});
