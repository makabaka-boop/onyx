import {
  deferred,
  fireEvent,
  render,
  screen,
  setupUser,
  waitFor,
} from "@tests/setup/test-utils";
import { toast } from "@opal/layouts";
import CancelRunButton from "@/app/craft/v1/tasks/components/CancelRunButton";
import { cancelScheduledRun } from "@/app/craft/v1/tasks/api";
import type {
  ScheduledRunSummary,
  ScheduledTaskRunStatus,
} from "@/app/craft/v1/tasks/types";

jest.mock("@/app/craft/v1/tasks/api", () => ({
  ...jest.requireActual("@/app/craft/v1/tasks/api"),
  cancelScheduledRun: (...args: unknown[]) => mockCancelScheduledRun(...args),
}));

jest.mock("@opal/layouts", () => {
  const actual = jest.requireActual("@opal/layouts");
  return {
    ...actual,
    toast: Object.assign(jest.fn(), {
      success: jest.fn(),
      error: jest.fn(),
      info: jest.fn(),
      warning: jest.fn(),
    }),
  };
});

const mockCancelScheduledRun: jest.MockedFunction<
  typeof cancelScheduledRun
> = jest.fn();

const toastMocks = toast as unknown as {
  success: jest.Mock;
  info: jest.Mock;
  error: jest.Mock;
};

function makeRun(
  status: ScheduledTaskRunStatus,
  overrides: Partial<ScheduledRunSummary> = {}
): ScheduledRunSummary {
  return {
    id: "run-1",
    status,
    trigger_source: "SCHEDULED",
    started_at: "2026-01-01T00:00:00Z",
    finished_at: null,
    session_id: null,
    summary: null,
    skip_reason: null,
    error_class: null,
    ...overrides,
  };
}

const TASK_ID = "task-1";

beforeEach(() => {
  jest.clearAllMocks();
});

describe("CancelRunButton visibility", () => {
  it.each([
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "SKIPPED",
    "AWAITING_APPROVAL",
  ] as const)("renders nothing for %s runs", (status) => {
    const { container } = render(
      <CancelRunButton taskId={TASK_ID} run={makeRun(status)} />
    );
    expect(container).toBeEmptyDOMElement();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it.each(["QUEUED", "RUNNING"] as const)(
    "shows the cancel action for %s runs",
    (status) => {
      render(<CancelRunButton taskId={TASK_ID} run={makeRun(status)} />);
      expect(
        screen.getByRole("button", { name: "Cancel run" })
      ).toBeInTheDocument();
    }
  );

  it("shows the cancelling state with a retry affordance for CANCEL_REQUESTED runs", () => {
    render(
      <CancelRunButton taskId={TASK_ID} run={makeRun("CANCEL_REQUESTED")} />
    );
    const button = screen.getByRole("button", { name: "Cancelling…" });
    expect(button).toHaveAttribute("aria-busy", "true");
  });
});

describe("CancelRunButton actions", () => {
  it("cancels a queued run and shows success only when the row is CANCELLED", async () => {
    const user = setupUser();
    mockCancelScheduledRun.mockResolvedValue(makeRun("CANCELLED"));

    render(<CancelRunButton taskId={TASK_ID} run={makeRun("QUEUED")} />);
    await user.click(screen.getByRole("button", { name: "Cancel run" }));

    expect(mockCancelScheduledRun).toHaveBeenCalledWith(TASK_ID, "run-1");
    await waitFor(() =>
      expect(toastMocks.success).toHaveBeenCalledWith("Run cancelled.")
    );
    expect(toastMocks.info).not.toHaveBeenCalled();
    expect(toastMocks.error).not.toHaveBeenCalled();
  });

  it("does not claim success while a running run is only CANCEL_REQUESTED", async () => {
    const user = setupUser();
    mockCancelScheduledRun.mockResolvedValue(makeRun("CANCEL_REQUESTED"));

    render(<CancelRunButton taskId={TASK_ID} run={makeRun("RUNNING")} />);
    await user.click(screen.getByRole("button", { name: "Cancel run" }));

    await waitFor(() => expect(toastMocks.info).toHaveBeenCalledTimes(1));
    expect(toastMocks.success).not.toHaveBeenCalled();
  });

  it("surfaces the failure reason and stays actionable when the request fails", async () => {
    const user = setupUser();
    mockCancelScheduledRun.mockRejectedValue(
      new Error("Could not stop the running session: ConnectError: boom")
    );

    render(<CancelRunButton taskId={TASK_ID} run={makeRun("RUNNING")} />);
    const button = screen.getByRole("button", { name: "Cancel run" });
    await user.click(button);

    await waitFor(() => expect(toastMocks.error).toHaveBeenCalledTimes(1));
    expect(toastMocks.error).toHaveBeenCalledWith(
      "Could not stop the running session: ConnectError: boom"
    );
    expect(toastMocks.success).not.toHaveBeenCalled();
    expect(button).not.toBeDisabled();
  });

  it("clicking the cancelling state re-sends the stop request", async () => {
    const user = setupUser();
    mockCancelScheduledRun.mockResolvedValue(makeRun("CANCEL_REQUESTED"));

    render(
      <CancelRunButton taskId={TASK_ID} run={makeRun("CANCEL_REQUESTED")} />
    );
    await user.click(screen.getByRole("button", { name: "Cancelling…" }));

    expect(mockCancelScheduledRun).toHaveBeenCalledWith(TASK_ID, "run-1");
  });

  it("ignores a second click while a request is in flight", async () => {
    const { promise, resolve } = deferred<ScheduledRunSummary>();
    mockCancelScheduledRun.mockReturnValue(promise);

    render(<CancelRunButton taskId={TASK_ID} run={makeRun("RUNNING")} />);
    const button = screen.getByRole("button", { name: "Cancel run" });
    fireEvent.click(button);
    fireEvent.click(button);
    expect(mockCancelScheduledRun).toHaveBeenCalledTimes(1);

    resolve(makeRun("CANCELLED"));
    await waitFor(() =>
      expect(toastMocks.success).toHaveBeenCalledWith("Run cancelled.")
    );
  });

  it("stops click propagation so the row does not open the session", async () => {
    const user = setupUser();
    const onRowClick = jest.fn();
    mockCancelScheduledRun.mockResolvedValue(makeRun("CANCELLED"));

    render(
      <div onClick={onRowClick}>
        <CancelRunButton
          taskId={TASK_ID}
          run={makeRun("RUNNING", { session_id: "session-1" })}
        />
      </div>
    );
    await user.click(screen.getByRole("button", { name: "Cancel run" }));

    expect(onRowClick).not.toHaveBeenCalled();
    expect(mockCancelScheduledRun).toHaveBeenCalledTimes(1);
  });
});
