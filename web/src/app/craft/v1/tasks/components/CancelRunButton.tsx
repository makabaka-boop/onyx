"use client";

import { useState, type MouseEvent } from "react";
import { useTranslations } from "next-intl";
import { useSWRConfig } from "swr";
import { Button } from "@opal/components";
import { SvgLoader, SvgStopCircle } from "@opal/icons";
import { toast } from "@opal/layouts";
import { cn } from "@opal/utils";
import { cancelScheduledRun } from "@/app/craft/v1/tasks/api";
import type { ScheduledRunSummary } from "@/app/craft/v1/tasks/types";
import { SWR_KEYS } from "@/lib/swr-keys";

const SpinningLoader: React.FunctionComponent<{
  size?: number;
  className?: string;
}> = (props) => (
  <SvgLoader {...props} className={cn("animate-spin", props.className)} />
);

interface CancelRunButtonProps {
  taskId: string;
  run: ScheduledRunSummary;
}

export default function CancelRunButton({
  taskId,
  run,
}: CancelRunButtonProps) {
  const t = useTranslations("craft.tasks.runHistory.cancel");
  const { mutate } = useSWRConfig();
  const [pending, setPending] = useState(false);

  // Only queued and in-flight runs can be cancelled. Terminal rows keep
  // their original result and show no action.
  if (
    run.status !== "QUEUED" &&
    run.status !== "RUNNING" &&
    run.status !== "CANCEL_REQUESTED"
  ) {
    return null;
  }

  const cancelling = run.status === "CANCEL_REQUESTED";

  const revalidateRunHistory = () => {
    // The table owns paginated keys under this prefix; refresh every variant.
    void mutate(
      (key) =>
        typeof key === "string" &&
        key.startsWith(SWR_KEYS.scheduledTaskRuns(taskId))
    );
  };

  const handleClick = async (event: MouseEvent<HTMLButtonElement>) => {
    // Keep the row click from opening the session.
    event.preventDefault();
    event.stopPropagation();
    if (pending) return;
    setPending(true);
    try {
      const updated = await cancelScheduledRun(taskId, run.id);
      revalidateRunHistory();
      // The response is authoritative: a running run only reaches CANCELLED
      // after the session stop is confirmed.
      if (updated.status === "CANCELLED") {
        toast.success(t("toasts.cancelled"));
      } else {
        toast.info(t("toasts.stopping"));
      }
    } catch (error) {
      // Re-fetch the row: the run may have finished while the request was in
      // flight, so never show success from the client side alone.
      revalidateRunHistory();
      toast.error(
        error instanceof Error ? error.message : t("toasts.requestFailed")
      );
    } finally {
      setPending(false);
    }
  };

  return (
    <span
      className="flex justify-end"
      onClick={(event) => event.stopPropagation()}
    >
      <Button
        size="sm"
        prominence="tertiary"
        icon={cancelling && !pending ? SpinningLoader : SvgStopCircle}
        loading={pending}
        onClick={(event) => void handleClick(event)}
        tooltip={cancelling ? t("cancellingTooltip") : t("tooltip")}
        aria-label={cancelling ? t("cancellingButton") : t("button")}
        aria-busy={pending || cancelling}
        data-testid={`cancel-run-${run.id}`}
        data-run-status={run.status}
      >
        {cancelling ? t("cancellingButton") : t("button")}
      </Button>
    </span>
  );
}
