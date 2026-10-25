"""scheduled task run cancellation statuses

Adds ``CANCEL_REQUESTED`` and ``CANCELLED`` to the
``scheduled_task_run.status`` vocabulary for per-run cancellation.

The column is a non-native enum (VARCHAR), and SQLAlchemy creates no CHECK
constraint for non-native enums by default, so the common case needs no DDL.
This migration is defensive: if a deployment carries a CHECK constraint on
the column (for example one created with ``create_constraint=True``), it is
replaced in place with the expanded value list. Downgrade restores the
original list. No data is rewritten.

Revision ID: c3f1a2b4d5e6
Revises: b209881eaf72
Create Date: 2026-10-24 00:00:00.000000

"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "c3f1a2b4d5e6"
down_revision = "b209881eaf72"
branch_labels = None
depends_on = None


_TABLE = "scheduled_task_run"

# Declaration order kept in enum order so the reconstructed constraint reads
# naturally; the CHECK itself is order-insensitive.
_ALL_STATUSES = (
    "QUEUED",
    "RUNNING",
    "CANCEL_REQUESTED",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
    "AWAITING_APPROVAL",
    "CANCELLED",
)
_PRE_CANCEL_STATUSES = (
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
    "AWAITING_APPROVAL",
)

# Find CHECK constraints attached to the status column. We match on the
# constraint definition mentioning ``status`` rather than a fixed name, since
# naming conventions differ across deployments.
_FIND_STATUS_CHECKS = """
SELECT conname
FROM pg_constraint
WHERE conrelid = to_regclass('scheduled_task_run')
  AND contype = 'c'
  AND pg_get_constraintdef(oid) ILIKE '%status%'
"""


def _replace_status_check(statuses: tuple[str, ...]) -> None:
    bind = op.get_bind()
    constraint_names = [
        row[0] for row in bind.execute(text(_FIND_STATUS_CHECKS)).fetchall()
    ]
    values = ", ".join(f"'{status}'" for status in statuses)
    for conname in constraint_names:
        # Names come from the catalog, not user input; quote them anyway.
        quoted_name = conname.replace('"', '""')
        op.execute(
            f'ALTER TABLE {_TABLE} DROP CONSTRAINT "{quoted_name}"'
        )
        op.execute(
            f'ALTER TABLE {_TABLE} ADD CONSTRAINT "{quoted_name}" '
            f"CHECK (status IN ({values}))"
        )


def upgrade() -> None:
    _replace_status_check(_ALL_STATUSES)


def downgrade() -> None:
    # Existing CANCELLED / CANCEL_REQUESTED rows would violate the restored
    # constraint; drop them back to the pre-feature vocabulary first. These
    # are only present after this revision shipped, so a downgrade already
    # implies discarding the feature's rows from triage views.
    op.execute(
        f"UPDATE {_TABLE} SET status = 'FAILED' "
        "WHERE status IN ('CANCEL_REQUESTED', 'CANCELLED')"
    )
    _replace_status_check(_PRE_CANCEL_STATUSES)
