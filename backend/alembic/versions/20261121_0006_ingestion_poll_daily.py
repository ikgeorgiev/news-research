"""Keep daily counts for compacted routine RSS polls."""
import sqlalchemy as sa
from alembic import op

revision = "20261121_0006"
down_revision = "20261121_0005"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "ingestion_poll_daily",
        sa.Column("day", sa.Date(), primary_key=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("runs", sa.BigInteger(), nullable=False),
        sa.Column("items_seen", sa.BigInteger(), nullable=False),
        sa.Column("backoff_skips", sa.BigInteger(), nullable=False),
    )
    routine = sa.text("""
        finished_at IS NOT NULL AND items_inserted = 0 AND (
            (status = 'success' AND (error_text IS NULL OR error_text = ''))
            OR (status = 'skipped_backoff' AND items_seen = 0)
        )
    """)
    op.create_index(
        "ix_ingestion_runs_routine", "ingestion_runs", ["finished_at"],
        postgresql_where=routine, sqlite_where=routine,
    )


def downgrade():
    # A schema downgrade cannot reconstruct individual polls from daily totals.
    if op.get_bind().scalar(sa.text("SELECT 1 FROM ingestion_poll_daily LIMIT 1")):
        raise RuntimeError(
            "Daily poll counts exist. Restore a pre-compaction backup, or export and clear the counts before downgrading."
        )
    op.drop_index("ix_ingestion_runs_routine", table_name="ingestion_runs")
    op.drop_table("ingestion_poll_daily")
