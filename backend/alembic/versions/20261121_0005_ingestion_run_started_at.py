"""Index ingestion runs by start time across all sources."""
from alembic import op

revision = "20261121_0005"
down_revision = "20261031_0004"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index("ix_ingestion_runs_started_at", "ingestion_runs", ["started_at"])


def downgrade():
    op.drop_index("ix_ingestion_runs_started_at", table_name="ingestion_runs")
