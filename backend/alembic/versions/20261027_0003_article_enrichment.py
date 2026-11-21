"""Durable post-commit article coverage queue."""
from alembic import op
import sqlalchemy as sa

revision = "20261027_0003"
down_revision = "20260325_0002"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "article_enrichment",
        sa.Column("article_id", sa.Integer(), sa.ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id", ondelete="CASCADE"), nullable=False),
        sa.Column("page_url", sa.Text(), nullable=False),
        sa.Column("feed_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_token", sa.String(32)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_article_enrichment_due", "article_enrichment", ["status", "available_at"])


def downgrade():
    op.drop_table("article_enrichment")
