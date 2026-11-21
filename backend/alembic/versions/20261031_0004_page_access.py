"""Persistent page refusal, rate-limit and in-flight controls."""
from alembic import op
import sqlalchemy as sa
revision='20261031_0004'
down_revision='20261027_0003'
branch_labels=None
depends_on=None
def upgrade():
    inspector=sa.inspect(op.get_bind())
    if inspector.has_table('source_page_controls'):
        # Legacy bootstrap may already have created the current model's table.
        expected={'key','refusal_status','refused_at','reason','next_allowed_at','lease_token',
                  'lease_until','last_status','last_attempt_at','network_attempts'}
        columns={c['name'] for c in inspector.get_columns('source_page_controls')}
        if columns!=expected or inspector.get_pk_constraint('source_page_controls')['constrained_columns']!=['key']:
            raise RuntimeError('Existing page controls have an incompatible schema; preserve and review them.')
        return
    op.create_table('source_page_controls',
        sa.Column('key',sa.String(256),primary_key=True),
        sa.Column('refusal_status',sa.Integer()),sa.Column('refused_at',sa.DateTime(timezone=True)),
        sa.Column('reason',sa.String(80)),sa.Column('next_allowed_at',sa.DateTime(timezone=True)),
        sa.Column('lease_token',sa.String(32)),sa.Column('lease_until',sa.DateTime(timezone=True)),
        sa.Column('last_status',sa.Integer()),sa.Column('last_attempt_at',sa.DateTime(timezone=True)),
        sa.Column('network_attempts',sa.Integer(),nullable=False,server_default='0'))
def downgrade():
    raise RuntimeError('Refusal controls must not be deleted by a routine rollback.')
