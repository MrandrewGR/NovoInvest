# migrations/versions/v2_add_tg_ubot_table.py

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '<revision_id>'
down_revision = '20241225_initial_migration'  # или любая ваша актуальная ревизия
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'tg_ubot',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('message_id', sa.Integer, unique=True, index=True, nullable=False),
        sa.Column('chat_id', sa.String, index=True, nullable=False),
        sa.Column('date', sa.DateTime, nullable=False),
        sa.Column('content', sa.Text, nullable=True),
        sa.Column('media_path', sa.String, nullable=True),
    )

def downgrade():
    op.drop_table('tg_ubot')
