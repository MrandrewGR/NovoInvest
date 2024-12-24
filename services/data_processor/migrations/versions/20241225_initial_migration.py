# migrations/versions/20241225_initial_migration.py
"""Initial migration

Revision ID: 20241225_initial_migration
Revises:
Create Date: 2024-12-25 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20241225_initial_migration'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'telegram_messages',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('message_id', sa.Integer, unique=True, index=True, nullable=False),
        sa.Column('chat_id', sa.String, index=True, nullable=False),
        sa.Column('date', sa.DateTime, nullable=False),
        sa.Column('content', sa.Text, nullable=True),
        sa.Column('media_path', sa.String, nullable=True),
    )

def downgrade():
    op.drop_table('telegram_messages')
