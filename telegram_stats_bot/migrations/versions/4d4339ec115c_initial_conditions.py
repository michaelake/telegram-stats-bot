"""Initial conditions

Revision ID: 4d4339ec115c
Revises: 
Create Date: 2024-11-02 22:01:52.455115

"""
from typing import Union, Sequence
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision:      str      = '4d4339ec115c'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on:    Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    _ = op.create_table('messages_utc',
        sa.Column('message_id', sa.BigInteger(), nullable=True),
        sa.Column('date', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('from_user', sa.BigInteger(), nullable=True),
        sa.Column('forward_from_message_id', sa.BigInteger(), nullable=True),
        sa.Column('forward_from', sa.BigInteger(), nullable=True),
        sa.Column('forward_from_chat', sa.BigInteger(), nullable=True),
        sa.Column('caption', sa.Text(), nullable=True),
        sa.Column('text', sa.Text(), nullable=True),
        sa.Column('sticker_set_name', sa.Text(), nullable=True),
        sa.Column('new_chat_title', sa.Text(), nullable=True),
        sa.Column('reply_to_message', sa.BigInteger(), nullable=True),
        sa.Column('file_id', sa.Text(), nullable=True),
        sa.Column('type', sa.Text(), nullable=True),
        sa.Column('text_index_col', postgresql.TSVECTOR(), sa.Computed("to_tsvector('english', coalesce(text, ''))", ), nullable=False)
    )
    op.create_index('messages_utc_date_index', 'messages_utc', ['date'], unique=False)
    op.create_index('messages_utc_from_user_index', 'messages_utc', ['from_user'], unique=False)
    op.create_index('messages_utc_type_index', 'messages_utc', ['type'], unique=False)
    op.create_index('text_idx', 'messages_utc', ['text_index_col'], unique=False, postgresql_using='gin')

    _ = op.create_table('user_events',
        sa.Column('message_id', sa.BigInteger(), nullable=True),
        sa.Column('user_id', sa.BigInteger(), nullable=True),
        sa.Column('date', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('event', sa.Text(), nullable=True)
    )
    op.create_index('ix_user_events_message_id', 'user_events', ['message_id'], unique=False)

    _ = op.create_table('user_names',
        sa.Column('user_id', sa.BigInteger(), nullable=True),
        sa.Column('date', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('username', sa.Text(), nullable=True),
        sa.Column('display_name', sa.Text(), nullable=True)
    )
    op.create_index('user_names_user_id_date_index', 'user_names', ['user_id', 'date'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    pass