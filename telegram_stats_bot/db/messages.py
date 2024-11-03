from collections.abc import Mapping
from datetime import datetime
from typing import Any
from sqlalchemy import Computed, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import TIMESTAMP, BigInteger, Text
from sqlalchemy.dialects.postgresql import TSVECTOR

from telegram_stats_bot.db.base import Base

class Message(Base):
    __tablename__: str = "messages_utc"

    message_id:        Mapped[int]       = mapped_column("message_id", BigInteger, nullable=True)
    date:              Mapped[datetime]  = mapped_column(TIMESTAMP(timezone=True),  nullable=True)
    from_user:         Mapped[int]       = mapped_column(BigInteger, nullable=True)
    forward_from_message_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    forward_from:      Mapped[int]       = mapped_column(BigInteger, nullable=True)
    forward_from_chat: Mapped[int]       = mapped_column(BigInteger, nullable=True)
    caption:           Mapped[str]       = mapped_column(Text,       nullable=True)
    text:              Mapped[str]       = mapped_column(Text,       nullable=True)
    sticker_set_name:  Mapped[str]       = mapped_column(Text,       nullable=True)
    new_chat_title:    Mapped[str]       = mapped_column(Text,       nullable=True)
    reply_to_message:  Mapped[int]       = mapped_column(BigInteger, nullable=True)
    file_id:           Mapped[str]       = mapped_column(Text,       nullable=True)
    type:              Mapped[str]       = mapped_column(Text,       nullable=True)

    text_index_col: Mapped[str] = mapped_column(TSVECTOR,
        Computed("to_tsvector('english', coalesce(text, ''))"))

    __table_args__: tuple[Any, ...] = (
        Index("text_idx", text_index_col, postgresql_using="gin"),
        Index("messages_utc_date_index",      date),
        Index("messages_utc_from_user_index", from_user),
        Index("messages_utc_type_index",      type),
    )

    # Só precisa estar aqui porque o framework requer uma chave
    __mapper_args__: Mapping[str, Any] = {
        "primary_key": [ message_id ],
    }


