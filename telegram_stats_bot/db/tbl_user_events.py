from collections.abc import Mapping
from datetime import datetime
from typing import Any
from sqlalchemy import TIMESTAMP, BigInteger, Index, Text
from sqlalchemy.orm import Mapped, mapped_column
from telegram_stats_bot.db.base import Base

class UserEvent(Base):
    __tablename__: str = "user_events"

    message_id: Mapped[int]      = mapped_column(BigInteger, nullable=True)
    user_id:    Mapped[int]      = mapped_column(BigInteger, nullable=True)
    date:       Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    event:      Mapped[str]      = mapped_column(Text,       nullable=True)

    __table_args__: tuple[Any, ...] = (
        Index("ix_user_events_message_id", message_id),
    )

    __mapper_args__: Mapping[str, Any] = {
        "primary_key": [ message_id, user_id ],
    }
