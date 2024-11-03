from collections.abc import Mapping
from datetime import datetime
from typing import Any
from sqlalchemy import TIMESTAMP, BigInteger, Index, Text
from sqlalchemy.orm import Mapped, mapped_column
from telegram_stats_bot.db.base import Base

class UserName(Base):
    __tablename__: str = "user_names"

    user_id:      Mapped[int]      = mapped_column(BigInteger, nullable=True)
    date:         Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    username:     Mapped[str]      = mapped_column(Text,       nullable=True)
    display_name: Mapped[str]      = mapped_column(Text,       nullable=True)

    __table_args__: tuple[Any, ...] = (
        Index("user_names_user_id_date_index", user_id, date),
    )

    __mapper_args__: Mapping[str, Any] = {
        "primary_key": [ user_id ],
    }
