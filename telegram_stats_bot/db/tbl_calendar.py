from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal
from sqlalchemy import TIMESTAMP, BigInteger, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from telegram_stats_bot.db.base import Base

State = Literal[
    "inactive",
    "no_repeat",
    "every_second",
    "every_minute",
    "every_hour",
    "every_day",
    "every_week",
    "every_month",
    "every_year",
]


class Event(Base):
    __tablename__: str = "calendar_event"

    id:          Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id:     Mapped[int] = mapped_column(BigInteger)
    user_id:     Mapped[int] = mapped_column(BigInteger)
    title:       Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    created_at:  Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    updated_at:  Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))

    occurrences: Mapped[list["Occurrence"]] = relationship(back_populates="event")


class Occurrence(Base):
    __tablename__: str = "calendar_occurrence"

    calendar_event_id: Mapped[int]      = mapped_column(ForeignKey("calendar_event.id"))
    date:              Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))

    event: Mapped["Event"] = relationship(back_populates="occurrences")

    __mapper_args__: Mapping[str, Any] = {
        "primary_key": [ calendar_event_id, date ],
    }


