# !/usr/bin/env python
#
# A logging and statistics bot for Telegram based on python-telegram-bot.
# Copyright (C) 2020
# Michael DM Dryden <mk.dryden@utoronto.ca>
#
# This file is part of telegram-stats-bot.
#
# telegram-stats-bot is free software: you can redistribute it and/or modify
# it under the terms of the GNU Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Public License
# along with this program. If not, see [http://www.gnu.org/licenses/].
import datetime
import logging
import json
import os
from typing import Union

from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists

from telegram_stats_bot.db.messages import Message
from telegram_stats_bot.db.user_events import UserEvent

from .parse import MessageDict, UserEventDict

logger = logging.getLogger(__name__)

def date_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


class JSONStore(object):
    def __init__(self, path: str):
        self.store = path

    def append_data(self, name: str, data: Union[MessageDict, UserEventDict]):
        with open(os.path.join(self.store, f"{name}.json"), 'a') as f:
            f.write(json.dumps(data, default=date_converter) + "\n")


class PostgresStore(object):
    def __init__(self, connection_url: str):
        self.engine = create_engine(connection_url, echo=False, isolation_level="AUTOCOMMIT")
        if not database_exists(self.engine.url):
            logging.critical("Database {} does not exist".format(connection_url))

    def append_data(self, name: str, data: Union[MessageDict, UserEventDict]):
        data['date'] = str(data['date'])
        if name == 'messages':
            with Session(self.engine) as session:
                msg = Message(**data)
                session.add(msg)
                session.commit()

        elif name == 'user_events':
            with Session(self.engine) as session:
                evt = UserEvent(**data)
                session.add(evt)
                session.commit()
        else:
            logger.warning("Tried to append to invalid table %s", name)

    def update_data(self, name: str, data: Union[MessageDict, UserEventDict]):
        data['date'] = str(data['date'])
        if name == 'messages':
            with Session(self.engine) as session:
                _ = session.execute(
                    update(Message)
                        .where(Message.message_id == data["message_id"])
                        .values(**data)
                )
                session.commit()

        elif name == 'user_events':
            with Session(self.engine) as session:
                _ = session.execute(
                    update(UserEvent)
                        .where(UserEvent.message_id == data["message_id"])
                        .values(**data)
                )
            
        else:
            logger.warning("Tried to update to invalid table %s", name)

