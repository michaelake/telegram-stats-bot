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

import sys
from typing import Union

from apscheduler.executors.base import logging # pyright: ignore[reportMissingTypeStubs]

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict # pyright: ignore[reportUnreachable]

from datetime import datetime

import telegram

logger = logging.getLogger(__name__)

class MessageDict(TypedDict):
    message_id:              int
    date:                    Union[str, datetime]
    from_user:               Union[int, None]
    forward_from_message_id: Union[int, None]
    forward_from:            Union[int, None]
    forward_from_chat:       Union[int, None]
    caption:                 Union[str, None]
    text:                    Union[str, None]
    sticker_set_name:        Union[str, None]
    new_chat_title:          Union[str, None]
    reply_to_message:        Union[int, None]
    file_id:                 Union[str, None]
    type:                    Union[str, None]

class UserEventDict(TypedDict):
    message_id: Union[int, None]
    user_id:    Union[int, None]
    date:       Union[str, datetime]
    event:      str

def parse_message(message: telegram.Message) -> tuple[MessageDict, list[UserEventDict]]:
    message_dict = MessageDict(
        message_id              = message.message_id,
        date                    = message.date,
        from_user               = None,
        forward_from_message_id = message.forward_from_message_id,
        forward_from            = None,
        forward_from_chat       = None,
        caption                 = message.caption,
        text                    = message.text,
        sticker_set_name        = None,
        new_chat_title          = message.new_chat_title,
        reply_to_message        = None,
        file_id                 = None,
        type                    = None,
    )
    user_event_dict = []

    if message.from_user:
        message_dict['from_user'] = message.from_user.id

    if message.forward_from:
        message_dict['forward_from'] = message.forward_from.id

    if message.forward_from_chat:
        message_dict['forward_from_chat'] = message.forward_from_chat.id

    if message.reply_to_message:
        message_dict['reply_to_message'] = message.reply_to_message.message_id

    message_type = parse_message_type(message)
    assert message_type != None

    message_dict['type'] = message_type
    
    if message_type == 'animation':
        assert message.animation != None
        message_dict['file_id'] = message.animation.file_id

    elif message_type == 'audio':
        assert message.audio != None
        message_dict['file_id'] = message.audio.file_id

    elif message_type == 'document':
        assert message.document != None
        message_dict['file_id'] = message.document.file_id

    elif message_type == 'sticker':
        assert message.sticker != None
        message_dict['file_id']          = message.sticker.file_id
        message_dict['sticker_set_name'] = message.sticker.set_name

    elif message_type == 'new_chat_members':
        user_event_dict: list[UserEventDict] = []
        for member in message.new_chat_members:
            user_event_dict.append(UserEventDict(
                user_id    = member.id,
                message_id = message.message_id,
                date       = message.date,
                event      = 'joined',
            ))

    elif message_type == 'left_chat_member':
        assert message.left_chat_member != None
        user_event_dict = [
            UserEventDict(
                message_id = message.message_id,
                user_id    = message.left_chat_member.id,
                date       = message.date,
                event      = 'left'
            )
        ]

    return message_dict, user_event_dict

message_types = [
    "text",
    "animation",
    "audio",
    "document",
    "game",
    "photo",
    "sticker",
    "video",
    "video_note",
    "voice",
    "location",
    "poll",
    "new_chat_title",
    "new_chat_photo",
    "pinned_message",
    "new_chat_members",
    "left_chat_member",
]

def parse_message_type(message: telegram.Message) -> str|None:
    for prop in message_types:
        if hasattr(message, prop):
            return prop
    return None


