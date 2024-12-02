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
import logging
import json
import argparse
from typing import Union
import warnings
import os
import telegram
import random
import appdirs
from datetime import datetime
from telegram.error import BadRequest
from telegram.ext import JobQueue, MessageHandler, ContextTypes, Application, filters
from telegram import Update, MessageEntity

from telegram_stats_bot import global_vars
from telegram_stats_bot.commands import load_commands

from .parse import parse_message
from .log_storage import JSONStore, PostgresStore
from .stats import StatsRunner, get_parser, HelpException
from .utils import is_valid_date

warnings.filterwarnings("ignore")

logging.basicConfig(
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level  = logging.INFO
)

logging.getLogger('httpx').setLevel(logging.WARNING)  # Mute normal http requests

logger = logging.getLogger(__name__)
logger.info("Python version: %s", sys.version)

try:
    with open("./sticker-keys.json", 'r') as f:
        stickers = json.load(f)
except FileNotFoundError:
    stickers = {}
sticker_idx = None
sticker_id = None


async def log_message(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    if update.edited_message and update.effective_message:
        edited_message, user = parse_message(update.effective_message)
        if bak_store:
            bak_store.append_data('edited-messages', edited_message)
        store.update_data('messages', edited_message)
        return

    assert update.effective_message != None
    message, user = parse_message(update.effective_message)

    if message:
        if bak_store:
            bak_store.append_data('messages', message)
        store.append_data('messages', message)
    if len(user) > 0:
        for event in user:
            if not event:
                continue
            if bak_store:
                bak_store.append_data('user_events', event)
            store.append_data('user_events', event)



async def test_can_read_all_group_messages(context: ContextTypes.DEFAULT_TYPE):
    if not context.bot.can_read_all_group_messages:
        logger.error("Bot privacy is set to enabled, cannot log messages!!!")


async def update_usernames(context: ContextTypes.DEFAULT_TYPE):
    stats = global_vars.stats

    assert stats != None
    assert context.job != None
    assert context.job.chat_id != None

    user_ids = stats.get_message_user_ids()
    db_users = stats.get_db_users()

    tg_users: dict[int, Union[tuple[str, str], None]]
    tg_users = {user_id: None for user_id in user_ids}
    to_update = {}
    for u_id in tg_users:
        try:
            chat_member: telegram.ChatMember = await context.bot.get_chat_member(chat_id=context.job.chat_id,
                                                                                 user_id=u_id)
            user = chat_member.user
            tg_users[u_id] = user.name, user.full_name
            if tg_users[u_id] != db_users[u_id]:
                if tg_users[u_id][1] == db_users[u_id][1]:  # Flag these so we don't insert new row
                    to_update[u_id] = tg_users[u_id][0], None
                else:
                    to_update[u_id] = tg_users[u_id]
        except KeyError:  # First time user
            to_update[u_id] = tg_users[u_id]
        except BadRequest:  # Handle users no longer in chat or haven't messaged since bot joined
            logger.debug("Couldn't get user %s", u_id)  # debug level because will spam every hour
    stats.update_user_ids(to_update)
    if stats.users_lock.acquire(timeout=10):
        stats.users = stats.get_db_users()
        stats.users_lock.release()
    else:
        logger.warning("Couldn't acquire username lock.")
        return
    logger.info("Usernames updated")
    
async def check_dates_and_notify(context: ContextTypes.DEFAULT_TYPE):
    assert context.job != None
    logger.info("Checking birthdays")
    bday_dict = {}
    bday_json = os.path.join(other_path, 'bday.json')
    if os.path.exists(bday_json):
        with open(bday_json, 'r') as file:
            bday_dict.update(json.load(file))
            
    today = datetime.now().strftime('%d/%m/%Y')
       
    for user, date in bday_dict.items():
        if today == date:
            assert context.job.chat_id != None
            # Envia a mensagem de aniversário
            _ = await context.bot.send_message(
                chat_id = context.job.chat_id,
                text    = f"Hoje é o aniversário de {user}!"
            )

async def responses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    ff_text = 'Você já ouviu falar do aclamado MMORPG Final Fantasy XIV? Com uma versão gratuita expandida, você pode jogar todo o conteúdo de A Realm Reborn e a premiada expansão Stormblood até o nível 70 gratuitamente, sem restrições de tempo de jogo.'
    if message and message.entities:
        bot_username = context.bot.username
        for entity in message.entities:
            if entity.type == MessageEntity.MENTION:
                assert message.text != None
                mentioned_username = message.text[entity.offset:entity.offset + entity.length]
                if mentioned_username == f"@{bot_username}":
                    if any(kw in message.text for kw in ["ffxiv", 'ff14']):
                        _ = await message.reply_text(ff_text)
                    elif 'raqueta' in message.text:
                        raq_path = os.path.join(other_path, 'raqueta.txt')
                        with open(raq_path, 'r') as file:
                            raqueta = file.readlines()
                        _ = await message.reply_text(text=f"```{raqueta}```",parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
    
class CommandLineArgs(argparse.Namespace):
    token:        str = ''
    chat_id:      int = 0
    postgres_url: str = ''
    json_path:    str = ''
    tz:           str = ''

class Program:
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    _ = parser.add_argument('token', type=str, help="Telegram bot token")
    _ = parser.add_argument('chat_id', type=int, help="Telegram chat id to monitor.")
    _ = parser.add_argument('postgres_url', type=str, help="Sqlalchemy-compatible postgresql url.")
    _ = parser.add_argument('--json-path',
        type = str,
        help = "Either full path to backup storage folder or prefix (will be stored in app data dir).",
        default = ""
    )
    _ = parser.add_argument('--tz',
        type=str,
        help="tz database time zone string, e.g. Europe/London",
        default='Etc/UTC'
    )

    args        = parser.parse_args(namespace=CommandLineArgs())
    application = Application.builder().token(args.token).build()
    
    other_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'other')
    if not os.path.exists(other_path): os.mkdir(other_path)
    global_vars.other_path = other_path
    
    if args.json_path:
        path: str = args.json_path
        if not os.path.split(path)[0]:  # Empty string for left part of path
            path = os.path.join(appdirs.user_data_dir('telegram-stats-bot'), path)

        os.makedirs(path, exist_ok=True)
        bak_store = JSONStore(path)
    else:
        bak_store = None

    # Use psycopg 3
    if args.postgres_url.startswith('postgresql://'):
        args.postgres_url = args.postgres_url.replace('postgresql://', 'postgresql+psycopg://', 1)

    store = PostgresStore(args.postgres_url)
    global_vars.stats = StatsRunner(store.engine, tz=args.tz)

    load_commands(application)

    res_handler = MessageHandler(filters.Entity(MessageEntity.MENTION), responses)
    application.add_handler(res_handler)

    if args.chat_id != 0:
        log_handler = MessageHandler(filters.Chat(chat_id=args.chat_id), log_message)
        application.add_handler(log_handler)

    job_queue = application.job_queue
    assert isinstance(job_queue, JobQueue)
    
    update_users_job = job_queue.run_repeating(update_usernames, interval=3600, first=5, chat_id=args.chat_id)
    test_privacy_job = job_queue.run_once(test_can_read_all_group_messages, 0)
    
    # workaround the run_daily bug (doens't work), use run_repeating instead)
    now = datetime.now()
    time_until_target_morn = (now.replace(hour=9, minute=0, second=0, microsecond=0) - now).total_seconds()
    check_bdays_morn = job_queue.run_repeating(check_dates_and_notify, interval=86400, first=time_until_target_morn, chat_id=args.chat_id)

    time_until_target_noon = (now.replace(hour=14, minute=0, second=0, microsecond=0) - now).total_seconds()
    check_bdays_noon = job_queue.run_repeating(check_dates_and_notify, interval=86400, first=time_until_target_noon, chat_id=args.chat_id)
    
    application.run_polling()
