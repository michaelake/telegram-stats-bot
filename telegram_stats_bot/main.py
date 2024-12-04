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
import warnings
import os
import appdirs
from telegram.ext import Application

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers import load_handlers

from .log_storage import JSONStore, PostgresStore
from .stats import StatsRunner

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
    
class CommandLineArgs(argparse.Namespace):
    token:        str = ''
    chat_id:      int = 0
    postgres_url: str = ''
    json_path:    str = ''
    tz:           str = ''

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
        global_vars.bak_store = JSONStore(path)
    else:
        global_vars.bak_store = None

    # Use psycopg 3
    if args.postgres_url.startswith('postgresql://'):
        args.postgres_url = args.postgres_url.replace('postgresql://', 'postgresql+psycopg://', 1)

    global_vars.store   = PostgresStore(args.postgres_url)
    global_vars.stats   = StatsRunner(global_vars.store.engine, tz=args.tz)
    global_vars.chat_id = args.chat_id

    load_handlers(application)
    application.run_polling()
