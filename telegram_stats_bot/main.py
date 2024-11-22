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
import shlex
from typing import Callable
import warnings
import os
import telegram
import random
import appdirs
from datetime import datetime
from telegram.error import BadRequest
from telegram.ext import CommandHandler, JobQueue, MessageHandler, ContextTypes, Application, filters
from telegram import Update, MessageEntity

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

stats = None

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


async def get_chatid(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    assert update.message != None
    assert update.effective_chat != None
    _ = await update.message.reply_text(text=f"Chat id: {update.effective_chat.id}")


async def test_can_read_all_group_messages(context: ContextTypes.DEFAULT_TYPE):
    if not context.bot.can_read_all_group_messages:
        logger.error("Bot privacy is set to enabled, cannot log messages!!!")


async def update_usernames(context: ContextTypes.DEFAULT_TYPE):
    assert stats != None
    assert context.job != None
    assert context.job.chat_id != None

    user_ids = stats.get_message_user_ids()
    db_users = stats.get_db_users()

    tg_users: dict[int, tuple[str, str]|None]
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


async def print_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    assert stats != None
    assert update.effective_user != None
    assert context.args != None

    if update.effective_user.id not in stats.users:
        return

    stats_parser = get_parser(stats)
    image = None

    try:
        ns = stats_parser.parse_args(shlex.split(" ".join(context.args)))
    except HelpException as e:
        text = e.msg
        assert text != None
        await send_help(text, context, update)
        return
    except argparse.ArgumentError as e:
        text = str(e)
        await send_help(text, context, update)
        return
    else:
        args = vars(ns)
        func: Callable[..., tuple[str, bool, str]] = args.pop('func')

        try:
            if args['user']:
                try:
                    uid: int = args['user']
                    args['user'] = uid, stats.users[uid][0]
                except KeyError:
                    await send_help("unknown userid", context, update)
                    return
        except KeyError:
            pass

        try:
            if args['me'] and not args['user']:  # Lets auto-user work by ignoring auto-input me arg
                args['user'] = update.effective_user.id, update.effective_user.name
            del args['me']
        except KeyError:
            pass

        try:
            text, md, image = func(**args)
        except HelpException as e:
            text = e.msg
            assert text != None

            await send_help(text, context, update)
            return

    if image:
        assert update.effective_message != None
        _ = await update.effective_message.reply_photo(
            caption    = '`' + " ".join(context.args) + '`',
            photo      = image,
            parse_mode = telegram.constants.ParseMode.MARKDOWN_V2
        )
        
    if text:
        assert update.effective_message != None
        if md == False:
            _ = await update.effective_message.reply_text(text=text)
        else:
            _ = await update.effective_message.reply_text(text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)




async def bday_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    assert context.args != None

    args = context.args
    md_flag = True
    
    if len(args) >= 1:
        opt = args[0]
    else:
        opt = 'agenda'

    if len(args) == 3:
        user = args[1]
        date = args[2]
        if not is_valid_date(date):
            assert update.message != None
            _ = await update.message.reply_text(text='Acho que a data está errada :(')
            return
    elif len(args) == 2:
        user = args[1]
        date = None
    else:
        user = date = None
    
    bday_json = os.path.join(other_path, 'bday.json')
    
    bday_dict = {} 
    
    if os.path.exists(bday_json):
        with open(bday_json, 'r') as file:
            bday_dict.update(json.load(file))
            
    if opt == 'add' and user and date:
        bday_dict[user] = date
        text = f"Aniversário do usuário {user} atualizado para {date}."
    
    elif opt == 'remove' and user:
        if user in bday_dict:
            del bday_dict[user]
            text = f"Aniversário do usuário {user} removido."
        else:
            text = f"Usuário {user} não encontrado."
    
    elif opt == 'agenda':
        text = "\n".join([f"{user}: {date}" for user, date in bday_dict.items()])
        if not text:
            text = "Ninguém me disse os aniversários."
    elif opt == 'mes':
        current_month = datetime.now().month
        month_bday = {
            user: date
                for user, date in bday_dict.items()
                    if datetime.strptime(date, '%d/%m/%Y').month == current_month
        }
        if len(month_bday.items()) > 0:
            text = "\n".join([f"{user}: {date}" for user, date in month_bday.items()])
        else: text = 'Ninguém faz aniversário nesse mês!'
    elif opt == 'dia':
        current_day = datetime.now().day
        day_bday = {
            user: date
                for user, date in bday_dict.items()
                    if datetime.strptime(date, '%d/%m/%Y').day == current_day
        }
        if len(day_bday.items()) > 0:
            text = "Parabéns {}!\n\nhttps://www.youtube.com/watch?v=1Mcdh2Vf2Xk"
            text = text.format(" e ".join([f"{user}" for user, date in day_bday.items()]))
            md_flag = False
        else: 
            text = 'Ninguém faz aniversário hoje!'
    else:
        text = "Comando ou argumentos inválidos."
        
    with open(bday_json, 'w') as file:
        json.dump(bday_dict, file)

    assert update.message != None
    if md_flag:
        _ = await update.message.reply_text(
            text       = f"```\n{text}\n```",
            parse_mode = telegram.constants.ParseMode.MARKDOWN_V2
        )
    else:
        _ = await update.message.reply_text(text=f"\n{text}\n")

async def dice_dicer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    assert args != None
    
    text = ""
    if len(args) > 1:
        _max = 1
    else:
        if int(args[0]):
            _max = int(args[0])
            res  = random.randint(1,_max+1)
            text = f"\nJoguei um D{_max} e peguei {res}\n"
        else:
            text = "Meus dados só tem números!"

    if not text:
        return

    assert update.message != None
    _ = await update.message.reply_text(text = text)


async def info_giver(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    info_path = os.path.join(other_path, 'infos.txt')
    print(info_path)
    try:
        with open(info_path, 'r') as infos:
            text = infos.read()
    except FileNotFoundError:
        text = "q"

    assert update.message != None
    _ = await update.message.reply_text(
        text       = f"```\n{text}\n```",
        parse_mode = telegram.constants.ParseMode.MARKDOWN_V2
    )
    
async def send_help(text: str, context: ContextTypes.DEFAULT_TYPE, update: Update):
    """
    Send help text to user. Tries to send a direct message if possible.
    :param text: text to send
    :param context:
    :param update:
    :return:
    """
    assert update.effective_user != None
    assert context.bot != None

    try:
        _ = await context.bot.send_message(
            chat_id    = update.effective_user.id,
            text       = f"```\n{text}\n```",
            parse_mode = telegram.constants.ParseMode.MARKDOWN_V2
        )
    except telegram.error.Forbidden:  # If user has never chatted with bot
        assert update.message != None
        _ = await update.message.reply_text(text=f"```\n{text}\n```",
                                        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

async def check_dates_and_notify(context: ContextTypes.DEFAULT_TYPE):
    assert context.job != None
    assert context.job
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
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    _ = parser.add_argument('token', type=str, help="Telegram bot token")
    _ = parser.add_argument('chat_id', type=int, help="Telegram chat id to monitor.")
    _ = parser.add_argument('postgres_url', type=str, help="Sqlalchemy-compatible postgresql url.")
    _ = parser.add_argument('--json-path',
        type = str,
        help = "Either full path to backup storage folder or prefix (will be stored in app data dir.",
        default = ""
    )
    _ = parser.add_argument('--tz',
        type=str,
        help="tz database time zone string, e.g. Europe/London",
        default='Etc/UTC'
    )

    args = parser.parse_args()
    assert isinstance(args.token,        str) # pyright: ignore[reportAny]
    assert isinstance(args.json_path,    str) # pyright: ignore[reportAny] 
    assert isinstance(args.postgres_url, str) # pyright: ignore[reportAny] 
    assert isinstance(args.tz,           str) # pyright: ignore[reportAny] 
    assert isinstance(args.chat_id,      int) # pyright: ignore[reportAny] 

    application = Application.builder().token(args.token).build()
    
    other_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'other')
    if not os.path.exists(other_path): os.mkdir(other_path)
    
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
    stats = StatsRunner(store.engine, tz=args.tz)

    stats_handler = CommandHandler(['stats', 's'], print_stats)
    application.add_handler(stats_handler)

    chat_id_handler = CommandHandler('chatid', get_chatid, filters=~filters.UpdateType.EDITED)
    application.add_handler(chat_id_handler)
    
    bday_handler = CommandHandler(['niver', 'n'], bday_info)                                                                                                                                                                                   
    application.add_handler(bday_handler)
    
    info_handler = CommandHandler(['help', 'h'], info_giver)
    application.add_handler(info_handler)
    
    dice_handler = CommandHandler(['dados', 'd'], dice_dicer)
    application.add_handler(dice_handler)

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
