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

import logging
import json
import argparse
import shlex
import warnings
import os
import telegram
import random
import appdirs
import pytz
from datetime import datetime, time
from telegram.error import BadRequest
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackContext, JobQueue, ContextTypes, Application, \
    filters
from telegram import Update
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .parse import parse_message
from .log_storage import JSONStore, PostgresStore
from .stats import StatsRunner, get_parser, HelpException
from .utils import is_valid_date

warnings.filterwarnings("ignore")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)  # Mute normal http requests

logger = logging.getLogger(__name__)

stats = None

try:
    with open("./sticker-keys.json", 'r') as f:
        stickers = json.load(f)
except FileNotFoundError:
    stickers = {}
sticker_idx = None
sticker_id = None


async def log_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.edited_message:
        edited_message, user = parse_message(update.effective_message)
        if bak_store:
            bak_store.append_data('edited-messages', edited_message)
        store.update_data('messages', edited_message)
        return

    try:
        logger.info(update.effective_message.message_id)
    except AttributeError:
        logger.warning("No effective_message attribute")
    message, user = parse_message(update.effective_message)

    if message:
        if bak_store:
            bak_store.append_data('messages', message)
        store.append_data('messages', message)
    if len(user) > 0:
        for i in user:
            if i:
                if bak_store:
                    bak_store.append_data('user_events', i)
                store.append_data('user_events', i)


async def get_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(text=f"Chat id: {update.effective_chat.id}")


async def test_can_read_all_group_messages(context: CallbackContext):
    if not context.bot.can_read_all_group_messages:
        logger.error("Bot privacy is set to enabled, cannot log messages!!!")


async def update_usernames(context: ContextTypes.DEFAULT_TYPE):  # context.job.context contains the chat_id
    user_ids = stats.get_message_user_ids()
    db_users = stats.get_db_users()
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


async def print_stats(update: Update, context: CallbackContext):
    if update.effective_user.id not in stats.users:
        return

    stats_parser = get_parser(stats)
    image = None

    try:
        ns = stats_parser.parse_args(shlex.split(" ".join(context.args)))
    except HelpException as e:
        text = e.msg
        await send_help(text, context, update)
        return
    except argparse.ArgumentError as e:
        text = str(e)
        await send_help(text, context, update)
        return
    else:
        args = vars(ns)
        func = args.pop('func')

        try:
            if args['user']:
                try:
                    uid = args['user']
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
            text, image = func(**args)
        except HelpException as e:
            text = e.msg
            await send_help(text, context, update)
            return

    if text:
        await update.effective_message.reply_text(text=text,
                                                  parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    if image:
        await update.effective_message.reply_photo(caption='`' + " ".join(context.args) + '`', photo=image,
                                                   parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)




async def bday_info(update: Update, context: CallbackContext):
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
            await update.message.reply_text(text='Acho que a data está errada :(')
            return
    elif len(args) == 2:
        user = args[1]
        date = None
    else:
        user = date = None
    
    # Caminho para o arquivo JSON
    bday_json = os.path.join(other_path, 'bday.json')
    
    # Inicializa dicionário de aniversários
    bday_dict = {} 
    
    # Ler o arquivo JSON existente
    if os.path.exists(bday_json):
        with open(bday_json, 'r') as file:
            bday_dict.update(json.load(file))

    # Adicionar novo aniversário
    if opt == 'add' and user and date:
        bday_dict[user] = date
        text = f"Aniversário do usuário {user} atualizado para {date}."
    
    # Remover aniversário
    elif opt == 'remove' and user:
        if user in bday_dict:
            del bday_dict[user]
            text = f"Aniversário do usuário {user} removido."
        else:
            text = f"Usuário {user} não encontrado."
    
    # Listar todos os aniversários
    elif opt == 'agenda':
        text = "\n".join([f"{user}: {date}" for user, date in bday_dict.items()])
        if not text:
            text = "Ninguém me disse os aniversários."
    elif opt == 'mes':
        current_month = datetime.now().month
        month_bday = {user: date for user, date in bday_dict.items() if datetime.strptime(date, '%d/%m/%Y').month == current_month}
        if len(month_bday.items()) > 0:
            text = "\n".join([f"{user}: {date}" for user, date in month_bday.items()])
        else: text = 'Ninguém faz aniversário nesse mês!'
    elif opt == 'dia':
        current_day = datetime.now().day
        day_bday = {user: date for user, date in bday_dict.items() if datetime.strptime(date, '%d/%m/%Y').day == current_day}
        if len(day_bday.items()) > 0:
            text = "Parabéns " + " e ".join([f"{user}" for user, date in day_bday.items()]) + "!\n\nhttps://www.youtube.com/watch?v=1Mcdh2Vf2Xk"
            md_flag = False
        else: 
            text = 'Ninguém faz aniversário hoje!'
    else:
        text = "Comando ou argumentos inválidos."
        
    with open(bday_json, 'w') as file:
        json.dump(bday_dict, file)

    if md_flag:
        await update.message.reply_text(text=f"```\n{text}\n```",
                                        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text(text=f"\n{text}\n")

async def dice_dicer(update: Update, context: CallbackContext):
    args = context.args
    
    if len(args) > 1:
        _max = 1
    else:
        if int(args[0]):
            _max = int(args[0])
            res = random.randint(1,_max+1)
            text=f"\nJoguei um D{_max} e peguei {res}\n"
        else:
            text="Meus dados só tem números!"

    await update.message.reply_text(text=text)


async def info_giver(update: Update):
    text = '''
    Aqui está tudo que eu sei fazer!
    
    /help, /h: mostra meus comandos,
    /dados <número>, /d <número>: Gera um número aleatório de 1 a número (inclusivo),
    /niver, /n: 
        'agenda' - Lista todos os aniversários salvos,
        'mes' - Lista todos os aniversariantes do mês,
        'dia' - Lista de aniversariantes do dia,
        'add' - Adiciona um novo aniversariante na agenda,
        'remove' - Remove um aniversariante da agenda,
            
    /stats, /s (bstats, bs no contexto betamiko): 
        'counts' - "get_chat_counts",
        'count-dist' - 'get_chat_ecdf',
        'hours' - "get_counts_by_hour",
        'days' - "get_counts_by_day",
        'week' - "get_week_by_hourday",
        'history' - "get_message_history",
        'titles' - 'get_title_history',
        'user' - 'get_user_summary',
        'corr' - "get_user_correlation",
        'delta' - "get_message_deltas",
        'types' - "get_type_stats",
        'words' - "get_word_stats",
        'random' - "get_random_message"
    '''
    await update.message.reply_text(text=f"```\n{text}\n```",
                                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
    
async def send_help(text: str, context: CallbackContext, update: Update):
    """
    Send help text to user. Tries to send a direct message if possible.
    :param text: text to send
    :param context:
    :param update:
    :return:
    """
    try:
        await context.bot.send_message(chat_id=update.effective_user.id,
                                       text=f"```\n{text}\n```",
                                       parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
    except telegram.error.Forbidden:  # If user has never chatted with bot
        await update.message.reply_text(text=f"```\n{text}\n```",
                                        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

'''async def check_dates_helper(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    await update.message.reply_text('help')
    
    context.job_queue.run_daily(
        check_dates_and_notify,
        time=time(hour=12, minute=20),
        chat_id=chat_id
    )'''
    
async def check_dates_and_notify(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Checking birthdays")
    bday_dict = {}
    bday_json = os.path.join(other_path, 'bday.json')
    if os.path.exists(bday_json):
        with open(bday_json, 'r') as file:
            bday_dict.update(json.load(file))
            
    today = datetime.now().strftime('%d/%m/%Y')
       
    for user, date in bday_dict.items():
            if today == date:
                # Envia a mensagem de aniversário
                await context.bot.send_message(chat_id=context.job.chat_id,
                                               text=f"Hoje é o aniversário de {user}!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('token', type=str, help="Telegram bot token")
    parser.add_argument('chat_id', type=int, help="Telegram chat id to monitor.")
    parser.add_argument('postgres_url', type=str, help="Sqlalchemy-compatible postgresql url.")
    parser.add_argument('--json-path', type=str,
                        help="Either full path to backup storage folder or prefix (will be stored in app data dir.",
                        default="")
    parser.add_argument('--tz', type=str,
                        help="tz database time zone string, e.g. Europe/London",
                        default='Etc/UTC')
    args = parser.parse_args()

    application = Application.builder().token(args.token).build()

    other_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'other')
    if not os.path.exists(other_path): os.mkdir(other_path)
    
    if args.json_path:
        path = args.json_path
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

    stats_handler = CommandHandler(['bstats', 'bs'], print_stats)
    application.add_handler(stats_handler)

    chat_id_handler = CommandHandler('chatid', get_chatid, filters=~filters.UpdateType.EDITED)
    application.add_handler(chat_id_handler)
    
    bday_handler = CommandHandler(['niver', 'n'], bday_info)                                                                                                                                                                                   
    application.add_handler(bday_handler)
    
    info_handler = CommandHandler(['help', 'h'], info_giver)
    application.add_handler(info_handler)
    
    dice_handler = CommandHandler(['dados', 'd'], dice_dicer)
    application.add_handler(dice_handler)
    
    if args.chat_id != 0:
        log_handler = MessageHandler(filters.Chat(chat_id=args.chat_id), log_message)
        application.add_handler(log_handler)

    job_queue = application.job_queue
    update_users_job = job_queue.run_repeating(update_usernames, interval=3600, first=5, chat_id=args.chat_id)
    test_privacy_job = job_queue.run_once(test_can_read_all_group_messages, 0)
    
    # workaround the run_daily bug (doens't work), use run_repeating instead)
    now = datetime.now()
    time_until_target_morn = (now.replace(hour=9, minute=0, second=0, microsecond=0) - now).total_seconds()
    check_bdays_morn = job_queue.run_repeating(check_dates_and_notify, interval=86400, first=time_until_target_morn, chat_id=args.chat_id)

    time_until_target_noon = (now.replace(hour=14, minute=0, second=0, microsecond=0) - now).total_seconds()
    check_bdays_noon = job_queue.run_repeating(check_dates_and_notify, interval=86400, first=time_until_target_noon, chat_id=args.chat_id)
    
    application.run_polling()
