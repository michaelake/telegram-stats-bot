from datetime import datetime, time
import json
import logging
import os
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import run_repeating

logger = logging.getLogger(__name__)

@run_repeating(interval=86400, first=time(hour=9),  chat_id=global_vars.chat_id)
@run_repeating(interval=86400, first=time(hour=14), chat_id=global_vars.chat_id)
async def check_dates_and_notify(context: ContextTypes.DEFAULT_TYPE):
    logger.info("test " + str(datetime.now()))
    assert global_vars.other_path != None
    assert context.job != None
    logger.info("Checking birthdays")
    bday_dict = {}
    bday_json = os.path.join(global_vars.other_path, 'bday.json')
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
