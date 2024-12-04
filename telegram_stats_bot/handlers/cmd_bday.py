import json
import os
from datetime import datetime

import telegram
from telegram import Update
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import command
from telegram_stats_bot.utils import is_valid_date

@command(["niver", "n"])
async def command_niver(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            _ = await update.message.reply_text(text='Acho que a data está errada :(') #)
            return
    elif len(args) == 2:
        user = args[1]
        date = None
    else:
        user = date = None

    assert global_vars.other_path != None
    bday_json = os.path.join(global_vars.other_path, 'bday.json')
    
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
