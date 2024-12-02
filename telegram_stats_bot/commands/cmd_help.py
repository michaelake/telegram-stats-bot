import os

import telegram
from telegram import Update
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.commands.decorator import command

@command(["help", "h"])
async def info_giver(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    info_path = os.path.join(global_vars.other_path, 'infos.txt')
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
