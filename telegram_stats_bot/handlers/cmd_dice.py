import random
from telegram import Update
from telegram.ext import ContextTypes

from telegram_stats_bot.handlers.decorator import command

@command(["dice", "d"])
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
