from telegram import Update
from telegram.ext import ContextTypes, filters
from telegram_stats_bot.commands.decorator import command

@command("chatid", filters=~filters.UpdateType.EDITED)
async def command_chatid(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    assert update.message != None
    assert update.effective_chat != None
    _ = await update.message.reply_text(text=f"Chat id: {update.effective_chat.id}")
