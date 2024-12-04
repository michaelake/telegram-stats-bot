import logging

from telegram import Update
from telegram.ext import ContextTypes, filters
from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import message
from telegram_stats_bot.parse import parse_message

logger = logging.getLogger(__name__)

@message(filters.Chat(chat_id=global_vars.chat_id))
async def log_message(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    store     = global_vars.store
    bak_store = global_vars.bak_store
    assert store != None

    logger.debug(update)

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

    for event in user:
        if not event:
            continue
        if bak_store:
            bak_store.append_data('user_events', event)
        store.append_data('user_events', event)
