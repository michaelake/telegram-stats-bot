import logging

from telegram.ext import ContextTypes
from telegram_stats_bot.handlers.decorator import run_once

logger = logging.getLogger(__name__)

@run_once(when=0)
async def test_can_read_all_group_messages(context: ContextTypes.DEFAULT_TYPE):
    if not context.bot.can_read_all_group_messages:
        logger.error("Bot privacy is set to enabled, cannot log messages!!!")
