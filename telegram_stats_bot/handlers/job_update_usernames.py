import logging
from typing import Union

import telegram
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import run_repeating

logger = logging.getLogger(__name__)

@run_repeating(interval=3600, first=5, chat_id=global_vars.chat_id)
async def update_usernames(context: ContextTypes.DEFAULT_TYPE):
    stats = global_vars.stats

    assert stats != None
    assert context.job != None
    assert context.job.chat_id != None

    user_ids = stats.get_message_user_ids()
    db_users = stats.get_db_users()

    tg_users: dict[int, Union[tuple[str, str], None]]
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
