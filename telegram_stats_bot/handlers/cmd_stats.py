import argparse
import shlex
from typing import Callable 
from telegram import Update
import telegram
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.handlers.decorator import command
from telegram_stats_bot.stats import HelpException, get_parser

@command(["stats", "s"])
async def command_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stats = global_vars.stats

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

