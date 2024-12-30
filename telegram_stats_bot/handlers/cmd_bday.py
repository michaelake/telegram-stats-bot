import json
import logging
import os
from datetime import datetime, time, timedelta
from sqlalchemy.orm import Session

import telegram
from telegram import Update
from telegram.ext import ContextTypes

from telegram_stats_bot import global_vars
from telegram_stats_bot.db.tbl_calendar import Event, Occurrence
from telegram_stats_bot.handlers.decorator import command
from telegram_stats_bot.utils import is_valid_date

logger = logging.getLogger(__name__)

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


@command(["calendar", "cal"])
async def command_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await subcommand_help(update, context)
        return

    subcommands = {
        "help":   subcommand_help,
        "list":   subcommand_list,
        "info":   subcommand_info,
        "add":    subcommand_add,
        "edit":   subcommand_edit,
        "remove": subcommand_remove,
    }

    try:
        subidx = context.args[0]
        subcmd = subcommands[subidx]
        await subcmd(update, context)
    except KeyError:
        await subcommand_help(update, context)
        

def parse_datetime_args(args: list[str]) -> tuple[datetime, list[str]]:
    idx  = 0
    date_obj = None
    time_obj = None
    for date_fmt in [ "%d/%m/%Y", "%Y-%m-%d" ]:
        try:
            date_obj = datetime.strptime(args[idx], date_fmt).date()
            idx += 1
            break
        except ValueError:
            pass

    for time_fmt in [ "%H:%M:%S%z", "%H:%M:%S", "%H:%M%z", "%H:%M" ]:
        try:
            time_obj = datetime.strptime(args[idx], time_fmt).time()
            print(time_fmt, time_obj, time_obj.tzinfo)
            idx += 1
            break
        except ValueError:
            pass

    if not date_obj and not time_obj:
        raise ValueError("We need either date or time")

    if not date_obj:
        date_obj = datetime.now().date()

    if not time_obj:
        time_obj = time()
    
    fulldate = datetime.combine(date_obj, time_obj)
    return fulldate, args[idx:]


async def subcommand_help(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message != None
    _ = await update.message.reply_text(text="/calendar <help|list|info|add|remove>")


async def subcommand_list(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message        != None
    assert update.effective_chat != None

    assert global_vars.store
    engine = global_vars.store.get_engine()

    msg = "```\nPróximos eventos:\nData - Código - Título\n"
    evt = "{date} - {id} - {title}\n"

    now      = datetime.now()
    one_week = now + timedelta(weeks=1)

    with Session(engine) as session:
        result: list[Event] = (session.query(Event)
            .where(Event.chat_id == update.effective_chat.id)
            .where(Occurrence.date >= now)
            .where(Occurrence.date <= one_week)
            .all()
        )
        for entry in result:
            occurrence = entry.occurrences[0]
            logger.info(occurrence)
            msg += evt.format(
                id    = entry.id,
                date  = occurrence.date.strftime("%d/%m/%Y %H:%M"),
                title = entry.title.replace("`", "\\`"),
            )
    msg += "```"
    logger.info(msg)
    _ = await update.message.reply_text(text=msg, parse_mode="MarkdownV2")


async def subcommand_info(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message != None
    _ = update.message.reply_text(text="List mine")


async def subcommand_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message
    assert update.effective_user
    assert update.effective_chat

    if not context.args:
        _ = await update.message.reply_text(text="/calendar add <date> <time> <title>")
        return

    start_date, rest = parse_datetime_args(context.args[1:])
    title = " ".join(rest)

    if not start_date or not rest:
        _ = await update.message.reply_text(text="/calendar add <date> <time> <title>")
        return

    assert global_vars.store
    engine = global_vars.store.get_engine()

    with Session(engine) as session:
        event = Event(
            chat_id     = update.effective_chat.id,
            user_id     = update.effective_user.id,
            title       = title,
            description = "",
            created_at  = datetime.now(),
            updated_at  = datetime.now(),
        )
        session.add(event)
        session.flush()

        occ = Occurrence(
            calendar_event_id = event.id,
            date              = start_date,
        )
        session.add(occ)
        session.commit()

        username = update.effective_user.name
        evt_txt  = f"{username} registrou o evento #{event.id}: {event.title}."
        _ = await update.message.reply_text(text=evt_txt)


async def subcommand_edit(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message != None
    _ = update.message.reply_text(text="Edit")


async def subcommand_remove(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    assert update.message != None
    _ = update.message.reply_text(text="Remove")
    
    
