# !/usr/bin/env python
#
# A logging and statistics bot for Telegram based on python-telegram-bot.
# Copyright (C) 2020
# Michael DM Dryden <mk.dryden@utoronto.ca>
#
# This file is part of telegram-stats-bot.
#
# telegram-stats-bot is free software: you can redistribute it and/or modify
# it under the terms of the GNU Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Public License
# along with this program. If not, see [http://www.gnu.org/licenses/].
import string
import secrets
import re
import datetime
import random

from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.base import ColumnCollection


md_match = re.compile(r"(\[[^][]*]\(http[^()]*\))|([_*[\]()~>#+-=|{}.!\\])")


def escape_markdown(string: str) -> str:
    def url_match(match: re.Match):
        if match.group(1):
            return f'{match.group(1)}'
        return f'\\{match.group(2)}'

    return re.sub(md_match, url_match, string)


# Modified from https://stackoverflow.com/a/49726653/3946475
class TsStat(FunctionElement):
    name = "ts_stat"

    @property
    def columns(self):
        word = Column('word', Text)
        ndoc = Column('ndoc', Integer)
        nentry = Column('nentry', Integer)
        return ColumnCollection(columns=((col.name, col) for col in (word, ndoc, nentry)))


@compiles(TsStat, 'postgresql')
def pg_ts_stat(element, compiler, **kw):
    kw.pop("asfrom", None)  # Ignore and set explicitly
    arg1, = element.clauses
    # arg1 is a FromGrouping, which would force parens around the SELECT.
    stmt = compiler.process(
        arg1.element, asfrom=False, literal_binds=True, **kw)

    return f"ts_stat({random_quote(stmt)})"


def random_quote(statement: str) -> str:
    quote_str = ''.join(secrets.choice(string.ascii_uppercase) for _ in range(8))  # Randomize dollar quotes
    return f"${quote_str}${statement}${quote_str}$"

def is_valid_date(date_string, date_format="%d/%m/%Y"):
    try:
        datetime.datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        return False

def roll_dice(dice):
    match dice:
        case 'd1':
            res = random.randrange(1,2)
        case 'd4':
            res = random.randrange(1,5)
        case 'd6':
            res = random.randrange(1,7)
        case 'd8':
            res = random.randrange(1,9)
        case 'd10':
            res = random.randrange(1,11)
        case 'd12':
            res = random.randrange(1,13)
        case 'd20':
            res = random.randrange(1,21)
        case 'd100':
            res = random.randrange(1,101)
        case 'd':
            res = random.Random()
        case _:
            res = 0
    return res