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

import logging
from sre_compile import dis
import sys
from textwrap import dedent
from typing import IO, Any, Callable, Optional, Sequence, Text, NoReturn, TypedDict, Union
from threading import Lock
from io import BytesIO
import argparse
import inspect
import re
from datetime import timedelta, datetime
from matplotlib.axes import Axes
from pandas._libs.properties import AxisProperty
from pandas.core.api import DataFrame
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.ext import to_tsquery
from sqlalchemy.sql.functions import count, current_timestamp, user
from sqlalchemy_utils.aggregates import sqlalchemy
from typing_extensions import override, reveal_type

import pandas as pd
import seaborn as sns
import numpy as np
from matplotlib.figure import Figure
from matplotlib.dates import date2num
from sqlalchemy.engine import Engine, Row
from sqlalchemy import desc, select, func, text, update

from telegram_stats_bot.db.tbl_messages import Message
from telegram_stats_bot.db.tbl_user_names import UserName

from .utils import escape_markdown, TsStat, random_quote
from . import __version__

sns.set_context('paper')
sns.set_style('whitegrid')
sns.set_palette("Set2")

logging.getLogger('matplotlib').setLevel(logging.WARNING)  # Mute matplotlib debug messages
logger = logging.getLogger()


def output_fig(fig: Figure) -> BytesIO:
    bio = BytesIO()
    bio.name = 'plot.png'
    fig.savefig(bio, bbox_inches='tight', dpi=200, format='png') # pyright: ignore[reportUnknownMemberType]
    _ = bio.seek(0)
    return bio


class HelpException(Exception):
    def __init__(self, msg: Optional[str] = None):
        self.msg = msg
        super().__init__()


class InternalParser(argparse.ArgumentParser):
    @override
    def error(self, message: Text) -> NoReturn:
        try:
            raise  # Reraises mostly ArgumentError for bad arg
        except RuntimeError:
            raise HelpException(message)

    @override
    def print_help(self, file: Optional[IO[str]] = None) -> None:
        raise HelpException(self.format_help())

    @override
    def _print_message(self, message: str, file: Optional[IO[str]] = None) -> None:
        raise HelpException(message)

    @override
    def exit(self, status: Optional[int] = None, message: Optional[str] = None) -> NoReturn:
        if message:
            print(message)
        sys.exit(status)


StatsRunnerResult = tuple[Optional[str], Optional[bool], Optional[BytesIO]]


class StatsRunner(object):
    allowed_methods = {
        "counts":  "get_chat_counts",
        "ecdf":    "get_chat_ecdf",
        "hours":   "get_counts_by_hour",
        "days":    "get_counts_by_day",
        "week":    "get_week_by_hourday",
        "history": "get_message_history",
        "titles":  "get_title_history",
        "user":    "get_user_summary",
        "corr":    "get_user_correlation",
        "delta":   "get_message_deltas",
        "types":   "get_type_stats",
        "words":   "get_word_stats",
        "random":  "get_random_message",
    }

    engine: Engine
    tz:     str
    users:  dict[int, tuple[str, str]]
    users_lock: Lock

    def __init__(self, engine: Engine, tz: str = 'Etc/UTC'):
        self.engine = engine
        self.tz     = tz
        self.users  = self.get_db_users()
        self.users_lock = Lock()

    def get_message_user_ids(self) -> list[int]:
        """Returns list of unique user ids from messages in database."""
        query = select(Message.from_user.distinct())
        with self.engine.connect() as con:
            result = con.execute(query)
        return [ user for user, in result.fetchall() if user is not None ] # pyright: ignore[reportAny]

    def get_db_users(self) -> dict[int, tuple[str, str]]:
        """Returns dictionary mapping user ids to usernames and full names."""

        subquery = select(
            UserName.user_id,
            UserName.username,
            UserName.display_name,
            func.row_number()
                .over(
                    partition_by = UserName.user_id,
                    order_by     = desc(UserName.date),
                )
                .label("rn"),
        ).alias("t")

        query = select(
            subquery.columns["user_id"],
            subquery.columns["username"],
            subquery.columns["display_name"],
        ).where(subquery.columns["rn"] == 1)

        with self.engine.connect() as con:
            result = con.execute(query)

        return { row[0]: (row[1], row[2]) for row in result }

    def update_user_ids(self, user_dict: dict[int, tuple[str, str]]):
        """
        Updates user names table with user_dict
        :param user_dict: mapping of user ids to (username, display name)
        """
        for uid in user_dict:
            username, display_name = user_dict[uid]

            # O BD não é normalizado. As queries originais estão preservadas nos comentários.
            # Sempre insere-se os dados do usuário de novo, e atualiza username se mudar.
            # Não usamos Session.add para representar essas operações pois a semântica
            # não é exatamente a mesma.
            
            # INSERT INTO user_names(user_id, date, username, display_name)
            # VALUES (:uid, current_timestamp, :username, :display_name);
            insert_query = insert(UserName).values(
                user_id      = uid,
                date         = current_timestamp(),
                username     = username,
                display_name = display_name,
            )

            # UPDATE user_names
            # SET username = :username
            # WHERE user_id = :uid AND username IS DISTINCT FROM :username;
            update_query = (update(UserName)
                .values(display_name = display_name)
                .where(
                    UserName.user_id == uid,
                    UserName.display_name.is_distinct_from(display_name),
                )
            )

            with self.engine.connect() as con:
                _ = con.execute(update_query)
                if display_name:
                    _ = con.execute(insert_query)

    def get_chat_counts(self,
        n:      int = 20,
        lquery: str = "",
        mtype:  str = "",
        start:  str = "",
        end:    str = "",
    ) -> StatsRunnerResult:
        """
        Get top chat users
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param mtype: Limit results to message type (text, sticker, photo, etc.)
        :param n: Number of users to show
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        if n <= 0:
            raise HelpException(f'n must be greater than 0, got: {n}')

        count_lbl = "msg_count"
        count_col = count().label(count_lbl)

        # SELECT "from_user", COUNT(*) as "count"
        # FROM "messages_utc"
        # {query_where}
        # GROUP BY "from_user"
        # ORDER BY "count" DESC;
        query = (select(Message.from_user, count_col)
            .group_by(Message.from_user)
            .order_by(count_col.desc())
        )

        if lquery:
            query = query.where(
                Message.text_index_col
                    .bool_op("@@")(to_tsquery(lquery)) # .match(lquery) uses plainto_tsquery instead
            )

        if mtype:
            valid_mtype = (
                'text',  'sticker', 'photo',    'animation',
                'video', 'voice',   'location', 'video_note',
                'audio', 'document', 'poll'
            )
            if mtype not in valid_mtype:
                raise HelpException(f'mtype {mtype} is invalid.')
            query = query.where(Message.type == mtype)

        if start:
            query = query.where(Message.date >= pd.to_datetime(start)) # pyright: ignore[reportUnknownMemberType]

        if end:
            query = query.where(Message.date < pd.to_datetime(end)) # pyright: ignore[reportUnknownMemberType] 

        with self.engine.connect() as con:
            df = pd.read_sql_query(query, con, index_col='from_user') # pyright: ignore[reportUnknownMemberType]

        if len(df) == 0:
            return "Sem mensagens correspondente", None, None

        # Filters out @usernames
        user_df = pd.Series(self.users, name="user") # pyright: ignore[reportUnknownVariableType]
        user_df = user_df.apply(lambda x: x[0])      # pyright: ignore[reportUnknownLambdaType, reportUnknownMemberType, reportUnknownVariableType]
        df = df.join(user_df)                        # pyright: ignore[reportUnknownMemberType]

        msg_count      = df[count_lbl]                     # pyright: ignore[reportUnknownVariableType]
        df['Percent']  = msg_count / msg_count.sum() * 100 # pyright: ignore[reportUnknownMemberType]
        df             = df[['user', count_lbl, 'Percent']]

        if mtype:
            df.columns = ['User', mtype, 'Percent']
        elif lquery:
            df.columns = ['User', 'lquery', 'Percent']
        else:
            df.columns = ['User', 'Total Messages', 'Percent']

        out_text  = "```\n"
        out_text += df.iloc[:n].to_string( # pyright: ignore[reportUnknownMemberType]
            index  = False,
            header = True,
            float_format = lambda x: f"{x:.1f}",
        )
        out_text += "```"
        return out_text, None, None

    def get_chat_ecdf(self,
        lquery: str  = "",
        mtype:  str  = "",
        start:  str  = "",
        end:    str  = "",
        log:    bool = False,
    ) -> StatsRunnerResult:
        """
        Get message counts by number of users as an ECDF plot.
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param mtype: Limit results to message type (text, sticker, photo, etc.)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param log: Plot with log scale.
        """
        count_lbl = "msg_count"
        count_col = count().label(count_lbl)

        # SELECT "from_user", COUNT(*) as "count"
        # FROM "messages_utc"
        # {query_where}
        # GROUP BY "from_user"
        # ORDER BY "count" DESC;
        query = (select(Message.from_user, count_col)
            .group_by(Message.from_user)
            .order_by(count_col.desc())
        )

        if lquery:
            query = query.where(
                Message.text_index_col
                    .bool_op("@@")(to_tsquery(lquery))
            )

        if mtype:
            valid_mtype = (
                'text',  'sticker', 'photo',    'animation',
                'video', 'voice',   'location', 'video_note',
                'audio', 'document', 'poll'
            )
            if mtype not in valid_mtype:
                raise HelpException(f'mtype {mtype} is invalid.')
            query = query.where(Message.type == mtype)

        if start:
            query = query.where(Message.date >= pd.to_datetime(start)) # pyright: ignore[reportUnknownMemberType]

        if end:
            query = query.where(Message.date < pd.to_datetime(end)) # pyright: ignore[reportUnknownMemberType] 

        with self.engine.connect() as con:
            df = pd.read_sql_query(query, con) # pyright: ignore[reportUnknownMemberType] 
        
        user_df = pd.Series(self.users, name="user") # pyright: ignore[reportUnknownVariableType]
        user_df = user_df.apply(lambda x: x[0])      # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType, reportUnknownVariableType]
        df = df.join(user_df, on='from_user')        # pyright: ignore[reportUnknownMemberType]
        
        if len(df) == 0:
            return "No matching messages", None, None

        fig = Figure(constrained_layout=True)
        subplot = fig.subplots()  # pyright: ignore[reportUnknownMemberType] 

        _ = sns.ecdfplot(df, y=count_lbl, stat='count', log_scale=log, ax=subplot)
        _ = subplot.set_xlabel('Usuários')  # pyright: ignore[reportUnknownMemberType] 
        _ = subplot.set_ylabel('Mensagens') # pyright: ignore[reportUnknownMemberType] 

        if lquery:
            _ = subplot.set_title(f"Mensagens por Usuário por {lquery}") # pyright: ignore[reportUnknownMemberType] 
        else:
            _ = subplot.set_title("Mensagens por Usuário") # pyright: ignore[reportUnknownMemberType] 

        sns.despine(fig=fig)

        bio = output_fig(fig)

        user_list = (', '
            .join([
                escape_markdown(df.at[i, 'user']) # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                    for i in range(0,5)
            ])
            .replace("@","")
        )
        lgd  = "Nesse gráfico podemos ver a distribuição acumulada de mensagens "
        lgd += "por usuários, ou seja, quantos usuários contribuíram para dada "
        lgd += "quantidade de mensagens.\n\n"
        lgd += f"Os cinco que mais contribuíram para o total de mensagens foram: {user_list}."
        return lgd, None, bio

    def get_counts_by_hour(self,
        user:   Optional[tuple[int, str]] = None,
        lquery: Optional[str] = None,
        start:  Optional[str] = None,
        end:    Optional[str] = None
    ) -> StatsRunnerResult:
        """
        Get plot of messages for hours of the day
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        query_conditions: list[str]   = []
        sql_dict: dict[str, Union[int, datetime]] = {}

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start) # pyright: ignore[reportUnknownMemberType]  
            query_conditions.append("date >= :start_dt")  

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)  # pyright: ignore[reportUnknownMemberType]   
            query_conditions.append("date < :end_dt")

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

        query_where = ""
        if query_conditions:
            query_where = f"WHERE {' AND '.join(query_conditions)}"

        query = f"""
                 SELECT date_trunc('hour', date) as day, count(*) as messages
                 FROM messages_utc
                 {query_where}
                 GROUP BY day
                 ORDER BY day
                 """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)  # pyright: ignore[reportUnknownMemberType]   

        if len(df) == 0:
            return "Sem mensagem correspondente", None, None

        df['day'] = pd.to_datetime(df.day) # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]   
        df['day'] = df.day.dt.tz_convert(self.tz)  # pyright: ignore[reportUnknownMemberType]
        df = df.set_index('day') # pyright: ignore[reportUnknownMemberType]
        df = df.asfreq('h', fill_value=0)  # Insert 0s for periods with no messages
        assert type(df) == pd.DataFrame
        assert type(df.index) == AxisProperty

        if (df.index.max() - df.index.min()) < pd.Timedelta('24 hours'):  # Deal with data covering < 24 hours
            df = df.reindex(pd.date_range(df.index.min(), periods=24, freq='h')) # pyright: ignore[reportUnknownMemberType]
            assert type(df.index) == AxisProperty

        df['hour'] = df.index.hour

        if user:
            # Aggregate over 1 week periods
            df = df.groupby('hour').resample('7D').sum().drop(columns='hour') # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] 
            assert type(df) == pd.DataFrame # pyright: ignore[reportUnknownArgumentType]  
            df['hour'] = df.index.get_level_values('hour') # pyright: ignore[reportUnknownMemberType] 

        fig = Figure(constrained_layout=True)
        subplot = fig.subplots() # pyright: ignore[reportUnknownMemberType] 

        CommonKeywordArgs = TypedDict("CommonKeywordArgs", {
            "x":       str,
            "y":       str,
            "hue":     str,
            "data":    DataFrame,
            "ax":      Axes,
            "legend":  bool,
            "palette": str,
        })

        plot_common_kwargs: CommonKeywordArgs = {
            "x":      "hour",
            "y":      "messages",
            "hue":    "hour",
            "data":    df,
            "ax":      subplot,
            "legend":  False,
            "palette": "flare"
        }
        
        _ = sns.stripplot(
            jitter = 0.4,
            size   = 2,
            alpha  = 0.5,
            zorder = 1,
            **plot_common_kwargs
        )

        _ = sns.boxplot(
            whis         = 1,
            showfliers   = False,
            whiskerprops = {"zorder": 10},
            boxprops     = {"zorder": 10},
            zorder       = 10,
            **plot_common_kwargs
        )
        
        top = df['messages'].quantile(0.999, interpolation='higher') # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] 
        assert type(top) == float                                    # pyright: ignore[reportUnknownArgumentType]
        _ = subplot.set_ylim(bottom=0, top=top)

        _ = subplot.axvspan(11.5, 23.5, zorder=0, color=(0, 0, 0, 0.05)) # pyright: ignore[reportUnknownMemberType]  
        _ = subplot.set_xlim(-1, 24)  # Set explicitly to plot properly even with missing data

        if lquery:
            _ = subplot.set_title(f"Mensagens por Hora para {lquery}") # pyright: ignore[reportUnknownMemberType]   
        elif user:
            _ = subplot.set_title(f"Mensagens por Hora para {user[1]}") # pyright: ignore[reportUnknownMemberType]   
        if user:
            _ = subplot.set_ylabel('Mensagens por Semana') # pyright: ignore[reportUnknownMemberType]   
        else:
            _ = subplot.set_ylabel('Mensagens por Dia') # pyright: ignore[reportUnknownMemberType]   
            _ = subplot.set_title("Mensagens por Hora") # pyright: ignore[reportUnknownMemberType]   

        sns.despine(fig=fig)
        bio = output_fig(fig)
        return None, None, bio

    def get_counts_by_day(self,
        user:   Optional[tuple[int, str]] = None,
        lquery: Optional[str] = None,
        start:  Optional[str] = None,
        end:    Optional[str] = None,
        plot:   Optional[str] = None,
    ) -> StatsRunnerResult:
        """
        Get plot of messages for days of the week
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param plot: Type of plot. ('box' or 'violin')
        """
        query_conditions: list[str] = []
        sql_dict: dict[str, Union[datetime, int]] = {}

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start) # pyright: ignore[reportUnknownMemberType]
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end) # pyright: ignore[reportUnknownMemberType]  
            query_conditions.append("date < :end_dt")

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

        query_where = ""
        if query_conditions:
            query_where = f"WHERE {' AND '.join(query_conditions)}"

        query = f"""
                     SELECT date_trunc('day', date)
                         as day, count(*) as messages
                     FROM messages_utc
                     {query_where}
                     GROUP BY day
                     ORDER BY day
                 """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict) # pyright: ignore[reportUnknownMemberType]    

        if len(df) == 0:
            return "Sem mensagem correspondente", None, None

        df['day'] = pd.to_datetime(df.day)        # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        df['day'] = df.day.dt.tz_convert(self.tz) # pyright: ignore[reportUnknownMemberType] 
        df = df.set_index('day')                  # pyright: ignore[reportUnknownMemberType] 
        df = df.asfreq('d', fill_value=0)  # Fill periods with no messages

        assert type(df) == pd.DataFrame
        assert type(df.index) == AxisProperty

        if (df.index.max() - df.index.min()) < pd.Timedelta('7 days'):  # Deal with data covering < 7 days
            df = df.reindex(pd.date_range(df.index.min(), periods=7, freq='d')) # pyright: ignore[reportUnknownMemberType]  
            assert type(df.index) == AxisProperty

        df['dow'] = df.index.weekday
        df['day_name'] = df.index.day_name()
        df = df.sort_values('dow')  # Make sure start is Monday # pyright: ignore[reportUnknownMemberType]   

        fig = Figure(constrained_layout=True)
        subplot = fig.subplots() # pyright: ignore[reportUnknownMemberType]   
        if plot == 'box':
            _ = sns.boxplot(
                x    = 'day_name',
                y    = 'messages',
                data = df,
                whis = 1,
                showfliers = False,
                ax    = subplot,
                color = sns.color_palette()[2],
            )
        elif plot == 'violin' or plot is None:
            _ = sns.violinplot(
                x    = 'day_name',
                y    = 'messages',
                data = df,
                cut  = 0,
                inner = "box",
                scale = 'width',
                ax   = subplot,
                color = sns.color_palette()[2]
            )
        else:
            raise HelpException("plot precisa ser 'box' ou 'violin'")

        _ = subplot.axvspan(4.5, 6.5, zorder=0, color=(0, .8, 0, 0.1)) # pyright: ignore[reportUnknownMemberType]   
        _ = subplot.set_xlabel('')                                     # pyright: ignore[reportUnknownMemberType]   
        _ = subplot.set_ylabel('Mensagens por dia')                    # pyright: ignore[reportUnknownMemberType]   
        _ = subplot.set_xlim(-0.5, 6.5)  # Need to set this explicitly to show full range of days with na data

        if lquery:
            _ = subplot.set_title(f"Mensagens por Dia da Semana para {lquery}")  # pyright: ignore[reportUnknownMemberType]   
        elif user:
            _ = subplot.set_title(f"Mensagens por Dia da Semana para {user[1]}") # pyright: ignore[reportUnknownMemberType]   
        else:
            _ = subplot.set_title("Mensagens por Dia da Semana")                 # pyright: ignore[reportUnknownMemberType]   

        sns.despine(fig=fig)

        bio = output_fig(fig)
        lgd = 'Esse gráfico mostra a quantidade de mensagens no grupo por dia da semana!'
        
        return lgd, None, bio

    def get_week_by_hourday(self,
        lquery: Optional[str]             = None,
        user:   Optional[tuple[int, str]] = None,
        start:  Optional[str]             = None,
        end:    Optional[str]             = None
    ) -> tuple[Optional[str], Optional[bool], Optional[BytesIO]]:
        """
        Get plot of messages over the week by day and hour.
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        query_conditions = []
        sql_dict = {}

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

        query_where = ""
        if query_conditions:
            query_where = f"WHERE {' AND '.join(query_conditions)}"

        query = f"""
                     SELECT date_trunc('hour', date)
                         as msg_time, count(*) as messages
                     FROM messages_utc
                     {query_where}
                     GROUP BY msg_time
                     ORDER BY msg_time
                 """
        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)

        if len(df) == 0:
            return "Sem mensagens.", None, None

        df['msg_time'] = pd.to_datetime(df.msg_time)
        df['msg_time'] = df.msg_time.dt.tz_convert(self.tz)
        df = df.set_index('msg_time')
        df = df.asfreq('h', fill_value=0)  # Fill periods with no messages
        df['dow'] = df.index.weekday
        df['hour'] = df.index.hour
        df['day_name'] = df.index.day_name()
        df_grouped = df[['messages', 'hour', 'day_name']].groupby(['hour', 'day_name']).sum().unstack()
        df_grouped = df_grouped.loc[:, 'messages']
        df_grouped = df_grouped.reindex(columns=['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                                                 'Friday', 'Saturday', 'Sunday'])

        row_sums = df_grouped.sum(axis=1)
        df_percent = df_grouped.div(row_sums, axis=0) * 100

        fig = Figure(constrained_layout=True)
        ax = fig.subplots()

        sns.heatmap(df_percent, yticklabels=df_grouped.index, xticklabels=['S', 'T', 'Q', 'Q', 'S', 'S', 'D'], linewidths=1,
                    square=True, fmt='.1f', vmin=0,
                    cbar_kws={"orientation": "vertical"}, cmap="BuPu", ax=ax)
        ax.tick_params(axis='y', rotation=0)
        ax.set(xlabel="", ylabel="")
        ax.xaxis.tick_top()
        
        lgd = None
        
        if lquery:
            ax.set_title(f"Porcentagem de mensagens por dia por hora para {lquery}")
            lgd = 'Nesse gráfico temos a relação das mensagens por hora por dia no período\! Quanto mais escuro for o quadrado, mais foi falado naquele dia da semana em relação a hora\.'
        elif user:
            ax.set_title(f"Porcentagem de mensagens por dia por hora para {user[1]}")
            lgd = f'Nesse gráfico temos a relação das mensagens por hora por dia pelo {user[1]}\! Quanto mais escuro for o quadrado, mais ele falou naquele dia da semana em relação a hora\.'
        else:
            ax.set_title("Porcentagem de mensagens por dia por hora")
            lgd = 'Nesse gráfico temos a relação das mensagens por hora por dia desde que comecei a contar\!Quanto mais escuro for o quadrado, mais foi falado naquele dia da semana em relação a hora\.'
            
        bio = output_fig(fig)
        return lgd, None, bio

    def get_message_history(self,
        user:     Optional[tuple[int, str]] = None,
        lquery:   Optional[str] = None,
        averages: Optional[int] = None,
        start:    Optional[str] = None,
        end:      Optional[str] = None,
    ) -> tuple[Optional[str], Optional[bool], Optional[BytesIO]]:
        """
        Make a plot of message history over time
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param averages: Moving average width (in days)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        query_conditions = []
        sql_dict = {}

        if averages:
            if averages < 0:
                raise HelpException("médias precisam ser>= 0")

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

        query_where = ""
        if query_conditions:
            query_where = f"WHERE {' AND '.join(query_conditions)}"

        query = f"""
                    SELECT date_trunc('day', date)
                        as day, count(*) as messages
                    FROM messages_utc
                    {query_where}
                    GROUP BY day
                    ORDER BY day
                 """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)

        if len(df) == 0:
            return "Sem mensagens correspondentes", None, None

        df['day'] = pd.to_datetime(df.day)
        df['day'] = df.day.dt.tz_convert(self.tz)
        df = df.set_index('day')
        df = df.resample('1D').sum()

        if averages is None:
            averages = len(df) // 20
            if averages <= 1:
                averages = 0
        if averages:
            df['msg_rolling'] = df['messages'].rolling(averages, center=True).mean()
            alpha = 0.5
        else:
            alpha = 1

        fig = Figure(constrained_layout=True)
        subplot = fig.subplots()
        df.plot(y='messages', alpha=alpha, legend=False, ax=subplot, color=sns.color_palette()[2])
        if averages:
            df.plot(y='msg_rolling', legend=False, ax=subplot)
        subplot.set_ylabel("Mensagens")
        subplot.set_xlabel("Data")
        if lquery:
            subplot.set_title(f"Histórico da busca: {lquery}")
        elif user:
            subplot.set_title(f"Histórico de mensagens do {user[1]}")
        else:
            subplot.set_title("Histórico de mensagens")
        sns.despine(fig=fig)
        fig.tight_layout()

        bio = output_fig(fig)

        return None, None, bio

    def get_title_history(self,
        start:    Optional[str] = None,
        end:      Optional[str] = None,
        duration: bool          = False,
    ) -> StatsRunnerResult:
        """
        Make a plot of group titles history over time
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param duration: If true, order by duration instead of time.
        """
        
        query_conditions = []
        sql_dict = {}

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        query_where = ""
        if query_conditions:
            query_where = f"AND {' AND '.join(query_conditions)}"

        query = f"""
                    SELECT date, new_chat_title
                    FROM messages_utc
                    WHERE type = 'new_chat_title' {query_where}
                    ORDER BY date;
                 """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)

        if len(df) == 0:
            return "No chat titles in range", None, None

        df['idx'] = np.arange(len(df))
        df['diff'] = -df['date'].diff(-1)
        df['end'] = df['date'] + df['diff']

        if end:
            last = pd.Timestamp(sql_dict['end_dt'], tz=self.tz).tz_convert('utc')
        else:
            last = pd.Timestamp(datetime.utcnow(), tz='utc')

        df_end = df['end']
        df_end.iloc[-1] = last
        df.loc[:, 'end'] = df_end
        df.loc[:, 'diff'].iloc[-1] = df.iloc[-1]['end'] - df.iloc[-1]['date']

        fig = Figure(constrained_layout=True, figsize=(12, 1+0.15 * len(df)))
        ax = fig.subplots()

        lgd = None
        
        if duration:
            df = df.sort_values('diff')
            df = df.reset_index(drop=True)
            df['idx'] = df.index

            ax.barh(df.idx, df['diff'].dt.days + df['diff'].dt.seconds / 86400, tick_label=df.new_chat_title, color=sns.color_palette()[2])

            ax.margins(0.2)
            ax.set_ylabel("")
            ax.set_xlabel("Duração (dias)")
            ax.set_ylim(-1, (df.idx.max() + 1))
            ax.set_title("Histórico de Nomes do Grupo")
            ax.grid(False, which='both', axis='x')
            sns.despine(fig=fig, left=True)
            
            lgd = "Nesse gráfico podemos ver quanto tempo os nomes do grupo ficaram ativos!"

        else:
            x = df.iloc[:-1].end
            y = df.iloc[:-1].idx + .5

            ax.scatter(x, y, zorder=4, color=sns.color_palette()[2])
            titles = list(zip(df.date.apply(date2num),
                              df.end.apply(date2num) - df.date.apply(date2num)))

            point_dict = {}
            for n, i in enumerate(titles):
                ax.broken_barh([i], (n, 1))
                ax.annotate(n, xy=(i[0] + i[1], n), xycoords='data',
                            xytext=(12, 5), textcoords='offset points',
                            horizontalalignment='left', verticalalignment='center', rotation=0)
                point_dict[n] = df.new_chat_title[n]
            
            lgd_list = [f'{n}: {title}' for n, title in point_dict.items()]
            lgd = "Com esse gráfico podemos ver quando o grupo mudou de nome!\n\nAbaixo podemos ver os 10 últimos nomes:\n\n" + "\n".join(lgd_list[-10:])
            ax.set_ylim(-1, (df.idx.max() + 1))
            ax.set_xlim(titles[0][0] - 1, None)

            ax.margins(0.2)
            ax.set_ylabel("")
            ax.set_xlabel("")
            ax.set_title("Histórico de Nomes do Grupo")
            ax.grid(False, which='both', axis='y')
            ax.tick_params(axis='y', which='both', labelleft=False, left=False)
            sns.despine(fig=fig, left=True)

        bio = output_fig(fig)

        return lgd, False, bio

    def get_user_summary(self, user: tuple[int, str]) -> StatsRunnerResult:
        """
        Get summary of a user.
        """
        sql_dict = { 'user': user[0] }

        count_query = """
                         SELECT COUNT(*)
                         FROM "messages_utc"
                         WHERE from_user = :user;
                      """

        days_query = """
                        SELECT EXTRACT(epoch FROM(NOW() - MIN(date))) / 86400 as "days"
                        FROM "messages_utc"
                        WHERE from_user = :user;
                     """

        event_query = """
                         SELECT date, event
                         FROM user_events
                         WHERE user_id = :user
                         ORDER BY "date";
                      """

        username_query = """
                             SELECT COUNT(*)
                             FROM "user_names"
                             WHERE user_id = :user;
                         """
                         
        type_query = """
                     SELECT type, count(*) as count
                     FROM messages_utc
                     WHERE type NOT IN ('new_chat_members', 'left_chat_member', 'new_chat_photo',
                                       'new_chat_title', 'migrate_from_group', 'pinned_message')
                                       AND from_user = :user
                     GROUP BY type
                     ORDER BY count DESC;
                     """
        

        with self.engine.connect() as con:
            result = con.execute(text(count_query), sql_dict)
            msg_count: int = result.fetchall()[0][0]
            result = con.execute(text(days_query), sql_dict)
            days: float = result.fetchall()[0][0]
            result = con.execute(text(event_query), sql_dict)
            events: list = result.fetchall()
            result = con.execute(text(username_query), sql_dict)
            name_count: int = result.fetchall()[0][0]
            
            df_u = pd.read_sql_query(text(type_query), con, params=sql_dict)
            df_u['User Percent'] = df_u['count'] / df_u['count'].sum() * 100
            df_u.columns = ['type', 'Count', 'Percent']
            
 
        event_text = '\n'.join([f'{event.event} on {pd.to_datetime(event.date).tz_convert(self.tz)}'
                                for event in events])

        # Add separator line
        if event_text:
            event_text = '\n' + event_text

        text_count = df_u.loc[df_u['type'] == 'text', 'Count'].iloc[0]
        sticker_count = df_u.loc[df_u['type'] == 'sticker', 'Count'].iloc[0]
        photo_count = df_u.loc[df_u['type'] == 'photo', 'Count'].iloc[0]
        gif_count = df_u.loc[df_u['type'] == 'animation', 'Count'].iloc[0]
        
        try:
            out_text = f"Mensagens enviadas: {msg_count}\n" \
                       f"Média de mensagens por dia: {msg_count / days:.2f}\n" \
                       f"Primeira mensagem foi a {days:.2f} atrás\n" \
                       f"Usernames registrados: {name_count}\n" \
                       f"Média do tempo de uso por username: {days / name_count:.2f} dias\n" \
                       f"Número de textos enviados: {text_count}\n" \
                       f"Número de stickers enviados: {sticker_count}\n" \
                       f"Número de fotos enviadas: {photo_count}\n" \
                       f"Número de gifs enviados: {gif_count}\n" + event_text
        except TypeError:
            return 'No data for user', None, None


        return f"Dados do usuário {user[1].lstrip('@')}: ```\n{out_text}\n```", None, None

    def get_user_correlation(self,
        user:   tuple[int, str],
        start:  Optional[str] = None,
        end:    Optional[str] = None,
        agg:    bool          = True,
        c_type: Optional[str] = None,
        n:      int           = 5,
        thresh: float         = 0.05,
    ) -> StatsRunnerResult:
        """
        Return correlations between you and other users.
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param agg: If True, calculate correlation over messages aggregated by hours of the week
        :param c_type: Correlation type to use. Either 'pearson' or 'spearman'
        :param n: Show n highest and lowest correlation scores
        :param thresh: Fraction of time bins that have data for both users to be considered valid (0-1)
        """
        query_conditions = []
        sql_dict = {}

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        query_where = ""
        if query_conditions:
            query_where = f"WHERE {' AND '.join(query_conditions)}"

        if n <= 0:
            raise HelpException(f'n must be greater than 0, got: {n}.')

        if not c_type:
            c_type = 'pearson'
        elif c_type not in ['pearson', 'spearman']:
            raise HelpException("correlação precisa ser 'pearson' ou 'spearman'.")

        if not 0 <= thresh <= 1:
            raise HelpException(f'n precisa estar entre [0, 1], solicitado: {n}')

        query = f"""
                SELECT msg_time, extract(ISODOW FROM msg_time) as dow, extract(HOUR FROM msg_time) as hour,
                       "user", messages
                FROM (
                         SELECT date_trunc('hour', date)
                                         as msg_time,
                                count(*) as messages, from_user as "user"
                         FROM messages_utc
                         {query_where}
                         GROUP BY msg_time, from_user
                         ORDER BY msg_time
                     ) t
                ORDER BY dow, hour;
                """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)

        if len(df) == 0:
            return 'Sem mensagens na pesquisa.', None, None

        df['msg_time'] = pd.to_datetime(df.msg_time)
        df['msg_time'] = df.msg_time.dt.tz_convert(self.tz)

        # Prune irrelevant messages (not sure if this actually improves performance)
        user_first_date = df.loc[df.user == user[0], 'msg_time'].iloc[0]
        df = df.loc[df.msg_time >= user_first_date]

        df = df.set_index('msg_time')

        user_dict = {'user': {user_id: value[0] for user_id, value in self.users.items()}}
        df = df.loc[df.user.isin(list(user_dict['user'].keys()))]  # Filter out users with no names
        df = df.replace(user_dict)  # Replace user ids with names
        #df['user'] = df['user'].str.replace(r'[^\x00-\x7F]', "", regex=True)

        if agg:
            df = df.pivot_table(index=['dow', 'hour'], columns='user', values='messages', aggfunc='sum')
            corrs = []
            for other_user in df.columns.values:
                if df[user[1]].sum() / df[other_user].sum() > thresh:
                    me_notna = df[user[1]].notna()
                    other_notna = df[other_user].notna()
                    idx = me_notna | other_notna
                    corrs.append(df.loc[idx, user[1]].fillna(0).corr(df.loc[idx, other_user].fillna(0)))
                else:
                    corrs.append(pd.NA)

            me = pd.Series(corrs, index=df.columns.values).sort_values(ascending=False).iloc[1:].dropna()
        else:
            df = df.pivot(columns='user', values='messages')

            if thresh == 0:
                df_corr = df.corr(method=c_type)
            else:
                df_corr = df.corr(method=c_type, min_periods=int(thresh * len(df)))
            me = df_corr[user[1]].sort_values(ascending=False).iloc[1:].dropna()

        if len(me) < 1:
            return "`Desculpa, poucos dados, tente com -aggtimes, diminuir -thresh, ou usando um período de tempo maior.`", None

        if n > len(me) // 2:
            n = int(len(me) // 2)

        out_text = me.to_string(header=False, float_format=lambda x: f"{x:.3f}")
        split = out_text.splitlines()
        out_text = "\n".join(['Maior correlação:'] + split[:n] + ['\nMenor correlação:'] + split[-n:])

        return f"Correlação do {escape_markdown(user[1])} com outros usuários:\n```\n{out_text}\n```", None, None

    def get_message_deltas(self,
        user:   tuple[int, str],
        lquery: Optional[str] = None,
        start:  Optional[str] = None,
        end:    Optional[str] = None,
        n:      int           = 10,
        thresh: int           = 500,
        **kwargs
    ) -> StatsRunnerResult:
        """
        Return the median difference in message time between you and other users.
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param n: Show n highest and lowest correlation scores
        :param thresh: Only consider users with at least this many message group pairs with you
        """
        query_conditions = []
        sql_dict = {}

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        query_where = ""
        if query_conditions:
            query_where = f"AND {' AND '.join(query_conditions)}"

        if n <= 0:
            raise HelpException(f'n precisa ser maior que 0')

        if thresh < 0:
            raise HelpException(f'n não pode ser negativo')

        def fetch_mean_delta(me: int, other: int, where: str, sql_dict: dict) -> tuple[timedelta, int]:
            query = f"""
                    select percentile_cont(0.5) within group (order by t_delta), count(t_delta)
                    from(
                        select start - lag("end", 1) over (order by start) as t_delta
                        from (
                                 select min(date) as start, max(date) as "end"
                                 from (select date, from_user,
                                              (dense_rank() over (order by date) -
                                               dense_rank() over (partition by from_user order by date)
                                                  ) as grp
                                       from messages_utc
                                       where from_user in (:me, :other) {where}
                                       order by date
                                      ) t
                                 group by from_user, grp
                                 order by start
                        ) t1
                    ) t2;
                    """

            sql_dict['me'] = me
            sql_dict['other'] = other

            with self.engine.connect() as con:
                result = con.execute(text(query), sql_dict)
            output: tuple[timedelta, int] = result.fetchall()[0]

            return output

        results = {other: fetch_mean_delta(user[0], other, query_where, sql_dict) for other in self.users
                   if user[0] != other}

        user_deltas = {self.users[other][0]: pd.to_timedelta(result[0]) for other, result in results.items()
                       if result[1] > thresh}

        me = pd.Series(user_deltas).sort_values()
        me = me.apply(lambda x: x.round('1s'))

        if len(me) < 1:
            return "\n```\nDesculpa, poucos dados, tente com -aggtimes, diminuir -thresh, ou usando um período de tempo maior.\n```", None, None

        out_text = me.iloc[:n].to_string(header=False, index=True)

        return f"**Tempo médio entre as mensagens de {escape_markdown(user[1])} e:**\n```\n{out_text}\n```", None, None

    def get_type_stats(self,
        start: Optional[str] = None,
        end:   Optional[str] = None,
        autouser             = None,
        **kwargs
    ) -> StatsRunnerResult:
        """
        Print table of message statistics by type.
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        user: tuple[int, str] = kwargs['user']
        query_conditions = []
        sql_dict = {}

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        query_where = ""
        if query_conditions:
            query_where = f" AND {' AND '.join(query_conditions)}"

        query = f"""
                    SELECT type, count(*) as count
                    FROM messages_utc
                    WHERE type NOT IN ('new_chat_members', 'left_chat_member', 'new_chat_photo',
                                       'new_chat_title', 'migrate_from_group', 'pinned_message')
                          {query_where}
                    GROUP BY type
                    ORDER BY count DESC;
                 """

        with self.engine.connect() as con:
            df = pd.read_sql_query(text(query), con, params=sql_dict)

        if len(df) == 0:
            return 'Sem mensagens no período', None, None

        df['Group Percent'] = df['count'] / df['count'].sum() * 100
        df.columns = ['type', 'Group Count', 'Group Percent']

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

            query = f"""
                        SELECT type, count(*) as user_count
                        FROM messages_utc
                        WHERE type NOT IN ('new_chat_members', 'left_chat_member', 'new_chat_photo',
                                           'new_chat_title', 'migrate_from_group', 'pinned_message')
                              AND {' AND '.join(query_conditions)}
                        GROUP BY type
                        ORDER BY user_count DESC;
                     """
            with self.engine.connect() as con:
                df_u = pd.read_sql_query(text(query), con, params=sql_dict)
            df_u['User Percent'] = df_u['user_count'] / df_u['user_count'].sum() * 100
            df_u.columns = ['type', 'User Count', 'User Percent']

            df = df.merge(df_u, on="type", how="outer")

        a = list(zip(df.columns.values, ["Total"] + df.iloc[:, 1:].sum().to_list()))
        df = pd.concat((df, pd.DataFrame({key: [value] for key, value in a})), ignore_index=True)

        df['Group Count'] = df['Group Count'].astype('Int64')
        try:
            df['User Count'] = df['User Count'].astype('Int64')
        except KeyError:
            pass

        out_text = df.to_string(index=False, header=True, float_format=lambda x: f"{x:.1f}")

        if user:
            return f"**Mensagens por tipo - {escape_markdown(user[1])} vs grupo:**\n```\n{out_text}\n```", None, None
        else:
            return f"**Mensagens por tipo:**\n```\n{out_text}\n```", None, None

    def get_word_stats(self,
        n:     int = 4,
        limit: int = 20,
        start: Optional[str] = None,
        end:   Optional[str] = None,
        user:  Optional[tuple[int, str]] = None,
    ) -> tuple[str, None, None]:
        """
        Print table of lexeme statistics.
        :param n: Only consider lexemes with length of at least n
        :param limit: Number of top lexemes to return
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """

        tsquery = select(Message.text_index_col)

        if user:
            tsquery = tsquery.where(Message.from_user == user[0])

        start = "2024-01-01"
        if start:
            tsquery = tsquery.where(Message.date >= pd.to_datetime(start)) # pyright: ignore[reportUnknownMemberType]

        if end:
            tsquery = tsquery.where(Message.date < pd.to_datetime(end))    # pyright: ignore[reportUnknownMemberType]

        tsquery = tsquery.scalar_subquery()
        tsstat  = TsStat(tsquery)

        stmt = (
            select(tsstat.word, tsstat.ndoc, tsstat.nentry)
                .select_from(tsstat)
        )

        if n:
            stmt = stmt.where(func.length(tsstat.word) >= n)

        stmt = stmt.order_by(
            tsstat.nentry.desc(),
            tsstat.ndoc.desc(),
            tsstat.word,
        )

        if limit:
            stmt = stmt.limit(limit)

        with self.engine.connect() as con:
            df = pd.read_sql_query(stmt, con) # pyright: ignore[reportUnknownMemberType]

        if len(df) == 0:
            return 'No messages in range', None, None

        df.columns = ['Lexeme', 'Messages', 'Uses']

        out_text = df.to_string( # pyright: ignore[reportUnknownMemberType]
            index  = False,
            header = True,
            float_format = lambda x: f"{x:.1f}"
        )

        if user:
            return f"**Most frequently used lexemes, {escape_markdown(user[1].lstrip('@'))}\n```\n{out_text}\n```", None, None
        else:
            return f"**Most frequently used lexemes, all users:**\n```\n{out_text}\n```", None, None

    def get_random_message(self,
        lquery: Optional[str] = None,
        start:  Optional[str] = None,
        end:    Optional[str] = None,
        user:   Optional[tuple[int, str]] = None,
    ) -> StatsRunnerResult:
        """
        Display a random message.
        :param lquery: Limit results to lexical query (&, |, !, <n>)
        :param start: Start timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        :param end: End timestamp (e.g. 2019, 2019-01, 2019-01-01, "2019-01-01 14:21")
        """
        query_conditions = []
        sql_dict = {}

        if lquery:
            query_conditions.append(f"text_index_col @@ to_tsquery( {random_quote(lquery)} )")

        if user:
            sql_dict['user'] = user[0]
            query_conditions.append("from_user = :user")

        if start:
            sql_dict['start_dt'] = pd.to_datetime(start)
            query_conditions.append("date >= :start_dt")

        if end:
            sql_dict['end_dt'] = pd.to_datetime(end)
            query_conditions.append("date < :end_dt")

        query_where = ""
        if query_conditions:
            query_where = f"AND {' AND '.join(query_conditions)}"

        query = f"""
                    SELECT date, from_user, text
                    FROM messages_utc
                    WHERE type = 'text'
                    {query_where}
                    ORDER BY RANDOM()
                    LIMIT 1;
                """

        with self.engine.connect() as con:
            result = con.execute(text(query), sql_dict)
        try:
            date, from_user, out_text = result.fetchall()[0]
        except IndexError:
            return "Nenhuma mensagem correspondente", None, None

        return (
              f"*No dia {escape_markdown(date.strftime('%d/%m/%Y'))}, "
            + f"{escape_markdown(self.users[from_user][0]).lstrip('@')}"
            + f" nos iluminou com isso:*\n"
            + f"{escape_markdown(out_text)}\n"
        ), None, None


def get_parser(runner: StatsRunner) -> InternalParser:
    parser = InternalParser(prog="/stats")
    parser.set_defaults(func=runner.get_chat_counts)
    subparsers = parser.add_subparsers(title="Statistics:")

    assert __version__
    _ = parser.add_argument('-v', '--version', action='version', version=__version__)

    for name, func in runner.allowed_methods.items():
        try:
            parser_attr: Callable[..., Any] = getattr(runner, func)
        except AttributeError:
            logger.error(f"Unknown function {func}")
            continue # Invalid function, skip

        parser_doc = inspect.getdoc(parser_attr)
        if parser_doc:
            doc = parser_doc.splitlines()
        else:
            doc = [ "" ]


        subparser = subparsers.add_parser(name, help=doc[0])
        subparser.set_defaults(func=parser_attr)
        f_args = inspect.signature(parser_attr).parameters

        for _, arg in f_args.items():
            arg: inspect.Parameter
            if arg.name == 'self':
                continue
            if arg.name == 'user':
                group = subparser.add_mutually_exclusive_group()
                _ = group.add_argument('-me', action='store_true', help='calculate stats for yourself')
                _ = group.add_argument('-user', type=int, help=argparse.SUPPRESS)
            elif arg.name == 'autouser':
                subparser.set_defaults(me=True)
                _ = subparser.add_argument('-user', type=int, help=argparse.SUPPRESS)
            elif arg.name == 'kwargs':
                pass
            else:
                arg_doc = None
                if doc:
                    for line in doc:
                        match = re.match(rf"^:param {arg.name}: (.*)", line)
                        if match:
                            arg_doc = match.group(1)

                if isinstance(arg.annotation, bool): # pyright: ignore[reportAny]
                    _ = subparser.add_argument(f"-{arg.name}".replace('_', '-'),
                        action = 'store_true',
                        help   = arg_doc,
                    )
                else:
                    _ = subparser.add_argument(f"-{arg.name}".replace('_', '-'),
                        type = arg.annotation, # pyright: ignore[reportAny] 
                        help = arg_doc,
                        default = arg.default, # pyright: ignore[reportAny] 
                    )

    return parser
