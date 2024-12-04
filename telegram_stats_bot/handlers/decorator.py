import datetime

import logging
from readline import append_history_file
from typing import Any, Optional, Union

from telegram import Update
from telegram._utils.defaultvalue import DEFAULT_TRUE
from telegram._utils.types import RT, SCT, DVType, JSONDict
from telegram.ext import Application, CommandHandler, MessageHandler
from telegram.ext._utils.types import CCT, HandlerCallback, JobCallback
from telegram.ext.filters import BaseFilter

logger = logging.getLogger(__name__)

application: Optional[Application[Any, Any, Any, Any, Any, Any]] = None

class command(object):
    # Decorator that registers the command in the application

    command:  SCT[str]
    filters:  Optional[BaseFilter]
    block:    DVType[bool]
    has_args: Optional[Union[bool, int]]

    def __init__(
        self,
        command:  SCT[str],
        filters:  Optional[BaseFilter]       = None,
        block:    DVType[bool]               = DEFAULT_TRUE,
        has_args: Optional[Union[bool, int]] = None,
    ):
        self.command  = command
        self.filters  = filters
        self.block    = block
        self.has_args = has_args

    def __call__(self, handler: HandlerCallback[Update, CCT, RT]) -> HandlerCallback[Update, CCT, RT]:
        assert application != None
        application.add_handler(CommandHandler(
            self.command,
            handler,
            self.filters,
            self.block,
            self.has_args
        ))
        return handler
        
        
class message(object):
    # Decorator that registers the command in the application

    filters: Optional[BaseFilter]
    block:   DVType[bool]

    def __init__(
        self,
        filters: Optional[BaseFilter] = None,
        block:   DVType[bool]         = DEFAULT_TRUE,
    ) -> None:
        self.filters  = filters
        self.block    = block

    def __call__(self, handler: HandlerCallback[Update, CCT, RT]) -> HandlerCallback[Update, CCT, RT]:
        assert application != None
        application.add_handler(MessageHandler(self.filters, handler, self.block))
        return handler
    
class run_repeating(object):

    interval:   Union[float, datetime.timedelta]
    first:      Optional[Union[float, datetime.timedelta, datetime.datetime, datetime.time]]
    last:       Optional[Union[float, datetime.timedelta, datetime.datetime, datetime.time]]
    data:       Optional[object]
    name:       Optional[str]
    chat_id:    Optional[int]
    user_id:    Optional[int]
    job_kwargs: Optional[JSONDict]

    def __init__(
        self,
        interval:   Union[float, datetime.timedelta],
        first:      Optional[Union[float, datetime.timedelta, datetime.datetime, datetime.time]] = None,
        last:       Optional[Union[float, datetime.timedelta, datetime.datetime, datetime.time]] = None,
        data:       Optional[object]   = None,
        name:       Optional[str]      = None,
        chat_id:    Optional[int]      = None,
        user_id:    Optional[int]      = None,
        job_kwargs: Optional[JSONDict] = None,
    ) -> None:
        self.interval   = interval
        self.first      = first
        self.last       = last
        self.data       = data
        self.name       = name
        self.chat_id    = chat_id
        self.user_id    = user_id
        self.job_kwargs = job_kwargs

    def __call__(self, callback: JobCallback[CCT]) -> JobCallback[CCT]:
        assert application != None
        assert application.job_queue != None

        _ = application.job_queue.run_repeating(
            callback,
            self.interval,
            self.first,
            self.last,
            self.data,
            self.name,
            self.chat_id,
            self.user_id,
            self.job_kwargs,
        )
        return callback

    
class run_once(object):

    when:       Union[float, datetime.timedelta, datetime.datetime, datetime.time]
    data:       Optional[object]
    name:       Optional[str]
    chat_id:    Optional[int]
    user_id:    Optional[int]
    job_kwargs: Optional[JSONDict]

    def __init__(
        self,
        when:       Union[float, datetime.timedelta, datetime.datetime, datetime.time],
        data:       Optional[object]    = None,
        name:       Optional[str]       = None,
        chat_id:    Optional[int]       = None,
        user_id:    Optional[int]       = None,
        job_kwargs: Optional[JSONDict]  = None,
    ) -> None:
        self.when       = when
        self.data       = data
        self.name       = name
        self.chat_id    = chat_id
        self.user_id    = user_id
        self.job_kwargs = job_kwargs

    def __call__(self, callback: JobCallback[CCT]) -> JobCallback[CCT]:
        assert application != None
        assert application.job_queue != None
        _ = application.job_queue.run_once(
            callback,
            self.when,
            self.data,
            self.name,
            self.chat_id,
            self.user_id,
            self.job_kwargs,
        )
        return callback
