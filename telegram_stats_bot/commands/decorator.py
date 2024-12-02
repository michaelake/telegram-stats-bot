from collections.abc import Coroutine
from typing import Any, Callable, Optional, Union

from telegram import Update
from telegram._utils.defaultvalue import DEFAULT_TRUE
from telegram._utils.types import SCT, DVType
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.ext.filters import BaseFilter

application: Optional[Application[Any, Any, Any, Any, Any, Any]] = None

CommandHandlerFn = Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, None]]

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

    def __call__(self, handler: CommandHandlerFn) -> CommandHandlerFn:
        assert application != None
        application.add_handler(CommandHandler(
            self.command,
            handler,
            self.filters,
            self.block,
            self.has_args
        ))
        return handler
        
        

    
