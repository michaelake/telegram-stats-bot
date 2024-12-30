import importlib

import os
from typing import Any
from telegram.ext import Application
from . import decorator

def load_handlers(application: Application[Any, Any, Any, Any, Any, Any]):
    decorator.application = application

    allowed_prefixes = [ "cmd_", "msg_", "job_" ]

    for module in os.listdir(os.path.dirname(__file__)):
        if module[:4] not in allowed_prefixes or module[-3:] != ".py":
            continue
        _ = importlib.import_module(__name__ + "." + module[:-3])
