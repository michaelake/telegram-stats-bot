import importlib

import os
from typing import Any
from telegram.ext import Application
from . import decorator

__all__: list[str] = []

def load_commands(application: Application[Any, Any, Any, Any, Any, Any]):
    decorator.application = application

    for module in os.listdir(os.path.dirname(__file__)):
        if module[:4] != "cmd_" or module[-3:] != ".py":
            continue
        _ = importlib.import_module(__name__ + "." + module[:-3])
