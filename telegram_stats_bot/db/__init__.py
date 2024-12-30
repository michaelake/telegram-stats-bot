import importlib
import os

from .base import Base

def load_tables():
    allowed_prefixes = [ "tbl_" ]

    for module in os.listdir(os.path.dirname(__file__)):
        if module[:4] not in allowed_prefixes or module[-3:] != ".py":
            continue
        _ = importlib.import_module(__name__ + "." + module[:-3])

load_tables()
metadata = Base.metadata;
