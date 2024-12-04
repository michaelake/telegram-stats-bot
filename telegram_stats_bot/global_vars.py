from os import PathLike
from typing import Any, Optional

from telegram_stats_bot.log_storage import JSONStore, PostgresStore


stats:      Optional[Any] = None
other_path: Optional[PathLike[str]] = None
chat_id:    int = 0
store:      Optional[PostgresStore] = None
bak_store:  Optional[JSONStore]     = None
