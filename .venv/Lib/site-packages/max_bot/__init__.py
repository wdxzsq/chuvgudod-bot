from importlib.metadata import version, PackageNotFoundError

try:
    # Имя дистрибутива (pymaxbot) не совпадает с именем пакета (max_bot).
    __version__ = version("pymaxbot")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .bot import Bot
from .dispatcher.dispatcher import Dispatcher
from .polling.polling import run_polling
from .types.callback import Callback

__all__ = [
    "__version__",
    "Bot",
    "Dispatcher",
    "run_polling",
    "Callback",
]