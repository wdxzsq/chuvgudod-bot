import logging

from ..types.message import Message
from ..types.callback import Callback

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self, bot):
        self.bot = bot
        self.handlers = []
        self.callback_handlers = []

    def message(self, commands=None):
        def decorator(func):
            self.handlers.append({
                "func": func,
                "commands": commands,
            })
            return func
        return decorator

    def callback(self, payloads=None):
        def decorator(func):
            self.callback_handlers.append({
                "func": func,
                "payloads": payloads,
            })
            return func
        return decorator

    async def process_update(self, update):
        if not isinstance(update, dict):
            return
        update_type = update.get("update_type")

        if update_type == "message_callback":
            await self._process_callback(update)
        elif isinstance(update.get("message"), dict):
            await self._process_message(update["message"])

    # Сообщение/callback получает только первый подходящий обработчик — даже
    # если он упал. Иначе при ошибке событие «проваливалось» бы в следующий
    # обработчик (например, в общий @dp.message()).

    async def _process_message(self, raw_message: dict):
        message = Message(raw_message, self.bot)
        for handler in self.handlers:
            commands = handler["commands"]
            if commands:
                words = message.text.split() if message.text else []
                if not words:
                    continue
                if not any(words[0] == f"/{cmd}" for cmd in commands):
                    continue
            try:
                await handler["func"](message)
            except Exception:
                logger.exception("Ошибка обработчика сообщения")
            return

    async def _process_callback(self, update: dict):
        cb = Callback(update, self.bot)
        for handler in self.callback_handlers:
            payloads = handler["payloads"]
            if payloads and cb.payload not in payloads:
                continue
            try:
                await handler["func"](cb)
            except Exception:
                logger.exception("Ошибка обработчика колбэка")
            return
