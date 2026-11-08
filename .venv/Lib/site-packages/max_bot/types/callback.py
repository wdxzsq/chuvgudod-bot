from .message import Message, _as_dict


class Callback:

    def __init__(self, update: dict, bot):
        self.bot = bot
        update = _as_dict(update)
        cb_data = _as_dict(update.get("callback"))

        self.callback_id = cb_data.get("callback_id")
        payload = cb_data.get("payload")
        self.payload = payload if isinstance(payload, str) else None

        user = _as_dict(cb_data.get("user"))
        self.user_id = user.get("user_id")

        # message — сообщение с кнопками, sender в нём — бот, не пользователь.
        # Для ответа используем reply(), который шлёт напрямую на user_id из callback.
        raw_msg = update.get("message")
        self.message = Message(raw_msg, bot) if isinstance(raw_msg, dict) else None

    async def answer(self, notification: str = None):
        """Ответить на callback (снять состояние загрузки с кнопки)."""
        await self.bot.answer_callback(self.callback_id, notification=notification)

    async def reply(self, text: str, format=None, buttons=None):
        """Отправить сообщение пользователю, нажавшему кнопку."""
        return await self.bot.send_message(
            chat_id=self.user_id, text=text, format=format, buttons=buttons
        )