def _as_dict(value) -> dict:
    """Данные из API не проверены: всё, что не dict, считаем пустым."""
    return value if isinstance(value, dict) else {}


class Message:

    def __init__(self, data, bot):
        data = _as_dict(data)
        self.data = data
        self.bot = bot

        body = _as_dict(data.get("body"))

        text = body.get("text")
        self.text = text if isinstance(text, str) else None
        self.mid = body.get("mid")

        sender = _as_dict(data.get("sender"))
        self.chat_id = sender.get("user_id")

        recipient = _as_dict(data.get("recipient"))
        self.dialog_chat_id = recipient.get("chat_id")

    async def answer(self, text: str, format=None, buttons=None):
        if not self.chat_id:
            return
        return await self.bot.send_message(chat_id=self.chat_id, text=text, format=format, buttons=buttons)

    async def send_document(self, file_path: str):
        if not self.chat_id:
            return
        await self.bot.send_document(chat_id=self.chat_id, file_path=file_path)

    async def send_documents(self, file_path: list[str]):
        if not self.chat_id:
            return
        await self.bot.send_documents(chat_id=self.chat_id, file_path=file_path)

    async def send_image(self, file_path: str, text: str = None):
        if not self.chat_id:
            return
        return await self.bot.send_image(chat_id=self.chat_id, file_path=file_path, text=text)

    async def send_images(self, file_paths: list[str], text: str = None):
        if not self.chat_id:
            return
        await self.bot.send_images(chat_id=self.chat_id, file_paths=file_paths, text=text)

    async def delete(self):
        if self.mid:
            try:
                await self.bot.delete_message(self.mid)
            except Exception:
                pass
