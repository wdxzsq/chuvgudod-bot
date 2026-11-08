import os
import httpx

from .client import MaxClient
from .types.message import Message


class Bot:

    def __init__(self, token: str):
        self.client = MaxClient(token)

    # ─── MESSAGES ─────────────────────────────────────────────────────────────

    async def send_message(self, chat_id: int, text: str, format=None, buttons=None):
        body = {"text": text}
        if format:
            body["format"] = format

        if buttons:
            body["attachments"] = [_build_keyboard(buttons)]

        params = {"user_id": chat_id}
        data = await self.client.request("POST", "/messages", json=body, params=params)
        return Message(data["message"], self)

    async def delete_message(self, mid: str):
        params = {"message_id": mid}
        return await self.client.request("DELETE", "/messages", params=params)

    # ─── FILES ────────────────────────────────────────────────────────────────

    async def upload_file(self, file_path: str):
        r = await self.client.request("POST", "/uploads", type_param="file")
        upload_url = r.get("url")
        with open(file_path, "rb") as f:
            files = {"data": (os.path.basename(file_path), f)}
            return await self.client.request("POST", upload_url, files=files, base_url_blank=True)

    async def send_document(self, chat_id: int, file_path: str):
        upload = await self.upload_file(file_path)
        file_token = upload["token"]
        body = {
            "attachments": [{"type": "file", "payload": {"token": file_token}}]
        }
        data = await self.client.request("POST", "/messages", json=body, params={"user_id": chat_id})
        return Message(data["message"], self)

    async def send_documents(self, chat_id: int, file_path: list[str]):
        for path in file_path:
            upload = await self.upload_file(path)
            file_token = upload["token"]
            body = {
                "attachments": [{"type": "file", "payload": {"token": file_token}}]
            }
            await self.client.request("POST", "/messages", json=body, params={"user_id": chat_id})

    # ─── IMAGES ───────────────────────────────────────────────────────────────

    async def upload_image(self, file_path: str):
        r = await self.client.request("POST", "/uploads", type_param="image")
        upload_url = r.get("url")
        with open(file_path, "rb") as f:
            files = {"data": (os.path.basename(file_path), f)}
            result = await self.client.request("POST", upload_url, files=files, base_url_blank=True)
        # Ответ: {"photos": {"<id>": {"token": "..."}}}
        photos = result.get("photos", {})
        token = next(iter(photos.values()))["token"]
        return {"token": token}

    async def send_image(self, chat_id: int, file_path: str, text: str = None, format: str = None, buttons=None):
        upload = await self.upload_image(file_path)
        image_token = upload["token"]
        body = {"text": text} if text else {}
        if format:
            body["format"] = format
        body["attachments"] = [{"type": "image", "payload": {"token": image_token}}]
        if buttons:
            body["attachments"].append(_build_keyboard(buttons))
        data = await self.client.request("POST", "/messages", json=body, params={"user_id": chat_id})
        return Message(data["message"], self)

    async def send_images(self, chat_id: int, file_paths: list[str], text: str = None, format: str = None):
        tokens = []
        for path in file_paths:
            upload = await self.upload_image(path)
            tokens.append(upload["token"])
        body = {"text": text} if text else {}
        body["attachments"] = [{"type": "image", "payload": {"token": t}} for t in tokens]
        await self.client.request("POST", "/messages", json=body, params={"user_id": chat_id})

    # ─── CALLBACKS ────────────────────────────────────────────────────────────

    async def answer_callback(self, callback_id: str, notification: str = None):
        # API требует notification или message — передаём пробел если нечего показывать
        body = {"notification": notification or " "}
        await self.client.request("POST", "/answers", json=body, params={"callback_id": callback_id})


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _build_keyboard(buttons: list[list[dict]]) -> dict:
    rows = []
    for row in buttons:
        btn_row = []
        for btn in row:
            btn_row.append({
                "type": btn.get("type", "callback"),
                "text": btn["text"],
                "payload": btn.get("payload", ""),
            })
        rows.append(btn_row)
    return {
        "type": "inline_keyboard",
        "payload": {"buttons": rows}
    }