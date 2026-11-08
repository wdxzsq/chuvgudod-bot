import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


async def run_polling(bot, dispatcher, timeout=30, max_pending_per_user=10):
    """Long polling.

    События разных пользователей обрабатываются параллельно, события одного
    пользователя — строго по очереди. Если у пользователя в очереди уже
    max_pending_per_user событий, новые отбрасываются (защита от флуда).
    """

    marker = None
    pending = {}   # user_id -> сколько событий ждут или обрабатываются
    locks = {}     # user_id -> asyncio.Lock
    tasks = set()  # держим ссылки, чтобы задачи не собрал GC

    async def handle(key, update):
        lock = locks.setdefault(key, asyncio.Lock())
        try:
            async with lock:
                await dispatcher.process_update(update)
        except Exception:
            logger.exception("Ошибка обработки события")
        finally:
            pending[key] -= 1
            if pending[key] == 0:
                del pending[key]
                locks.pop(key, None)

    logger.info("Бот запущен, ожидаю сообщения")

    while True:
        try:
            params = {"timeout": timeout}
            if marker is not None:
                params["marker"] = marker

            response = await bot.client.request("GET", "/updates", params=params)
            if not isinstance(response, dict):
                continue

            if response.get("marker") is not None:
                marker = response["marker"]

            updates = response.get("updates")
            if not isinstance(updates, list):
                continue

            for update in updates:
                key = _user_key(update)
                if pending.get(key, 0) >= max_pending_per_user:
                    logger.warning("Слишком много событий от пользователя %s, событие пропущено", key)
                    continue
                pending[key] = pending.get(key, 0) + 1
                task = asyncio.create_task(handle(key, update))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

        except httpx.ReadTimeout:
            continue

        except Exception:
            logger.exception("Ошибка при получении обновлений")
            await asyncio.sleep(1)


def _user_key(update):
    """user_id отправителя события (None, если определить не удалось)."""
    if not isinstance(update, dict):
        return None
    callback = update.get("callback")
    if isinstance(callback, dict):
        user = callback.get("user")
    else:
        message = update.get("message")
        user = message.get("sender") if isinstance(message, dict) else None
    user_id = user.get("user_id") if isinstance(user, dict) else None
    return user_id if isinstance(user_id, (int, str)) else None
