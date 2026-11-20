"""
Бот «Чувгуша» для MAX — вариант для хостинга (вебхуки, bothost.ru).

Логика та же, что в main.py; отличается только блок «Запуск» внизу:
вместо опроса сервера бот поднимает веб-сервер, а MAX сам присылает ему события.

Нужно на хостинге:
    requirements.txt:  maxapi[fastapi]>=1.2.0, uvicorn, python-dotenv
    переменные окружения (в панели бота):
        MAX_BOT_TOKEN   токен бота
        ADMIN_CHAT_ID   id чата модераторов
        WEBHOOK_SECRET  любая строка 5–256 символов (латиница, цифры, _ и -)
    включить «Использовать домен»; главный файл — main2.py
    адрес вебхука берётся из DOMAIN, который выдаёт хостинг
    (или задайте WEBHOOK_URL вручную: https://ваш-домен/webhook)

База хранится в /app/data (сохраняется между обновлениями).

Блоки помечены # region / # endregion — их можно сворачивать.
"""

# region Импорты
import asyncio
import inspect
import logging
import os
import re
import sqlite3
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from maxapi import Bot, Dispatcher, F
from maxapi.context import MemoryContext, State, StatesGroup
from maxapi.filters.command import Command, CommandStart
from maxapi.types import (
    BotAdded,
    BotCommand,
    BotStarted,
    CallbackButton,
    MessageCallback,
    MessageCreated,
    NewMessageLink,
    RequestContactButton,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
from maxapi.webhook.fastapi import FastAPIMaxWebhook

try:
    from maxapi.filters.middleware import BaseMiddleware
except ImportError:   # в другой версии библиотеки путь может отличаться
    BaseMiddleware = None

# Читаем переменные из файла .env рядом с ботом (pip install python-dotenv).
# Должно выполняться ДО чтения os.getenv ниже.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logging.warning("python-dotenv не установлен: .env не будет прочитан")
# endregion


# region Настройки
logging.basicConfig(level=logging.INFO)

# База лежит в /app/data — Bothost не стирает эту папку при обновлении кода.
# Если такой папки нет (запуск на компьютере), используется ./data.
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    DATA_DIR = Path("data")
    DATA_DIR.mkdir(exist_ok=True)
DB_PATH = os.getenv("DB_PATH") or str(DATA_DIR / "users.db")

# Вебхук
PORT = int(os.getenv("PORT", "8080"))                  # порт даёт хостинг
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")       # MAX шлёт его в заголовке X-Max-Bot-Api-Secret


def _resolve_webhook() -> tuple[str, str]:
    """Возвращает (полный адрес вебхука, путь на сервере) — они всегда совпадают.
    Если в WEBHOOK_URL указан только домен без пути, добавляется /webhook."""
    default_path = os.getenv("WEBHOOK_PATH", "/webhook")
    explicit = os.getenv("WEBHOOK_URL", "").strip()
    if explicit:
        parsed = urlparse(explicit)
        path = parsed.path if parsed.path not in ("", "/") else default_path
        return f"{parsed.scheme}://{parsed.netloc}{path}", path

    domain = os.getenv("DOMAIN", "").strip().rstrip("/")
    if not domain:
        return "", default_path
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain + default_path, default_path


WEBHOOK_URL, WEBHOOK_PATH = _resolve_webhook()
IMAGES_DIR = Path(__file__).resolve().parent / "images"   # картинки к ответам бота (см. images/README.txt)

# Чат модераторов. 0 = не настроен (задания сохраняются, но в чат не уходят)
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
# Необязательно: user_id тех, кому разрешено нажимать «зачесть/отклонить».
# Пусто = любой участник чата модераторов.
ADMIN_IDS: set[int] = set()

QUEUE_BATCH = 20            # сколько карточек показывает /queue за раз
ERROR_ALERT_INTERVAL = 30   # не чаще 1 уведомления об ошибке в чат админов раз в N секунд

# Ограничения на ввод (защита от огромных сообщений)
MAX_FIO_LEN = 100            # ФИО
MAX_LINK_LEN = 500           # ответ-ссылка / подпись к фото
MAX_OPEN_ANSWER_LEN = 100    # открытый вопрос «тремя словами»
MAX_ATTACHMENTS = 5          # файлов в одном ответе
MAX_QUESTION_LEN = 1000      # вопрос организаторам
QUESTIONS_PER_HOUR = 5       # не больше вопросов в час от одного участника
TAP_COOLDOWN = 1.2           # сек: повторное нажатие той же кнопки игнорируется
SLOW_HANDLER_SECONDS = 1.0   # дольше этого обработчик попадёт в лог как медленный
MAX_ANNOUNCE_LEN = 3500      # своя рассылка из чата админов
FALLBACK_COOLDOWN = 3        # сек: не чаще одного «возврата панели» на пользователя

# Рассылки по расписанию
MSK = timezone(timedelta(hours=3))   # Чебоксары: UTC+3, без перехода на летнее время
MAX_LATE_HOURS = 3           # если бот был выключен и опоздал больше чем на N часов — не слать

# Расписание «дня Х» (по Москве). Для проверки поставьте время на пару минут вперёд.
TASKS_CLOSE_AT = datetime(2026, 11, 28, 13, 5, tzinfo=MSK)    # после этого приём заданий закрыт
SHOP_OPEN_AT = datetime(2026, 11, 28, 13, 40, tzinfo=MSK)     # после этого открыта витрина
MAX_ORDERS = 2               # не более 2 позиций на участника

bot = Bot(token=os.getenv("MAX_BOT_TOKEN"))
dp = Dispatcher()
def balls(n: int) -> str:
    """«1 балл», «3 балла», «5 баллов» — число со словом в правильной форме."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        word = "баллов"
    elif n % 10 == 1:
        word = "балл"
    elif 2 <= n % 10 <= 4:
        word = "балла"
    else:
        word = "баллов"
    return f"{n} {word}"


# endregion


# region Тексты (сценарий)
TEXT_GREETING = "Привет, я бот Чувгуша!"

TEXT_ASK_FIO = (
    "Как тебя зовут? Укажи в ответном сообщении своё полное ФИО, "
    "чтобы я мог зарегистрировать тебя в системе участника."
)

TEXT_ASK_PHONE = (
    "Нажми кнопку ниже и поделись своим номером телефона, "
    "чтобы организаторы могли связаться с тобой при необходимости!"
)

# Эмодзи — заглушки вместо картинок из сценария, замените на свои
TEXT_MEDIABINGO = (
    "Медиабинго — твой интерактивный маршрут по созданию контента! "
    "Это не просто игра, а серия заданий, посвящённых фото, видео, постам, "
    "сторис и другим форматам. Всё, что поможет тебе по-новому взглянуть на "
    "деловую программу и наполнить соцсети ярким контентом!\n\n"
    "Как начисляются баллы:\n\n"
    "— 🟩 Клетки — простые задания, от 5 до 10 баллов.\n"
    "— 🟨 Клетки — средняя сложность, по 15 баллов.\n"
    "— 🟥 Клетки — самые прокачанные, по 30 баллов.\n\n"
    "Ты сам выбираешь, с чего начать — задания можно выполнять в любом порядке. "
    "После отправки ответа следи за статусом:\n"
    "— ⏳ задание отправлено на проверку.\n"
    "— ❌ условия не соблюдены, задание не зачтено.\n"
    "— ✅ всё супер, задание выполнено правильно.\n\n"
    "Создавай смело — впереди много классного!\n\n"
    "На заработанные баллы сможешь купить много разного мерча университета. "
    "Обмен откроется в {shop_time}, а посмотреть, что и сколько стоит, можно уже сейчас — "
    "на витрине."
)

TEXT_PROFILE = (
    "Твои баллы: {points}\n\n"
    "Здесь ты можешь отслеживать, сколько баллов уже заработал за активное "
    "участие в Дне открытых дверей. Накапливай баллы: публикуй контент с "
    "мероприятия, выполняй задания, проявляй активность — так ты сможешь "
    "обменять баллы на классные подарки прямо сегодня, 28 ноября!"
)

# --- Тексты, которых в сценарии нет (заглушки, поменяйте под себя) ---
TEXT_CONSENT = (
    "Для регистрации нужно согласие на обработку персональных данных. "
    "Нажми кнопку ниже, если согласен(на)."
)
TEXT_CONSENT_DONE = "✅ Согласие на обработку персональных данных получено."
TEXT_PANEL = "Выбери задание: первая цифра — уровень, вторая — номер задания."
TEXT_NEED_REGISTRATION = "Сначала нужно пройти регистрацию — нажми /start."
TEXT_RESTART_QUIZ = "Не получилось сохранить ответ. Открой задание заново."
BUTTON_CONSENT = "✅ Согласен(на)"
BUTTON_SHARE_PHONE = "📱 Поделиться номером"
BUTTON_PROFILE = "📊 Мои баллы"
BUTTON_BACK = "⬅️ Назад"
TEXT_BAD_PHOTO = "Пришли скриншот картинкой."
TEXT_NEED_FILES = "Нужно прислать скриншотов: {n}. Отправь их все одним сообщением."
TEXT_TOO_MANY_FILES = "Слишком много файлов — отправь не больше 5."
TEXT_BAD_OPEN = "Ответь коротко — тремя словами (не длиннее 100 символов)."
BUTTON_RULES = "📖 Правила"

# --- Рассылки по расписанию (тексты из сценария) ---
TEXT_BROADCAST_THANKS = (
    "Спасибо тебе, абитуриент! Твои ответы, идеи и контент были по-настоящему "
    "наполнены смыслом. Все задания закрыты, а значит — самое время подвести итоги! "
    "Проверь свои баллы и не забудь потратить их на классные подарки в обменнике — "
    "ведь твой труд точно заслуживает награды!\n\n"
    "Спасибо, что был частью этой истории. Продолжай создавать, вдохновлять и шуметь "
    "вместе с нами!"
)
TEXT_BROADCAST_EXCHANGE = (
    "Виртуальный обменник\n\n"
    "Наконец он наступил – тот момент, когда можно обменять свои баллы на "
    "эксклюзивные подарки! Выбери лот, забронируй в боте, подойди к администратору "
    "у стола с мерчом и забери подарок. Один участник может получить не более 2 позиций.\n\n"
    "Жми на кнопку ниже и начинай свой выгодный обмен прямо сейчас!"
)

# --- Витрина, закрытие заданий, поздравления (тексты-заглушки — поменяйте под себя) ---
TEXT_TASKS_CLOSED = "Приём ответов завершён."
BUTTON_SHOP = "🎁 Витрина"
BUTTON_ORDERS = "🧾 Мои покупки"
BUTTON_CONFIRM = "✅ Оплатить"
BUTTON_CANCEL = "↩️ Отмена"
TEXT_SHOP = (
    "🎁 Витрина\n\n"
    "Твои баллы: {balance}\n"
    "Выбрано позиций: {count} из {max}\n\n"
    "{lots}\n\n"
    "Выбери подарок:"
)
TEXT_SHOP_PREVIEW = (
    "🎁 Витрина\n\n"
    "Покупки откроются в {time}.\n\n"
    "Твои баллы: {balance}\n\n"
    "{lots}"
)
TEXT_SOLD_OUT = "Этот подарок закончился."
TEXT_CONFIRM = "Оплатить «{name}» — {price}?\n\nТвои баллы: {balance} → останется {left}."
TEXT_NO_POINTS = "Не хватает баллов: нужно {price}, у тебя {balance}."
TEXT_LIMIT = "Можно выбрать не более {max} позиций."
TEXT_ORDERED = "✅ Оплачено: «{name}» — {price}. Осталось баллов: {left}."
TEXT_ORDERS_EMPTY = "Пока ничего не куплено."
TEXT_ORDERS = (
    "🧾 Мои покупки\n{fio}\n\n{items}\n\n"
    "Оплачено: {total}.\n\n"
    "Покажи этот экран администратору у стола с мерчом."
)
TEXT_PANEL_SHOP = "Витрина открыта — выбирай подарки!"
TEXT_REJECTED = "❌ Задание {short}: условия не соблюдены, задание не зачтено."
TEXT_REJECTED_RETRY = "Попробуй ещё раз: открой задание и отправь новый ответ."
TEXT_RETRY = "Попробуй ещё раз — отправь новый ответ сообщением."
TEXT_CONGRATS = (
    "Поздравляю! Ты получил {points} за выполнение задания Медиабинго! "
    "Продолжай в том же духе — впереди ещё много интересных заданий и возможностей "
    "увеличить свой рейтинг!"
)
# После закрытия заданий «впереди ещё задания» уже неверно — короткий вариант
TEXT_CONGRATS_SHORT = "Поздравляю! Ты получил {points} за выполнение задания Медиабинго!"

# --- Вопросы организаторам (тексты-заглушки — поменяйте под себя) ---
BUTTON_ASK = "❓ Задать вопрос"
TEXT_ASK_QUESTION = "Напиши свой вопрос одним сообщением — организаторы ответят прямо здесь."
TEXT_QUESTION_SENT = "✅ Вопрос отправлен. Ответ придёт в этот чат."
TEXT_QUESTION_TOO_LONG = "Слишком длинный вопрос — сократи, пожалуйста, до 1000 символов."
TEXT_QUESTION_LIMIT = "Ты задал много вопросов подряд. Попробуй чуть позже."
TEXT_ANSWER = "💬 Ответ организаторов:\n\n{text}"
# endregion


# region Задания (сценарий)
TAIL = "После проверки модераторами, тебе будут начислены баллы."


def story_task(points: int, action: str, shots: int = 1) -> str:
    """Задание «выложи историю в Макс» — одни и те же короткие шаги для всех.
    Принимаются только скриншоты истории (ссылку на историю в Макс создать нельзя)."""
    if shots == 1:
        publish, send = "Выложи в историю в Макс с хештегом #мывчувгу.", "Пришли в бот скриншот истории."
    else:
        publish = "Выложи их в историю в Макс с хештегом #мывчувгу."
        send = f"Пришли в бот скриншоты истории ({shots} штуки) одним сообщением."
    return (
        f"{balls(points)} за задание.\n"
        f"1. {action}.\n"
        f"2. {publish}\n"
        f"3. {send}\n"
        f"4. {TAIL}"
    )


# short  — подпись на кнопке
# icon   — значок сложности (показывается, пока нет статуса)
# points — варианты начисления для модератора
TASKS = {
    "1-1": {
        "short": "1.1", "icon": "🟩", "points": [5, 10],
        "title": "1 уровень · Задание 1",
        "text": (
            "Ответь на вопрос и получи 5 баллов за правильный ответ. "
            "За два верных ответа получишь 10 баллов.\n\n"
            "5 баллов – Сколько факультетов в ЧувГУ?\n"
            "5 баллов – Кто является маскотом ЧувГУ?"
        ),
    },
    "1-2": {
        "short": "1.2", "icon": "🟩", "points": [10],
        "title": "1 уровень · Задание 2",
        "text": story_task(10, "Сними историю про атмосферу Дня открытых дверей"),
    },
    "1-3": {
        "short": "1.3", "icon": "🟩", "points": [5],   # модератор оценивает только вопрос про Ульянова
        "reference": "Илья Николаевич Ульянов — педагог-просветитель, директор народных "
                     "училищ Симбирской губернии (отец В. И. Ленина)",
        "title": "1 уровень · Задание 3",
        "text": (
            "Ответь на вопрос и получи 5 баллов за правильный ответ. "
            "За два верных ответа получишь 10 баллов.\n\n"
            "5 баллов – Кто такой Ульянов И.Н., имя которого носит университет? "
            "Напиши тремя словами.\n"
            "5 баллов – Где находится Приемная комиссия ЧувГУ? "
            "Кампус «Университетский» (ул. Университетская, 38) или "
            "кампус «Технопарк» (пр. Московский, 15)."
        ),
    },
    "2-1": {
        "short": "2.1", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 1",
        "text": story_task(15, "Сделай фото с синим предметом"),
    },
    "2-2": {
        "short": "2.2", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 2",
        "text": story_task(15, "Сделай селфи с Ульяном"),
    },
    "2-3": {
        "short": "2.3", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 3",
        "text": (
            f"{balls(15)} за задание.\n"
            "1. Подпишись на канал ЧувГУ в Макс: https://max.ru/chuvsu\n"
            "2. Пришли в бот скриншот подписки.\n"
            f"3. {TAIL}"
        ),
    },
    "3-1": {
        "short": "3.1", "icon": "🟥", "points": [30],
        "min_files": 3,   # нужно три скриншота (по одному с каждой площадки), одним сообщением
        "title": "3 уровень · Задание 1",
        "text": story_task(30, "Сделай фото на площадках 3 факультетов", shots=3),
    },
    "3-2": {
        "short": "3.2", "icon": "🟥", "points": [30],
        "title": "3 уровень · Задание 2",
        "text": story_task(
            30, "Посети мероприятие в концертном зале Дворца ЧувГУ и расскажи в истории, "
                "почему важно правильно выбирать вуз"
        ),
    },
    "extra": {
        "short": "Доп.", "icon": "🟩", "points": [10],
        "title": "Дополнительные баллы",
        "text": story_task(10, "Сделай фото на площадке Военного учебного центра"),
    },
}

# Краткая суть задания — показывается модераторам в карточке
TASK_META = {
    "1-2": "История о Дне открытых дверей, #мывчувгу (скриншот)",
    "1-3": "Кто такой Ульянов И.Н. (ответ тремя словами)",
    "2-1": "История: фото с синим предметом, #мывчувгу (скриншот)",
    "2-2": "История: селфи с Ульяном, #мывчувгу (скриншот)",
    "2-3": "Подписка на канал ЧувГУ в Макс (скриншот)",
    "3-1": "История: фото на площадках 3 факультетов, #мывчувгу (3 скриншота)",
    "3-2": "История о мероприятии в концертном зале Дворца ЧувГУ, #мывчувгу (скриншот)",
    "extra": "История: фото на площадке Военного учебного центра, #мывчувгу (скриншот)",
}
for _task_id, _brief in TASK_META.items():
    TASKS[_task_id]["brief"] = _brief

# Викторины. Вопросы с кнопками проверяются автоматически, на модерацию не уходят.
#   points  — баллы за один верный ответ
#   correct — номер правильного варианта (0 = первый)
#   open    — вопрос со свободным ответом: уходит на модерацию (только у 1.3)
# Данные собраны из открытых источников — перед запуском сверьте с организаторами.
QUIZZES = {
    "1-1": {
        "intro": "Ответь на вопрос и получи 5 баллов за правильный ответ. "
                 "За два верных ответа получишь 10 баллов.",
        "points": 5,
        "questions": [
            {"text": "Сколько факультетов в ЧувГУ?",
             "options": ["12", "15", "16", "18"], "correct": 2},
            {"text": "Кто является маскотом ЧувГУ?",
             "options": ["Волк Гром", "Сова Ульян", "Лис Рыжик"], "correct": 1},
        ],
    },
    "1-3": {
        "intro": "Ответь на вопрос и получи 5 баллов за правильный ответ. "
                 "За два верных ответа получишь 10 баллов.",
        "points": 5,
        "open": {
            "text": "Кто такой Ульянов И.Н., имя которого носит университет? "
                    "Напиши тремя словами",
            "label": "Кто такой Ульянов И.Н., имя которого носит университет?",
            "points": 5,
        },
        "questions": [
            {"text": "Где находится Приемная комиссия ЧувГУ?",
             "options": ["Университетский", "Технопарк"],
             "details": ["Кампус «Университетский» — ул. Университетская, 38",
                         "Кампус «Технопарк» — пр. Московский, 15"],
             "correct": 0},
        ],
    },
}

# Как кнопки раскладываются по рядам в панели
PANEL_ROWS = [
    ["1-1", "1-2", "1-3"],
    ["2-1", "2-2", "2-3"],
    ["3-1", "3-2", "extra"],
]

# Статусы: нет записи (не начато) / pending / rejected / approved
STATUS_ICON = {"pending": "⏳", "rejected": "❌", "approved": "✅"}
STATUS_TEXT = {
    "pending": "⏳ задание отправлено на проверку.",
    "rejected": "❌ условия не соблюдены, задание не зачтено.",
    "approved": "✅ всё супер, задание выполнено правильно.",
}
# endregion


# region Витрина: товары
# Название, цена и количество — из таблицы организаторов.
#   stock — сколько штук всего: когда купят столько, подарок «закончился».
#   short — короткая подпись на кнопке (если название длинное).
LOTS = [
    {"id": "ruler", "name": "Линейка", "price": 35, "stock": 15},
    {"id": "pencil", "name": "Карандаш", "price": 35, "stock": 20},
    {"id": "sharpener", "name": "Точилка-ластик", "short": "Точилка", "price": 35, "stock": 10},
    {"id": "cardcase", "name": "Чехол для карты", "short": "Чехол", "price": 35, "stock": 13},
    {"id": "keychain", "name": "Брелок", "price": 40, "stock": 20},
    {"id": "popsocket", "name": "Попсокет", "price": 40, "stock": 10},
    {"id": "bracelet", "name": "Резиновый браслет", "short": "Браслет", "price": 35, "stock": 3},
    {"id": "pen", "name": "Ручка", "price": 35, "stock": 10},
    {"id": "pencilcase", "name": "Пенал", "price": 55, "stock": 15},
    {"id": "mug", "name": "Кружка", "price": 85, "stock": 10},
    {"id": "notebook", "name": "Блокнот", "price": 60, "stock": 50},
    {"id": "keychain_ulyan", "name": "Брелок-обвес Ульян", "short": "Обвес Ульян", "price": 80, "stock": 100},
    {"id": "socks", "name": "Носки", "price": 85, "stock": 10},
    {"id": "xmas_ball", "name": "Елочный шар", "price": 90, "stock": 2},
    {"id": "bottle", "name": "Бутылка для воды", "short": "Бутылка", "price": 100, "stock": 15},
    {"id": "shopper", "name": "Шоппер", "price": 115, "stock": 5},
    {"id": "banana_bag", "name": "Сумка-банан", "price": 115, "stock": 1},
    {"id": "backpack", "name": "Рюкзак", "price": 145, "stock": 4},
]
LOTS_BY_ID = {lot["id"]: lot for lot in LOTS}
# endregion


# region База данных
def db_init():
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "user_id INTEGER PRIMARY KEY, "
            "consent INTEGER NOT NULL DEFAULT 0, "
            "fio TEXT, "
            "phone TEXT, "
            "panel_mid TEXT, "      # id «живого» сообщения-панели
            "panel_view TEXT)"      # что на нём сейчас показано
        )
        # Каждая отправка задания — отдельная строка (история попыток)
        db.execute(
            "CREATE TABLE IF NOT EXISTS submissions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "user_id INTEGER NOT NULL, "
            "task_id TEXT NOT NULL, "
            "content TEXT, "                          # текст ответа / ссылка
            "status TEXT NOT NULL DEFAULT 'pending', "  # pending / approved / rejected
            "points INTEGER NOT NULL DEFAULT 0, "
            "moderator TEXT, "
            "created_at TEXT DEFAULT (datetime('now')), "
            "decided_at TEXT, "
            "src_mid TEXT)"                           # id сообщения участника (для пересылки)
        )
        # Бронь подарков на витрине
        db.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "user_id INTEGER NOT NULL, "
            "lot_id TEXT NOT NULL, "
            "price INTEGER NOT NULL, "
            "created_at TEXT DEFAULT (datetime('now')))"
        )
        # Токены картинок, уже загруженных в MAX (чтобы не грузить файл при каждой отправке)
        db.execute("CREATE TABLE IF NOT EXISTS image_tokens (key TEXT PRIMARY KEY, token TEXT NOT NULL)")
        # Вопросы участников организаторам
        db.execute(
            "CREATE TABLE IF NOT EXISTS questions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "user_id INTEGER NOT NULL, "
            "text TEXT, "
            "src_mid TEXT, "                 # вложения участника (для пересылки)
            "card_mid TEXT, "                # карточка в чате админов (на неё отвечают)
            "answered INTEGER NOT NULL DEFAULT 0, "
            "answer TEXT, "
            "answered_by TEXT, "
            "created_at TEXT DEFAULT (datetime('now')))"
        )
        # Свои рассылки из чата админов
        db.execute(
            "CREATE TABLE IF NOT EXISTS announcements ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "text TEXT NOT NULL, "
            "status TEXT NOT NULL DEFAULT 'draft', "   # draft / sending / done / cancelled
            "created_by TEXT, "
            "created_at TEXT DEFAULT (datetime('now')))"
        )
        # Рассылки по расписанию: кому уже отправлено (user_id = 0 — отметка «рассылка завершена»)
        db.execute(
            "CREATE TABLE IF NOT EXISTS broadcast_log ("
            "broadcast_id TEXT NOT NULL, user_id INTEGER NOT NULL, "
            "PRIMARY KEY (broadcast_id, user_id))"
        )
        # миграция для баз, созданных раньше
        for table, column, definition in (
            ("users", "panel_mid", "TEXT"),
            ("users", "panel_view", "TEXT"),
            ("submissions", "src_mid", "TEXT"),
        ):
            cols = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


# --- «живая» панель: одно сообщение на пользователя ---
def get_panel(user_id: int):
    """-> (id сообщения-панели, что на нём показано)"""
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT panel_mid, panel_view FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
    return row or (None, None)


def set_panel(user_id: int, mid: str | None, view: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "UPDATE users SET panel_mid = ?, panel_view = ? WHERE user_id = ?",
            (mid, view, user_id),
        )


# --- рассылки ---
def unsent_users(broadcast_id: str) -> list[int]:
    """Зарегистрированные участники, которым эта рассылка ещё не отправлена."""
    with sqlite3.connect(DB_PATH) as db:
        rows = db.execute(
            "SELECT user_id FROM users WHERE fio IS NOT NULL AND phone IS NOT NULL "
            "AND user_id NOT IN (SELECT user_id FROM broadcast_log WHERE broadcast_id = ?)",
            (broadcast_id,),
        ).fetchall()
    return [r[0] for r in rows]


def mark_sent(broadcast_id: str, user_id: int):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "INSERT OR IGNORE INTO broadcast_log (broadcast_id, user_id) VALUES (?, ?)",
            (broadcast_id, user_id),
        )


def is_broadcast_done(broadcast_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT 1 FROM broadcast_log WHERE broadcast_id = ? AND user_id = 0", (broadcast_id,)
        ).fetchone()
    return row is not None


def mark_broadcast_done(broadcast_id: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "INSERT OR IGNORE INTO broadcast_log (broadcast_id, user_id) VALUES (?, 0)",
            (broadcast_id,),
        )


# --- вопросы организаторам и свои рассылки ---
def create_question(user_id: int, text: str, src_mid: str | None) -> int:
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "INSERT INTO questions (user_id, text, src_mid) VALUES (?, ?, ?)", (user_id, text, src_mid)
        )
        return cur.lastrowid


def set_question_card(qid: int, card_mid: str | None):
    with sqlite3.connect(DB_PATH) as db:
        db.execute("UPDATE questions SET card_mid = ? WHERE id = ?", (card_mid, qid))


def get_question(qid: int):
    """-> (id, user_id, text, src_mid, card_mid, answered) или None"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT id, user_id, text, src_mid, card_mid, answered FROM questions WHERE id = ?", (qid,)
        ).fetchone()


def get_question_by_card(card_mid: str):
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT id FROM questions WHERE card_mid = ?", (card_mid,)).fetchone()
    return get_question(row[0]) if row else None


def mark_answered(qid: int, answer: str, by: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "UPDATE questions SET answered = 1, answer = ?, answered_by = ? WHERE id = ?",
            (answer, by, qid),
        )


def count_recent_questions(user_id: int, minutes: int = 60) -> int:
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT COUNT(*) FROM questions WHERE user_id = ? AND created_at >= datetime('now', ?)",
            (user_id, f"-{minutes} minutes"),
        ).fetchone()[0]


def list_unanswered(limit: int):
    """-> [(id, user_id, text), ...] — вопросы без ответа, старые первыми."""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT id, user_id, text FROM questions WHERE answered = 0 ORDER BY id LIMIT ?", (limit,)
        ).fetchall()


def count_unanswered() -> int:
    with sqlite3.connect(DB_PATH) as db:
        return db.execute("SELECT COUNT(*) FROM questions WHERE answered = 0").fetchone()[0]


def create_announcement(text: str, by: str) -> int:
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute("INSERT INTO announcements (text, created_by) VALUES (?, ?)", (text, by))
        return cur.lastrowid


def get_announcement(ann_id: int):
    """-> (id, text, status) или None"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT id, text, status FROM announcements WHERE id = ?", (ann_id,)
        ).fetchone()


def set_announcement_status(ann_id: int, old: str, new: str) -> bool:
    """Смена статуса только из ожидаемого (защита от двойного нажатия)."""
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "UPDATE announcements SET status = ? WHERE id = ? AND status = ?", (new, ann_id, old)
        )
        return cur.rowcount > 0


# --- токены загруженных в MAX картинок (чтобы не грузить файл при каждой отправке) ---
def get_image_token(key: str) -> str | None:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT token FROM image_tokens WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_image_token(key: str, token: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR REPLACE INTO image_tokens (key, token) VALUES (?, ?)", (key, token))


def clear_image_token(key: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM image_tokens WHERE key = ?", (key,))


def delete_user_data(user_id: int):
    """Удаляет все данные одного участника (для проверки регистрации заново)."""
    with sqlite3.connect(DB_PATH) as db:
        for table in ("submissions", "orders", "questions", "broadcast_log", "users"):
            db.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))


def mark_broadcast_started(broadcast_id: str):
    """Отметка «рассылка начата» (user_id = -1): с этого момента её этап считается наступившим."""
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "INSERT OR IGNORE INTO broadcast_log (broadcast_id, user_id) VALUES (?, -1)",
            (broadcast_id,),
        )


def broadcast_state(broadcast_id: str):
    """-> (начата, завершена, сколько участников получили)"""
    with sqlite3.connect(DB_PATH) as db:
        ids = {r[0] for r in db.execute(
            "SELECT user_id FROM broadcast_log WHERE broadcast_id = ?", (broadcast_id,)
        )}
    return (-1 in ids or 0 in ids), (0 in ids), sum(1 for i in ids if i > 0)


def clear_broadcast(broadcast_id: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM broadcast_log WHERE broadcast_id = ?", (broadcast_id,))


def count_registered() -> int:
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT COUNT(*) FROM users WHERE fio IS NOT NULL AND phone IS NOT NULL"
        ).fetchone()[0]


def orders_summary():
    """-> (сколько покупок, на сколько баллов)"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute("SELECT COUNT(*), COALESCE(SUM(price), 0) FROM orders").fetchone()


def ensure_user(user_id: int):
    """Строка пользователя нужна заранее: к ней привязывается «живая» панель."""
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))


# --- баллы за задание, баланс и заказы ---
def get_task_points(user_id: int, task_ids: list[str]) -> int:
    marks = ",".join("?" * len(task_ids))
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT COALESCE(SUM(points), 0) FROM submissions "
            f"WHERE user_id = ? AND status = 'approved' AND task_id IN ({marks})",
            (user_id, *task_ids),
        ).fetchone()
    return row[0]


def get_spent(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT COALESCE(SUM(price), 0) FROM orders "
            "WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row[0]


def get_balance(user_id: int) -> int:
    """Баллы, доступные для обмена: заработано минус потрачено."""
    return get_points(user_id) - get_spent(user_id)


def count_orders(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT COUNT(*) FROM orders WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row[0]


def create_order(user_id: int, lot_id: str, price: int, stock: int | None = None):
    """Покупка одной транзакцией: баллы, лимит позиций и остаток проверяются атомарно.
    -> ('ok', id) | ('limit'|'points'|'stock', None)"""
    db = sqlite3.connect(DB_PATH, isolation_level=None)
    try:
        db.execute("BEGIN IMMEDIATE")
        earned = db.execute(
            "SELECT COALESCE(SUM(points), 0) FROM submissions "
            "WHERE user_id = ? AND status = 'approved'", (user_id,)
        ).fetchone()[0]
        spent, count = db.execute(
            "SELECT COALESCE(SUM(price), 0), COUNT(*) FROM orders WHERE user_id = ?", (user_id,)
        ).fetchone()
        taken = db.execute("SELECT COUNT(*) FROM orders WHERE lot_id = ?", (lot_id,)).fetchone()[0]

        if stock is not None and taken >= stock:
            result = ("stock", None)
        elif count >= MAX_ORDERS:
            result = ("limit", None)
        elif earned - spent < price:
            result = ("points", None)
        else:
            cur = db.execute(
                "INSERT INTO orders (user_id, lot_id, price) VALUES (?, ?, ?)",
                (user_id, lot_id, price),
            )
            result = ("ok", cur.lastrowid)
        db.execute("COMMIT")
        return result
    except Exception:
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        db.close()


def clear_orders() -> int:
    """Удаляет все покупки (для тестов). -> сколько удалено"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute("DELETE FROM orders").rowcount


def lot_taken_counts() -> dict:
    """{id подарка: сколько штук уже куплено}"""
    with sqlite3.connect(DB_PATH) as db:
        return dict(db.execute("SELECT lot_id, COUNT(*) FROM orders GROUP BY lot_id").fetchall())


def get_orders(user_id: int):
    """-> [(id, lot_id, price), ...]"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT id, lot_id, price FROM orders WHERE user_id = ? ORDER BY id",
            (user_id,),
        ).fetchall()


# --- пользователи ---
def has_consent(user_id: int) -> bool:
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT consent FROM users WHERE user_id = ?", (user_id,)).fetchone()
    return bool(row and row[0])


def give_consent(user_id: int):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "INSERT INTO users (user_id, consent) VALUES (?, 1) "
            "ON CONFLICT(user_id) DO UPDATE SET consent = 1",
            (user_id,),
        )


def is_registered(user_id: int) -> bool:
    """Зарегистрирован = есть ФИО и телефон."""
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT 1 FROM users WHERE user_id = ? AND fio IS NOT NULL AND phone IS NOT NULL",
            (user_id,),
        ).fetchone()
    return row is not None


def save_profile(user_id: int, fio: str, phone: str):
    with sqlite3.connect(DB_PATH) as db:
        db.execute("UPDATE users SET fio = ?, phone = ? WHERE user_id = ?", (fio, phone, user_id))


def get_user_info(user_id: int):
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT fio, phone FROM users WHERE user_id = ?", (user_id,)).fetchone()
    return row or (None, None)


# --- задания и баллы ---
def get_statuses(user_id: int) -> dict:
    """Статус по каждому заданию = статус последней отправки: {'1-1': 'pending', ...}"""
    with sqlite3.connect(DB_PATH) as db:
        rows = db.execute(
            "SELECT task_id, status FROM submissions s WHERE user_id = ? "
            "AND id = (SELECT MAX(id) FROM submissions "
            "          WHERE user_id = s.user_id AND task_id = s.task_id)",
            (user_id,),
        ).fetchall()
    return dict(rows)


def get_points(user_id: int) -> int:
    """Баллы = сумма по зачтённым отправкам."""
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT COALESCE(SUM(points), 0) FROM submissions "
            "WHERE user_id = ? AND status = 'approved'",
            (user_id,),
        ).fetchone()
    return row[0]


def create_submission(user_id: int, task_id: str, content: str, src_mid: str | None = None) -> int:
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "INSERT INTO submissions (user_id, task_id, content, src_mid) VALUES (?, ?, ?, ?)",
            (user_id, task_id, content, src_mid),
        )
        return cur.lastrowid


def get_submission(sub_id: int):
    """-> (user_id, task_id, content, status, src_mid) или None"""
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT user_id, task_id, content, status, src_mid FROM submissions WHERE id = ?",
            (sub_id,),
        ).fetchone()


def list_pending(limit: int) -> list[int]:
    """Номера отправок, ожидающих проверки (старые первыми)."""
    with sqlite3.connect(DB_PATH) as db:
        rows = db.execute(
            "SELECT id FROM submissions WHERE status = 'pending' ORDER BY id LIMIT ?", (limit,)
        ).fetchall()
    return [r[0] for r in rows]


def count_pending() -> int:
    with sqlite3.connect(DB_PATH) as db:
        return db.execute("SELECT COUNT(*) FROM submissions WHERE status = 'pending'").fetchone()[0]


def create_quiz_result(user_id: int, task_id: str, points: int, marks: str = "") -> bool:
    """Результат викторины записывается сразу (marks — верно/неверно по вопросам: «10»).
    Одна попытка: False, если запись уже есть."""
    status = "approved" if points > 0 else "rejected"
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "INSERT INTO submissions (user_id, task_id, content, status, points, moderator, decided_at) "
            "SELECT ?, ?, ?, ?, ?, 'авто', datetime('now') "
            "WHERE NOT EXISTS (SELECT 1 FROM submissions WHERE user_id = ? AND task_id = ?)",
            (user_id, task_id, marks, status, points, user_id, task_id),
        )
        return cur.rowcount > 0


def get_quiz_marks(user_id: int, task_id: str) -> str:
    """Отметки по вопросам с кнопками, например «10» = первый верно, второй неверно."""
    part_id = f"{task_id}q" if QUIZZES[task_id].get("open") else task_id
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute(
            "SELECT content FROM submissions WHERE user_id = ? AND task_id = ? ORDER BY id DESC LIMIT 1",
            (user_id, part_id),
        ).fetchone()
    return (row[0] or "") if row else ""


def decide_submission(sub_id: int, status: str, points: int, moderator: str) -> bool:
    """Выносит решение. False, если отправку уже кто-то проверил (защита от двойного клика)."""
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "UPDATE submissions SET status = ?, points = ?, moderator = ?, "
            "decided_at = datetime('now') WHERE id = ? AND status = 'pending'",
            (status, points, moderator, sub_id),
        )
        return cur.rowcount > 0
# endregion


# region Клавиатуры и вспомогательные функции
class Registration(StatesGroup):
    consent = State()
    fio = State()
    phone = State()


class Ask(StatesGroup):
    waiting = State()   # человек пишет вопрос организаторам


class Submit(StatesGroup):
    waiting = State()   # участник открыл задание и ждём его ответ
    choosing = State()  # ответил на открытый вопрос, выбирает вариант кнопкой


def consent_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_CONSENT, payload="consent"))
    return kb.as_markup()


def phone_keyboard():
    """Кнопка «поделиться контактом» — MAX сам подставляет номер."""
    kb = InlineKeyboardBuilder()
    kb.row(RequestContactButton(text=BUTTON_SHARE_PHONE))
    return kb.as_markup()


# Режим проверки: «/preview thanks» и «/preview exchange» переводят ЭТОТ аккаунт в состояние
# «после 13:05» / «после 13:40», чтобы проверить всё заранее. Остальных участников не касается.
PREVIEW_PHASE: dict[int, str] = {}   # user_id -> "closed" | "shop"


# Рассылки, которые уже начались (по времени или вручную командой /send): они переключают этап
# для ВСЕХ участников, независимо от часов. Хранится в базе, после перезапуска восстанавливается.
FORCED_PHASE: set[str] = set()


def tasks_closed(user_id: int | None = None) -> bool:
    return (
        datetime.now(MSK) >= TASKS_CLOSE_AT
        or "thanks" in FORCED_PHASE
        or shop_open(user_id)          # раз витрина открыта, приём заданий точно закрыт
        or PREVIEW_PHASE.get(user_id) == "closed"
    )


def shop_open(user_id: int | None = None) -> bool:
    return (
        datetime.now(MSK) >= SHOP_OPEN_AT
        or "exchange" in FORCED_PHASE
        or PREVIEW_PHASE.get(user_id) == "shop"
    )


def rules_text() -> str:
    """Правила Медиабинго; время открытия обмена берётся из SHOP_OPEN_AT."""
    return TEXT_MEDIABINGO.format(shop_time=SHOP_OPEN_AT.strftime("%H:%M"))


def panel_text(user_id: int | None = None) -> str:
    """Текст главного экрана. После закрытия заданий это текст первой рассылки —
    поэтому «Назад» из профиля возвращает именно к нему."""
    if shop_open(user_id):
        return TEXT_PANEL_SHOP
    if tasks_closed(user_id):
        return TEXT_BROADCAST_THANKS
    return TEXT_PANEL


def closed_panel_keyboard(user_id: int | None = None):
    """После закрытия заданий: витрина и профиль."""
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_SHOP, payload="shop"))
    kb.row(CallbackButton(text=BUTTON_PROFILE, payload="profile"))
    return kb.as_markup()


def panel_keyboard(user_id: int):
    """Главная панель. До 13:05 — задания, профиль, правила; после — витрина и профиль."""
    if tasks_closed(user_id):
        return closed_panel_keyboard(user_id)
    statuses = get_statuses(user_id)
    kb = InlineKeyboardBuilder()
    for row in PANEL_ROWS:
        kb.row(*[
            CallbackButton(
                text=f"{task_icon(user_id, t, statuses)} {TASKS[t]['short']}",
                payload=f"task:{t}",
            )
            for t in row
        ])
    kb.row(
        CallbackButton(text=BUTTON_PROFILE, payload="profile"),
        CallbackButton(text=BUTTON_RULES, payload="rules"),
    )
    kb.row(CallbackButton(text=BUTTON_SHOP, payload="shop"))   # смотреть цены можно сразу
    return kb.as_markup()


def profile_keyboard():
    """Экран «Мои баллы»: здесь (и только здесь) есть кнопка «Задать вопрос»."""
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_ASK, payload="ask"))
    kb.row(CallbackButton(text=BUTTON_BACK, payload="menu"))
    return kb.as_markup()


def home_keyboard():
    """«Назад» с экрана вопроса — на главный экран своего этапа."""
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_BACK, payload="home"))
    return kb.as_markup()


def back_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_BACK, payload="menu"))
    return kb.as_markup()


def quiz_text(task_id: str, qi: int) -> str:
    quiz = QUIZZES[task_id]
    question = quiz["questions"][qi]
    text = (
        f"{TASKS[task_id]['title']}\n\n{quiz['intro']}\n\n"
        f"{balls(quiz['points'])} – {question['text']}"
    )
    # Длинные варианты (адреса) выводим в тексте, а на кнопках остаются короткие подписи
    if question.get("details"):
        text += "\n\n" + "\n".join(question["details"])
    return text


def open_question_text(task_id: str) -> str:
    quiz = QUIZZES[task_id]
    return (
        f"{TASKS[task_id]['title']}\n\n{quiz['intro']}\n\n"
        f"{balls(quiz['open']['points'])} – {quiz['open']['text']}"
    )


def pending_text(task_id: str) -> str:
    """Короткое подтверждение после отправки ответа."""
    return f"{TASKS[task_id]['title']}\n\nСтатус: {STATUS_TEXT['pending']}"


def quiz_keyboard(task_id: str, qi: int, marks: str):
    """Варианты ответа; в payload несём номер вопроса и отметки о прошлых ответах (без состояния)."""
    kb = InlineKeyboardBuilder()
    for i, option in enumerate(QUIZZES[task_id]["questions"][qi]["options"]):
        kb.row(CallbackButton(text=option, payload=f"quiz:{task_id}:{qi}:{marks}:{i}"))
    kb.row(CallbackButton(text=BUTTON_BACK, payload="menu"))
    return kb.as_markup()


def moderation_keyboard(sub_id: int, task_id: str):
    """Кнопки для чата модераторов."""
    points = TASKS[task_id]["points"]
    kb = InlineKeyboardBuilder()
    if len(points) == 1:
        kb.row(CallbackButton(text=f"✅ Зачесть +{balls(points[0])}", payload=f"mod:{sub_id}:ok:{points[0]}"))
    else:
        kb.row(*[
            CallbackButton(text=f"✅ +{balls(p)}", payload=f"mod:{sub_id}:ok:{p}") for p in points
        ])
    kb.row(CallbackButton(text="❌ Отклонить", payload=f"mod:{sub_id}:no:0"))
    return kb.as_markup()


def quiz_total(task_id: str) -> int:
    """Максимум баллов за викторину."""
    quiz = QUIZZES[task_id]
    total = quiz["points"] * len(quiz["questions"])
    if quiz.get("open"):
        total += quiz["open"]["points"]
    return total


def quiz_earned(user_id: int, task_id: str) -> int:
    """Баллы за викторину (для 1.3 — часть с кнопками записана как «1-3q»)."""
    return get_task_points(user_id, [task_id, f"{task_id}q"])


def task_icon(user_id: int, task_id: str, statuses: dict) -> str:
    """Значок на кнопке: сложность -> статус. У викторин ещё учитывается доля верных ответов."""
    status = statuses.get(task_id)
    if not status:
        return TASKS[task_id]["icon"]
    if task_id in QUIZZES and status != "pending":
        earned, total = quiz_earned(user_id, task_id), quiz_total(task_id)
        if earned >= total:
            return "✅"
        return "❌" if earned == 0 else "🔸"
    return STATUS_ICON[status]


def can_submit(user_id: int, task_id: str) -> bool:
    """Можно ли сейчас отправлять ответ на задание."""
    if tasks_closed(user_id):
        return False
    status = get_statuses(user_id).get(task_id)
    if task_id in QUIZZES:
        return not status          # викторина — одна попытка
    return status not in ("pending", "approved")


def quiz_breakdown(user_id: int, task_id: str, status: str) -> str:
    """Что верно, а что нет — без подсказки правильных ответов."""
    quiz = QUIZZES[task_id]
    marks = get_quiz_marks(user_id, task_id)
    lines = []
    if quiz.get("open"):
        icon = {"pending": "⏳", "approved": "✅", "rejected": "❌"}[status]
        lines.append(f"{icon} {quiz['open']['label']}")
    for i, question in enumerate(quiz["questions"]):
        if i < len(marks):
            lines.append(f"{'✅' if marks[i] == '1' else '❌'} {question['text']}")
    return "\n".join(lines)


def task_text(user_id: int, task_id: str) -> str:
    task = TASKS[task_id]
    text = f"{task['title']}\n\n{task['text']}"
    status = get_statuses(user_id).get(task_id)
    if not status:
        return text

    if task_id in QUIZZES:
        earned, total = quiz_earned(user_id, task_id), quiz_total(task_id)
        if status == "pending":
            line = STATUS_TEXT["pending"]
        elif earned >= total:
            line = STATUS_TEXT["approved"]
        elif earned == 0:
            line = STATUS_TEXT["rejected"]
        else:
            line = "🔸 верно частично."
        text += f"\n\nСтатус: {line}"
        if status != "pending" or earned > 0:
            text += f"\nНачислено: {earned} из {balls(total)}."
        breakdown = quiz_breakdown(user_id, task_id, status)
        return f"{text}\n\n{breakdown}" if breakdown else text

    return f"{text}\n\nСтатус: {STATUS_TEXT[status]}"


def task_view_text(user_id: int, task_id: str) -> str:
    """Экран задания: текст + статус (+ пометка, если приём ответов закрыт)."""
    text = task_text(user_id, task_id)
    status = get_statuses(user_id).get(task_id)
    retryable = status == "rejected" and task_id not in QUIZZES
    if tasks_closed(user_id) and (not status or retryable):
        text += f"\n\n{TEXT_TASKS_CLOSED}"
    elif retryable:
        text += f"\n{TEXT_RETRY}"   # отказ — не конец: можно отправить ответ заново
    return text


# --- витрина ---
def is_sold_out(lot: dict, taken: dict | None = None) -> bool:
    taken = lot_taken_counts() if taken is None else taken
    return taken.get(lot["id"], 0) >= lot["stock"]


def shop_text(user_id: int) -> str:
    taken = lot_taken_counts()
    lines = []
    for i, lot in enumerate(LOTS, 1):
        line = f"{i}. {lot['name']} — {balls(lot['price'])}"
        if is_sold_out(lot, taken):
            line += " — закончилась"
        lines.append(line)
    lots = "\n".join(lines)
    if not shop_open(user_id):   # до открытия — только посмотреть, что и сколько стоит
        return TEXT_SHOP_PREVIEW.format(
            time=SHOP_OPEN_AT.strftime("%H:%M"), balance=get_balance(user_id), lots=lots
        )
    return TEXT_SHOP.format(
        balance=get_balance(user_id), count=count_orders(user_id), max=MAX_ORDERS, lots=lots
    )


def shop_keyboard(user_id: int | None = None):
    """Таблица кнопок: по два подарка в ряд. До открытия витрины — только «Назад»."""
    if not shop_open(user_id):
        return back_keyboard()
    taken = lot_taken_counts()
    kb = InlineKeyboardBuilder()
    for i in range(0, len(LOTS), 2):
        row = []
        for lot in LOTS[i:i + 2]:
            short = lot.get("short", lot["name"])
            label = f"✖ {short}" if is_sold_out(lot, taken) else short   # цены — в тексте над кнопками
            row.append(CallbackButton(text=label, payload=f"lot:{lot['id']}"))
        kb.row(*row)
    kb.row(CallbackButton(text=BUTTON_ORDERS, payload="orders"))
    return kb.as_markup()


def confirm_keyboard(lot_id: str):
    kb = InlineKeyboardBuilder()
    kb.row(
        CallbackButton(text=BUTTON_CONFIRM, payload=f"buy:{lot_id}"),
        CallbackButton(text=BUTTON_CANCEL, payload="shop"),
    )
    return kb.as_markup()


def shop_back_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_BACK, payload="shop"))
    return kb.as_markup()


def orders_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text=BUTTON_SHOP, payload="shop"))
    return kb.as_markup()


def lot_name(lot_id: str) -> str:
    return LOTS_BY_ID.get(lot_id, {}).get("name", lot_id)


def orders_text(user_id: int) -> str:
    """Экран «Мои покупки» — «оплаченная корзина», которую участник показывает администратору."""
    fio, _ = get_user_info(user_id)
    rows = get_orders(user_id)
    if not rows:
        return f"🧾 Мои покупки\n{fio or '—'}\n\n{TEXT_ORDERS_EMPTY}"
    items = "\n".join(f"№{oid} · {lot_name(lot_id)} · {balls(price)}" for oid, lot_id, price in rows)
    total = sum(price for _, _, price in rows)
    return TEXT_ORDERS.format(fio=fio or "—", items=items, total=balls(total))


def display_name(user) -> str:
    """Имя для подписи модератора."""
    name = " ".join(p for p in (getattr(user, "first_name", None), getattr(user, "last_name", None)) if p)
    return name or getattr(user, "username", None) or str(user.user_id)


def extract_phone(message) -> str | None:
    """Достаёт номер из вложения-контакта (vCard)."""
    attachments = getattr(message.body, "attachments", None) or []
    for att in attachments:
        att_type = getattr(att.type, "value", att.type)
        if str(att_type) != "contact":
            continue
        vcf = getattr(getattr(att, "payload", None), "vcf_info", None) or ""
        m = re.search(r"TEL[^:\r\n]*:([+\d\s\-()]+)", vcf)
        if m:
            digits = re.sub(r"\D", "", m.group(1))
            return "+" + digits if digits else None
    return None


def looks_like_fio(text: str) -> bool:
    """От 2 до 5 слов, только буквы / дефис / точка, слово не длиннее 30 символов."""
    parts = text.split()
    return 2 <= len(parts) <= 5 and all(
        len(p) <= 30 and re.fullmatch(r"[A-Za-zА-Яа-яЁё.\-']+", p) for p in parts
    )
# endregion


# region Панель: одно «живое» сообщение на пользователя
# Панель (меню / задание / профиль) — всегда ОДНО сообщение и всегда последнее в чате.
# При новом показе старое удаляется, поэтому спамом панелей чат не забить,
# а при проверке задания модератором панель обновляется на месте.
_last_screen: dict[int, float] = {}


def too_fast(user_id: int) -> bool:
    """Защита от флуда: не чаще одного возврата панели раз в FALLBACK_COOLDOWN секунд."""
    now = time.time()
    if now - _last_screen.get(user_id, 0.0) < FALLBACK_COOLDOWN:
        return True
    _last_screen[user_id] = now
    return False


def extract_mid(sent) -> str | None:
    """Достаёт id сообщения из ответа send_message."""
    message = getattr(sent, "message", sent)
    body = getattr(message, "body", None)
    return getattr(body, "mid", None)


async def delete_message_safe(mid: str | None):
    delete = getattr(bot, "delete_message", None)
    if not mid or delete is None:
        return
    try:
        await delete(message_id=mid)
    except Exception:
        logging.warning("Не удалось удалить сообщение %s", mid)


async def edit_message_safe(mid: str | None, text: str, keyboard) -> bool:
    edit = getattr(bot, "edit_message", None)
    if not mid or edit is None:
        return False
    try:
        await edit(message_id=mid, text=text, attachments=[keyboard] if keyboard else [])
        return True
    except Exception:
        logging.warning("Не удалось обновить сообщение %s", mid)
        return False


def find_image(*names: str) -> Path | None:
    """Файл картинки из папки images/ (png, jpg, jpeg, webp) — первая найденная из names."""
    for name in names:
        for ext in (".png", ".jpg", ".jpeg", ".webp"):
            path = IMAGES_DIR / f"{name}{ext}"
            if path.exists():
                return path
    return None


def image_key(path: Path) -> str:
    """Ключ кэша токена: при замене файла (другой размер/дата) картинка загрузится заново."""
    stat = path.stat()
    return f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}"


def _optimized(path: Path) -> Path:
    """Крупные картинки сжимаются (до 1000 px, jpg) — загрузка быстрее. Нужен Pillow; иначе — файл как есть."""
    try:
        if path.stat().st_size <= 250_000:
            return path
        from PIL import Image
        out = Path(tempfile.gettempdir()) / "chuvgu_img_opt" / f"{path.stem}_{int(path.stat().st_mtime)}.jpg"
        if not out.exists():
            out.parent.mkdir(parents=True, exist_ok=True)
            with Image.open(path) as im:
                im = im.convert("RGB")
                im.thumbnail((1000, 1000))
                im.save(out, "JPEG", quality=85, optimize=True)
        return out if out.stat().st_size < path.stat().st_size else path
    except Exception:
        return path


class _ImageByToken:
    """Запасной вариант вложения «картинка по токену» (если в библиотеке нет своего класса)."""

    def __init__(self, token: str):
        self.token = token

    def model_dump(self, *args, **kwargs):
        return {"type": "image", "payload": {"token": self.token}}


def _token_attachment(token: str):
    """Вложение «уже загруженная картинка» — без повторной загрузки и ожидания обработки."""
    try:
        from maxapi.enums.upload_type import UploadType
        from maxapi.types.attachments.upload import AttachmentPayload, AttachmentUpload
        return AttachmentUpload(type=UploadType.IMAGE, payload=AttachmentPayload(token=token))
    except Exception:
        return _ImageByToken(token)


_token_reuse_ok = True   # выключится само, если отправка по токену не поддерживается


def remember_image_token(sent, key: str):
    """После первой загрузки достаём токен картинки из ответа MAX и запоминаем."""
    try:
        message = getattr(sent, "message", sent)
        for att in getattr(getattr(message, "body", None), "attachments", None) or []:
            att_type = getattr(att, "type", "")
            if str(getattr(att_type, "value", att_type)) == "image":
                token = getattr(getattr(att, "payload", None), "token", None)
                if token:
                    set_image_token(key, token)
                    return
    except Exception:
        logging.warning("Не удалось запомнить токен картинки", exc_info=True)


def panel_image(user_id: int) -> str:
    """Картинка главного экрана по этапу: задания открыты / закрыты / витрина."""
    if shop_open(user_id):
        return "gift"
    if tasks_closed(user_id):
        return "thanks"
    return "menu"


async def send_screen_message(text: str, keyboard=None, image=None, **target):
    """Сообщение с необязательной картинкой (имя или несколько имён на выбор).
    Картинка грузится в MAX один раз, дальше уходит по сохранённому токену — это быстро.
    Нет файла или картинка не отправилась — сообщение уходит без неё."""
    global _token_reuse_ok
    names = (image,) if isinstance(image, str) else tuple(image or ())
    path = find_image(*names) if names else None
    buttons = [keyboard] if keyboard else []

    if path:
        key = image_key(path)
        token = get_image_token(key) if _token_reuse_ok else None
        if token:
            try:
                return await bot.send_message(
                    text=text, attachments=[_token_attachment(token), *buttons], **target
                )
            except (AttributeError, TypeError, ValueError):
                _token_reuse_ok = False   # отправка по токену не поддерживается этой версией библиотеки
                logging.warning("Отправка картинки по токену не поддерживается — грузим файл каждый раз", exc_info=True)
            except Exception:
                logging.warning("Токен картинки не подошёл — загружаю заново", exc_info=True)
                clear_image_token(key)
        try:
            from maxapi.types import InputMedia
            sent = await bot.send_message(
                text=text, attachments=[InputMedia(path=str(_optimized(path))), *buttons], **target
            )
            remember_image_token(sent, key)
            return sent
        except Exception:
            logging.warning("Не удалось отправить картинку %s — шлю без неё", names, exc_info=True)

    return await bot.send_message(text=text, attachments=buttons or None, **target)


_screen_locks: dict[int, asyncio.Lock] = {}
_cleanup_tasks: set = set()


def _background(coro):
    """Запуск в фоне (например, удаление старого сообщения), чтобы не задерживать отклик."""
    task = asyncio.create_task(coro)
    _cleanup_tasks.add(task)
    task.add_done_callback(_cleanup_tasks.discard)


async def show_screen(user_id: int, text: str, keyboard, view: str,
                      chat_id: int | None = None, image=None):
    """Новое сообщение-панель внизу чата (с картинкой, если она есть); предыдущее удаляется.
    Для одного участника выполняется строго по очереди — быстрые повторные нажатия не плодят дубли."""
    async with _screen_locks.setdefault(user_id, asyncio.Lock()):
        old_mid, _ = get_panel(user_id)
        target = {"chat_id": chat_id} if chat_id else {"user_id": user_id}
        sent = await send_screen_message(text, keyboard, image, **target)
        new_mid = extract_mid(sent)
        set_panel(user_id, new_mid, view)
        if old_mid and old_mid != new_mid:
            _background(delete_message_safe(old_mid))


def track(event: MessageCallback, user_id: int, view: str):
    """Нажатие кнопки на панели: запоминаем, что эта панель — «живая» и что на ней показано."""
    set_panel(user_id, event.message.body.mid, view)


def render_view(user_id: int, view: str | None):
    """Что показывать для вида панели. None — вид не обновляется автоматически."""
    if view in ("menu", "notice"):
        return panel_text(user_id), panel_keyboard(user_id)
    if view == "profile":
        return TEXT_PROFILE.format(points=get_balance(user_id)), profile_keyboard()
    if view == "rules":
        return rules_text(), back_keyboard()
    if view == "shop":
        return shop_text(user_id), shop_keyboard(user_id)
    if view == "orders":
        return orders_text(user_id), orders_keyboard()
    if view and view.startswith("task:"):
        task_id = view.split(":", 1)[1]
        if task_id in TASKS:
            return task_view_text(user_id, task_id), back_keyboard()
    return None   # "flow" / "reg" (идёт ввод) — не трогаем


async def refresh_panel(user_id: int):
    """Обновить панель участника на месте (после решения модератора)."""
    mid, view = get_panel(user_id)
    rendered = render_view(user_id, view)
    if not rendered:
        return
    if view in ("menu", "notice"):
        # меню — новым сообщением, чтобы вернулась картинка
        await show_screen(user_id, rendered[0], rendered[1], "menu", image=panel_image(user_id))
    elif view == "profile":
        await show_screen(user_id, rendered[0], rendered[1], "profile", image="approved")
    elif mid:
        await edit_message_safe(mid, *rendered)
# endregion


# region Старт: кнопка «Начать» и /start
async def run_start_flow(chat_id: int, user_id: int, context: MemoryContext):
    await context.clear()

    # Уже зарегистрирован — просто показываем панель (без повторных правил)
    if is_registered(user_id):
        await show_screen(
        user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id,
        image=panel_image(user_id),
    )
        return

    ensure_user(user_id)
    await send_screen_message(TEXT_GREETING, image="welcome", chat_id=chat_id)

    if has_consent(user_id):
        await show_screen(user_id, TEXT_ASK_FIO, None, "reg", chat_id)
        await context.set_state(Registration.fio)
    else:
        await show_screen(user_id, TEXT_CONSENT, consent_keyboard(), "reg", chat_id)
        await context.set_state(Registration.consent)


@dp.bot_started()
async def on_bot_started(event: BotStarted, context: MemoryContext):
    await run_start_flow(event.chat_id, event.user.user_id, context)


@dp.message_created(CommandStart())
async def on_start_command(event: MessageCreated, context: MemoryContext):
    user_id = event.message.sender.user_id
    if too_fast(user_id):
        return
    await run_start_flow(event.message.recipient.chat_id, user_id, context)
# endregion


# region Команда /profile
@dp.message_created(Command("profile"))
async def on_profile_command(event: MessageCreated):
    user_id = event.message.sender.user_id
    if not is_registered(user_id):
        await event.message.answer(TEXT_NEED_REGISTRATION)
        return
    if too_fast(user_id):
        return
    await show_screen(
        user_id, TEXT_PROFILE.format(points=get_balance(user_id)), profile_keyboard(),
        "profile", event.message.recipient.chat_id, image="approved",
    )
# endregion


# region Служебные команды (чат админов)
# Зарегистрированы ДО обработчиков состояний и «запасного» обработчика —
# иначе они перехватят команду раньше.
@dp.message_created(Command("chatid"))
async def on_chatid(event: MessageCreated):
    await event.message.answer(f"chat_id этого чата: {event.message.recipient.chat_id}")


@dp.message_created(Command("queue"))
async def on_queue(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return   # в других чатах команда не работает

    ids = list_pending(QUEUE_BATCH)
    if not ids:
        await event.message.answer("Очередь пуста ✅")
        return

    total = count_pending()
    await event.message.answer(f"Ожидают проверки: {total}. Показываю первые {len(ids)}.")
    for sub_id in ids:
        await send_to_moderation(sub_id)


_BG_TASKS: set = set()   # ссылки на фоновые рассылки (чтобы их не убрал сборщик мусора)
BROADCAST_EFFECT = {
    "thanks": "закроет приём заданий у всех",
    "exchange": "откроет витрину у всех (приём заданий тоже закроется)",
}


def _broadcast_arg(event: MessageCreated) -> tuple[str, list[str]]:
    parts = (event.message.body.text or "").split()
    name = parts[1].lstrip("/#") if len(parts) > 1 else ""
    return name, parts


# «/send thanks» — показать, сколько получателей; «/send thanks go» — запустить рассылку сейчас.
# Этап (закрытие заданий / витрина) переключается у всех, как будто наступило время.
@dp.message_created(Command("send"))
async def on_send(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    name, parts = _broadcast_arg(event)
    items = {item["id"]: item for item in SCHEDULE}
    if name not in items:
        await event.message.answer("Использование: /send thanks или /send exchange (запуск: добавьте слово go)")
        return

    _, done, sent = broadcast_state(name)
    if done:
        await event.message.answer(
            f"Рассылка «{name}» уже завершена (получили {sent}). Повторить: /undo {name}, затем /send {name}."
        )
        return
    if name in _RUNNING:
        await event.message.answer(f"Рассылка «{name}» уже идёт.")
        return

    total = len(unsent_users(name))
    if not (len(parts) > 2 and parts[2].lower() in ("go", "да", "yes")):
        await event.message.answer(
            f"Рассылка «{name}»: получателей {total}. Она {BROADCAST_EFFECT[name]}.\n"
            f"Запустить: /send {name} go"
        )
        return

    await event.message.answer(f"Запускаю рассылку «{name}»: {total} получателей. Итог придёт сюда.")
    task = asyncio.create_task(run_broadcast(items[name]))   # в фоне: вебхуку нужно ответить быстро
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


# «/undo thanks» — откатить принудительный запуск (до времени рассылки): этап снова по часам,
# рассылку можно запустить заново. Уже отправленные сообщения у участников остаются.
@dp.message_created(Command("undo"))
async def on_undo(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    name, _ = _broadcast_arg(event)
    items = {item["id"]: item for item in SCHEDULE}
    if name not in items:
        await event.message.answer("Использование: /undo thanks или /undo exchange")
        return
    if name in _RUNNING:
        await event.message.answer("Идёт отправка — подождите её завершения.")
        return
    if datetime.now(MSK) >= items[name]["at"]:
        await event.message.answer(
            "Время этой рассылки уже наступило — после отката бот отправил бы её снова сам. "
            "Полный откат — очистка базы на хостинге."
        )
        return
    clear_broadcast(name)
    FORCED_PHASE.discard(name)
    await event.message.answer(
        f"Рассылка «{name}» откатена: этап снова по времени ({items[name]['at']:%d.%m %H:%M} МСК)."
    )


# «/status» — сводка: этапы, рассылки, очередь проверки, покупки
@dp.message_created(Command("status"))
async def on_status(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    now = datetime.now(MSK)
    lines = [
        f"📊 Статус на {now:%d.%m %H:%M} (МСК)",
        f"Зарегистрировано: {count_registered()}",
        f"Ожидают проверки: {count_pending()}",
        f"Вопросов без ответа: {count_unanswered()}",
        f"Приём заданий: {'закрыт' if tasks_closed() else 'открыт'}",
        f"Витрина: {'открыта' if shop_open() else 'закрыта'}",
    ]
    for item in SCHEDULE:
        started, done, sent = broadcast_state(item["id"])
        state = "завершена" if done else "идёт" if item["id"] in _RUNNING else "начата" if started else "ждёт"
        lines.append(f"Рассылка «{item['id']}» ({item['at']:%d.%m %H:%M}): {state}, получили {sent}")
    count, total = orders_summary()
    lines.append(f"Покупок: {count} на {balls(total)}")
    await event.message.answer("\n".join(lines))


def _replied_mid(message) -> str | None:
    """id сообщения, на которое ответили (reply), если это ответ."""
    link = getattr(message, "link", None)
    if not link:
        return None
    link_type = getattr(link, "type", "")
    if str(getattr(link_type, "value", link_type)) != "reply":
        return None
    return getattr(getattr(link, "message", None), "mid", None)


async def _deliver_answer(event: MessageCreated, qid: int, text: str):
    result = await relay_answer(qid, text, display_name(event.message.sender))
    if result == "missing":
        await event.message.answer(f"Вопрос #{qid} не найден.")
    elif result == "failed":
        await event.message.answer(f"Не удалось доставить ответ на вопрос #{qid} (участник мог заблокировать бота).")
    # при успехе карточка вопроса обновляется сама


# Ответ модератора: «ответить» (reply) на карточку вопроса — текст уйдёт этому участнику.
# Реагирует только на ответы в чате админов; на всё остальное не влияет.
@dp.message_created(F.message.link, F.message.recipient.chat_id == ADMIN_CHAT_ID)
async def on_admin_reply(event: MessageCreated):
    mid = _replied_mid(event.message)
    question = get_question_by_card(mid) if mid else None
    text = (event.message.body.text or "").strip()
    if question and text:
        await _deliver_answer(event, question[0], text)


# «/reply 12 текст ответа» — то же без «ответить»
@dp.message_created(Command("reply"))
async def on_reply_command(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    parts = (event.message.body.text or "").split(maxsplit=2)
    number = parts[1].lstrip("#№") if len(parts) > 1 else ""
    if not number.isdigit() or len(parts) < 3 or not parts[2].strip():
        await event.message.answer("Использование: /reply 12 текст ответа")
        return
    await _deliver_answer(event, int(number), parts[2].strip())


# «/questions» — вопросы без ответа
@dp.message_created(Command("questions"))
async def on_questions_command(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    rows = list_unanswered(10)
    if not rows:
        await event.message.answer("Вопросов без ответа нет ✅")
        return
    lines = [f"❓ Без ответа: {count_unanswered()} (показаны первые {len(rows)})"]
    for qid, user_id, text in rows:
        fio, _ = get_user_info(user_id)
        lines.append(f"#{qid} · {fio or user_id}: {text[:80]}")
    lines.append("\nОтветить: /reply N текст — или «ответить» на карточку вопроса")
    await event.message.answer("\n".join(lines))


# «/announce текст» — своя рассылка всем зарегистрированным. Сначала черновик с кнопками.
@dp.message_created(Command("announce"))
async def on_announce(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    parts = (event.message.body.text or "").split(maxsplit=1)
    body = parts[1].strip() if len(parts) > 1 else ""
    if not body:
        await event.message.answer("Использование: /announce текст сообщения для всех участников")
        return
    if len(body) > MAX_ANNOUNCE_LEN:
        await event.message.answer(f"Слишком длинно: {len(body)} символов, можно до {MAX_ANNOUNCE_LEN}.")
        return

    ann_id = create_announcement(body, display_name(event.message.sender))
    total = count_registered()
    kb = InlineKeyboardBuilder()
    kb.row(
        CallbackButton(text=f"✅ Отправить ({total})", payload=f"ann:{ann_id}:go"),
        CallbackButton(text="❌ Отмена", payload=f"ann:{ann_id}:cancel"),
    )
    await event.message.answer(
        f"📢 Черновик рассылки #{ann_id}\nПолучателей: {total}\n\n{body}", attachments=[kb.as_markup()]
    )


@dp.message_callback(F.callback.payload.startswith("ann:"))
async def on_announce_action(event: MessageCallback):
    if ADMIN_CHAT_ID and event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    moderator = event.callback.user
    if ADMIN_IDS and moderator.user_id not in ADMIN_IDS:
        await event.answer(notification="Нет прав")
        return

    _, ann_id, action = event.callback.payload.split(":")
    ann_id = int(ann_id)
    name = display_name(moderator)
    card = event.message.body.text or ""

    if action == "cancel":
        if set_announcement_status(ann_id, "draft", "cancelled"):
            await event.message.edit(text=f"{card}\n\n❌ Отменено — {name}", attachments=[])
        else:
            await event.answer(notification="Уже обработано")
        return

    if not set_announcement_status(ann_id, "draft", "sending"):
        await event.answer(notification="Уже обработано")
        return
    await event.message.edit(text=f"{card}\n\n⏳ Отправляется — {name}", attachments=[])
    task = asyncio.create_task(run_announcement(ann_id))   # в фоне: вебхуку нужно ответить быстро
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


async def run_announcement(ann_id: int):
    ann = get_announcement(ann_id)
    if not ann:
        return
    key = f"ann{ann_id}"
    ok = failed = 0
    for user_id in unsent_users(key):
        try:
            await bot.send_message(user_id=user_id, text=ann[1])
            ok += 1
            try:
                await reshow_home_if_idle(user_id)   # панель остаётся последним сообщением
            except Exception:
                logging.warning("Не удалось вернуть панель %s", user_id, exc_info=True)
        except Exception:
            failed += 1
            logging.exception("Рассылка #%s: не доставлено %s", ann_id, user_id)
        mark_sent(key, user_id)
        await asyncio.sleep(0.05)
    mark_broadcast_done(key)
    set_announcement_status(ann_id, "sending", "done")
    summary = f"📢 Рассылка #{ann_id}: отправлено {ok}, ошибок {failed}"
    logging.info(summary)
    if ADMIN_CHAT_ID:
        try:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=summary)
        except Exception:
            logging.exception("Не удалось отправить итог рассылки")


# «/stock» — сколько каких подарков осталось
@dp.message_created(Command("stock"))
async def on_stock_command(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    taken = lot_taken_counts()
    lines = ["📦 Остатки подарков (осталось / всего)"]
    for lot in LOTS:
        left = max(lot["stock"] - taken.get(lot["id"], 0), 0)
        lines.append(f"{lot['name']}: {left} / {lot['stock']}" + ("  — закончилась" if left == 0 else ""))
    await event.message.answer("\n".join(lines))


# «/warmup» — один раз прогнать все картинки через MAX, чтобы у участников они открывались мгновенно
# (токены запоминаются в базе). Запускайте после загрузки или замены картинок.
@dp.message_created(Command("warmup"))
async def on_warmup(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    names = sorted({p.stem for p in IMAGES_DIR.glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")})
    if not names:
        await event.message.answer("В папке images/ нет картинок.")
        return
    await event.message.answer(f"Прогреваю картинки ({len(names)}) — это займёт около минуты. Итог напишу сюда.")
    _background(_run_warmup(names))


async def _run_warmup(names: list[str]):
    done = cached = failed = 0
    for name in names:
        path = find_image(name)
        if path and get_image_token(image_key(path)):
            cached += 1
            continue
        try:
            await send_screen_message(f"🖼 {name}", image=name, chat_id=ADMIN_CHAT_ID)
            done += 1
        except Exception:
            failed += 1
            logging.exception("Прогрев картинки %s не удался", name)
    ok = bool(get_image_token(image_key(find_image(names[0])))) if find_image(names[0]) else False
    await bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=f"🖼 Прогрев готов: загружено {done}, уже было {cached}, ошибок {failed}."
             + ("" if ok else "\n⚠️ Токены не сохранились — картинки будут грузиться при каждой отправке."),
    )


# «/resetstock» — вернуть остатки подарков к исходным (для тестов): удаляет ВСЕ покупки,
# баллы возвращаются участникам. Нужно подтверждение словом go; если витрина уже открыта
# по реальному расписанию — ещё и слово «точно».
@dp.message_created(Command("resetstock"))
async def on_resetstock(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    parts = (event.message.body.text or "").split()
    count, spent = orders_summary()
    if not (len(parts) > 1 and parts[1].lower() in ("go", "да", "yes")):
        await event.message.answer(
            f"Покупок сейчас: {count} (на {balls(spent)}). Команда удалит ВСЕ покупки у всех участников: "
            "остатки подарков вернутся к исходным, баллы вернутся участникам.\n"
            "Подтвердить: /resetstock go"
        )
        return
    if datetime.now(MSK) >= SHOP_OPEN_AT and not (len(parts) > 2 and parts[2].lower() == "точно"):
        await event.message.answer(
            "Витрина уже открыта по расписанию, покупки могут быть настоящими. "
            "Если это точно нужно: /resetstock go точно"
        )
        return
    removed = clear_orders()
    await event.message.answer(
        f"Готово: удалено покупок — {removed}. Остатки подарков снова исходные (смотрите /stock)."
    )


# «/help» — список всех команд для модераторов
ADMIN_HELP = (
    "📋 Команды модераторов\n\n"
    "Проверка и общение\n"
    "/status — сводка: регистрации, очередь, вопросы, этапы, рассылки, покупки\n"
    "/queue — задания, ждущие проверки\n"
    "/questions — вопросы участников без ответа\n"
    "/reply 12 текст — ответить на вопрос №12 (или «ответить» на карточку вопроса)\n"
    "/announce текст — своя рассылка всем зарегистрированным (с подтверждением кнопкой)\n\n"
    "Этапы мероприятия\n"
    "/send thanks — рассылка «спасибо» и закрытие заданий: сколько получателей\n"
    "/send exchange — рассылка про витрину и открытие покупок: сколько получателей\n"
    "(добавьте слово go, чтобы запустить: /send thanks go)\n"
    "/undo thanks, /undo exchange — откатить запуск (только до времени рассылки)\n\n"
    "Витрина\n"
    "/stock — сколько каких подарков осталось\n"
    "/resetstock — вернуть остатки к исходным (удаляет все покупки, баллы возвращаются); подтверждение: go\n\n"
    "Проверка и тесты\n"
    "/preview thanks, /preview exchange — показать себе этап (reset — вернуть реальное время)\n"
    "/resetme — удалить свои данные и пройти регистрацию заново\n"
    "/warmup — заранее загрузить картинки (после их замены)\n"
    "/chatid — id этого чата\n"
    "/help — этот список\n\n"
    "У участников только /start и /profile."
)


@dp.message_created(Command("help"))
async def on_help(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    await event.message.answer(ADMIN_HELP)


# «/resetme» — удалить ВАШИ данные (регистрацию, ответы, покупки), чтобы пройти всё заново.
# Данные других участников не затрагиваются.
@dp.message_created(Command("resetme"))
async def on_resetme(event: MessageCreated, context: MemoryContext):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    user_id = event.message.sender.user_id
    old_panel, _ = get_panel(user_id)
    delete_user_data(user_id)
    PREVIEW_PHASE.pop(user_id, None)
    await context.clear()
    await delete_message_safe(old_panel)
    await event.message.answer("Ваши данные удалены. Напишите боту /start, чтобы пройти регистрацию заново.")


# «/preview thanks» — как после 13:05, «/preview exchange» — как после 13:40 (в этом аккаунте),
# «/preview reset» — вернуть реальное время. Второе слово пишется без «/» и без «#».
@dp.message_created(Command("preview"))
async def on_preview(event: MessageCreated):
    if not ADMIN_CHAT_ID or event.message.recipient.chat_id != ADMIN_CHAT_ID:
        return
    user_id = event.message.sender.user_id
    parts = (event.message.body.text or "").split()
    name = parts[1].lstrip("/#") if len(parts) > 1 else ""

    if name == "reset":
        PREVIEW_PHASE.pop(user_id, None)
        await event.message.answer("Режим проверки выключен: бот снова работает по реальному времени.")
        return

    items = {item["id"]: item for item in SCHEDULE}
    if name not in items:
        await event.message.answer(
            "Использование: " + " или ".join(f"/preview {i}" for i in [*items, "reset"])
        )
        return

    PREVIEW_PHASE[user_id] = "shop" if name == "exchange" else "closed"
    await send_broadcast_item(items[name], user_id)
# endregion


# region Регистрация: согласие на ПД
# Подсказки регистрации показываются через show_screen: повторный запрос заменяет
# предыдущий (старый удаляется), поэтому сообщениями их не накопить.
@dp.message_callback(F.callback.payload == "consent")
async def on_consent(event: MessageCallback, context: MemoryContext):
    user_id = event.callback.user.user_id
    give_consent(user_id)
    await event.message.edit(text=f"{TEXT_CONSENT}\n\n{TEXT_CONSENT_DONE}", attachments=[])
    set_panel(user_id, None, "reg")   # это сообщение остаётся в истории и не удаляется
    await show_screen(user_id, TEXT_ASK_FIO, None, "reg")
    await context.set_state(Registration.fio)


# Пока согласия нет — любое сообщение заменяет запрос согласия новым (без дублей)
@dp.message_created(Registration.consent)
async def on_consent_pending(event: MessageCreated):
    user_id = event.message.sender.user_id
    if too_fast(user_id):
        return
    await show_screen(
        user_id, TEXT_CONSENT, consent_keyboard(), "reg", event.message.recipient.chat_id
    )
# endregion


# region Регистрация: ФИО
@dp.message_created(Registration.fio)
async def on_fio(event: MessageCreated, context: MemoryContext):
    chat_id = event.message.recipient.chat_id
    user_id = event.message.sender.user_id
    fio = (event.message.body.text or "").strip()   # фото/файл без текста -> пусто -> переспрашиваем

    if len(fio) > MAX_FIO_LEN or not looks_like_fio(fio):
        if not too_fast(user_id):
            await show_screen(user_id, TEXT_ASK_FIO, None, "reg", chat_id)
        return

    await context.update_data(fio=fio)
    await context.set_state(Registration.phone)
    await show_screen(user_id, TEXT_ASK_PHONE, phone_keyboard(), "reg", chat_id)
# endregion


# region Регистрация: телефон
@dp.message_created(Registration.phone)
async def on_phone(event: MessageCreated, context: MemoryContext):
    chat_id = event.message.recipient.chat_id
    user_id = event.message.sender.user_id
    phone = extract_phone(event.message)

    # запасной вариант: номер, набранный текстом (длинный текст даже не разбираем)
    if not phone:
        raw = (event.message.body.text or "").strip()
        if len(raw) <= 25:
            digits = re.sub(r"\D", "", raw)
            if len(digits) == 11 and digits.startswith("8"):
                digits = "7" + digits[1:]
            if 10 <= len(digits) <= 15:
                phone = "+" + digits

    if not phone:
        if not too_fast(user_id):
            await show_screen(user_id, TEXT_ASK_PHONE, phone_keyboard(), "reg", chat_id)
        return

    data = await context.get_data()
    save_profile(user_id, data["fio"], phone)
    logging.info("Зарегистрирован: %s, %s", data["fio"], phone)

    await context.clear()
    await send_screen_message(rules_text(), image="registered", chat_id=chat_id)
    await show_screen(
        user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id,
        image=panel_image(user_id),
    )
# endregion


# region Панель: меню, задания, профиль, правила (кнопки)
@dp.message_callback(F.callback.payload == "menu")
async def on_menu(event: MessageCallback, context: MemoryContext):
    await context.clear()   # вышли из режима «жду ответ на задание»
    user_id = event.callback.user.user_id
    track(event, user_id, "menu")   # чтобы это сообщение удалилось при показе нового
    await show_screen(
        user_id, panel_text(user_id), panel_keyboard(user_id), "menu", image=panel_image(user_id)
    )


@dp.message_callback(F.callback.payload == "profile")
async def on_profile_button(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "profile")   # чтобы это сообщение удалилось при показе нового
    await show_screen(
        user_id, TEXT_PROFILE.format(points=get_balance(user_id)), profile_keyboard(), "profile",
        image="approved",
    )


@dp.message_callback(F.callback.payload == "rules")
async def on_rules_button(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "rules")
    await event.message.edit(text=rules_text(), attachments=[back_keyboard()])


@dp.message_callback(F.callback.payload.startswith("task:"))
async def on_task(event: MessageCallback, context: MemoryContext):
    user_id = event.callback.user.user_id
    task_id = event.callback.payload.split(":", 1)[1]
    if task_id not in TASKS:
        return

    await context.clear()

    # Результат, «уже отправлено» или приём закрыт — просто экран задания
    if not can_submit(user_id, task_id):
        track(event, user_id, f"task:{task_id}")
        await event.message.edit(text=task_view_text(user_id, task_id), attachments=[back_keyboard()])
        return

    quiz = QUIZZES.get(task_id)
    if quiz and quiz.get("open"):
        # сначала вопрос со свободным ответом (уйдёт на модерацию), потом кнопки
        track(event, user_id, "flow")
        await event.message.edit(text=open_question_text(task_id), attachments=[back_keyboard()])
    elif quiz:
        track(event, user_id, "flow")
        await event.message.edit(text=quiz_text(task_id, 0), attachments=[quiz_keyboard(task_id, 0, "")])
        return
    else:
        track(event, user_id, f"task:{task_id}")
        await event.message.edit(text=task_view_text(user_id, task_id), attachments=[back_keyboard()])

    # Ждём ответ участника сообщением
    await context.set_state(Submit.waiting)
    await context.update_data(task_id=task_id)
# endregion


# region Викторины: ответы кнопками
@dp.message_callback(F.callback.payload.startswith("quiz:"))
async def on_quiz_answer(event: MessageCallback, context: MemoryContext):
    user_id = event.callback.user.user_id
    _, task_id, qi, marks, opt = event.callback.payload.split(":")
    qi, opt = int(qi), int(opt)

    quiz = QUIZZES.get(task_id)
    if not quiz:
        return

    # Уже проходил, или приём закрыт (например, нажатие на старую кнопку)
    if not can_submit(user_id, task_id):
        track(event, user_id, f"task:{task_id}")
        await event.message.edit(text=task_view_text(user_id, task_id), attachments=[back_keyboard()])
        return

    questions = quiz["questions"]
    if qi >= len(questions) or opt >= len(questions[qi]["options"]) or len(marks) != qi:
        return

    track(event, user_id, "flow")
    marks += "1" if opt == questions[qi]["correct"] else "0"

    # Есть следующий вопрос
    if qi + 1 < len(questions):
        await event.message.edit(
            text=quiz_text(task_id, qi + 1), attachments=[quiz_keyboard(task_id, qi + 1, marks)]
        )
        return

    score = marks.count("1") * quiz["points"]

    # Последний вопрос. Если у задания есть открытый вопрос — берём ответ на него
    open_answer = None
    if quiz.get("open"):
        open_answer = (await context.get_data()).get("open_answer")
        if not open_answer:
            await event.message.edit(text=TEXT_RESTART_QUIZ, attachments=[back_keyboard()])
            return

    # Часть с кнопками — автоматически (для 1.3 пишется отдельной строкой «1-3q»)
    part_id = f"{task_id}q" if open_answer else task_id
    track(event, user_id, f"task:{task_id}")
    if not create_quiz_result(user_id, part_id, score, marks):
        await event.message.edit(text=task_text(user_id, task_id), attachments=[back_keyboard()])
        return

    sub_id = create_submission(user_id, task_id, open_answer) if open_answer else None

    # Итог: новое сообщение с картинкой и разбором «что верно, что нет»
    if open_answer:
        image = "pending"
    elif score >= quiz_total(task_id):
        image = "quiz_full"
    else:
        image = "rejected" if score == 0 else "quiz_partial"
    await show_screen(
        user_id, task_text(user_id, task_id), back_keyboard(), f"task:{task_id}", image=image
    )
    await context.clear()

    if sub_id:
        await send_to_moderation(sub_id)   # открытый ответ — модераторам
# endregion


# region Приём ответов участников
def validate_answer(attachments: list, min_files: int = 1) -> str | None:
    """None — ответ годится (нужны скриншоты картинкой), иначе подсказка участнику."""
    if len(attachments) > MAX_ATTACHMENTS:
        return TEXT_TOO_MANY_FILES
    if len(attachments) < min_files:
        return TEXT_BAD_PHOTO if min_files == 1 else TEXT_NEED_FILES.format(n=min_files)
    return None


@dp.message_created(Submit.waiting)
async def on_submission(event: MessageCreated, context: MemoryContext):
    message = event.message
    chat_id = message.recipient.chat_id
    user_id = message.sender.user_id
    data = await context.get_data()
    task_id = data.get("task_id")

    text = (message.body.text or "").strip()
    attachments = message.body.attachments or []
    if not task_id or not (text or attachments):
        return

    if tasks_closed(user_id):
        await context.clear()
        await show_screen(
            user_id, f"{TASKS[task_id]['title']}\n\n{TEXT_TASKS_CLOSED}",
            panel_keyboard(user_id), "menu", chat_id,
        )
        return

    # Уже отправлено/зачтено; у викторин — любая запись блокирует повтор
    status = get_statuses(user_id).get(task_id)
    if status in ("pending", "approved") or (task_id in QUIZZES and status):
        await context.clear()
        screen = pending_text(task_id) if status == "pending" else task_text(user_id, task_id)
        await show_screen(user_id, screen, panel_keyboard(user_id), "menu", chat_id)
        return

    # Открытый вопрос викторины: запоминаем ответ и показываем вопрос с кнопками
    quiz = QUIZZES.get(task_id)
    if quiz and quiz.get("open"):
        if not text or len(text) > MAX_OPEN_ANSWER_LEN:
            await message.answer(TEXT_BAD_OPEN)
            return
        await context.update_data(open_answer=text)
        await context.set_state(Submit.choosing)
        await show_screen(
            user_id, quiz_text(task_id, 0), quiz_keyboard(task_id, 0, ""), "flow", chat_id
        )
        return

    # Обычное задание: проверяем формат и размер
    problem = validate_answer(attachments, TASKS[task_id].get("min_files", 1))
    if problem:
        await message.answer(problem)
        return

    sub_id = create_submission(
        user_id, task_id, text[:MAX_LINK_LEN], message.body.mid if attachments else None
    )
    await context.clear()

    # Подтверждение уходит вместе с панелью — панель остаётся последним сообщением
    await show_screen(
        user_id, pending_text(task_id), panel_keyboard(user_id), "menu", chat_id, image="pending"
    )
    await send_to_moderation(sub_id)
# endregion


# region Вопросы организаторам (сторона участника)
IDLE_VIEWS = ("menu", "notice", "profile", "rules", "shop", "orders")   # экраны, которые можно заменить


async def show_home(user_id: int, chat_id: int | None = None):
    """Главный экран по этапу: задания (до рассылок) / ожидание витрины / витрина."""
    if shop_open(user_id):
        await show_screen(user_id, shop_text(user_id), shop_keyboard(user_id), "shop", chat_id)
    else:
        await show_screen(
            user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id,
            image=panel_image(user_id),
        )


async def reshow_home_if_idle(user_id: int):
    """После сообщения организаторов возвращаем панель вниз, если человек сейчас не вводит ответ."""
    _, view = get_panel(user_id)
    if view in IDLE_VIEWS:
        await show_home(user_id)


def question_card_text(qid: int, user_id: int, text: str, answer: tuple[str, str] | None = None) -> str:
    fio, phone = get_user_info(user_id)
    card = (
        f"❓ Вопрос #{qid}\n"
        f"Участник: {fio or '—'}\n"
        f"Телефон: {phone or '—'}\n"
        f"ID: {user_id}\n\n{text}"
    )
    if answer:
        return f"{card}\n\n✅ Ответ ({answer[1]}): {answer[0]}"
    return f"{card}\n\n↩️ Ответьте на это сообщение (reply) — ответ уйдёт участнику. Или: /reply {qid} текст"


async def send_question_to_admins(qid: int):
    if not ADMIN_CHAT_ID:
        logging.warning("ADMIN_CHAT_ID не задан — вопрос #%s не ушёл в чат модераторов", qid)
        return
    _, user_id, text, src_mid, _, _ = get_question(qid)
    if src_mid:   # вложения участника — «голой» пересылкой, затем карточка
        try:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, link=NewMessageLink(type="forward", mid=src_mid))
        except Exception:
            logging.exception("Не удалось переслать вложения к вопросу #%s", qid)
    sent = await bot.send_message(chat_id=ADMIN_CHAT_ID, text=question_card_text(qid, user_id, text))
    set_question_card(qid, extract_mid(sent))


async def relay_answer(qid: int, answer_text: str, by: str) -> str:
    """Отправляет ответ организатора участнику. -> 'ok' | 'missing' | 'failed'"""
    question = get_question(qid)
    if not question:
        return "missing"
    _, user_id, text, _, card_mid, _ = question
    try:
        await bot.send_message(user_id=user_id, text=TEXT_ANSWER.format(text=answer_text))
    except Exception:
        logging.exception("Не удалось доставить ответ на вопрос #%s", qid)
        return "failed"
    mark_answered(qid, answer_text, by)
    await edit_message_safe(card_mid, question_card_text(qid, user_id, text, (answer_text, by)), None)
    try:
        await reshow_home_if_idle(user_id)
    except Exception:
        logging.warning("Не удалось вернуть панель после ответа", exc_info=True)
    return "ok"


@dp.message_callback(F.callback.payload == "ask")
async def on_ask(event: MessageCallback, context: MemoryContext):
    """Кнопка «Задать вопрос» (есть только на экране «Мои баллы»)."""
    user_id = event.callback.user.user_id
    await context.clear()
    await context.set_state(Ask.waiting)
    track(event, user_id, "flow")   # чтобы экран «Мои баллы» удалился при показе нового
    await show_screen(user_id, TEXT_ASK_QUESTION, home_keyboard(), "flow", image="ask")


@dp.message_callback(F.callback.payload == "home")
async def on_home(event: MessageCallback, context: MemoryContext):
    """«Назад» с экрана вопроса: на главный экран своего этапа."""
    user_id = event.callback.user.user_id
    await context.clear()
    track(event, user_id, "menu")   # чтобы это сообщение удалилось при показе нового
    await show_home(user_id)


@dp.message_created(Ask.waiting)
async def on_question(event: MessageCreated, context: MemoryContext):
    message = event.message
    chat_id = message.recipient.chat_id
    user_id = message.sender.user_id
    text = (message.body.text or "").strip()
    attachments = message.body.attachments or []
    if not text and not attachments:
        return
    if len(text) > MAX_QUESTION_LEN:
        await message.answer(TEXT_QUESTION_TOO_LONG)
        return

    await context.clear()
    if count_recent_questions(user_id) >= QUESTIONS_PER_HOUR:
        await message.answer(TEXT_QUESTION_LIMIT)
    else:
        qid = create_question(user_id, text or "(без текста, см. вложение)",
                              message.body.mid if attachments else None)
        await send_question_to_admins(qid)
        await message.answer(TEXT_QUESTION_SENT)
    await show_home(user_id, chat_id)
# endregion


# region Модерация: отправка карточки в чат админов
async def send_to_moderation(sub_id: int):
    if not ADMIN_CHAT_ID:
        logging.warning("ADMIN_CHAT_ID не задан — отправка #%s не ушла в чат модераторов", sub_id)
        return

    sub = get_submission(sub_id)
    if not sub:
        return
    user_id, task_id, content, _, src_mid = sub
    fio, phone = get_user_info(user_id)

    text = (
        f"📝 Проверка #{sub_id}\n"
        f"Участник: {fio or '—'}\n"
        f"Телефон: {phone or '—'}\n"
        f"ID: {user_id}\n"
        f"Задание: {TASKS[task_id]['title']}\n"
        f"Нужно: {TASKS[task_id].get('brief', '—')}"
    )
    if content:
        text += f"\n\nОтвет:\n{content}"
    if TASKS[task_id].get("reference"):
        text += f"\n\nЭталон: {TASKS[task_id]['reference']}"

    # MAX не принимает кнопки в сообщении-пересылке (ошибка errors.forward.attaches.not-empty),
    # поэтому вложения участника пересылаются ОТДЕЛЬНЫМ сообщением, а карточка с кнопками идёт следом.
    note = ""
    if src_mid:
        # MAX не принимает в пересылке ни кнопки, ни текст (ошибки forward.attaches / forward.text),
        # поэтому она уходит «голой», а карточка с кнопками — сразу следом.
        try:
            await bot.send_message(
                chat_id=ADMIN_CHAT_ID, link=NewMessageLink(type="forward", mid=src_mid)
            )
        except Exception:
            logging.exception("Не удалось переслать вложения")
            note = "\n\n⚠️ Вложения переслать не удалось — смотрите у участника"

    await bot.send_message(
        chat_id=ADMIN_CHAT_ID, text=text + note,
        attachments=[moderation_keyboard(sub_id, task_id)],
    )
# endregion


# region Модерация: кнопки «зачесть / отклонить»
@dp.message_callback(F.callback.payload.startswith("mod:"))
async def on_moderation(event: MessageCallback):
    moderator = event.callback.user
    if ADMIN_IDS and moderator.user_id not in ADMIN_IDS:
        await event.answer(notification="Нет прав на проверку")
        return

    _, sub_id, action, points = event.callback.payload.split(":")
    sub_id, points = int(sub_id), int(points)

    sub = get_submission(sub_id)
    if not sub:
        return
    user_id, task_id = sub[0], sub[1]

    approved = action == "ok"
    if approved and points not in TASKS[task_id]["points"]:
        return

    name = display_name(moderator)
    ok = decide_submission(
        sub_id, "approved" if approved else "rejected", points if approved else 0, name
    )
    if not ok:
        await event.answer(notification="Это задание уже проверено")
        return

    # Карточка: дописываем вердикт и убираем кнопки
    verdict = f"✅ Зачтено, +{balls(points)}" if approved else "❌ Отклонено"
    await event.message.edit(
        text=f"{event.message.body.text or ''}\n\n{verdict} — {name}", attachments=[]
    )

    # Участнику: без лишних сообщений — результат приходит экраном с панелью под ним
    # и заменяет предыдущий экран. Если человек сейчас вводит ответ — не перебиваем.
    _, view = get_panel(user_id)
    if view in ("menu", "notice", "profile", "rules"):
        if approved:
            template = TEXT_CONGRATS_SHORT if tasks_closed(user_id) else TEXT_CONGRATS
            await show_screen(user_id, template.format(points=balls(points)),
                              panel_keyboard(user_id), "notice",
                              image=(f"approved_{points}", "approved"))
            return
        if task_id not in QUIZZES:
            text = TEXT_REJECTED.format(short=TASKS[task_id]["short"])
            if not tasks_closed(user_id):
                text += f"\n{TEXT_REJECTED_RETRY}"
            await show_screen(user_id, text, panel_keyboard(user_id), "notice", image="rejected")
            return
    await refresh_panel(user_id)
# endregion


# region Служебное: id чата модераторов
@dp.bot_added()
async def on_bot_added(event: BotAdded):
    chat_id = event.chat_id
    logging.info("Бота добавили в чат, chat_id = %s", chat_id)

    # Пока ADMIN_CHAT_ID не задан, разрешаем любой чат — иначе не узнать его id.
    if not ADMIN_CHAT_ID or chat_id == ADMIN_CHAT_ID:
        return

    # Чужой чат: бот сам выходит из него
    leave = getattr(bot, "delete_me_from_chat", None)
    if leave is None:
        logging.warning("В этой версии maxapi нет метода выхода из чата, чат %s остался", chat_id)
        return
    try:
        await leave(chat_id=chat_id)
        logging.info("Вышел из постороннего чата %s", chat_id)
    except Exception:
        logging.exception("Не удалось выйти из чата %s", chat_id)
# endregion


# region Обработка ошибок
_last_alert = 0.0


async def report_error(error: Exception):
    """Пишет ошибку в лог и (не чаще раза в ERROR_ALERT_INTERVAL с) сообщает в чат админов."""
    global _last_alert
    logging.error("Ошибка в обработчике", exc_info=error)

    if ADMIN_CHAT_ID and time.time() - _last_alert > ERROR_ALERT_INTERVAL:
        _last_alert = time.time()
        try:
            await bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"⚠️ Ошибка бота: {type(error).__name__}: {str(error)[:300]}",
            )
        except Exception:
            logging.exception("Не удалось отправить уведомление об ошибке")


_recent_events: dict = {}


def _is_duplicate(event_object) -> bool:
    """Повторная доставка того же события или двойное нажатие одной и той же кнопки."""
    now = time.monotonic()
    callback = getattr(event_object, "callback", None)
    if callback is not None:
        user_id = getattr(getattr(callback, "user", None), "user_id", None)
        checks = [
            (("cb", getattr(callback, "callback_id", None)), 60.0),
            (("tap", user_id, getattr(callback, "payload", None)), TAP_COOLDOWN),
        ]
    else:
        mid = getattr(getattr(getattr(event_object, "message", None), "body", None), "mid", None)
        checks = [(("msg", mid), 60.0)]

    checks = [(key, window) for key, window in checks if None not in key]
    for key, window in checks:
        last = _recent_events.get(key)
        if last is not None and now - last < window:
            return True
    for key, _ in checks:
        _recent_events[key] = now
    if len(_recent_events) > 5000:   # чистим старое
        for key in [k for k, t in _recent_events.items() if now - t > 120]:
            _recent_events.pop(key, None)
    return False


if BaseMiddleware:
    class ErrorMiddleware(BaseMiddleware):
        """Ловит исключения в любом обработчике, отсекает дубли событий и пишет в лог медленные обработчики."""

        async def __call__(self, handler, event_object, data):
            if _is_duplicate(event_object):
                return
            started = time.perf_counter()
            try:
                return await handler(event_object, data)
            except Exception as error:
                await report_error(error)
            finally:
                elapsed = time.perf_counter() - started
                if elapsed > SLOW_HANDLER_SECONDS:
                    callback = getattr(event_object, "callback", None)
                    what = getattr(callback, "payload", None) or type(event_object).__name__
                    logging.warning("Медленный обработчик: %.1f с (%s)", elapsed, what)
# endregion


# region Витрина: покупка с подтверждением
_last_buy: dict[int, float] = {}


@dp.message_callback(F.callback.payload == "shop")
async def on_shop(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "shop")   # до открытия — просмотр цен, после — покупки
    await event.message.edit(text=shop_text(user_id), attachments=[shop_keyboard(user_id)])


@dp.message_callback(F.callback.payload.startswith("lot:"))
async def on_lot(event: MessageCallback):
    """Выбор подарка -> экран подтверждения (или причина, почему нельзя)."""
    user_id = event.callback.user.user_id
    lot = LOTS_BY_ID.get(event.callback.payload.split(":", 1)[1])
    if not lot or not shop_open(user_id):
        return
    track(event, user_id, "flow")

    balance = get_balance(user_id)
    if is_sold_out(lot):
        text, keyboard = TEXT_SOLD_OUT, shop_back_keyboard()
    elif count_orders(user_id) >= MAX_ORDERS:
        text, keyboard = TEXT_LIMIT.format(max=MAX_ORDERS), shop_back_keyboard()
    elif balance < lot["price"]:
        text = TEXT_NO_POINTS.format(price=balls(lot["price"]), balance=balls(balance))
        keyboard = shop_back_keyboard()
    else:
        text = TEXT_CONFIRM.format(
            name=lot["name"], price=balls(lot["price"]), balance=balance, left=balance - lot["price"]
        )
        keyboard = confirm_keyboard(lot["id"])
    await event.message.edit(text=text, attachments=[keyboard])


@dp.message_callback(F.callback.payload.startswith("buy:"))
async def on_buy(event: MessageCallback):
    """Подтверждение покупки. Баллы и лимит проверяются в БД атомарно."""
    user_id = event.callback.user.user_id
    lot = LOTS_BY_ID.get(event.callback.payload.split(":", 1)[1])
    if not lot or not shop_open(user_id):
        return

    now = time.time()                       # защита от двойного нажатия
    if now - _last_buy.get(user_id, 0.0) < 2:
        return
    _last_buy[user_id] = now

    result, order_id = create_order(user_id, lot["id"], lot["price"], lot["stock"])
    if result == "ok":
        track(event, user_id, "orders")   # чтобы экран подтверждения удалился
        paid = TEXT_ORDERED.format(name=lot["name"], price=balls(lot["price"]), left=get_balance(user_id))
        await show_screen(
            user_id, f"{paid}\n\n{orders_text(user_id)}", orders_keyboard(), "orders", image="gift"
        )
        return
    track(event, user_id, "flow")

    if result == "stock":
        text, keyboard = TEXT_SOLD_OUT, shop_back_keyboard()
    elif result == "limit":
        text, keyboard = TEXT_LIMIT.format(max=MAX_ORDERS), shop_back_keyboard()
    else:
        text = TEXT_NO_POINTS.format(price=balls(lot["price"]), balance=balls(get_balance(user_id)))
        keyboard = shop_back_keyboard()
    await event.message.edit(text=text, attachments=[keyboard])


@dp.message_callback(F.callback.payload == "orders")
async def on_orders(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "orders")
    await event.message.edit(text=orders_text(user_id), attachments=[orders_keyboard()])
# endregion


# region Рассылки по расписанию
# Время указывается по Москве (Чебоксары: UTC+3, переходов на летнее время нет).
SCHEDULE = [
    {
        "id": "thanks",
        "at": TASKS_CLOSE_AT,            # с этого момента приём заданий закрыт
        "text": TEXT_BROADCAST_THANKS,
        "then": "panel",                 # само сообщение становится панелью (кнопки под ним)
    },
    {
        "id": "exchange",
        "at": SHOP_OPEN_AT,              # с этого момента открыта витрина
        "text": TEXT_BROADCAST_EXCHANGE,
        "then": "shop",                  # под сообщением — таблица подарков и баллы
    },
]


async def send_broadcast_item(item: dict, user_id: int):
    """Рассылка. Для «thanks» кнопки (витрина скоро + профиль) идут прямо под сообщением,
    отдельная панель не отправляется. Для «exchange» ниже идёт экран витрины."""
    if item.get("then") == "shop":
        await send_screen_message(item["text"], image="gift", user_id=user_id)
        await show_screen(user_id, shop_text(user_id), shop_keyboard(user_id), "shop")
    else:
        await show_screen(
            user_id, item["text"], closed_panel_keyboard(user_id), "menu", image="thanks"
        )


_RUNNING: set[str] = set()   # рассылки, которые идут прямо сейчас


def load_forced_phases():
    """После перезапуска вспоминаем, какие рассылки уже начинались (в том числе вручную)."""
    for item in SCHEDULE:
        started, _, _ = broadcast_state(item["id"])
        if started:
            FORCED_PHASE.add(item["id"])


async def run_broadcast(item: dict):
    """Отправка рассылки всем, кому она ещё не ушла. Можно прервать и продолжить (после сбоя)."""
    if item["id"] in _RUNNING:
        return
    _RUNNING.add(item["id"])
    try:
        mark_broadcast_started(item["id"])    # этап наступил для всех — сразу, до отправки
        FORCED_PHASE.add(item["id"])

        ok = failed = 0
        for user_id in unsent_users(item["id"]):
            try:
                await send_broadcast_item(item, user_id)
                ok += 1
            except Exception:
                failed += 1
                logging.exception("Рассылка %s: не доставлено %s", item["id"], user_id)
            mark_sent(item["id"], user_id)   # не повторяем и при ошибке
            await asyncio.sleep(0.05)        # мягко по лимитам API
        mark_broadcast_done(item["id"])

        summary = f"📣 Рассылка «{item['id']}»: отправлено {ok}, ошибок {failed}"
        logging.info(summary)
        if ADMIN_CHAT_ID:
            try:
                await bot.send_message(chat_id=ADMIN_CHAT_ID, text=summary)
            except Exception:
                logging.exception("Не удалось отправить итог рассылки")
    finally:
        _RUNNING.discard(item["id"])


async def scheduler():
    """Раз в 20 секунд проверяет, не пора ли отправить рассылку."""
    while True:
        try:
            now = datetime.now(MSK)
            for item in SCHEDULE:
                if now < item["at"] or is_broadcast_done(item["id"]) or item["id"] in _RUNNING:
                    continue
                if now - item["at"] > timedelta(hours=MAX_LATE_HOURS):
                    logging.warning("Рассылка %s пропущена: время прошло более %s ч назад",
                                    item["id"], MAX_LATE_HOURS)
                    mark_broadcast_done(item["id"])
                    continue
                await run_broadcast(item)
        except Exception as error:
            await report_error(error)
        await asyncio.sleep(20)


# endregion


# region Запасной обработчик: панель всегда внизу
# Должен быть ПОСЛЕДНИМ среди обработчиков сообщений: срабатывает, только если
# ничего другого не подошло (любой текст, фото, неизвестная команда).
# Старая панель удаляется, так что их не накапливается; плюс защита от флуда.
@dp.message_created()
async def on_any_message(event: MessageCreated):
    chat_id = event.message.recipient.chat_id
    if chat_id == ADMIN_CHAT_ID:
        return   # в чате модераторов панель не нужна
    user_id = event.message.sender.user_id
    if not is_registered(user_id) or too_fast(user_id):
        return
    await show_screen(
        user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id,
        image=panel_image(user_id),
    )
# endregion


# region Запуск (вебхук)
webhook_kwargs = {"dp": dp, "bot": bot}
if WEBHOOK_SECRET and "secret" in inspect.signature(FastAPIMaxWebhook.__init__).parameters:
    webhook_kwargs["secret"] = WEBHOOK_SECRET
webhook = FastAPIMaxWebhook(**webhook_kwargs)


async def subscribe_webhook():
    """Говорит MAX, куда слать события. Сначала удаляются старые подписки (например, на другой
    адрес), потом создаётся нужная. Всё через методы библиотеки (она сама доверяет сертификату MAX).
    Запускается через несколько секунд после старта сервера, с повторами: адрес уже должен отвечать."""
    if not WEBHOOK_URL:
        logging.error("Адрес вебхука не задан: включите «Использовать домен» или укажите WEBHOOK_URL")
        return

    for attempt in range(1, 6):
        await asyncio.sleep(3)
        try:
            try:
                await bot.delete_webhook()
            except Exception:
                logging.warning("Не удалось удалить старые подписки (не критично)", exc_info=True)

            kwargs = {"url": WEBHOOK_URL}
            if WEBHOOK_SECRET:
                kwargs["secret"] = WEBHOOK_SECRET
            await bot.subscribe_webhook(**kwargs)
            logging.info("Вебхук подключён: %s", WEBHOOK_URL)
            return
        except Exception:
            logging.exception("Подписка на вебхук не удалась (попытка %s из 5)", attempt)
            await asyncio.sleep(5)
    logging.error("Вебхук так и не подключился. Проверьте адрес %s и токен", WEBHOOK_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()
    load_forced_phases()

    # Защита от падений в обработчиках
    if BaseMiddleware:
        register = getattr(dp, "register_inner_middleware", None) or getattr(dp, "middleware", None)
        if register:
            register(ErrorMiddleware())
    else:
        logging.warning("BaseMiddleware не найден — перехват ошибок в обработчиках отключён")

    try:
        await bot.set_commands(
            BotCommand(name="start", description="Начать"),
            BotCommand(name="profile", description="Мои баллы"),
        )   # /queue, /chatid и /preview намеренно не в списке — это служебные команды
    except Exception:
        logging.exception("Не удалось задать список команд")

    async with webhook.lifespan(app):
        tasks = [
            asyncio.create_task(scheduler()),          # рассылки по расписанию
            asyncio.create_task(subscribe_webhook()),  # подписка на события MAX
        ]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()


app = FastAPI(lifespan=lifespan)
webhook.setup(app, path=WEBHOOK_PATH)

if not WEBHOOK_SECRET:
    logging.warning("WEBHOOK_SECRET не задан: любой, кто знает адрес вебхука, сможет слать боту «события»")


@app.middleware("http")
async def check_secret(request, call_next):
    """Пропускает к вебхуку только запросы с нашим секретом от MAX."""
    if (
        WEBHOOK_SECRET
        and request.method == "POST"
        and request.url.path == WEBHOOK_PATH
        and request.headers.get("X-Max-Bot-Api-Secret") != WEBHOOK_SECRET
    ):
        return JSONResponse({"detail": "forbidden"}, status_code=403)
    return await call_next(request)


@app.get("/health")
async def health():
    """Для проверки хостингом и вручную: https://ваш-домен/health"""
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
# endregion