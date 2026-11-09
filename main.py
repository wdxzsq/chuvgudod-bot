"""
Бот «Чувгуша» для MAX — вариант для проверки на компьютере (long polling).

Запуск:
    pip install maxapi python-dotenv
    файл .env рядом с main.py:
        MAX_BOT_TOKEN=токен_бота
        ADMIN_CHAT_ID=id_чата_модераторов   (узнать командой /chatid)
    python main.py

Для хостинга с вебхуками — main2.py. Логика у файлов одна и та же,
отличается только блок «Запуск» внизу.

Блоки помечены # region / # endregion — их можно сворачивать
(PyCharm и VS Code поддерживают это из коробки).
"""

# region Импорты
import asyncio
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone

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

DB_PATH = os.getenv("DB_PATH", "users.db")
PORTRAIT_PATH = "chuvgusha.png"   # необязательно: портрет Чувгуши рядом с main.py

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
    "— 🟩 Клетки — простые задания, 5-10 б\n"
    "— 🟨 Клетки — средняя сложность, по 15 баллов\n"
    "— 🟥 Клетки — самые прокачанные, по 30 баллов\n\n"
    "Ты сам выбираешь, с чего начать — задания можно выполнять в любом порядке. "
    "После отправки ответа следи за статусом:\n"
    "— ⏳ задание отправлено на проверку\n"
    "— ❌ условия не соблюдены, задание не зачтено\n"
    "— ✅ всё супер, задание выполнено правильно\n\n"
    "Создавай смело — впереди много классного!\n\n"
    "На заработанные баллы сможешь купить много разного мерча университета."
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
TEXT_BAD_LINK = "Пришли ссылку на пост текстом, одним сообщением (не длиннее 500 символов)."
TEXT_BAD_PHOTO = "Пришли снимок экрана картинкой."
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
BUTTON_WAIT_SHOP = "⏳ Витрина скоро появится"
TEXT_WAIT_SHOP = "Витрина откроется в {time}"
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
TEXT_CONFIRM = "Оплатить «{name}» — {price} б?\n\nТвои баллы: {balance} → останется {left}"
TEXT_NO_POINTS = "Не хватает баллов: нужно {price}, у тебя {balance}."
TEXT_LIMIT = "Можно выбрать не более {max} позиций."
TEXT_ORDERED = "✅ Оплачено: «{name}» — {price} б. Осталось баллов: {left}."
TEXT_ORDERS_EMPTY = "Пока ничего не куплено."
TEXT_ORDERS = (
    "🧾 Мои покупки\n{fio}\n\n{items}\n\n"
    "Оплачено баллами: {total} б\n\n"
    "Покажи этот экран администратору у стола с мерчом."
)
TEXT_PANEL_SHOP = "Витрина открыта — выбирай подарки!"
TEXT_REJECTED = "❌ Задание {short}: условия не соблюдены, задание не зачтено."
TEXT_REJECTED_RETRY = "Попробуй ещё раз: открой задание и отправь новый ответ."
TEXT_RETRY = "Попробуй ещё раз — отправь новый ответ сообщением."
TEXT_CONGRATS = (
    "Поздравляю! Ты получил {points} баллов за выполнение задания Медиабинго! "
    "Продолжай в том же духе — впереди ещё много интересных заданий и возможностей "
    "увеличить свой рейтинг!"
)
# После закрытия заданий «впереди ещё задания» уже неверно — короткий вариант
TEXT_CONGRATS_SHORT = "Поздравляю! Ты получил {points} баллов за выполнение задания Медиабинго!"
# endregion


# region Задания (сценарий)
TAIL = "После проверки модераторами, тебе будут начислены баллы."

# short  — подпись на кнопке
# icon   — значок сложности (показывается, пока нет статуса)
# points — варианты начисления для модератора (у викторин можно дать 5 или 10)
TASKS = {
    "1-1": {
        "short": "1.1", "icon": "🟩", "points": [5, 10],
        "title": "1 уровень · Задание 1",
        "text": (
            "Ответь на вопрос и получи 5 б за правильный ответ. "
            "За два верных ответа получишь 10 б.\n\n"
            "5 б. – Сколько факультетов в ЧГУ?\n"
            "5 б. – Кто является маскотом ЧувГУ?"
        ),
    },
    "1-2": {
        "short": "1.2", "icon": "🟩", "points": [10],
        "title": "1 уровень · Задание 2",
        "text": (
            "10 б. –\n"
            "1. Сними историю о том, как проходит День открытых дверей ЧувГУ — "
            "покажи атмосферу. Выложи в личном аккаунте ВК или Макс с хештегом #мывчувгу\n"
            "2. Отправь получившуюся ссылку в чат-бот.\n"
            f"3. {TAIL}"
        ),
    },
    "1-3": {
        "short": "1.3", "icon": "🟩", "points": [5],   # модератор оценивает только вопрос про Ульянова
        "reference": "Илья Николаевич Ульянов — педагог-просветитель, директор народных "
                     "училищ Симбирской губернии (отец В. И. Ленина)",
        "title": "1 уровень · Задание 3",
        "text": (
            "Ответь на вопрос и получи 5 б за правильный ответ. "
            "За два верных ответа получишь 10 б.\n\n"
            "5 б. – Кто такой Ульянов И.Н., имя которого носит университет? "
            "Напиши тремя словами\n"
            "5 б. – Где находится Приемная комиссия ЧувГУ? "
            "(Кампус «Университетский» (ул. Университетская, 38) или "
            "Кампус «Технопарк» (пр. Московский, 15))"
        ),
    },
    "2-1": {
        "short": "2.1", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 1",
        "text": (
            "15 б. –\n"
            "1. Сделай фото с синим предметом, выложи в своём профиле ВК или Макс "
            "с хештегом #мывчувгу\n"
            "2. Пришли ссылку на пост в этот бот\n"
            f"3. {TAIL}"
        ),
    },
    "2-2": {
        "short": "2.2", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 2",
        "text": (
            "15 б. –\n"
            "1. Сделай селфи с Ульяном, выложи в своём профиле ВК или Макс "
            "с хештегом #мывчувгу\n"
            "2. Пришли ссылку на пост в этот бот\n"
            f"3. {TAIL}"
        ),
    },
    "2-3": {
        "short": "2.3", "icon": "🟨", "points": [15],
        "title": "2 уровень · Задание 3",
        "text": (
            "15 б. –\n"
            "1. Подпишись на каналы ЧувГУ в ВК и МАX:\n"
            "https://vk.com/priemkomchgu\n"
            "https://max.ru/chuvsu\n"
            "2. Пришли снимки экрана с подпиской в этот бот\n"
            f"3. {TAIL}"
        ),
    },
    "3-1": {
        "short": "3.1", "icon": "🟥", "points": [30],
        "title": "3 уровень · Задание 1",
        "text": (
            "30 б. –\n"
            "1. Сделай фото, где ты на площадках 3 факультетов, выложи их в личном "
            "аккаунте с хештегом #мывчувгу\n"
            "2. Пришли ссылку на пост в этот бот\n"
            f"3. {TAIL}"
        ),
    },
    "3-2": {
        "short": "3.2", "icon": "🟥", "points": [30],
        "title": "3 уровень · Задание 2",
        "text": (
            "30 б. –\n"
            "Посети официальное мероприятие в концертном зале Дворца ЧГУ. "
            "Поделись в своих социальных сетях инсайтом или расскажи, почему нужно "
            "правильно выбирать вуз для поступления?\n"
            "1. Опубликуй результат в своём профиле ВКонтакте или Макс и не забудь "
            "указать хештеги #мывчувгу\n"
            "2. Отправь получившуюся ссылку сюда в чат-бот.\n"
            f"3. {TAIL}"
        ),
    },
    "extra": {
        "short": "Доп.", "icon": "🟩", "points": [10],
        "title": "Дополнительные баллы",
        "text": (
            "10 б. –\n"
            "1. Сделай фото на площадке Военного учебного центра, выложи в своём профиле "
            "ВК или Макс с хештегом #мывчувгу\n"
            "2. Отправь получившуюся ссылку сюда в чат-бот.\n"
            f"3. {TAIL}"
        ),
    },
}

# Краткая суть задания (показывается модераторам в карточке) и что принимаем в ответ:
#   link — ссылка текстом, photo — картинка (скриншот), text — короткий текст
TASK_META = {
    "1-2": ("История о Дне открытых дверей ЧувГУ, пост в ВК/Макс с #мывчувгу", "link"),
    "1-3": ("Кто такой Ульянов И.Н. (ответ тремя словами)", "text"),
    "2-1": ("Фото с синим предметом, пост в ВК/Макс с #мывчувгу", "link"),
    "2-2": ("Селфи с Ульяном, пост в ВК/Макс с #мывчувгу", "link"),
    "2-3": ("Подписка на каналы ЧувГУ в ВК и MAX (скриншоты)", "photo"),
    "3-1": ("Фото на площадках 3 факультетов, пост с #мывчувгу", "link"),
    "3-2": ("Пост (ВК/Макс, #мывчувгу) о мероприятии в концертном зале Дворца ЧГУ", "link"),
    "extra": ("Фото на площадке Военного учебного центра, пост с #мывчувгу", "link"),
}
for _task_id, (_brief, _accept) in TASK_META.items():
    TASKS[_task_id]["brief"] = _brief
    TASKS[_task_id]["accept"] = _accept

# Викторины. Вопросы с кнопками проверяются автоматически, на модерацию не уходят.
#   points  — баллы за один верный ответ
#   correct — номер правильного варианта (0 = первый)
#   open    — вопрос со свободным ответом: уходит на модерацию (только у 1.3)
# Данные собраны из открытых источников — перед запуском сверьте с организаторами.
QUIZZES = {
    "1-1": {
        "intro": "Ответь на вопрос и получи 5 б за правильный ответ. "
                 "За два верных ответа получишь 10 б.",
        "points": 5,
        "questions": [
            {"text": "Сколько факультетов в ЧГУ?",
             "options": ["12", "15", "18", "20"], "correct": 1},
            {"text": "Кто является маскотом ЧувГУ?",
             "options": ["Волк", "Сова Ульян", "Лис"], "correct": 1},
        ],
    },
    "1-3": {
        "intro": "Ответь на вопрос и получи 5 б за правильный ответ. "
                 "За два верных ответа получишь 10 б.",
        "points": 5,
        "open": {
            "text": "Кто такой Ульянов И.Н., имя которого носит университет? "
                    "Напиши тремя словами",
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
    "pending": "⏳ задание отправлено на проверку",
    "rejected": "❌ условия не соблюдены, задание не зачтено",
    "approved": "✅ всё супер, задание выполнено правильно",
}
# endregion


# region Витрина: товары
# Список и стоимость из сценария («Виртуальный обменник»).
# short — короткая подпись для кнопки (если название длинное).
LOTS = [
    {"id": "pen", "name": "Ручка", "price": 35},
    {"id": "pencil", "name": "Карандаш", "price": 35},
    {"id": "ruler", "name": "Линейка", "price": 35},
    {"id": "sharpener", "name": "Точилка", "price": 45},
    {"id": "notebook", "name": "Блокнот", "price": 55},
    {"id": "pencilcase", "name": "Пенал", "price": 90},
    {"id": "keychain", "name": "Брелок", "price": 115},
    {"id": "mug", "name": "Кружка", "price": 150},
    {"id": "bottle", "name": "Бутылка", "price": 155},
    {"id": "shopper", "name": "Шопер", "price": 160},
    {"id": "waistbag", "name": "Поясная сумка", "short": "Сумка", "price": 165},
    {"id": "tshirt", "name": "Футболка", "price": 175},
    {"id": "backpack", "name": "Рюкзак", "price": 185},
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


def delete_user_data(user_id: int):
    """Удаляет все данные одного участника (для проверки регистрации заново)."""
    with sqlite3.connect(DB_PATH) as db:
        for table in ("submissions", "orders", "broadcast_log", "users"):
            db.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))


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


def create_order(user_id: int, lot_id: str, price: int):
    """Бронь одной транзакцией: баллы и лимит позиций проверяются атомарно.
    -> ('ok', id) | ('limit'|'points', None)"""
    db = sqlite3.connect(DB_PATH, isolation_level=None)
    try:
        db.execute("BEGIN IMMEDIATE")
        earned = db.execute(
            "SELECT COALESCE(SUM(points), 0) FROM submissions "
            "WHERE user_id = ? AND status = 'approved'", (user_id,)
        ).fetchone()[0]
        spent, count = db.execute(
            "SELECT COALESCE(SUM(price), 0), COUNT(*) FROM orders "
            "WHERE user_id = ?", (user_id,)
        ).fetchone()

        if count >= MAX_ORDERS:
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


def create_quiz_result(user_id: int, task_id: str, points: int) -> bool:
    """Результат викторины записывается сразу. Одна попытка: False, если запись уже есть."""
    status = "approved" if points > 0 else "rejected"
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "INSERT INTO submissions (user_id, task_id, status, points, moderator, decided_at) "
            "SELECT ?, ?, ?, ?, 'авто', datetime('now') "
            "WHERE NOT EXISTS (SELECT 1 FROM submissions WHERE user_id = ? AND task_id = ?)",
            (user_id, task_id, status, points, user_id, task_id),
        )
        return cur.rowcount > 0


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


def tasks_closed(user_id: int | None = None) -> bool:
    return datetime.now(MSK) >= TASKS_CLOSE_AT or PREVIEW_PHASE.get(user_id) in ("closed", "shop")


def shop_open(user_id: int | None = None) -> bool:
    return datetime.now(MSK) >= SHOP_OPEN_AT or PREVIEW_PHASE.get(user_id) == "shop"


def panel_text(user_id: int | None = None) -> str:
    """Текст главного экрана. После закрытия заданий это текст первой рассылки —
    поэтому «Назад» из профиля возвращает именно к нему."""
    if shop_open(user_id):
        return TEXT_PANEL_SHOP
    if tasks_closed(user_id):
        return TEXT_BROADCAST_THANKS
    return TEXT_PANEL


def closed_panel_keyboard(user_id: int | None = None):
    """После закрытия заданий: кнопка витрины (или ожидания) и профиль."""
    kb = InlineKeyboardBuilder()
    if shop_open(user_id):
        kb.row(CallbackButton(text=BUTTON_SHOP, payload="shop"))
    else:
        kb.row(CallbackButton(text=BUTTON_WAIT_SHOP, payload="wait_shop"))   # «неактивная» кнопка
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
        f"{quiz['points']} б. – {question['text']}"
    )
    # Длинные варианты (адреса) выводим в тексте, а на кнопках остаются короткие подписи
    if question.get("details"):
        text += "\n\n" + "\n".join(question["details"])
    return text


def open_question_text(task_id: str) -> str:
    quiz = QUIZZES[task_id]
    return (
        f"{TASKS[task_id]['title']}\n\n{quiz['intro']}\n\n"
        f"{quiz['open']['points']} б. – {quiz['open']['text']}"
    )


def pending_text(task_id: str) -> str:
    """Короткое подтверждение после отправки ответа."""
    return f"{TASKS[task_id]['title']}\n\nСтатус: {STATUS_TEXT['pending']}"


def quiz_keyboard(task_id: str, qi: int, score: int):
    """Варианты ответа; в payload несём номер вопроса и набранные баллы (без состояния)."""
    kb = InlineKeyboardBuilder()
    for i, option in enumerate(QUIZZES[task_id]["questions"][qi]["options"]):
        kb.row(CallbackButton(text=option, payload=f"quiz:{task_id}:{qi}:{score}:{i}"))
    kb.row(CallbackButton(text=BUTTON_BACK, payload="menu"))
    return kb.as_markup()


def moderation_keyboard(sub_id: int, task_id: str):
    """Кнопки для чата модераторов."""
    points = TASKS[task_id]["points"]
    kb = InlineKeyboardBuilder()
    if len(points) == 1:
        kb.row(CallbackButton(text=f"✅ Зачесть +{points[0]} б", payload=f"mod:{sub_id}:ok:{points[0]}"))
    else:
        kb.row(*[
            CallbackButton(text=f"✅ +{p} б", payload=f"mod:{sub_id}:ok:{p}") for p in points
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


def task_text(user_id: int, task_id: str) -> str:
    task = TASKS[task_id]
    text = f"{task['title']}\n\n{task['text']}"
    status = get_statuses(user_id).get(task_id)
    if not status:
        return text

    if task_id in QUIZZES and status != "pending":
        earned, total = quiz_earned(user_id, task_id), quiz_total(task_id)
        if earned >= total:
            line = STATUS_TEXT["approved"]
        elif earned == 0:
            line = STATUS_TEXT["rejected"]
        else:
            line = "🔸 верно частично"
        return f"{text}\n\nСтатус: {line}\nНачислено: {earned} из {total} б"

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
def shop_text(user_id: int) -> str:
    lots = "\n".join(f"{i}. {lot['name']} — {lot['price']} б" for i, lot in enumerate(LOTS, 1))
    return TEXT_SHOP.format(
        balance=get_balance(user_id), count=count_orders(user_id), max=MAX_ORDERS, lots=lots
    )


def shop_keyboard():
    """Таблица кнопок: по два подарка в ряд."""
    kb = InlineKeyboardBuilder()
    for i in range(0, len(LOTS), 2):
        kb.row(*[
            CallbackButton(
                text=f"{lot.get('short', lot['name'])} · {lot['price']} б",
                payload=f"lot:{lot['id']}",
            )
            for lot in LOTS[i:i + 2]
        ])
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
    items = "\n".join(f"№{oid} · {lot_name(lot_id)} · {price} б" for oid, lot_id, price in rows)
    total = sum(price for _, _, price in rows)
    return TEXT_ORDERS.format(fio=fio or "—", items=items, total=total)


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


async def show_screen(user_id: int, text: str, keyboard, view: str, chat_id: int | None = None):
    """Новое сообщение-панель внизу чата; предыдущее удаляется."""
    old_mid, _ = get_panel(user_id)
    target = {"chat_id": chat_id} if chat_id else {"user_id": user_id}
    sent = await bot.send_message(
        text=text, attachments=[keyboard] if keyboard else None, **target
    )
    new_mid = extract_mid(sent)
    set_panel(user_id, new_mid, view)
    if old_mid and old_mid != new_mid:
        await delete_message_safe(old_mid)


def track(event: MessageCallback, user_id: int, view: str):
    """Нажатие кнопки на панели: запоминаем, что эта панель — «живая» и что на ней показано."""
    set_panel(user_id, event.message.body.mid, view)


def render_view(user_id: int, view: str | None):
    """Что показывать для вида панели. None — вид не обновляется автоматически."""
    if view in ("menu", "notice"):
        return panel_text(user_id), panel_keyboard(user_id)
    if view == "profile":
        return TEXT_PROFILE.format(points=get_balance(user_id)), back_keyboard()
    if view == "rules":
        return TEXT_MEDIABINGO, back_keyboard()
    if view == "shop":
        return shop_text(user_id), shop_keyboard()
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
    if mid and rendered:
        await edit_message_safe(mid, *rendered)
# endregion


# region Старт: кнопка «Начать» и /start
async def run_start_flow(chat_id: int, user_id: int, context: MemoryContext):
    await context.clear()

    # Уже зарегистрирован — просто показываем панель (без повторных правил)
    if is_registered(user_id):
        await show_screen(user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id)
        return

    ensure_user(user_id)
    attachments = None
    if os.path.exists(PORTRAIT_PATH):
        from maxapi.types import InputMedia
        attachments = [InputMedia(path=PORTRAIT_PATH)]
    await bot.send_message(chat_id=chat_id, text=TEXT_GREETING, attachments=attachments)

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
        user_id, TEXT_PROFILE.format(points=get_balance(user_id)), back_keyboard(),
        "profile", event.message.recipient.chat_id,
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
    await bot.send_message(chat_id=chat_id, text=TEXT_MEDIABINGO)
    await show_screen(user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id)
# endregion


# region Панель: меню, задания, профиль, правила (кнопки)
@dp.message_callback(F.callback.payload == "menu")
async def on_menu(event: MessageCallback, context: MemoryContext):
    await context.clear()   # вышли из режима «жду ответ на задание»
    user_id = event.callback.user.user_id
    track(event, user_id, "menu")
    await event.message.edit(text=panel_text(user_id), attachments=[panel_keyboard(user_id)])


@dp.message_callback(F.callback.payload == "profile")
async def on_profile_button(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "profile")
    await event.message.edit(
        text=TEXT_PROFILE.format(points=get_balance(user_id)), attachments=[back_keyboard()]
    )


@dp.message_callback(F.callback.payload == "rules")
async def on_rules_button(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    track(event, user_id, "rules")
    await event.message.edit(text=TEXT_MEDIABINGO, attachments=[back_keyboard()])


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
        await event.message.edit(text=quiz_text(task_id, 0), attachments=[quiz_keyboard(task_id, 0, 0)])
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
    _, task_id, qi, score, opt = event.callback.payload.split(":")
    qi, score, opt = int(qi), int(score), int(opt)

    quiz = QUIZZES.get(task_id)
    if not quiz:
        return

    # Уже проходил, или приём закрыт (например, нажатие на старую кнопку)
    if not can_submit(user_id, task_id):
        track(event, user_id, f"task:{task_id}")
        await event.message.edit(text=task_view_text(user_id, task_id), attachments=[back_keyboard()])
        return

    questions = quiz["questions"]
    if qi >= len(questions) or opt >= len(questions[qi]["options"]):
        return

    track(event, user_id, "flow")
    if opt == questions[qi]["correct"]:
        score += quiz["points"]

    # Есть следующий вопрос
    if qi + 1 < len(questions):
        await event.message.edit(
            text=quiz_text(task_id, qi + 1), attachments=[quiz_keyboard(task_id, qi + 1, score)]
        )
        return

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
    if not create_quiz_result(user_id, part_id, score):
        await event.message.edit(text=task_text(user_id, task_id), attachments=[back_keyboard()])
        return

    sub_id = create_submission(user_id, task_id, open_answer) if open_answer else None

    text = task_text(user_id, task_id)
    if open_answer and score > 0:
        text += f"\n\nНачислено: {score} б"   # часть с кнопками; остальное — после проверки
    await event.message.edit(text=text, attachments=[back_keyboard()])
    await context.clear()

    if sub_id:
        await send_to_moderation(sub_id)   # открытый ответ — модераторам
# endregion


# region Приём ответов участников
URL_RE = re.compile(r"(https?://|www\.|vk\.|max\.|t\.me/)\S+", re.IGNORECASE)


def validate_answer(accept: str, text: str, attachments: list) -> str | None:
    """None — ответ годится, иначе текст подсказки участнику."""
    if len(attachments) > MAX_ATTACHMENTS:
        return TEXT_TOO_MANY_FILES
    if accept == "photo":
        return None if attachments else TEXT_BAD_PHOTO
    # link: нужна ссылка текстом, ограниченной длины
    if not text or len(text) > MAX_LINK_LEN or not URL_RE.search(text):
        return TEXT_BAD_LINK
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
            user_id, quiz_text(task_id, 0), quiz_keyboard(task_id, 0, 0), "flow", chat_id
        )
        return

    # Обычное задание: проверяем формат и размер
    problem = validate_answer(TASKS[task_id].get("accept", "link"), text, attachments)
    if problem:
        await message.answer(problem)
        return

    sub_id = create_submission(
        user_id, task_id, text[:MAX_LINK_LEN], message.body.mid if attachments else None
    )
    await context.clear()

    # Подтверждение уходит вместе с панелью — панель остаётся последним сообщением
    await show_screen(user_id, pending_text(task_id), panel_keyboard(user_id), "menu", chat_id)
    await send_to_moderation(sub_id)
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
    verdict = f"✅ Зачтено, +{points} б" if approved else "❌ Отклонено"
    await event.message.edit(
        text=f"{event.message.body.text or ''}\n\n{verdict} — {name}", attachments=[]
    )

    # Участнику: без лишних сообщений — результат приходит экраном с панелью под ним
    # и заменяет предыдущий экран. Если человек сейчас вводит ответ — не перебиваем.
    _, view = get_panel(user_id)
    if view in ("menu", "notice", "profile", "rules"):
        if approved:
            template = TEXT_CONGRATS_SHORT if tasks_closed(user_id) else TEXT_CONGRATS
            await show_screen(user_id, template.format(points=points),
                              panel_keyboard(user_id), "notice")
            return
        if task_id not in QUIZZES:
            text = TEXT_REJECTED.format(short=TASKS[task_id]["short"])
            if not tasks_closed(user_id):
                text += f"\n{TEXT_REJECTED_RETRY}"
            await show_screen(user_id, text, panel_keyboard(user_id), "notice")
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


if BaseMiddleware:
    class ErrorMiddleware(BaseMiddleware):
        """Ловит исключения в любом обработчике, чтобы бот продолжал работать."""

        async def __call__(self, handler, event_object, data):
            try:
                return await handler(event_object, data)
            except Exception as error:
                await report_error(error)
# endregion


# region Витрина: покупка с подтверждением
_last_buy: dict[int, float] = {}


@dp.message_callback(F.callback.payload == "wait_shop")
async def on_wait_shop(event: MessageCallback):
    await event.answer(notification=TEXT_WAIT_SHOP.format(time=SHOP_OPEN_AT.strftime("%H:%M")))


@dp.message_callback(F.callback.payload == "shop")
async def on_shop(event: MessageCallback, context: MemoryContext):
    await context.clear()
    user_id = event.callback.user.user_id
    if not shop_open(user_id):
        await event.answer(notification=TEXT_WAIT_SHOP.format(time=SHOP_OPEN_AT.strftime("%H:%M")))
        return
    track(event, user_id, "shop")
    await event.message.edit(text=shop_text(user_id), attachments=[shop_keyboard()])


@dp.message_callback(F.callback.payload.startswith("lot:"))
async def on_lot(event: MessageCallback):
    """Выбор подарка -> экран подтверждения (или причина, почему нельзя)."""
    user_id = event.callback.user.user_id
    lot = LOTS_BY_ID.get(event.callback.payload.split(":", 1)[1])
    if not lot or not shop_open(user_id):
        return
    track(event, user_id, "flow")

    balance = get_balance(user_id)
    if count_orders(user_id) >= MAX_ORDERS:
        text, keyboard = TEXT_LIMIT.format(max=MAX_ORDERS), shop_back_keyboard()
    elif balance < lot["price"]:
        text, keyboard = TEXT_NO_POINTS.format(price=lot["price"], balance=balance), shop_back_keyboard()
    else:
        text = TEXT_CONFIRM.format(
            name=lot["name"], price=lot["price"], balance=balance, left=balance - lot["price"]
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

    result, order_id = create_order(user_id, lot["id"], lot["price"])
    if result == "ok":
        track(event, user_id, "orders")
        paid = TEXT_ORDERED.format(name=lot["name"], price=lot["price"], left=get_balance(user_id))
        text, keyboard = f"{paid}\n\n{orders_text(user_id)}", orders_keyboard()
        await event.message.edit(text=text, attachments=[keyboard])
        return
    track(event, user_id, "flow")

    if result == "limit":
        text, keyboard = TEXT_LIMIT.format(max=MAX_ORDERS), shop_back_keyboard()
    else:
        text = TEXT_NO_POINTS.format(price=lot["price"], balance=get_balance(user_id))
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
        await bot.send_message(user_id=user_id, text=item["text"])
        await show_screen(user_id, shop_text(user_id), shop_keyboard(), "shop")
    else:
        await show_screen(user_id, item["text"], closed_panel_keyboard(user_id), "menu")


async def run_broadcast(item: dict):
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


async def scheduler():
    """Раз в 20 секунд проверяет, не пора ли отправить рассылку."""
    while True:
        try:
            now = datetime.now(MSK)
            for item in SCHEDULE:
                if now < item["at"] or is_broadcast_done(item["id"]):
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
    await show_screen(user_id, panel_text(user_id), panel_keyboard(user_id), "menu", chat_id)
# endregion


# region Запуск
async def main():
    db_init()

    # Защита от падений в обработчиках
    if BaseMiddleware:
        register = getattr(dp, "register_inner_middleware", None) or getattr(dp, "middleware", None)
        if register:
            register(ErrorMiddleware())
    else:
        logging.warning("BaseMiddleware не найден — перехват ошибок в обработчиках отключён")

    await bot.set_commands(
        BotCommand(name="start", description="Начать"),
        BotCommand(name="profile", description="Мои баллы"),
    )   # /queue, /chatid и /preview намеренно не в списке — это служебные команды
    await bot.delete_webhook()   # для polling вебхук должен быть снят

    asyncio.create_task(scheduler())   # рассылки по расписанию

    # Если соединение оборвалось — перезапускаем опрос, а не падаем
    while True:
        try:
            await dp.start_polling(bot, skip_updates=True)
            break
        except Exception as error:
            await report_error(error)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
# endregion