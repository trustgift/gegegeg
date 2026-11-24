import asyncio
import html
import json
import logging
import os
import secrets
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramNetworkError,
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    PhoneMigrateError,
    RpcMcgetFailError,
    SessionPasswordNeededError,
    AuthKeyUnregisteredError,
    AuthKeyDuplicatedError,
    FrozenMethodInvalidError,
    UserDeactivatedBanError,
    UserDeactivatedError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)
from telethon.tl import functions
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import PeerUser


# ============================================================
# CONFIG (ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ХОСТИНГА)
# ============================================================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TELETHON_SESSION = os.getenv("TELETHON_SESSION", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

ADMIN_IDS: Set[int] = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

RAW_DB_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE_URL = RAW_DB_URL.replace("Postgresql://", "postgresql://").replace("postgres://", "postgresql://")

TON_WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS", "").strip()
SUBSCRIPTION_PRICE_TON = int(os.getenv("SUBSCRIPTION_PRICE_TON", "5"))
TRIAL_DURATION_SECONDS = 30 * 60  # 30 минут
MAX_KEY_TRANSFERS = 3             # Максимум переносов подписки по ключу

# Настройки API see.tg (kartoshka.free)
SEETG_TOKEN = os.getenv("SEETG_TOKEN", "").strip()
SEETG_BASE_URL = "https://kartoshka.free/v1"
SEETG_REQUEST_DELAY = 0.35

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "0"))

# БЫСТРЫЙ ПАРАЛЛЕЛЬНЫЙ РЕЖИМ (масштабируется от кол-ва аккаунтов в пуле)
MONITOR_INTERVAL = 4        # Пауза между кругами обхода (сек)
MONITOR_PAGE_LIMIT = 2      # Проверять 2 самых свежих лота с модели за круг
PER_ACCOUNT_DELAY = 1.2     # Персональная задержка на каждый аккаунт в пуле (сек)
MONITOR_SEND_DELAY = 0.2    # Пауза между отправкой сообщений в чат (сек)
BATCH_STEP_DELAY = 0.25     # Пауза между пачками моделей (сек)

# ФИЛЬТРЫ ПО УМОЛЧАНИЮ ("НЕ ШАРЯЩИЕ")
DEFAULT_MAX_NFT_GIFTS = 3        # Максимум 3 NFT-подарка в профиле
DEFAULT_MAX_TOTAL_GIFTS = 60     # Максимум 60 подарков всего (обычные + NFT)
DEFAULT_MAX_RATING = 5           # Рейтинг строго < 5
DEFAULT_MIN_PRICE = 0
DEFAULT_MAX_PRICE = 0
DEFAULT_USERNAME_MODE = "required"
DEFAULT_STRICT_NEWBIE = True     # Отсеивать тех, у кого свой канал или слова скупа/NFT в био
DEFAULT_NO_PREMIUM = True        # Отсеивать пользователей с Telegram Premium
DEFAULT_AI_FILTER = True         # ИИ-фильтр через Google Gemini API
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"

# Кандидаты моделей для проверки доступности по ключу
GEMINI_CANDIDATE_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
]

TRADER_STOP_WORDS = (
    "nft", "нфт", "gift", "гифт", "подарк", "скуп", "продам", "куплю",
    "otc", "отс", "гарант", "garant", "ton", "тон", "usdt", "crypto", "крипт",
    "канал", "t.me/", "http", "shop", "store", "market", "маркет",
    "trade", "трейд", "p2p", "обмен", "admin", "manager", "менеджер",
)

DEAD_SESSION_ERRORS = (
    AuthKeyUnregisteredError,
    AuthKeyDuplicatedError,
    FrozenMethodInvalidError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
SUPPORTS_STARS_ONLY_PARAM = True

SEND_EXISTING_ON_FIRST_SCAN = False
GIFTS_PER_PAGE = 8
REQUEST_PAGE_LIMIT = 20


# ============================================================
# LOGGING & IPv4 NETWORK SESSION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("gift-parser")


class IPv4AiohttpSession(AiohttpSession):
    """Принудительный IPv4 для Docker-хостингов (исправляет Connection reset by peer)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._connector_init["family"] = socket.AF_INET
        self._connector_init["ttl_dns_cache"] = 300


def validate_config():
    errors = []
    if not isinstance(API_ID, int) or API_ID <= 0:
        errors.append("API_ID")
    if not API_HASH:
        errors.append("API_HASH")
    if not BOT_TOKEN:
        errors.append("BOT_TOKEN")
    if not SEETG_TOKEN:
        errors.append("SEETG_TOKEN")
    if not ADMIN_IDS:
        errors.append("ADMIN_IDS")
    if errors:
        raise SystemExit(
            "\nНе заполнены переменные окружения: " + ", ".join(errors) +
            "\nДобавь их в настройках хостинга (ENV)."
        )


bot = Bot(token=BOT_TOKEN, session=IPv4AiohttpSession())
dp = Dispatcher(storage=MemoryStorage())


@dp.update.outer_middleware()
async def network_retry_middleware(handler, event, data):
    for attempt in range(3):
        try:
            return await handler(event, data)
        except TelegramNetworkError as e:
            if attempt == 2:
                raise
            log.warning("Сетевой сброс Telegram API (%s), повтор %s/3...", e, attempt + 1)
            await asyncio.sleep(0.4)


http_session: Optional[aiohttp.ClientSession] = None
db_pool: Optional[asyncpg.Pool] = None

BASE_GIFTS: List["BaseGift"] = []
BASE_GIFTS_BY_ID: Dict[int, "BaseGift"] = {}

OWNERS_BLACKLIST: Dict[str, str] = {}
OWNER_CACHE: Dict[str, Dict[str, Any]] = {}
SEETG_CACHE: Dict[str, Dict[str, Any]] = {}
SENT_GIFTS: Dict[str, Dict[str, Any]] = {}
CLAIMED_LOGS: Dict[str, Dict[str, Any]] = {}

SUBSCRIPTIONS: Dict[str, Any] = {
    "users": {},
    "keys": {},
    "pending_payments": {},
}

SEETG_BILLING: Dict[str, Any] = {
    "free_remaining": "?",
    "balance_ton": 0.0,
}

seetg_api_lock = asyncio.Lock()
last_seetg_request = 0.0
claim_lock = asyncio.Lock()
subs_lock = asyncio.Lock()
db_save_tasks: Dict[str, asyncio.Task] = {}

MONITOR_SETTINGS: Dict[str, Any] = {
    "enabled": False,
    "target_chat_id": TARGET_CHAT_ID,
    "min_price": DEFAULT_MIN_PRICE,
    "max_price": DEFAULT_MAX_PRICE,
    "username_mode": DEFAULT_USERNAME_MODE,
    "model_ids": [],
    "max_owner_nft_gifts": DEFAULT_MAX_NFT_GIFTS,
    "max_owner_total_gifts": DEFAULT_MAX_TOTAL_GIFTS,
    "max_rating_level": DEFAULT_MAX_RATING,
    "strict_newbie_mode": DEFAULT_STRICT_NEWBIE,
    "no_premium_only": DEFAULT_NO_PREMIUM,
    "ai_filter_enabled": DEFAULT_AI_FILTER,
    "gemini_api_key": GEMINI_API_KEY,
    "gemini_model": DEFAULT_GEMINI_MODEL,
    "gemini_available_models": [],
}

monitor_task: Optional[asyncio.Task] = None
trial_watcher_task: Optional[asyncio.Task] = None
accounts_watcher_task: Optional[asyncio.Task] = None
monitor_running = False
monitor_first_scan_done = False
monitor_lock = asyncio.Lock()


# ============================================================
# УВЕДОМЛЕНИЯ АДМИНАМ В ЛС
# ============================================================

async def notify_admins(text: str) -> None:
    for admin_id in ADMIN_IDS:
        if admin_id > 0:
            try:
                await bot.send_message(admin_id, text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception as e:
                log.warning("Не удалось отправить уведомление админу %s: %s", admin_id, e)


# ============================================================
# GOOGLE GEMINI AI FILTER + АВТООПРЕДЕЛЕНИЕ ДОСТУПНЫХ МОДЕЛЕЙ
# ============================================================

def get_active_gemini_key() -> str:
    return (MONITOR_SETTINGS.get("gemini_api_key") or GEMINI_API_KEY or "").strip()


def get_active_gemini_model() -> str:
    return (MONITOR_SETTINGS.get("gemini_model") or DEFAULT_GEMINI_MODEL).strip()


def build_gemini_generate_url(api_key: str, model_name: str) -> str:
    clean_model = model_name.replace("models/", "").strip()
    if api_key.startswith("AQ."):
        return f"https://aiplatform.googleapis.com/v1/publishers/google/models/{clean_model}:generateContent?key={api_key}"
    return f"https://generativelanguage.googleapis.com/v1beta/models/{clean_model}:generateContent?key={api_key}"


async def discover_gemini_models_for_key(api_key: str) -> tuple[bool, List[str], str]:
    """
    Проверяет ключ (AIza... или AQ....) и определяет, какие модели Gemini можно подключить.
    Возвращает: (успех, список_доступных_моделей, текст_ошибки_или_статуса)
    """
    global http_session
    if http_session is None or http_session.closed:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        http_session = aiohttp.ClientSession(connector=connector)

    available: List[str] = []
    last_error = ""

    # 1. Для классических AI Studio ключей (AIza...) запрашиваем официальный /v1beta/models
    if not api_key.startswith("AQ."):
        list_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
        try:
            async with http_session.get(list_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for m in data.get("models") or []:
                        methods = m.get("supportedGenerationMethods") or []
                        name = str(m.get("name") or "").replace("models/", "")
                        if "generateContent" in methods and "gemini" in name.lower():
                            if not any(bad in name.lower() for bad in ("embedding", "imagen", "veo", "tts", "audio")):
                                available.append(name)
                else:
                    err_data = await resp.text()
                    last_error = f"HTTP {resp.status}: {err_data[:140]}"
        except Exception as e:
            last_error = str(e)

    # 2. Если список пуст или это ключ AQ. (Vertex Express) — пингуем каждую кандидат-модель напрямую
    if not available:
        ping_payload = {
            "contents": [{"parts": [{"text": "Reply OK"}]}],
            "generationConfig": {"maxOutputTokens": 5, "temperature": 0.0},
        }
        for candidate in GEMINI_CANDIDATE_MODELS:
            url = build_gemini_generate_url(api_key, candidate)
            try:
                async with http_session.post(url, json=ping_payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        available.append(candidate)
                    else:
                        err_txt = await resp.text()
                        last_error = f"HTTP {resp.status}: {err_txt[:140]}"
            except Exception as e:
                last_error = str(e)

    # Сортируем так, чтобы приоритетные модели (2.5-flash и 2.5-flash-lite) шли первыми
    priority = {m: idx for idx, m in enumerate(GEMINI_CANDIDATE_MODELS)}
    available = sorted(dict.fromkeys(available), key=lambda x: (priority.get(x, 99), x))

    if available:
        return True, available, "OK"
    return False, [], last_error or "Ни одна модель Gemini не ответила 200 OK"


async def check_owner_with_gemini_ai(owner: "OwnerInfo", gift_title: str, price: int) -> tuple[bool, str]:
    global http_session
    if not MONITOR_SETTINGS.get("ai_filter_enabled"):
        return True, "ИИ выключен"

    api_key = get_active_gemini_key()
    if not api_key:
        return True, "нет ключа Gemini"

    if http_session is None or http_session.closed:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        http_session = aiohttp.ClientSession(connector=connector)

    prompt = (
        "Ты антифрод-аналитик Telegram NFT маркета. Определи, является ли продавец обычным простым пользователем "
        "(«новичком» / «не шарящим», который случайно или по незнанию выставил подарок), ИЛИ это скрытый перекуп, "
        "трейдер, твинк скупа, крипто-юзер или опытный коллекционер.\n\n"
        f"Данные продавца:\n"
        f"- Имя: {owner.label}\n"
        f"- Username: @{owner.username or 'нет'}\n"
        f"- Био (О себе): {owner.bio or 'пусто'}\n"
        f"- Рейтинг звёзд: {owner.rating_level}\n"
        f"- Всего подарков: {owner.total_gifts_count} (из них NFT: {owner.nft_gifts_count})\n"
        f"- Telegram Premium: {'да' if owner.is_premium else 'нет'}\n"
        f"- Выставил подарок: {gift_title} за {price} Stars\n\n"
        "Признаки перекупа/твинка: сленг крипты/NFT/трейдинга, ссылки, шаблонные никнеймы ферм/авторегов, "
        "слишком много подарков или завышенный рейтинг.\n"
        "Ответь СТРОГО в формате JSON без markdown: "
        '{"pass": true/false, "reason": "кратко до 6 слов на русском"}'
    )

    model_name = get_active_gemini_model()
    url = build_gemini_generate_url(api_key, model_name)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 90,
            "responseMimeType": "application/json",
        },
    }

    try:
        async with http_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                err_txt = await resp.text()
                log.warning("Gemini API (%s) HTTP %s: %s", model_name, resp.status, err_txt[:150])
                return True, "ошибка API (пропущен)"

            data = await resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return True, "пустой ответ ИИ"

            raw_text = (
                candidates[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(raw_text)
            passed = bool(parsed.get("pass", True))
            reason = str(parsed.get("reason") or "оценка ИИ").strip()
            return passed, reason

    except Exception as e:
        log.warning("Gemini AI check fallback (%s)", e)
        return True, "таймаут ИИ"


# ============================================================
# MULTI-ACCOUNT POOL (АВТО-ОТКЛЮЧЕНИЕ, УВЕДОМЛЕНИЯ, ВОЗВРАТ)
# ============================================================

@dataclass
class AccountWorker:
    acc_id: str
    session_str: str
    client: TelegramClient
    label: str = "Аккаунт"
    user_id: int = 0
    dc_id: int = 0
    flood_until: float = 0.0
    flood_notified: bool = False
    last_request_ts: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


ACCOUNTS_POOL: List[AccountWorker] = []
pool_rr_index = 0
pool_select_lock = asyncio.Lock()
pending_auth_client: Optional[TelegramClient] = None


def save_accounts_pool() -> None:
    payload = [
        {
            "acc_id": w.acc_id,
            "session": w.session_str,
            "label": w.label,
            "user_id": w.user_id,
            "dc_id": w.dc_id,
        }
        for w in ACCOUNTS_POOL
    ]
    schedule_db_save("telethon_accounts", payload, immediate=True)


def set_worker_flood(worker: AccountWorker, seconds: int, context_name: str = "API") -> None:
    wait_time = max(5, int(seconds) + 2)
    already_in_flood = time.time() < worker.flood_until
    worker.flood_until = max(worker.flood_until, time.time() + wait_time)

    if not already_in_flood or not worker.flood_notified:
        worker.flood_notified = True
        mins = max(1, round(wait_time / 60))
        active_left = get_active_accounts_count()
        total = len(ACCOUNTS_POOL)
        log.warning(
            "⏳ Аккаунт %s поймал FloodWait (%s сек / ~%s мин) на %s. Активных осталось: %s/%s",
            worker.label, wait_time, mins, context_name, active_left, total
        )
        asyncio.create_task(
            notify_admins(
                f"⏳ <b>Аккаунт ушёл в FloodWait</b>\n\n"
                f"👤 Профиль: <b>{escape_text(worker.label)}</b> [DC {worker.dc_id}]\n"
                f"⏱ Пауза: <b>{wait_time} сек. (~{mins} мин.)</b>\n"
                f"📊 Активных в пуле осталось: <b>{active_left} из {total}</b>\n\n"
                "<i>Аккаунт временно отключён. Как только флуд закончится, он автоматически вернётся в работу.</i>"
            )
        )


async def handle_dead_session(worker: AccountWorker, reason: str) -> None:
    if worker not in ACCOUNTS_POOL:
        return
    ACCOUNTS_POOL.remove(worker)
    save_accounts_pool()
    try:
        if worker.client.is_connected():
            await worker.client.disconnect()
    except Exception:
        pass

    human_reason = {
        "FrozenMethodInvalidError": "Аккаунт заморожен Telegram (Frozen)",
        "AuthKeyDuplicatedError": "Сессия использовалась с двух IP одновременно (AuthKeyDuplicated)",
        "AuthKeyUnregisteredError": "Сессия разлогинена (AuthKeyUnregistered)",
        "UserDeactivatedBanError": "Аккаунт заблокирован Telegram (Banned)",
        "UserDeactivatedError": "Аккаунт удалён (Deactivated)",
    }.get(reason, reason)

    active_left = get_active_accounts_count()
    total = len(ACCOUNTS_POOL)
    log.error("❌ Сессия %s отлетела (%s) и удалена из пула!", worker.label, human_reason)
    await notify_admins(
        f"🚨 <b>ПРОБЛЕМА С АККАУНТОМ (УДАЛЁН ИЗ ПУЛА)!</b>\n\n"
        f"👤 Профиль: <b>{escape_text(worker.label)}</b> [ID: <code>{worker.user_id}</code>]\n"
        f"❌ Причина: <b>{escape_text(human_reason)}</b>\n"
        f"📊 Осталось в пуле: <b>{active_left} активных / {total} всего</b>\n\n"
        "Добавь замену через /set_session или в меню 👥 АККАУНТЫ."
    )


async def add_account_to_pool(session_str: str, save: bool = True) -> Optional[AccountWorker]:
    session_str = session_str.strip()
    if not session_str:
        return None

    for existing in ACCOUNTS_POOL:
        if existing.session_str == session_str:
            return existing

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            return None
        me = await client.get_me()
    except Exception as e:
        log.warning("⚠️ Пропуск битой/сдублированной сессии при загрузке (%s)", e.__class__.__name__)
        try:
            await client.disconnect()
        except Exception:
            pass
        return None

    for idx, existing in enumerate(list(ACCOUNTS_POOL)):
        if existing.user_id == me.id:
            try:
                await existing.client.disconnect()
            except Exception:
                pass
            ACCOUNTS_POOL.pop(idx)

    dc_id = getattr(client.session, "dc_id", 0) or 0
    uname = f"@{me.username}" if me.username else f"id:{me.id}"
    label = f"{me.first_name or 'User'} ({uname})"
    acc_id = secrets.token_hex(4)

    worker = AccountWorker(
        acc_id=acc_id,
        session_str=session_str,
        client=client,
        label=label,
        user_id=me.id,
        dc_id=dc_id,
    )
    ACCOUNTS_POOL.append(worker)
    if save:
        save_accounts_pool()
    log.info("✅ Аккаунт добавлен в пул: %s [ID: %s | DC %s] (Всего в пуле: %s)", label, me.id, dc_id, len(ACCOUNTS_POOL))
    return worker


async def remove_account_from_pool(acc_id: str) -> bool:
    for idx, w in enumerate(ACCOUNTS_POOL):
        if w.acc_id == acc_id:
            try:
                await w.client.disconnect()
            except Exception:
                pass
            ACCOUNTS_POOL.pop(idx)
            save_accounts_pool()
            return True
    return False


def get_active_accounts_count() -> int:
    now = time.time()
    return sum(1 for w in ACCOUNTS_POOL if now >= w.flood_until)


def get_min_flood_wait_remaining() -> int:
    if not ACCOUNTS_POOL:
        return 0
    now = time.time()
    if any(now >= w.flood_until for w in ACCOUNTS_POOL):
        return 0
    return max(1, int(min(w.flood_until - now for w in ACCOUNTS_POOL)))


async def acquire_worker() -> Optional[AccountWorker]:
    global pool_rr_index
    if not ACCOUNTS_POOL:
        return None

    async with pool_select_lock:
        now = time.time()
        n = len(ACCOUNTS_POOL)
        chosen: Optional[AccountWorker] = None

        for offset in range(n):
            idx = (pool_rr_index + offset) % n
            candidate = ACCOUNTS_POOL[idx]
            if now >= candidate.flood_until:
                chosen = candidate
                pool_rr_index = (idx + 1) % n
                break

        if chosen is None:
            return None

    async with chosen.lock:
        if not chosen.client.is_connected():
            await chosen.client.connect()
        loop_now = asyncio.get_running_loop().time()
        wait_for = PER_ACCOUNT_DELAY - (loop_now - chosen.last_request_ts)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        chosen.last_request_ts = asyncio.get_running_loop().time()

    return chosen


async def accounts_watcher_loop() -> None:
    last_health_check = 0.0
    while True:
        try:
            await asyncio.sleep(5)
            now = time.time()

            for w in list(ACCOUNTS_POOL):
                if w.flood_notified and now >= w.flood_until:
                    w.flood_notified = False
                    w.flood_until = 0.0
                    active_now = get_active_accounts_count()
                    total = len(ACCOUNTS_POOL)
                    log.info("🟢 FloodWait закончился для %s! Аккаунт вернулся в работу (%s/%s).", w.label, active_now, total)
                    await notify_admins(
                        f"🟢 <b>FloodWait закончился — аккаунт снова в работе!</b>\n\n"
                        f"👤 Профиль: <b>{escape_text(w.label)}</b> [DC {w.dc_id}]\n"
                        f"📊 Активных в пуле: <b>{active_now} из {total}</b>"
                    )

            if now - last_health_check >= 120:
                last_health_check = now
                for w in list(ACCOUNTS_POOL):
                    try:
                        if not w.client.is_connected():
                            await w.client.connect()
                        if not await w.client.is_user_authorized():
                            await handle_dead_session(w, "AuthKeyUnregisteredError")
                    except DEAD_SESSION_ERRORS as e:
                        await handle_dead_session(w, e.__class__.__name__)
                    except Exception as e:
                        log.warning("Ошибка проверки здоровья сессии %s: %s", w.label, e)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Ошибка в accounts_watcher_loop: %s", e)


# ============================================================
# DATA CLASSES & FSM STATES
# ============================================================

@dataclass
class BaseGift:
    gift_id: int
    title: str
    stars: int
    availability_resale: int
    resell_min_stars: int
    sold_out: bool


@dataclass
class OwnerInfo:
    key: str
    label: str
    username: Optional[str] = None
    user_id: Optional[int] = None
    bio: str = ""
    nft_gifts_count: int = 0
    total_gifts_count: int = 0
    rating_level: int = 0
    rating_stars: int = 0
    is_premium: bool = False
    has_channel: bool = False
    bio_has_trader_words: bool = False

    @property
    def display(self) -> str:
        if self.username:
            return f"@{self.username}"
        return self.label


@dataclass
class MarketGift:
    title: str
    num: int
    slug: str
    price: int
    owner: OwnerInfo

    @property
    def link(self) -> str:
        return f"https://t.me/nft/{self.slug}"


class AuthState(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_string_session = State()
    waiting_gemini_key = State()


class MonitorPriceState(StatesGroup):
    waiting_range = State()


class MonitorRatingState(StatesGroup):
    waiting_rating = State()


class MonitorNftLimitState(StatesGroup):
    waiting_limit = State()


class MonitorTotalGiftsState(StatesGroup):
    waiting_total_limit = State()


class BlacklistState(StatesGroup):
    waiting_value = State()


class KeyTransferState(StatesGroup):
    waiting_key = State()


# ============================================================
# POSTGRESQL STORAGE (+ LOCAL JSON FALLBACK)
# ============================================================

def load_local_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_local_json(path: str, data: Any) -> None:
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        log.error("Local JSON save error %s: %s", path, e)


async def init_db() -> None:
    global db_pool
    if not DATABASE_URL:
        log.warning("DATABASE_URL не задан! Данные будут сохраняться только локально.")
        return
    try:
        db_pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_kv_store (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                )
                """
            )
        log.info("✅ Подключение к PostgreSQL успешно установлено!")
    except Exception as e:
        log.error("❌ Ошибка подключения к PostgreSQL: %s", e)
        db_pool = None


async def db_get(key: str, default: Any) -> Any:
    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT value FROM bot_kv_store WHERE key = $1", key)
                if row and row["value"] is not None:
                    val = row["value"]
                    return json.loads(val) if isinstance(val, str) else val
        except Exception as e:
            log.error("DB GET error [%s]: %s", key, e)
    return load_local_json(f"{key}.json", default)


async def _db_set_worker(key: str, data_json: str, immediate: bool = False) -> None:
    if not immediate:
        await asyncio.sleep(1.0)
    if db_pool is None:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_kv_store (key, value, updated_at)
                VALUES ($1, $2::jsonb, $3)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                """,
                key,
                data_json,
                time.time(),
            )
    except Exception as e:
        log.error("DB SET error [%s]: %s", key, e)


def schedule_db_save(key: str, data: Any, immediate: bool = False) -> None:
    save_local_json(f"{key}.json", data)
    if db_pool is None:
        return
    data_json = json.dumps(data, ensure_ascii=False)
    old_task = db_save_tasks.get(key)
    if old_task and not old_task.done():
        old_task.cancel()
    db_save_tasks[key] = asyncio.create_task(_db_set_worker(key, data_json, immediate=immediate))


def save_owner_blacklist() -> None:
    schedule_db_save("owner_blacklist", OWNERS_BLACKLIST, immediate=True)


def save_owner_cache() -> None:
    if len(OWNER_CACHE) > 12000:
        for k in list(OWNER_CACHE.keys())[:-10000]:
            OWNER_CACHE.pop(k, None)
    schedule_db_save("owner_cache", OWNER_CACHE)


def save_seetg_cache() -> None:
    if len(SEETG_CACHE) > 18000:
        for k in list(SEETG_CACHE.keys())[:-15000]:
            SEETG_CACHE.pop(k, None)
    schedule_db_save("seetg_cache", SEETG_CACHE)


def save_sent_gifts() -> None:
    if len(SENT_GIFTS) > 5000:
        for k in list(SENT_GIFTS.keys())[:-4000]:
            SENT_GIFTS.pop(k, None)
    schedule_db_save("sent_gifts", SENT_GIFTS)


def save_subscriptions() -> None:
    schedule_db_save("subscriptions", SUBSCRIPTIONS, immediate=True)


def save_claimed_logs() -> None:
    if len(CLAIMED_LOGS) > 2500:
        for k in list(CLAIMED_LOGS.keys())[:-2000]:
            CLAIMED_LOGS.pop(k, None)
    schedule_db_save("claimed_logs", CLAIMED_LOGS, immediate=True)


def save_monitor_settings() -> None:
    schedule_db_save("monitor_settings", MONITOR_SETTINGS, immediate=True)


def apply_loaded_settings(data: Any) -> None:
    global MONITOR_SETTINGS
    if not isinstance(data, dict):
        data = {}

    MONITOR_SETTINGS["enabled"] = bool(data.get("enabled", False))
    MONITOR_SETTINGS["target_chat_id"] = int(data.get("target_chat_id", TARGET_CHAT_ID) or TARGET_CHAT_ID or 0)
    MONITOR_SETTINGS["min_price"] = max(0, int(data.get("min_price", DEFAULT_MIN_PRICE) or 0))
    MONITOR_SETTINGS["max_price"] = max(0, int(data.get("max_price", DEFAULT_MAX_PRICE) or 0))
    MONITOR_SETTINGS["max_owner_nft_gifts"] = max(0, int(data.get("max_owner_nft_gifts", DEFAULT_MAX_NFT_GIFTS) or 0))
    MONITOR_SETTINGS["max_owner_total_gifts"] = max(0, int(data.get("max_owner_total_gifts", DEFAULT_MAX_TOTAL_GIFTS) or 0))
    MONITOR_SETTINGS["max_rating_level"] = max(0, int(data.get("max_rating_level", DEFAULT_MAX_RATING) or 0))
    MONITOR_SETTINGS["strict_newbie_mode"] = bool(data.get("strict_newbie_mode", DEFAULT_STRICT_NEWBIE))
    MONITOR_SETTINGS["no_premium_only"] = bool(data.get("no_premium_only", DEFAULT_NO_PREMIUM))
    MONITOR_SETTINGS["ai_filter_enabled"] = bool(data.get("ai_filter_enabled", DEFAULT_AI_FILTER))

    db_gemini_key = str(data.get("gemini_api_key", "") or "").strip()
    MONITOR_SETTINGS["gemini_api_key"] = db_gemini_key or GEMINI_API_KEY
    MONITOR_SETTINGS["gemini_model"] = str(data.get("gemini_model", DEFAULT_GEMINI_MODEL) or DEFAULT_GEMINI_MODEL).strip()
    avail = data.get("gemini_available_models")
    MONITOR_SETTINGS["gemini_available_models"] = avail if isinstance(avail, list) else []

    username_mode = str(data.get("username_mode", DEFAULT_USERNAME_MODE)).lower()
    MONITOR_SETTINGS["username_mode"] = username_mode if username_mode in {"any", "required"} else "required"

    model_ids = data.get("model_ids", [])
    if isinstance(model_ids, list):
        MONITOR_SETTINGS["model_ids"] = [int(x) for x in model_ids if str(x).lstrip("-").isdigit()]
    else:
        MONITOR_SETTINGS["model_ids"] = []

    if not MONITOR_SETTINGS["model_ids"]:
        MONITOR_SETTINGS["enabled"] = False


# ============================================================
# HELPERS & SUBSCRIPTION LOGIC
# ============================================================

def is_admin(user_id: Optional[int]) -> bool:
    return bool(user_id) and (user_id in ADMIN_IDS)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except Exception:
        return default


def get_field(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def extract_owner_id(raw_id: Any) -> Optional[int]:
    if raw_id is None:
        return None
    if isinstance(raw_id, int):
        return raw_id
    if isinstance(raw_id, PeerUser):
        return raw_id.user_id
    if hasattr(raw_id, "user_id"):
        return safe_int(getattr(raw_id, "user_id"), 0) or None
    try:
        return int(raw_id)
    except Exception:
        return None


def extract_stars_amount(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        for item in value:
            amount = extract_stars_amount(item)
            if amount:
                return amount
        return 0
    amount = get_field(value, "amount")
    if amount is not None:
        return safe_int(amount)
    stars = get_field(value, "stars")
    if stars is not None:
        return safe_int(stars)
    return 0


def has_trader_keywords(name: str, bio: str) -> bool:
    combined = f"{name} {bio}".lower()
    return any(word in combined for word in TRADER_STOP_WORDS)


def owner_blacklist_keys(owner: OwnerInfo) -> Set[str]:
    keys: Set[str] = set()
    if owner.user_id:
        keys.add(f"id:{owner.user_id}")
    if owner.username:
        keys.add(f"username:{owner.username.lower()}")
    if owner.key:
        keys.add(owner.key)
    return keys


def is_owner_blacklisted(owner: OwnerInfo) -> bool:
    return any(key in OWNERS_BLACKLIST for key in owner_blacklist_keys(owner))


def get_target_chat_id() -> int:
    return safe_int(MONITOR_SETTINGS.get("target_chat_id"), 0)


def price_matches(price: int) -> bool:
    min_price = safe_int(MONITOR_SETTINGS.get("min_price"), 0)
    max_price = safe_int(MONITOR_SETTINGS.get("max_price"), 0)
    if min_price and price < min_price:
        return False
    if max_price and price > max_price:
        return False
    return True


def username_matches(owner: OwnerInfo) -> bool:
    mode = MONITOR_SETTINGS.get("username_mode", "required")
    return bool(owner.username) if mode == "required" else True


def owner_passes_target_filters(owner: OwnerInfo) -> tuple[bool, str]:
    max_nft = safe_int(MONITOR_SETTINGS.get("max_owner_nft_gifts"), 0)
    max_total = safe_int(MONITOR_SETTINGS.get("max_owner_total_gifts"), 0)
    max_rating = safe_int(MONITOR_SETTINGS.get("max_rating_level"), 0)
    strict_newbie = bool(MONITOR_SETTINGS.get("strict_newbie_mode", True))
    no_premium = bool(MONITOR_SETTINGS.get("no_premium_only", True))

    if no_premium and owner.is_premium:
        return False, "есть Telegram Premium"

    if strict_newbie:
        if owner.has_channel:
            return False, "привязан свой канал в профиле"
        if owner.bio_has_trader_words:
            return False, "стоп-слова трейдера/скупа в профиле"

    if max_rating > 0 and owner.rating_level >= max_rating:
        return False, f"рейтинг {owner.rating_level} >= {max_rating}"

    if max_total > 0 and owner.total_gifts_count > max_total:
        return False, f"всего подарков {owner.total_gifts_count} > {max_total}"

    if max_nft > 0 and owner.nft_gifts_count > max_nft:
        return False, f"NFT {owner.nft_gifts_count} > {max_nft}"

    return True, "ok"


def escape_text(value: Any) -> str:
    return html.escape(str(value or ""))


def generate_license_key() -> str:
    while True:
        token = f"KEY-{secrets.token_hex(4).upper()}-{secrets.token_hex(4).upper()}"
        if token not in SUBSCRIPTIONS["keys"]:
            return token


def has_active_subscription(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    u = SUBSCRIPTIONS["users"].get(str(user_id))
    if not u:
        return False
    plan = u.get("plan")
    if plan == "lifetime":
        return True
    if plan == "trial":
        expires_at = float(u.get("expires_at") or 0)
        return time.time() < expires_at
    return False


async def create_one_time_invite_link(chat_id: int, label: str = "sub") -> Optional[str]:
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=chat_id,
            name=f"{label}_{int(time.time())}"[:32],
            member_limit=1,
        )
        return invite.invite_link
    except Exception as e:
        log.error("Ошибка создания одноразовой ссылки в группу %s: %s", chat_id, e)
        return None


async def kick_user_from_group(chat_id: int, user_id: int) -> bool:
    if is_admin(user_id) or not chat_id:
        return False
    try:
        await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        await asyncio.sleep(0.5)
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        log.info("Пользователь %s исключён из группы %s", user_id, chat_id)
        return True
    except Exception as e:
        log.warning("Не удалось исключить %s из группы %s: %s", user_id, chat_id, e)
        return False


async def trial_watcher_loop() -> None:
    while True:
        try:
            await asyncio.sleep(15)
            chat_id = get_target_chat_id()
            if not chat_id:
                continue

            now = time.time()
            changed = False

            async with subs_lock:
                for uid_str, udata in list(SUBSCRIPTIONS["users"].items()):
                    if udata.get("plan") == "trial" and not udata.get("kicked"):
                        expires_at = float(udata.get("expires_at") or 0)
                        if now >= expires_at:
                            uid = int(uid_str)
                            await kick_user_from_group(chat_id, uid)
                            udata["kicked"] = True
                            udata["plan"] = "expired"
                            changed = True
                            try:
                                await bot.send_message(
                                    uid,
                                    "⏰ <b>Ваш пробный период (30 минут) истёк</b>, и вы были исключены из группы.\n\n"
                                    f"Чтобы получить постоянный доступ, оформите подписку навсегда ({SUBSCRIPTION_PRICE_TON} TON) через /start.",
                                    parse_mode="HTML",
                                )
                            except Exception:
                                pass

                if changed:
                    save_subscriptions()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Ошибка в trial_watcher_loop: %s", e)


# ============================================================
# SEE.TG (KARTOSHKA.FREE) RESOLVER
# ============================================================

async def pace_seetg_api() -> None:
    global last_seetg_request
    async with seetg_api_lock:
        now = asyncio.get_running_loop().time()
        wait_for = SEETG_REQUEST_DELAY - (now - last_seetg_request)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        last_seetg_request = asyncio.get_running_loop().time()


async def fetch_owner_from_seetg(slug: str) -> Dict[str, Any]:
    global http_session

    if slug in SEETG_CACHE:
        return SEETG_CACHE[slug]

    if http_session is None or http_session.closed:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        http_session = aiohttp.ClientSession(connector=connector)

    headers = {
        "Authorization": f"Bearer {SEETG_TOKEN}",
        "Accept": "application/json",
    }
    url = f"{SEETG_BASE_URL}/gift/{slug}"

    for _ in range(3):
        await pace_seetg_api()
        try:
            async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                free_rem = resp.headers.get("X-Free-Remaining")
                bal_ton = resp.headers.get("X-Balance")
                if free_rem is not None:
                    SEETG_BILLING["free_remaining"] = free_rem
                if bal_ton is not None:
                    try:
                        SEETG_BILLING["balance_ton"] = float(bal_ton)
                    except ValueError:
                        pass

                if resp.status == 402:
                    log.error("SEE.TG: Закончились бесплатные запросы и баланс TON (402)!")
                    return {"status": "no_balance"}

                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", 1.5))
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status == 404:
                    res_data = {"status": "not_found"}
                    SEETG_CACHE[slug] = res_data
                    save_seetg_cache()
                    return res_data

                if resp.status != 200:
                    return {"status": "http_error"}

                data = await resp.json()
                if not data.get("ok"):
                    return {"status": "api_error"}

                gift_obj = data.get("result", {}).get("gift") or {}
                owner = gift_obj.get("owner") or {}

                see_type = owner.get("seeType")
                tg_type = owner.get("telegramType")

                if see_type == "blockchain" or tg_type == "blockchain":
                    res_data = {"status": "blockchain"}
                    SEETG_CACHE[slug] = res_data
                    save_seetg_cache()
                    return res_data

                if see_type == "hidden" or owner.get("hidden"):
                    res_data = {"status": "hidden"}
                    SEETG_CACHE[slug] = res_data
                    save_seetg_cache()
                    return res_data

                if tg_type != "user":
                    res_data = {"status": f"not_user:{tg_type}"}
                    SEETG_CACHE[slug] = res_data
                    save_seetg_cache()
                    return res_data

                username = owner.get("username")
                if not username:
                    usernames = owner.get("usernames")
                    if isinstance(usernames, list) and usernames and isinstance(usernames[0], str):
                        username = usernames[0]
                if isinstance(username, str):
                    username = username.lstrip("@").strip() or None

                tg_id_raw = owner.get("telegramId")
                tg_id = int(tg_id_raw) if isinstance(tg_id_raw, (int, str)) and str(tg_id_raw).isdigit() else None

                res_data = {
                    "status": "ok",
                    "user_id": tg_id,
                    "username": username,
                    "name": owner.get("name"),
                }
                SEETG_CACHE[slug] = res_data
                save_seetg_cache()

                log.info(
                    "SEE.TG [%s] -> @%s (ID: %s) | Free: %s | Bal: %s TON",
                    slug, username or "нет", tg_id,
                    SEETG_BILLING["free_remaining"], SEETG_BILLING["balance_ton"]
                )
                return res_data

        except Exception as e:
            log.warning("SEE.TG: Ошибка запроса для %s: %s", slug, e)
            await asyncio.sleep(1.0)

    return {"status": "error"}


# ============================================================
# ТОЧНЫЙ ПОДСЧЁТ ТОЛЬКО NFT-ПОДАРКОВ И ПРОВЕРКА НОВИЧКА
# ============================================================

def is_nft_gift(obj: Any) -> bool:
    gift = getattr(obj, "gift", None) or getattr(obj, "stargift", None) or obj
    for target in (obj, gift):
        cls_name = type(target).__name__.lower()
        if "unique" in cls_name:
            return True
    for target in (obj, gift):
        if getattr(target, "slug", None):
            return True
        num = getattr(target, "num", None)
        if num is not None and num != 0:
            return True
        gift_num = getattr(target, "gift_num", None)
        if gift_num is not None and gift_num != 0:
            return True
    return False


async def fetch_user_nft_gifts_count(worker: AccountWorker, peer: Any, user_id: int, max_to_check: int = 10) -> int:
    try:
        async with worker.lock:
            if not worker.client.is_connected():
                await worker.client.connect()
            loop_now = asyncio.get_running_loop().time()
            wait_for = PER_ACCOUNT_DELAY - (loop_now - worker.last_request_ts)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            worker.last_request_ts = asyncio.get_running_loop().time()

            raw_gifts = []
            if hasattr(functions.payments, "GetSavedStarGiftsRequest"):
                try:
                    res = await worker.client(
                        functions.payments.GetSavedStarGiftsRequest(
                            peer=peer,
                            offset="",
                            limit=max_to_check + 5,
                            exclude_unlimited=True,
                        )
                    )
                    raw_gifts = getattr(res, "gifts", []) or []
                except TypeError:
                    res = await worker.client(
                        functions.payments.GetSavedStarGiftsRequest(
                            peer=peer,
                            offset="",
                            limit=30,
                        )
                    )
                    raw_gifts = getattr(res, "gifts", []) or []
            elif hasattr(functions.payments, "GetUserStarGiftsRequest"):
                res = await worker.client(
                    functions.payments.GetUserStarGiftsRequest(
                        user_id=peer,
                        offset="",
                        limit=30,
                    )
                )
                raw_gifts = getattr(res, "gifts", []) or []

            return sum(1 for g in raw_gifts if is_nft_gift(g))

    except FloodWaitError as e:
        set_worker_flood(worker, e.seconds, "GetSavedStarGifts")
        return 0
    except DEAD_SESSION_ERRORS as e:
        await handle_dead_session(worker, e.__class__.__name__)
        return 0
    except Exception as e:
        log.warning("Ошибка подсчёта NFT для %s: %s", user_id, e)
        return 0


async def resolve_owner_info(raw_gift: Any) -> Optional[OwnerInfo]:
    slug = str(get_field(raw_gift, "slug") or "")
    owner_id = extract_owner_id(get_field(raw_gift, "owner_id"))
    seetg_username: Optional[str] = None
    seetg_name: Optional[str] = None

    if not owner_id and slug:
        seetg_data = await fetch_owner_from_seetg(slug)
        if seetg_data.get("status") != "ok":
            return None
        owner_id = seetg_data.get("user_id")
        seetg_username = seetg_data.get("username")
        seetg_name = seetg_data.get("name")

    if not owner_id and not seetg_username:
        return None

    if MONITOR_SETTINGS.get("username_mode") == "required" and not seetg_username:
        cached = OWNER_CACHE.get(str(owner_id)) if owner_id else None
        if not cached or not cached.get("username"):
            return None

    if owner_id:
        cache_key = str(owner_id)
        cached = OWNER_CACHE.get(cache_key)

        if cached and cached.get("v") == 4:
            username = cached.get("username") or seetg_username or None
            label = cached.get("label") or seetg_name or f"id:{owner_id}"
            key = f"username:{username.lower()}" if username else f"id:{owner_id}"
            return OwnerInfo(
                key=key,
                label=label,
                username=username,
                user_id=owner_id,
                bio=cached.get("bio", ""),
                nft_gifts_count=cached.get("nft_gifts_count", 0),
                total_gifts_count=cached.get("total_gifts_count", 0),
                rating_level=cached.get("rating_level", 0),
                rating_stars=cached.get("rating_stars", 0),
                is_premium=bool(cached.get("is_premium", False)),
                has_channel=bool(cached.get("has_channel", False)),
                bio_has_trader_words=bool(cached.get("bio_has_trader_words", False)),
            )

        worker = await acquire_worker()
        if worker is None:
            return None

        try:
            if seetg_username:
                peer = await worker.client.get_input_entity(seetg_username)
            else:
                peer = await worker.client.get_input_entity(owner_id)

            full = await worker.client(GetFullUserRequest(id=peer))
            full_user = full.full_user

            user_obj = next((u for u in full.users if u.id == owner_id), None) or (full.users[0] if full.users else None)
            username = getattr(user_obj, "username", None) or seetg_username
            first_name = getattr(user_obj, "first_name", "") or ""
            last_name = getattr(user_obj, "last_name", "") or ""
            name = f"{first_name} {last_name}".strip() or seetg_name or ""
            about = getattr(full_user, "about", "") or ""

            is_premium = bool(getattr(user_obj, "premium", False))
            has_channel = bool(getattr(full_user, "personal_channel_id", None))
            bio_trader = has_trader_keywords(name, about)

            label = f"@{username}" if username else (name or f"id:{owner_id}")
            key = f"username:{username.lower()}" if username else f"id:{owner_id}"

            total_gifts = getattr(full_user, "stargifts_count", None)
            if total_gifts is None:
                total_gifts = getattr(full_user, "gifts_count", 0)
            total_gifts = safe_int(total_gifts, 0)

            stars_rating = getattr(full_user, "stars_rating", None)
            rating_level = 0
            rating_stars = 0
            if stars_rating:
                rating_level = safe_int(getattr(stars_rating, "level", 0))
                rating_stars = safe_int(getattr(stars_rating, "stars", 0))

            max_rating = safe_int(MONITOR_SETTINGS.get("max_rating_level"), 0)
            max_total = safe_int(MONITOR_SETTINGS.get("max_owner_total_gifts"), 0)
            max_nft = safe_int(MONITOR_SETTINGS.get("max_owner_nft_gifts"), DEFAULT_MAX_NFT_GIFTS)

            early_reject = (
                (max_rating > 0 and rating_level >= max_rating)
                or (max_total > 0 and total_gifts > max_total)
                or (MONITOR_SETTINGS.get("strict_newbie_mode") and (has_channel or bio_trader))
                or (MONITOR_SETTINGS.get("no_premium_only") and is_premium)
            )

            if early_reject:
                nft_gifts_count = 0
            elif max_nft > 0 and total_gifts <= max_nft:
                nft_gifts_count = total_gifts
            else:
                nft_gifts_count = await fetch_user_nft_gifts_count(worker, peer, owner_id, max_to_check=max_nft + 1)

            OWNER_CACHE[cache_key] = {
                "v": 4,
                "label": label,
                "username": username or "",
                "user_id": owner_id,
                "bio": about,
                "nft_gifts_count": nft_gifts_count,
                "total_gifts_count": total_gifts,
                "rating_level": rating_level,
                "rating_stars": rating_stars,
                "is_premium": is_premium,
                "has_channel": has_channel,
                "bio_has_trader_words": bio_trader,
            }
            save_owner_cache()

            log.info(
                "[%s] Профиль %s: подарков=%s, NFT=%s, рейтинг=%s, канал=%s, bio_скуп=%s",
                worker.label, label, total_gifts, nft_gifts_count, rating_level, has_channel, bio_trader
            )

            return OwnerInfo(
                key=key,
                label=label,
                username=username,
                user_id=owner_id,
                bio=about,
                nft_gifts_count=nft_gifts_count,
                total_gifts_count=total_gifts,
                rating_level=rating_level,
                rating_stars=rating_stars,
                is_premium=is_premium,
                has_channel=has_channel,
                bio_has_trader_words=bio_trader,
            )

        except FloodWaitError as e:
            set_worker_flood(worker, e.seconds, "GetFullUser")
            return None
        except DEAD_SESSION_ERRORS as e:
            await handle_dead_session(worker, e.__class__.__name__)
            return None
        except Exception as e:
            log.warning("Не удалось открыть профиль %s (%s)", owner_id, e)
            return None

    return None


# ============================================================
# MARKETPLACE API (ТОЛЬКО ПРОДАЖА ЗА STARS)
# ============================================================

async def get_resale_page(
    gift_id: int,
    offset: str = "",
    limit: int = REQUEST_PAGE_LIMIT,
    sort_by_price: bool = False,
):
    global SUPPORTS_STARS_ONLY_PARAM
    for _ in range(3):
        worker = await acquire_worker()
        if worker is None:
            return None
        try:
            if SUPPORTS_STARS_ONLY_PARAM:
                try:
                    return await worker.client(
                        functions.payments.GetResaleStarGiftsRequest(
                            gift_id=gift_id,
                            offset=offset,
                            limit=limit,
                            sort_by_price=sort_by_price,
                            sort_by_num=False,
                            stars_only=True,
                            for_craft=False,
                        )
                    )
                except TypeError as e:
                    if "stars_only" in str(e) or "for_craft" in str(e):
                        SUPPORTS_STARS_ONLY_PARAM = False
                    else:
                        raise

            return await worker.client(
                functions.payments.GetResaleStarGiftsRequest(
                    gift_id=gift_id,
                    offset=offset,
                    limit=limit,
                    sort_by_price=sort_by_price,
                    sort_by_num=False,
                )
            )

        except FloodWaitError as e:
            set_worker_flood(worker, e.seconds, "GetResaleStarGifts")
        except DEAD_SESSION_ERRORS as e:
            await handle_dead_session(worker, e.__class__.__name__)
        except RpcMcgetFailError:
            log.warning("Временный сбой серверов Telegram (RpcMcgetFailError), ждём 2.5 сек...")
            await asyncio.sleep(2.5)
    return None


def is_stars_only_listing(raw_gift: Any) -> bool:
    if bool(get_field(raw_gift, "resale_ton_only", False)):
        return False
    price = extract_stars_amount(get_field(raw_gift, "resell_amount"))
    return price > 0


# ============================================================
# MONITORING LOGIC (ПАРАЛЛЕЛЬНЫЙ ОБХОД + ИИ-ПРОВЕРКА)
# ============================================================

async def get_monitor_models() -> List[BaseGift]:
    await ensure_models_loaded()
    selected = {safe_int(x, 0) for x in (MONITOR_SETTINGS.get("model_ids") or [])}
    if not selected:
        return []
    return [g for g in BASE_GIFTS if g.gift_id in selected and g.availability_resale > 0]


async def send_monitor_gift(market_gift: MarketGift, base_gift: BaseGift, ai_note: str = "") -> bool:
    target_chat_id = get_target_chat_id()
    if not target_chat_id:
        return False

    owner = market_gift.owner
    num_line = f" #{market_gift.num}" if market_gift.num else ""
    rating_display = str(owner.rating_level) if owner.rating_level > 0 else "0 / новичок"
    profile_line = f"{owner.nft_gifts_count} NFT (всего: {owner.total_gifts_count} шт.)"
    prem_tag = "без Premium" if not owner.is_premium else "с Premium"
    ai_block = f"🧠 <b>Вердикт ИИ:</b> {escape_text(ai_note)}\n" if ai_note and MONITOR_SETTINGS.get("ai_filter_enabled") and get_active_gemini_key() else ""

    public_text = (
        "🚨 <b>НОВЫЙ ЛОГ (STARS)</b>\n\n"
        f"🎁 <b>Подарок:</b> {escape_text(market_gift.title)}{num_line}\n"
        f"📦 <b>Модель:</b> {escape_text(base_gift.title)}\n"
        f"💰 <b>Цена:</b> {market_gift.price} ⭐\n"
        f"👑 <b>Рейтинг владельца:</b> {rating_display}\n"
        f"🖼 <b>Профиль:</b> {profile_line} • {prem_tag}\n"
        f"{ai_block}\n"
        "👇 Нажми кнопку ниже, чтобы забрать контакт себе в ЛС:"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔒 ЗАБРАТЬ ЛОГ", callback_data=f"claim_log:{market_gift.slug}")]
        ]
    )

    try:
        msg = await bot.send_message(
            chat_id=target_chat_id,
            text=public_text,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )

        CLAIMED_LOGS[market_gift.slug] = {
            "slug": market_gift.slug,
            "title": market_gift.title,
            "num": market_gift.num,
            "model": base_gift.title,
            "price": market_gift.price,
            "link": market_gift.link,
            "owner_username": owner.username or "",
            "owner_label": owner.display,
            "owner_id": owner.user_id,
            "rating_level": rating_display,
            "nft_count": owner.nft_gifts_count,
            "total_gifts": owner.total_gifts_count,
            "claimed_by_id": None,
            "claimed_by_display": None,
            "group_chat_id": target_chat_id,
            "group_message_id": msg.message_id,
        }
        save_claimed_logs()
        return True
    except Exception as e:
        log.error("Failed to send alert for %s: %s", market_gift.slug, e)
        return False


async def process_monitor_model(base_gift: BaseGift, first_scan: bool) -> int:
    sent_count = 0

    if get_active_accounts_count() == 0:
        return 0

    try:
        result = await get_resale_page(
            gift_id=base_gift.gift_id,
            offset="",
            limit=MONITOR_PAGE_LIMIT,
            sort_by_price=False,
        )
        if result is None:
            return 0

        raw_gifts = getattr(result, "gifts", []) or []

        for raw in raw_gifts:
            slug = get_field(raw, "slug")
            if not slug or str(slug) in SENT_GIFTS:
                continue

            slug = str(slug)

            if first_scan and not SEND_EXISTING_ON_FIRST_SCAN:
                SENT_GIFTS[slug] = {"baseline": True, "gift_id": base_gift.gift_id}
                continue

            if not is_stars_only_listing(raw):
                continue

            price = extract_stars_amount(get_field(raw, "resell_amount"))
            if not price_matches(price):
                continue

            owner = await resolve_owner_info(raw)
            if not owner:
                if get_active_accounts_count() > 0:
                    SENT_GIFTS[slug] = {"skipped": "hidden_or_unavailable", "gift_id": base_gift.gift_id}
                continue

            if not username_matches(owner) or is_owner_blacklisted(owner):
                SENT_GIFTS[slug] = {"skipped": "no_username_or_blacklist", "gift_id": base_gift.gift_id}
                continue

            passed, reason = owner_passes_target_filters(owner)
            if not passed:
                log.info("Пропуск %s (%s): %s", slug, owner.display, reason)
                SENT_GIFTS[slug] = {"skipped": reason, "gift_id": base_gift.gift_id}
                continue

            ai_ok, ai_reason = await check_owner_with_gemini_ai(owner, base_gift.title, price)
            if not ai_ok:
                log.info("🤖 ИИ отсеял %s (%s): %s", slug, owner.display, ai_reason)
                SENT_GIFTS[slug] = {"skipped": f"AI: {ai_reason}", "gift_id": base_gift.gift_id}
                continue

            market_gift = MarketGift(
                title=str(get_field(raw, "title") or base_gift.title),
                num=safe_int(get_field(raw, "num")),
                slug=slug,
                price=price,
                owner=owner,
            )

            SENT_GIFTS[slug] = {
                "title": market_gift.title,
                "price": market_gift.price,
                "owner": market_gift.owner.display,
                "gift_id": base_gift.gift_id,
            }

            sent = await send_monitor_gift(market_gift, base_gift, ai_note=ai_reason)
            if sent:
                await asyncio.sleep(MONITOR_SEND_DELAY)
                sent_count += 1
            else:
                SENT_GIFTS.pop(slug, None)

    except Exception as e:
        log.exception("Ошибка проверки модели %s: %s", base_gift.title, e)

    return sent_count


async def monitor_cycle(first_scan: bool = False) -> int:
    async with monitor_lock:
        models = await get_monitor_models()
        if not models:
            return 0

        total_sent = 0
        idx = 0
        while idx < len(models):
            if not MONITOR_SETTINGS.get("enabled"):
                break
            active_accs = get_active_accounts_count()
            if active_accs == 0:
                break

            batch_size = min(5, max(1, active_accs))
            batch = models[idx:idx + batch_size]
            idx += batch_size

            results = await asyncio.gather(
                *(process_monitor_model(m, first_scan) for m in batch),
                return_exceptions=True,
            )
            for model, res in zip(batch, results):
                if isinstance(res, int) and res > 0:
                    total_sent += res
                    log.info("Монитор: %s | отправлено=%s", model.title, res)

            if total_sent > 0:
                save_sent_gifts()
            await asyncio.sleep(BATCH_STEP_DELAY)

        if total_sent > 0 or first_scan:
            save_sent_gifts()

        return total_sent


async def monitor_loop() -> None:
    global monitor_running, monitor_first_scan_done
    monitor_running = True
    monitor_first_scan_done = False

    log.info(
        "Монитор запущен (акков в пуле: %s, интервал: %s сек, ИИ: %s [%s]).",
        len(ACCOUNTS_POOL),
        MONITOR_INTERVAL,
        "ВКЛ" if (MONITOR_SETTINGS.get("ai_filter_enabled") and get_active_gemini_key()) else "ВЫКЛ",
        get_active_gemini_model(),
    )

    try:
        while MONITOR_SETTINGS.get("enabled"):
            first_scan = not monitor_first_scan_done
            selected_models = MONITOR_SETTINGS.get("model_ids") or []
            if not selected_models:
                MONITOR_SETTINGS["enabled"] = False
                save_monitor_settings()
                break

            if get_active_accounts_count() == 0:
                await asyncio.sleep(4)
                continue

            await monitor_cycle(first_scan=first_scan)
            monitor_first_scan_done = True
            await asyncio.sleep(MONITOR_INTERVAL)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("Аварийное завершение цикла монитора: %s", e)
    finally:
        monitor_running = False
        log.info("Монитор остановлен.")


async def start_monitor() -> bool:
    global monitor_task
    if not get_target_chat_id() or not ACCOUNTS_POOL:
        return False

    MONITOR_SETTINGS["enabled"] = True
    save_monitor_settings()

    if monitor_task and not monitor_task.done():
        return True

    monitor_task = asyncio.create_task(monitor_loop())
    return True


async def stop_monitor() -> None:
    global monitor_task
    MONITOR_SETTINGS["enabled"] = False
    save_monitor_settings()

    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    monitor_task = None


# ============================================================
# MODELS LOADER
# ============================================================

async def load_base_gifts() -> List[BaseGift]:
    global BASE_GIFTS, BASE_GIFTS_BY_ID
    log.info("Загрузка моделей подарков с маркета...")
    worker = await acquire_worker()
    if worker is None:
        return []

    try:
        result = await worker.client(functions.payments.GetStarGiftsRequest(hash=0))
        raw_gifts = getattr(result, "gifts", []) or []

        gifts: List[BaseGift] = []
        for raw in raw_gifts:
            gift_id = safe_int(get_field(raw, "id"))
            title = get_field(raw, "title")
            availability_resale = safe_int(get_field(raw, "availability_resale"))
            resell_min_stars = safe_int(get_field(raw, "resell_min_stars"))

            if not gift_id or not title or availability_resale <= 0:
                continue

            gifts.append(
                BaseGift(
                    gift_id=gift_id,
                    title=str(title),
                    stars=safe_int(get_field(raw, "stars")),
                    availability_resale=availability_resale,
                    resell_min_stars=resell_min_stars,
                    sold_out=bool(get_field(raw, "sold_out")),
                )
            )

        gifts.sort(key=lambda g: (g.resell_min_stars or 999999999, g.title.lower()))
        BASE_GIFTS = gifts
        BASE_GIFTS_BY_ID = {g.gift_id: g for g in gifts}
        log.info("Успешно загружено %s моделей.", len(gifts))
        return gifts
    except Exception as e:
        log.exception("Ошибка загрузки моделей: %s", e)
        return []


async def ensure_models_loaded() -> None:
    if not BASE_GIFTS:
        await load_base_gifts()


# ============================================================
# KEYBOARDS & UI
# ============================================================

def admin_main_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📡 МОНИТОРИНГ", callback_data="monitor_menu")],
        [InlineKeyboardButton(text=f"👥 АККАУНТЫ (ПУЛ: {len(ACCOUNTS_POOL)})", callback_data="accounts_menu")],
        [InlineKeyboardButton(text="🚫 ЧЁРНЫЙ СПИСОК", callback_data="blacklist")],
        [InlineKeyboardButton(text="⚙️ СТАТИСТИКА / АДМИНКА", callback_data="admin_panel")],
        [InlineKeyboardButton(text="🔄 ОБНОВИТЬ МОДЕЛИ", callback_data="reload_models")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def user_subscription_keyboard(user_id: int) -> InlineKeyboardMarkup:
    u = SUBSCRIPTIONS["users"].get(str(user_id)) or {}
    rows = []

    if not u.get("trial_used"):
        rows.append([InlineKeyboardButton(text="🎁 ПРОБНЫЙ ДОСТУП (30 МИН БЕСПЛАТНО)", callback_data="sub_trial")])

    if u.get("plan") != "lifetime":
        rows.append([InlineKeyboardButton(text=f"💎 ПОДПИСКА НАВСЕГДА ({SUBSCRIPTION_PRICE_TON} TON)", callback_data="sub_buy")])

    rows.append([InlineKeyboardButton(text="🔑 АКТИВИРОВАТЬ / ПЕРЕНЕСТИ КЛЮЧ", callback_data="sub_activate_key")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def monitor_keyboard() -> InlineKeyboardMarkup:
    enabled = bool(MONITOR_SETTINGS.get("enabled"))
    strict_on = bool(MONITOR_SETTINGS.get("strict_newbie_mode", True))
    no_prem = bool(MONITOR_SETTINGS.get("no_premium_only", True))
    ai_on = bool(MONITOR_SETTINGS.get("ai_filter_enabled", True))
    cur_model = get_active_gemini_model()

    rows = []
    if enabled:
        rows.append([InlineKeyboardButton(text="⏸ ОСТАНОВИТЬ", callback_data="monitor_stop")])
    else:
        rows.append([InlineKeyboardButton(text="▶️ ЗАПУСТИТЬ", callback_data="monitor_start")])

    rows.extend([
        [
            InlineKeyboardButton(text="💰 ЦЕНА", callback_data="monitor_price"),
            InlineKeyboardButton(text="👑 РЕЙТИНГ", callback_data="monitor_rating"),
        ],
        [
            InlineKeyboardButton(text="🖼 NFT ЛИМИТ", callback_data="monitor_nft_limit"),
            InlineKeyboardButton(text="📦 ВСЕГО ПОДАРКОВ", callback_data="monitor_total_gifts"),
        ],
        [
            InlineKeyboardButton(
                text=f"🎯 НЕ ШАРЯЩИЕ: {'ВКЛ' if strict_on else 'ВЫКЛ'}",
                callback_data="monitor_toggle_newbie",
            ),
            InlineKeyboardButton(
                text=f"⭐️ БЕЗ PREMIUM: {'ВКЛ' if no_prem else 'ВЫКЛ'}",
                callback_data="monitor_toggle_premium",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"🧠 ИИ-ФИЛЬТР: {'ВКЛ' if ai_on else 'ВЫКЛ'}",
                callback_data="monitor_toggle_ai",
            ),
            InlineKeyboardButton(
                text="🔑 GEMINI КЛЮЧ",
                callback_data="monitor_set_gemini",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"🤖 МОДЕЛЬ ИИ: {cur_model}"[:64],
                callback_data="monitor_choose_gemini_model",
            )
        ],
        [
            InlineKeyboardButton(text="👤 USERNAME", callback_data="monitor_username"),
            InlineKeyboardButton(text="🎁 МОДЕЛИ", callback_data="monitor_models"),
        ],
        [InlineKeyboardButton(text="🧹 СБРОСИТЬ ОТПРАВЛЕННЫЕ", callback_data="sent_clear")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="menu")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def monitor_status_text() -> str:
    enabled = bool(MONITOR_SETTINGS.get("enabled"))
    target = get_target_chat_id()
    min_price = safe_int(MONITOR_SETTINGS.get("min_price"), 0)
    max_price = safe_int(MONITOR_SETTINGS.get("max_price"), 0)
    max_rating = safe_int(MONITOR_SETTINGS.get("max_rating_level"), 0)
    max_nft = safe_int(MONITOR_SETTINGS.get("max_owner_nft_gifts"), 0)
    max_total = safe_int(MONITOR_SETTINGS.get("max_owner_total_gifts"), 0)
    strict_on = bool(MONITOR_SETTINGS.get("strict_newbie_mode", True))
    no_prem = bool(MONITOR_SETTINGS.get("no_premium_only", True))
    ai_on = bool(MONITOR_SETTINGS.get("ai_filter_enabled", True))
    active_key = get_active_gemini_key()
    has_ai_key = bool(active_key)
    cur_model = get_active_gemini_model()

    if min_price and max_price:
        price_text = f"{min_price}—{max_price} ⭐"
    elif max_price:
        price_text = f"до {max_price} ⭐"
    elif min_price:
        price_text = f"от {min_price} ⭐"
    else:
        price_text = "без ограничения"

    username_text = "только с username" if MONITOR_SETTINGS.get("username_mode") == "required" else "все (с/без username)"
    selected_models = MONITOR_SETTINGS.get("model_ids") or []
    model_text = "все модели" if not selected_models else f"{len(selected_models)} выбранных"
    target_text = str(target) if target else "❌ не задана"

    rating_info = f"строго < {max_rating} уровня" if max_rating > 0 else "выключен"
    nft_info = f"макс. {max_nft} шт." if max_nft > 0 else "выкл"
    total_info = f"макс. {max_total} шт." if max_total > 0 else "выкл"

    if ai_on and has_ai_key:
        key_type = "Vertex Express" if active_key.startswith("AQ.") else "AI Studio"
        ai_status = f"✅ ВКЛ ({cur_model} • {key_type})"
    elif ai_on and not has_ai_key:
        ai_status = "⚠️ ВКЛ, но не задан GEMINI_API_KEY"
    else:
        ai_status = "❌ ВЫКЛ"

    active_accs = get_active_accounts_count()
    total_accs = len(ACCOUNTS_POOL)
    parallel_batch = min(5, max(1, active_accs)) if active_accs else 1

    flood_rem = get_min_flood_wait_remaining()
    flood_line = (
        f"\n⏸ <b>ПАУЗА (все аккаунты в FloodWait): ещё {flood_rem // 60} мин.</b>\n"
        "<i>(Как только таймер закончится, аккаунты сами вернутся в работу)</i>"
        if total_accs > 0 and active_accs == 0
        else ""
    )

    return (
        "📡 <b>АВТОМАТИЧЕСКИЙ МОНИТОРИНГ (ТУРБО + ИИ)</b>\n\n"
        f"Статус: {'🟢 РАБОТАЕТ' if enabled else '🔴 ОСТАНОВЛЕН'}\n"
        f"Аккаунтов в работе: <b>{active_accs} из {total_accs}</b> (потоков: {parallel_batch}x)\n"
        f"Интервал круга: <b>{MONITOR_INTERVAL} сек.</b> (глубина: {MONITOR_PAGE_LIMIT} лота)\n"
        f"Целевая группа: <code>{escape_text(target_text)}</code>\n"
        f"Диапазон цены: <b>{escape_text(price_text)}</b>\n"
        f"Фильтр рейтинга: <b>{escape_text(rating_info)}</b>\n"
        f"NFT / Всего подарков: <b>{escape_text(nft_info)} / {escape_text(total_info)}</b>\n"
        f"Режим «Не шарящие»: <b>{'✅ ВКЛ' if strict_on else '❌ ВЫКЛ'}</b>\n"
        f"Фильтр Premium: <b>{'✅ только без Premium' if no_prem else 'любые'}</b>\n"
        f"ИИ-фильтр Gemini: <b>{ai_status}</b>\n"
        f"Юзернейм: <b>{escape_text(username_text)}</b>\n"
        f"Модели: <b>{escape_text(model_text)}</b>\n"
        f"See.tg: Free <b>{SEETG_BILLING['free_remaining']}</b> | Баланс <b>{SEETG_BILLING['balance_ton']} TON</b>\n"
        f"Обработано лотов: <b>{len(SENT_GIFTS)}</b>"
        f"{flood_line}\n\n"
        "💡 Для привязки группы напиши команду <code>/set_target_chat</code> прямо в ней."
    )


# ============================================================
# BOT HANDLERS: КОМАНДЫ И СТАРТ
# ============================================================

@dp.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено.")


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    if is_admin(user_id):
        if not ACCOUNTS_POOL:
            await message.answer(
                "⚠️ В пуле нет ни одного аккаунта Telethon.\nДобавь первый аккаунт ниже:",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🔑 ВСТАВИТЬ STRING SESSION", callback_data="paste_session_btn")],
                        [InlineKeyboardButton(text="📱 ВОЙТИ ПО НОМЕРУ", callback_data="add_session_btn")],
                    ]
                ),
            )
            return
        await ensure_models_loaded()
        await message.answer(
            "👑 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\nВыбери нужный раздел:",
            parse_mode="HTML",
            reply_markup=admin_main_keyboard(),
        )
        return

    u = SUBSCRIPTIONS["users"].get(str(user_id)) or {}
    if u.get("plan") == "lifetime":
        key_str = u.get("key") or "—"
        transfers = SUBSCRIPTIONS["keys"].get(key_str, {}).get("transfers_left", 0)
        text = (
            "💎 <b>У вас активна подписка НАВСЕГДА!</b>\n\n"
            f"🔑 Ваш ключ для переноса: <code>{escape_text(key_str)}</code>\n"
            f"🔄 Осталось переносов на другие аккаунты: <b>{transfers} из {MAX_KEY_TRANSFERS}</b>\n\n"
            "В группе нажимайте кнопку <b>«🔒 ЗАБРАТЬ ЛОГ»</b> под нужным подарком — контакт продавца сразу придёт сюда."
        )
    elif has_active_subscription(user_id):
        mins_left = max(1, int((float(u.get("expires_at") or 0) - time.time()) // 60))
        text = (
            f"🎁 <b>У вас активен пробный доступ</b> (осталось ~{mins_left} мин.).\n\n"
            f"После окончания бот автоматически исключит вас из группы. Вы можете оформить подписку навсегда за <b>{SUBSCRIPTION_PRICE_TON} TON</b>:"
        )
    else:
        text = (
            "👋 <b>ДОБРО ПОЖАЛОВАТЬ В ПРИВАТНЫЙ ПАРСЕР ПОДАРКОВ</b>\n\n"
            "• 🎁 <b>Пробный доступ на 30 минут</b> — бесплатно (1 раз на аккаунт).\n"
            f"• 💎 <b>Подписка навсегда</b> — <b>{SUBSCRIPTION_PRICE_TON} TON</b> (включает личный ключ с возможностью переноса на другой аккаунт до {MAX_KEY_TRANSFERS} раз).\n\n"
            "Выбери подходящий вариант ниже:"
        )

    await message.answer(text, parse_mode="HTML", reply_markup=user_subscription_keyboard(user_id))


# ============================================================
# ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ: ПОДПИСКА, ОПЛАТА, КЛЮЧИ, ЗАБОР ЛОГА
# ============================================================

@dp.callback_query(F.data == "sub_trial")
async def sub_trial_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if is_admin(user_id):
        await callback.answer("Вы администратор.", show_alert=True)
        return

    chat_id = get_target_chat_id()
    if not chat_id:
        await callback.answer("Группа парсера ещё не настроена администратором.", show_alert=True)
        return

    async with subs_lock:
        u = SUBSCRIPTIONS["users"].get(str(user_id)) or {}
        if u.get("trial_used"):
            await callback.answer("❌ Вы уже использовали пробный период на этом аккаунте.", show_alert=True)
            return

        invite_link = await create_one_time_invite_link(chat_id, label=f"trial_{user_id}")
        if not invite_link:
            await callback.answer("❌ Не удалось создать ссылку в группу. Сообщите администратору.", show_alert=True)
            return

        expires_at = time.time() + TRIAL_DURATION_SECONDS
        SUBSCRIPTIONS["users"][str(user_id)] = {
            "plan": "trial",
            "expires_at": expires_at,
            "trial_used": True,
            "key": None,
            "kicked": False,
        }
        save_subscriptions()

    await callback.message.edit_text(
        "✅ <b>Пробный доступ на 30 минут активирован!</b>\n\n"
        f"🔗 Одноразовая ссылка для входа в группу:\n{invite_link}\n\n"
        "⚠️ Ссылка сработает <b>только 1 раз</b>. Через 30 минут бот автоматически исключит вас из группы.",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    await callback.answer()


@dp.callback_query(F.data == "sub_buy")
async def sub_buy_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    u = SUBSCRIPTIONS["users"].get(str(user_id)) or {}
    if u.get("plan") == "lifetime":
        await callback.answer("У вас уже есть подписка навсегда!", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я ОПЛАТИЛ", callback_data="sub_paid_confirm")],
            [InlineKeyboardButton(text="◀️ НАЗАД", callback_data="sub_back_start")],
        ]
    )

    await callback.message.edit_text(
        f"💎 <b>ОФОРМЛЕНИЕ ПОДПИСКИ НАВСЕГДА ({SUBSCRIPTION_PRICE_TON} TON)</b>\n\n"
        f"1. Переведите ровно <b>{SUBSCRIPTION_PRICE_TON} TON</b> на кошелёк (сеть TON):\n"
        f"<code>{escape_text(TON_WALLET_ADDRESS)}</code>\n\n"
        "2. После перевода нажмите кнопку <b>«✅ Я ОПЛАТИЛ»</b> ниже.\n"
        "3. Администратор проверит поступление средств и выдаст одноразовую ссылку в группу и ключ переноса.",
        parse_mode="HTML",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "sub_back_start")
async def sub_back_start(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "👋 <b>ДОБРО ПОЖАЛОВАТЬ В ПРИВАТНЫЙ ПАРСЕР ПОДАРКОВ</b>\n\n"
        "• 🎁 <b>Пробный доступ на 30 минут</b> — бесплатно (1 раз на аккаунт).\n"
        f"• 💎 <b>Подписка навсегда</b> — <b>{SUBSCRIPTION_PRICE_TON} TON</b> (с ключом переноса до {MAX_KEY_TRANSFERS} раз).",
        parse_mode="HTML",
        reply_markup=user_subscription_keyboard(user_id),
    )
    await callback.answer()


@dp.callback_query(F.data == "sub_paid_confirm")
async def sub_paid_confirm_callback(callback: CallbackQuery):
    user = callback.from_user
    user_id = user.id

    async with subs_lock:
        if str(user_id) in SUBSCRIPTIONS["pending_payments"]:
            await callback.answer("⏳ Ваша заявка уже отправлена администраторам, ожидайте проверки.", show_alert=True)
            return

        SUBSCRIPTIONS["pending_payments"][str(user_id)] = {
            "username": user.username or "",
            "full_name": user.full_name or str(user_id),
            "created_at": time.time(),
        }
        save_subscriptions()

    user_tag = f"@{escape_text(user.username)}" if user.username else "без username"
    admin_text = (
        "💰 <b>НОВАЯ ЗАЯВКА НА ПОДПИСКУ (5 TON)</b>\n\n"
        f"👤 Пользователь: {escape_text(user.full_name)} ({user_tag})\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"💎 Сумма: <b>{SUBSCRIPTION_PRICE_TON} TON</b>\n\n"
        "Проверьте кошелёк и подтвердите выдачу доступа:"
    )
    admin_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ ОДОБРИТЬ", callback_data=f"pay_approve:{user_id}"),
                InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"pay_reject:{user_id}"),
            ]
        ]
    )

    for admin_id in ADMIN_IDS:
        if admin_id > 0:
            try:
                await bot.send_message(admin_id, admin_text, parse_mode="HTML", reply_markup=admin_kb)
            except Exception as e:
                log.warning("Не удалось уведомить админа %s: %s", admin_id, e)

    await callback.message.edit_text(
        "⏳ <b>Заявка отправлена администраторам!</b>\n\n"
        "Как только перевод будет подтверждён, бот пришлёт вам сюда одноразовую ссылку в группу и личный ключ.",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("pay_approve:"))
async def pay_approve_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    target_uid = int(callback.data.split(":")[1])
    chat_id = get_target_chat_id()
    if not chat_id:
        await callback.answer("Сначала привяжите группу через /set_target_chat!", show_alert=True)
        return

    async with subs_lock:
        SUBSCRIPTIONS["pending_payments"].pop(str(target_uid), None)
        existing_u = SUBSCRIPTIONS["users"].get(str(target_uid)) or {}

        if existing_u.get("plan") == "lifetime" and existing_u.get("key"):
            await callback.message.edit_text(f"✅ Пользователь <code>{target_uid}</code> уже одобрен ранее.", parse_mode="HTML")
            await callback.answer()
            return

        invite_link = await create_one_time_invite_link(chat_id, label=f"life_{target_uid}")
        if not invite_link:
            await callback.answer("❌ Ошибка создания инвайт-ссылки. Проверьте права бота в группе!", show_alert=True)
            return

        license_key = generate_license_key()
        SUBSCRIPTIONS["users"][str(target_uid)] = {
            "plan": "lifetime",
            "expires_at": None,
            "trial_used": True,
            "key": license_key,
            "kicked": False,
        }
        SUBSCRIPTIONS["keys"][license_key] = {
            "owner_id": target_uid,
            "transfers_left": MAX_KEY_TRANSFERS,
        }
        save_subscriptions()

    try:
        await bot.send_message(
            target_uid,
            "🎉 <b>Оплата подтверждена! Подписка НАВСЕГДА активирована.</b>\n\n"
            f"🔗 Ваша одноразовая ссылка для входа в группу:\n{invite_link}\n\n"
            f"🔑 Ваш персональный ключ для переноса подписки:\n<code>{license_key}</code>\n"
            f"<i>(Можно перенести на другой Telegram-аккаунт до {MAX_KEY_TRANSFERS} раз. Старый аккаунт при этом автоматически исключается из группы.)</i>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error("Не удалось отправить ключ пользователю %s: %s", target_uid, e)

    approver = f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)
    await callback.message.edit_text(
        f"✅ <b>Одобрено администратором {escape_text(approver)}</b>\n"
        f"Пользователь: <code>{target_uid}</code>\n"
        f"Выдан ключ: <code>{license_key}</code>",
        parse_mode="HTML",
    )
    await callback.answer("Одобрено!")


@dp.callback_query(F.data.startswith("pay_reject:"))
async def pay_reject_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    target_uid = int(callback.data.split(":")[1])
    async with subs_lock:
        SUBSCRIPTIONS["pending_payments"].pop(str(target_uid), None)
        save_subscriptions()

    try:
        await bot.send_message(
            target_uid,
            "❌ <b>Ваша заявка на оплату отклонена администратором</b> (перевод не найден).\n"
            "Если вы действительно отправили 5 TON, свяжитесь с поддержкой или повторите заявку через /start.",
            parse_mode="HTML",
        )
    except Exception:
        pass

    rejector = f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)
    await callback.message.edit_text(
        f"❌ <b>Заявка от <code>{target_uid}</code> отклонена</b> ({escape_text(rejector)}).",
        parse_mode="HTML",
    )
    await callback.answer("Отклонено.")


@dp.callback_query(F.data == "sub_activate_key")
async def sub_activate_key_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(KeyTransferState.waiting_key)
    await callback.message.answer(
        "🔑 <b>ПЕРЕНОС ПОДПИСКИ ПО КЛЮЧУ</b>\n\n"
        "Отправьте ваш ключ формата <code>KEY-XXXXXXXX-XXXXXXXX</code>.\n"
        "⚠️ Внимание: предыдущий аккаунт будет исключён из группы, а вам выдастся новая одноразовая ссылка для входа.\n\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.message(KeyTransferState.waiting_key)
async def sub_activate_key_input(message: Message, state: FSMContext):
    new_uid = message.from_user.id
    key_input = (message.text or "").strip().upper()
    chat_id = get_target_chat_id()

    if not chat_id:
        await state.clear()
        await message.answer("❌ Группа парсера не настроена. Обратитесь к администратору.")
        return

    async with subs_lock:
        key_data = SUBSCRIPTIONS["keys"].get(key_input)
        if not key_data:
            await message.answer("❌ Ключ не найден. Проверьте правильность ввода или нажмите /cancel.")
            return

        old_uid = int(key_data.get("owner_id") or 0)
        transfers_left = int(key_data.get("transfers_left") or 0)

        if old_uid == new_uid:
            await state.clear()
            await message.answer("✅ Этот ключ уже активирован на вашем текущем аккаунте!")
            return

        if transfers_left <= 0:
            await state.clear()
            await message.answer("❌ Лимит переносов по этому ключу исчерпан (0 из 3).")
            return

        invite_link = await create_one_time_invite_link(chat_id, label=f"tr_{new_uid}")
        if not invite_link:
            await state.clear()
            await message.answer("❌ Не удалось создать ссылку в группу. Обратитесь к администратору.")
            return

        if old_uid and old_uid != new_uid:
            await kick_user_from_group(chat_id, old_uid)
            if str(old_uid) in SUBSCRIPTIONS["users"]:
                SUBSCRIPTIONS["users"][str(old_uid)]["plan"] = "transferred"
                SUBSCRIPTIONS["users"][str(old_uid)]["key"] = None
                SUBSCRIPTIONS["users"][str(old_uid)]["kicked"] = True
            try:
                await bot.send_message(
                    old_uid,
                    "🔄 <b>Ваша подписка была перенесена по ключу на другой аккаунт.</b> Вы исключены из группы.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

        key_data["owner_id"] = new_uid
        key_data["transfers_left"] = transfers_left - 1

        SUBSCRIPTIONS["users"][str(new_uid)] = {
            "plan": "lifetime",
            "expires_at": None,
            "trial_used": True,
            "key": key_input,
            "kicked": False,
        }
        save_subscriptions()

    await state.clear()
    await message.answer(
        "✅ <b>Подписка успешно перенесена на этот аккаунт!</b>\n\n"
        f"🔗 Новая одноразовая ссылка для входа в группу:\n{invite_link}\n\n"
        f"🔄 Осталось переносов по ключу: <b>{transfers_left - 1} из {MAX_KEY_TRANSFERS}</b>",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@dp.callback_query(F.data.startswith("claim_log:"))
async def claim_log_callback(callback: CallbackQuery):
    user = callback.from_user
    user_id = user.id
    slug = callback.data.split(":", 1)[1]

    if not has_active_subscription(user_id):
        await callback.answer("⛔ У вас нет активной подписки или она истекла!", show_alert=True)
        return

    async with claim_lock:
        log_data = CLAIMED_LOGS.get(slug)
        if not log_data:
            await callback.answer("❌ Этот лог устарел или уже недоступен.", show_alert=True)
            return

        if log_data.get("claimed_by_id"):
            who = log_data.get("claimed_by_display") or "другой участник"
            await callback.answer(f"⛔ Лог уже забрал {who}!", show_alert=True)
            return

        owner_username = log_data.get("owner_username")
        owner_display = f"@{owner_username}" if owner_username else log_data.get("owner_label", "не указан")
        profile_url = f"https://t.me/{owner_username}" if owner_username else ""
        num_str = f" #{log_data['num']}" if log_data.get("num") else ""

        dm_text = (
            "🎯 <b>ВЫ ЗАБРАЛИ ЛОГ!</b>\n\n"
            f"🎁 <b>Подарок:</b> {escape_text(log_data.get('title'))}{num_str}\n"
            f"📦 <b>Модель:</b> {escape_text(log_data.get('model'))}\n"
            f"💰 <b>Цена:</b> {log_data.get('price')} ⭐\n"
            f"👤 <b>Продавец:</b> {escape_text(owner_display)}\n"
            f"👑 <b>Рейтинг:</b> {escape_text(log_data.get('rating_level'))}\n"
            f"🖼 <b>Профиль:</b> {log_data.get('nft_count')} NFT (всего: {log_data.get('total_gifts')} шт.)\n\n"
            f'🔗 <a href="{escape_text(log_data.get("link"))}">Открыть подарок на маркете</a>'
        )
        if profile_url:
            dm_text += f'\n🔎 <a href="{escape_text(profile_url)}">Написать продавцу</a>'

        try:
            await bot.send_message(
                chat_id=user_id,
                text=dm_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except TelegramForbiddenError:
            await callback.answer("⚠️ Сначала запустите бота в ЛС (/start), чтобы он мог присылать вам логи!", show_alert=True)
            return
        except Exception as e:
            log.error("Ошибка отправки лога %s в ЛС юзеру %s: %s", slug, user_id, e)
            await callback.answer("❌ Не удалось отправить лог в ЛС. Напишите боту /start.", show_alert=True)
            return

        claimer_display = f"@{user.username}" if user.username else f"{user.first_name} (ID:{user_id})"
        log_data["claimed_by_id"] = user_id
        log_data["claimed_by_display"] = claimer_display
        save_claimed_logs()

        edited_group_text = (
            f"✅ <b>Лог забрал:</b> {escape_text(claimer_display)}\n\n"
            f"🎁 <b>Подарок:</b> {escape_text(log_data.get('title'))}{num_str} ({log_data.get('price')} ⭐)\n"
            f"📦 <b>Модель:</b> {escape_text(log_data.get('model'))}"
        )

        try:
            await callback.message.edit_text(
                text=edited_group_text,
                parse_mode="HTML",
                reply_markup=None,
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning("Не удалось отредактировать сообщение лога в группе: %s", e)

    await callback.answer("✅ Лог отправлен вам в личные сообщения!")


# ============================================================
# УПРАВЛЕНИЕ ПУЛОМ АККАУНТОВ (ДОБАВЛЕНИЕ / УДАЛЕНИЕ)
# ============================================================

def accounts_menu_keyboard() -> InlineKeyboardMarkup:
    rows = []
    now = time.time()
    for w in ACCOUNTS_POOL:
        if now < w.flood_until:
            rem_min = max(1, int((w.flood_until - now) // 60))
            status_icon = f"⏳ {rem_min}м"
        else:
            status_icon = "🟢"
        rows.append([
            InlineKeyboardButton(
                text=f"{status_icon} {w.label} [DC {w.dc_id}] — Удалить"[:64],
                callback_data=f"del_acc:{w.acc_id}",
            )
        ])

    rows.append([InlineKeyboardButton(text="🔑 ДОБАВИТЬ ЧЕРЕЗ STRING SESSION", callback_data="paste_session_btn")])
    rows.append([InlineKeyboardButton(text="📱 ДОБАВИТЬ ПО НОМЕРУ (DC 1–5)", callback_data="add_session_btn")])
    rows.append([InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def accounts_menu_text() -> str:
    active = get_active_accounts_count()
    total = len(ACCOUNTS_POOL)
    parallel_batch = min(5, max(1, active)) if active else 1

    lines = [
        "👥 <b>УПРАВЛЕНИЕ ПУЛОМ АККАУНТОВ TELETHON</b>\n",
        f"Активных аккаунтов: <b>{active} из {total}</b>",
        f"Параллельных потоков: <b>{parallel_batch}x</b>\n",
    ]
    if not ACCOUNTS_POOL:
        lines.append("⚠️ Пул пуст. Добавь хотя бы 1 аккаунт ниже.")
    else:
        lines.append("Нажми на аккаунт в списке ниже, чтобы удалить его из пула:")

    return "\n".join(lines)


@dp.callback_query(F.data == "accounts_menu")
async def accounts_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        accounts_menu_text(),
        parse_mode="HTML",
        reply_markup=accounts_menu_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("del_acc:"))
async def del_acc_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    acc_id = callback.data.split(":", 1)[1]
    removed = await remove_account_from_pool(acc_id)
    if not ACCOUNTS_POOL:
        await stop_monitor()
    await callback.answer("🗑 Аккаунт удалён из пула." if removed else "Аккаунт не найден.")
    await callback.message.edit_text(
        accounts_menu_text(),
        parse_mode="HTML",
        reply_markup=accounts_menu_keyboard(),
    )


@dp.message(Command("set_target_chat"))
async def set_target_chat(message: Message):
    if not is_admin(message.from_user.id):
        return
    MONITOR_SETTINGS["target_chat_id"] = message.chat.id
    save_monitor_settings()
    await message.answer(f"✅ Целевая группа привязана!\nChat ID: <code>{message.chat.id}</code>", parse_mode="HTML")


@dp.message(Command("add_session"))
@dp.callback_query(F.data == "add_session_btn")
async def add_session_start(event: Message | CallbackQuery, state: FSMContext):
    if not is_admin(event.from_user.id):
        return
    await state.clear()
    await state.set_state(AuthState.waiting_phone)
    text = (
        "📱 <b>ДОБАВЛЕНИЕ АККАУНТА В ПУЛ ПО НОМЕРУ (DC 1–5)</b>\n\n"
        "Введи номер телефона (например: <code>+79991234567</code>):\n"
        "❌ Отмена — /cancel"
    )
    if isinstance(event, CallbackQuery):
        await event.message.answer(text, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, parse_mode="HTML")


@dp.message(AuthState.waiting_phone)
async def auth_phone(message: Message, state: FSMContext):
    global pending_auth_client
    if not is_admin(message.from_user.id):
        return
    raw_phone = (message.text or "").strip()
    digits = "".join(ch for ch in raw_phone if ch.isdigit())
    if len(digits) < 10:
        await message.answer("❌ Некорректный номер. Введи номер с кодом страны (например: <code>+79991234567</code>):", parse_mode="HTML")
        return

    phone = "+" + digits
    try:
        if pending_auth_client and pending_auth_client.is_connected():
            await pending_auth_client.disconnect()

        pending_auth_client = TelegramClient(StringSession(), API_ID, API_HASH)
        await pending_auth_client.connect()

        try:
            sent_code = await pending_auth_client.send_code_request(phone)
        except PhoneMigrateError as e:
            await pending_auth_client._switch_dc(e.new_dc)
            sent_code = await pending_auth_client.send_code_request(phone)

        await state.update_data(phone=phone, phone_code_hash=sent_code.phone_code_hash)
        await state.set_state(AuthState.waiting_code)
        dc_id = getattr(pending_auth_client.session, "dc_id", "?")
        await message.answer(
            f"✅ Код отправлен (<b>DC {dc_id}</b>).\n"
            "⚠️ Вводи код <b>с пробелом</b> (например: <code>12 345</code>):",
            parse_mode="HTML",
        )
    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка отправки кода: {e}")


@dp.message(AuthState.waiting_code)
async def auth_code(message: Message, state: FSMContext):
    global pending_auth_client
    if not is_admin(message.from_user.id) or pending_auth_client is None:
        return

    raw_text = (message.text or "").strip()
    if raw_text.startswith("+"):
        await message.answer("❌ Сейчас бот ждёт <b>код из Telegram</b>, а не номер телефона. Для сброса нажми /cancel", parse_mode="HTML")
        return

    code = "".join(ch for ch in raw_text if ch.isdigit())
    if not code or len(code) > 6:
        await message.answer("❌ Введи 5-значный код подтверждения (например: <code>12 345</code>):", parse_mode="HTML")
        return

    data = await state.get_data()
    try:
        await pending_auth_client.sign_in(
            phone=data.get("phone"),
            code=code,
            phone_code_hash=data.get("phone_code_hash"),
        )
        session_str = pending_auth_client.session.save()
        await pending_auth_client.disconnect()
        pending_auth_client = None
        await state.clear()

        worker = await add_account_to_pool(session_str, save=True)
        await ensure_models_loaded()
        await message.answer(
            f"✅ <b>Аккаунт добавлен в пул!</b>\n👤 {escape_text(worker.label)} [DC {worker.dc_id}]\n"
            f"Всего в пуле: <b>{len(ACCOUNTS_POOL)}</b>",
            parse_mode="HTML",
            reply_markup=accounts_menu_keyboard(),
        )
    except SessionPasswordNeededError:
        await state.set_state(AuthState.waiting_password)
        await message.answer("🔐 На аккаунте включён 2FA. Введи облачный пароль:")
    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
        await message.answer(f"❌ Неверный или истёкший код ({e.__class__.__name__}). Попробуй ещё раз или нажми /cancel")
    except Exception as e:
        await message.answer(f"❌ Ошибка входа: {e}\nНажми /cancel для сброса.")


@dp.message(AuthState.waiting_password)
async def auth_password(message: Message, state: FSMContext):
    global pending_auth_client
    if not is_admin(message.from_user.id) or pending_auth_client is None:
        return
    try:
        await pending_auth_client.sign_in(password=(message.text or "").strip())
        session_str = pending_auth_client.session.save()
        await pending_auth_client.disconnect()
        pending_auth_client = None
        await state.clear()

        worker = await add_account_to_pool(session_str, save=True)
        await ensure_models_loaded()
        await message.answer(
            f"✅ <b>Аккаунт добавлен в пул!</b>\n👤 {escape_text(worker.label)} [DC {worker.dc_id}]\n"
            f"Всего в пуле: <b>{len(ACCOUNTS_POOL)}</b>",
            parse_mode="HTML",
            reply_markup=accounts_menu_keyboard(),
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}")


@dp.message(Command("set_session"))
@dp.callback_query(F.data == "paste_session_btn")
async def paste_session_start(event: Message | CallbackQuery, state: FSMContext):
    if not is_admin(event.from_user.id):
        return

    await state.clear()
    await state.set_state(AuthState.waiting_string_session)
    text = (
        "🔑 <b>ДОБАВЛЕНИЕ STRING SESSION В ПУЛ</b>\n\n"
        "Отправь сюда строку сессии Telethon (начинается с <code>1...</code>).\n"
        "Бот проверит её, добавит в пул активных аккаунтов и сохранит в PostgreSQL.\n\n"
        "❌ Отмена — /cancel"
    )
    if isinstance(event, CallbackQuery):
        await event.message.answer(text, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, parse_mode="HTML")


@dp.message(AuthState.waiting_string_session)
async def paste_session_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    session_str = (message.text or "").strip()
    if len(session_str) < 50 or " " in session_str:
        await message.answer(
            "❌ Некорректная строка <code>StringSession</code> (слишком короткая или содержит пробелы).\n"
            "Отправь полную строку без лишнего текста или нажми /cancel.",
            parse_mode="HTML",
        )
        return

    try:
        await message.delete()
    except Exception:
        pass

    try:
        worker = await add_account_to_pool(session_str, save=True)
        if not worker:
            await message.answer("❌ Эта строка сессии не авторизована, заморожена или сброшена в Telegram. Попробуй другую или /cancel.")
            return

        await state.clear()
        await ensure_models_loaded()
        await message.answer(
            f"✅ <b>Аккаунт добавлен в пул и сохранён в PostgreSQL!</b>\n"
            f"👤 Профиль: <b>{escape_text(worker.label)}</b>\n"
            f"🆔 ID: <code>{worker.user_id}</code> | Дата-центр: <b>DC {worker.dc_id}</b>\n"
            f"📊 Всего аккаунтов в пуле: <b>{len(ACCOUNTS_POOL)}</b>",
            parse_mode="HTML",
            reply_markup=accounts_menu_keyboard(),
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка проверки строки сессии: <code>{escape_text(e)}</code>\nПопробуй ещё раз или /cancel", parse_mode="HTML")


# ============================================================
# АДМИН-МЕНЮ И НАСТРОЙКИ МОНИТОРИНГА (+ АВТОПОДБОР МОДЕЛЕЙ ИИ)
# ============================================================

@dp.callback_query(F.data == "menu")
async def menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступно только администраторам.", show_alert=True)
        return
    await callback.message.edit_text(
        "👑 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\nВыбери нужный раздел:",
        parse_mode="HTML",
        reply_markup=admin_main_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "monitor_menu")
async def monitor_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Только для администратора.", show_alert=True)
        return

    await callback.message.edit_text(
        monitor_status_text(),
        parse_mode="HTML",
        reply_markup=monitor_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == "monitor_start")
async def monitor_start_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    if not get_target_chat_id():
        await callback.answer("Сначала привяжи группу через /set_target_chat", show_alert=True)
        return
    if not ACCOUNTS_POOL:
        await callback.answer("Добавь хотя бы 1 аккаунт в меню 👥 АККАУНТЫ", show_alert=True)
        return
    if not MONITOR_SETTINGS.get("model_ids"):
        await callback.answer("Выбери хотя бы 1 модель в меню 🎁 МОДЕЛИ", show_alert=True)
        return

    await start_monitor()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer("🟢 Мониторинг запущен.")


@dp.callback_query(F.data == "monitor_stop")
async def monitor_stop_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await stop_monitor()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer("⏸ Мониторинг остановлен.")


@dp.callback_query(F.data == "monitor_toggle_newbie")
async def monitor_toggle_newbie_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    MONITOR_SETTINGS["strict_newbie_mode"] = not bool(MONITOR_SETTINGS.get("strict_newbie_mode", True))
    save_monitor_settings()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "monitor_toggle_premium")
async def monitor_toggle_premium_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    MONITOR_SETTINGS["no_premium_only"] = not bool(MONITOR_SETTINGS.get("no_premium_only", True))
    save_monitor_settings()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "monitor_toggle_ai")
async def monitor_toggle_ai_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    new_state = not bool(MONITOR_SETTINGS.get("ai_filter_enabled", True))
    if new_state and not get_active_gemini_key():
        await callback.answer("⚠️ Сначала задай ключ через кнопку «🔑 GEMINI КЛЮЧ» или ENV GEMINI_API_KEY!", show_alert=True)
        return
    MONITOR_SETTINGS["ai_filter_enabled"] = new_state
    save_monitor_settings()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer(f"🧠 ИИ-фильтр {'включён' if new_state else 'выключен'}.")


@dp.callback_query(F.data == "monitor_set_gemini")
async def monitor_set_gemini_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AuthState.waiting_gemini_key)
    await callback.message.answer(
        "🔑 <b>НАСТРОЙКА GOOGLE GEMINI API КЛЮЧА</b>\n\n"
        "Отправь сюда твой ключ от Google AI Studio (начинается на <code>AIza...</code> или <code>AQ....</code>).\n"
        "Бот сам проверит его и покажет, какие модели Gemini можно подключить.\n"
        "Чтобы удалить ключ — отправь <code>0</code>.\n\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.message(AuthState.waiting_gemini_key)
async def monitor_set_gemini_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    val = (message.text or "").strip()
    try:
        await message.delete()
    except Exception:
        pass

    if val == "0":
        MONITOR_SETTINGS["gemini_api_key"] = ""
        MONITOR_SETTINGS["gemini_available_models"] = []
        MONITOR_SETTINGS["ai_filter_enabled"] = False
        save_monitor_settings()
        await state.clear()
        await message.answer("🗑 Gemini API ключ удалён, ИИ-фильтр выключен.", reply_markup=monitor_keyboard())
        return

    if not (val.startswith("AIza") or val.startswith("AQ.")) or len(val) < 25:
        await message.answer(
            "❌ Неверный формат ключа (должен начинаться на <code>AIza...</code> или <code>AQ....</code>).\n"
            "Попробуй ещё раз или /cancel.",
            parse_mode="HTML",
        )
        return

    status_msg = await message.answer("⏳ Проверяю ключ и определяю доступные модели Gemini...")
    ok, models, err = await discover_gemini_models_for_key(val)

    if not ok:
        await status_msg.edit_text(
            f"❌ <b>Ключ не прошёл проверку!</b>\n"
            f"Ошибка Google API: <code>{escape_text(err)}</code>\n\n"
            "Отправь другой ключ или нажми /cancel.",
            parse_mode="HTML",
        )
        return

    chosen_model = models[0]
    MONITOR_SETTINGS["gemini_api_key"] = val
    MONITOR_SETTINGS["gemini_available_models"] = models
    MONITOR_SETTINGS["gemini_model"] = chosen_model
    MONITOR_SETTINGS["ai_filter_enabled"] = True
    save_monitor_settings()
    await state.clear()

    models_list_text = "\n".join(f"• <code>{escape_text(m)}</code>" for m in models[:10])
    key_kind = "Vertex AI Express (AQ.)" if val.startswith("AQ.") else "Google AI Studio (AIza)"

    await status_msg.edit_text(
        f"✅ <b>Ключ принят ({key_kind})!</b>\n\n"
        f"🤖 Подключена модель: <b>{escape_text(chosen_model)}</b>\n"
        f"📋 Доступные модели по этому ключу ({len(models)} шт.):\n{models_list_text}\n\n"
        "<i>Переключить модель можно кнопкой «🤖 МОДЕЛЬ ИИ» в меню мониторинга.</i>",
        parse_mode="HTML",
        reply_markup=monitor_keyboard(),
    )


@dp.callback_query(F.data == "monitor_choose_gemini_model")
async def monitor_choose_gemini_model_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    api_key = get_active_gemini_key()
    if not api_key:
        await callback.answer("⚠️ Сначала добавь ключ через «🔑 GEMINI КЛЮЧ»!", show_alert=True)
        return

    models = MONITOR_SETTINGS.get("gemini_available_models") or []
    if not models:
        await callback.answer("Проверяю доступные модели...")
        ok, models, err = await discover_gemini_models_for_key(api_key)
        if not ok:
            await callback.message.answer(f"❌ Не удалось получить список моделей: <code>{escape_text(err)}</code>", parse_mode="HTML")
            return
        MONITOR_SETTINGS["gemini_available_models"] = models
        save_monitor_settings()

    cur_model = get_active_gemini_model()
    rows = []
    for m in models[:12]:
        mark = "✅" if m == cur_model else "⚪️"
        rows.append([InlineKeyboardButton(text=f"{mark} {m}"[:64], callback_data=f"set_gem_mod:{m}"[:64])])
    rows.append([InlineKeyboardButton(text="🔄 ПЕРЕПРОВЕРИТЬ КЛЮЧ", callback_data="recheck_gemini_models")])
    rows.append([InlineKeyboardButton(text="◀️ НАЗАД", callback_data="monitor_menu")])

    await callback.message.edit_text(
        "🤖 <b>ВЫБОР МОДЕЛИ GOOGLE GEMINI</b>\n\n"
        "Нажми на любую доступную модель из списка ниже:\n"
        "<i>(Рекомендуется <code>gemini-2.5-flash</code> или <code>gemini-2.5-flash-lite</code> — они самые быстрые)</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@dp.callback_query(F.data == "recheck_gemini_models")
async def recheck_gemini_models_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    api_key = get_active_gemini_key()
    if not api_key:
        await callback.answer("Ключ не задан.", show_alert=True)
        return
    await callback.answer("Обновляю список моделей...")
    ok, models, err = await discover_gemini_models_for_key(api_key)
    if not ok:
        await callback.message.answer(f"❌ Ошибка проверки ключа: <code>{escape_text(err)}</code>", parse_mode="HTML")
        return
    MONITOR_SETTINGS["gemini_available_models"] = models
    if get_active_gemini_model() not in models:
        MONITOR_SETTINGS["gemini_model"] = models[0]
    save_monitor_settings()
    await monitor_choose_gemini_model_callback(callback)


@dp.callback_query(F.data.startswith("set_gem_mod:"))
async def set_gem_mod_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    chosen = callback.data.split(":", 1)[1]
    MONITOR_SETTINGS["gemini_model"] = chosen
    MONITOR_SETTINGS["ai_filter_enabled"] = True
    save_monitor_settings()
    await callback.answer(f"✅ Выбрана модель: {chosen}")
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


@dp.callback_query(F.data == "monitor_username")
async def monitor_username_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = MONITOR_SETTINGS.get("username_mode", "required")
    MONITOR_SETTINGS["username_mode"] = "required" if current == "any" else "any"
    save_monitor_settings()
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "monitor_price")
async def monitor_price_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MonitorPriceState.waiting_range)
    await callback.message.answer(
        "💰 Отправь диапазон цены в звёздах (например: <code>0 3000</code>).\n"
        "<code>0 0</code> — без ограничения.\n❌ Отмена — /cancel",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.message(MonitorPriceState.waiting_range)
async def monitor_price_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").strip().replace("-", " ").split()
    if len(parts) != 2:
        await message.answer("❌ Введи два числа через пробел, например: <code>0 3000</code>", parse_mode="HTML")
        return
    try:
        min_p, max_p = int(parts[0]), int(parts[1])
        if min_p < 0 or max_p < 0 or (min_p and max_p and min_p > max_p):
            raise ValueError
    except ValueError:
        await message.answer("❌ Некорректный диапазон.")
        return

    MONITOR_SETTINGS["min_price"] = min_p
    MONITOR_SETTINGS["max_price"] = max_p
    save_monitor_settings()
    await state.clear()
    await message.answer(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


@dp.callback_query(F.data == "monitor_rating")
async def monitor_rating_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MonitorRatingState.waiting_rating)
    await callback.message.answer(
        "👑 <b>Настройка фильтра рейтинга Telegram</b>\n\n"
        "• <code>5</code> — только юзеры с рейтингом <b>строго меньше 5</b>.\n"
        "• <code>0</code> — отключить проверку рейтинга.\n\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(MonitorRatingState.waiting_rating)
async def monitor_rating_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Отправь одно целое число (например, <code>5</code> или <code>0</code>).", parse_mode="HTML")
        return

    val = int(text)
    MONITOR_SETTINGS["max_rating_level"] = val
    save_monitor_settings()
    await state.clear()
    await message.answer(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


@dp.callback_query(F.data == "monitor_nft_limit")
async def monitor_nft_limit_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MonitorNftLimitState.waiting_limit)
    await callback.message.answer(
        "🖼 <b>Максимум NFT-подарков в профиле</b>\n\n"
        "• <code>3</code> — пропускать тех, у кого больше 3 NFT-подарков.\n"
        "• <code>0</code> — выключить фильтр.\n\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(MonitorNftLimitState.waiting_limit)
async def monitor_nft_limit_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Отправь число (например: <code>3</code> или <code>0</code>).", parse_mode="HTML")
        return

    val = int(text)
    MONITOR_SETTINGS["max_owner_nft_gifts"] = val
    save_monitor_settings()
    await state.clear()
    await message.answer(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


@dp.callback_query(F.data == "monitor_total_gifts")
async def monitor_total_gifts_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MonitorTotalGiftsState.waiting_total_limit)
    await callback.message.answer(
        "📦 <b>Максимум ВСЕХ подарков в профиле</b>\n\n"
        "• <code>60</code> — пропускать юзеров, у которых всего больше 60 подарков.\n"
        "• <code>0</code> — выключить фильтр.\n\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(MonitorTotalGiftsState.waiting_total_limit)
async def monitor_total_gifts_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Отправь число (например: <code>60</code> или <code>0</code>).", parse_mode="HTML")
        return

    val = int(text)
    MONITOR_SETTINGS["max_owner_total_gifts"] = val
    save_monitor_settings()
    await state.clear()
    await message.answer(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


def monitor_models_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    selected = set(MONITOR_SETTINGS.get("model_ids") or [])
    start = page * GIFTS_PER_PAGE
    end = start + GIFTS_PER_PAGE
    rows = []

    for gift in BASE_GIFTS[start:end]:
        mark = "✅" if gift.gift_id in selected else "⬜"
        rows.append([
            InlineKeyboardButton(
                text=f"{mark} {gift.title}"[:64],
                callback_data=f"toggle_monitor_model:{page}:{gift.gift_id}",
            )
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"monitor_models_page:{page - 1}"))
    if end < len(BASE_GIFTS):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"monitor_models_page:{page + 1}"))
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton(text="✅ ВЫБРАТЬ ВСЕ", callback_data=f"monitor_models_all:{page}"),
        InlineKeyboardButton(text="⬜ СБРОСИТЬ ВСЕ", callback_data=f"monitor_models_none:{page}"),
    ])
    rows.append([InlineKeyboardButton(text="◀️ НАЗАД", callback_data="monitor_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "monitor_models")
async def monitor_models_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    selected = MONITOR_SETTINGS.get("model_ids") or []
    await callback.message.edit_text(
        f"🎁 <b>МОДЕЛИ ДЛЯ МОНИТОРИНГА</b> (Выбрано: {len(selected)})\n\n✅ = парсим, ⬜ = пропускаем.",
        parse_mode="HTML",
        reply_markup=monitor_models_keyboard(0),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("monitor_models_page:"))
async def monitor_models_page_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    page = int(callback.data.split(":")[1])
    selected = MONITOR_SETTINGS.get("model_ids") or []
    await callback.message.edit_text(
        f"🎁 <b>МОДЕЛИ ДЛЯ МОНИТОРИНГА</b> (Выбрано: {len(selected)})\n\n✅ = парсим, ⬜ = пропускаем.",
        parse_mode="HTML",
        reply_markup=monitor_models_keyboard(page),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("toggle_monitor_model:"))
async def toggle_monitor_model_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, page_str, gift_id_str = callback.data.split(":")
    page, gift_id = int(page_str), int(gift_id_str)
    selected = set(MONITOR_SETTINGS.get("model_ids") or [])

    if gift_id in selected:
        selected.remove(gift_id)
    else:
        selected.add(gift_id)

    MONITOR_SETTINGS["model_ids"] = sorted(selected)
    save_monitor_settings()

    try:
        await callback.message.edit_text(
            f"🎁 <b>МОДЕЛИ ДЛЯ МОНИТОРИНГА</b> (Выбрано: {len(selected)})\n\n✅ = парсим, ⬜ = пропускаем.",
            parse_mode="HTML",
            reply_markup=monitor_models_keyboard(page),
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("monitor_models_all"))
async def monitor_models_all_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    page = int(callback.data.split(":")[1]) if ":" in callback.data else 0
    MONITOR_SETTINGS["model_ids"] = [g.gift_id for g in BASE_GIFTS]
    save_monitor_settings()
    await callback.message.edit_text(
        f"🎁 <b>МОДЕЛИ МОНИТОРИНГА</b>\n\nВыбраны все {len(BASE_GIFTS)} моделей.",
        parse_mode="HTML",
        reply_markup=monitor_models_keyboard(page),
    )
    await callback.answer("Выбраны все.")


@dp.callback_query(F.data.startswith("monitor_models_none"))
async def monitor_models_none_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    page = int(callback.data.split(":")[1]) if ":" in callback.data else 0
    MONITOR_SETTINGS["model_ids"] = []
    MONITOR_SETTINGS["enabled"] = False
    save_monitor_settings()
    await callback.message.edit_text(
        "🎁 <b>МОДЕЛИ МОНИТОРИНГА</b>\n\nВсе модели сняты. Мониторинг выключен.",
        parse_mode="HTML",
        reply_markup=monitor_models_keyboard(page),
    )
    await callback.answer("Сброшено.")


@dp.callback_query(F.data == "sent_clear")
async def sent_clear_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    SENT_GIFTS.clear()
    save_sent_gifts()
    await callback.answer("🧹 Список отправленных очищен (кэш see.tg сохранён).", show_alert=True)
    await callback.message.edit_text(monitor_status_text(), parse_mode="HTML", reply_markup=monitor_keyboard())


@dp.callback_query(F.data == "blacklist")
async def show_blacklist(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    items = list(OWNERS_BLACKLIST.items())
    text = "🚫 <b>ЧЁРНЫЙ СПИСОК ПУСТ</b>" if not items else f"🚫 <b>ЧЁРНЫЙ СПИСОК</b> ({len(items)})"
    rows = []
    for key, label in items[:20]:
        rows.append([InlineKeyboardButton(text=f"❌ {label}"[:64], callback_data=f"unban:{key}"[:64])])
    rows.append([InlineKeyboardButton(text="➕ ДОБАВИТЬ В ЧС", callback_data="blacklist_add")])
    rows.append([InlineKeyboardButton(text="🧹 ОЧИСТИТЬ ВСЁ", callback_data="blacklist_clear")])
    rows.append([InlineKeyboardButton(text="🏠 ГЛАВНОЕ", callback_data="menu")])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()


@dp.callback_query(F.data == "blacklist_add")
async def blacklist_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(BlacklistState.waiting_value)
    await callback.message.answer("🚫 Введи <b>@username</b> или числовой <b>user_id</b> продавца:\n❌ /cancel", parse_mode="HTML")
    await callback.answer()


@dp.message(BlacklistState.waiting_value)
async def blacklist_add_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not raw:
        return
    if raw.startswith("@"):
        key = f"username:{raw[1:].lower()}"
        label = raw
    elif raw.lstrip("-").isdigit():
        key = f"id:{int(raw)}"
        label = key
    else:
        await message.answer("❌ Нужен @username или числовой user_id.")
        return

    OWNERS_BLACKLIST[key] = label
    save_owner_blacklist()
    await state.clear()
    await message.answer(f"✅ Добавлен в ЧС: <code>{key}</code>", parse_mode="HTML")


@dp.callback_query(F.data.startswith("unban:"))
async def unban_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    key = callback.data.split(":", 1)[1]
    OWNERS_BLACKLIST.pop(key, None)
    save_owner_blacklist()
    await callback.answer(f"✅ Удалено: {key}")
    await show_blacklist(callback)


@dp.callback_query(F.data == "blacklist_clear")
async def clear_blacklist(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    OWNERS_BLACKLIST.clear()
    save_owner_blacklist()
    await callback.answer("🧹 ЧС очищен.")
    await show_blacklist(callback)


@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    lifetime_count = sum(1 for u in SUBSCRIPTIONS["users"].values() if u.get("plan") == "lifetime")
    trial_count = sum(1 for u in SUBSCRIPTIONS["users"].values() if u.get("plan") == "trial")
    ai_ok = MONITOR_SETTINGS.get("ai_filter_enabled") and get_active_gemini_key()

    await callback.message.edit_text(
        "⚙️ <b>СТАТИСТИКА И АДМИН-ПАНЕЛЬ</b>\n\n"
        f"👥 Аккаунтов в пуле: <b>{get_active_accounts_count()} активных / {len(ACCOUNTS_POOL)} всего</b>\n"
        f"🧠 ИИ Gemini: <b>{'✅ ВКЛ (' + get_active_gemini_model() + ')' if ai_ok else '❌ ВЫКЛ'}</b>\n"
        f"🗄 База данных: {'✅ PostgreSQL' if db_pool else '⚠️ Локальная'}\n"
        f"👑 Админов: {len([x for x in ADMIN_IDS if x > 0])}\n"
        f"💎 Подписок навсегда: <b>{lifetime_count}</b> | Триал: <b>{trial_count}</b>\n"
        f"📦 Моделей в базе: {len(BASE_GIFTS)}\n"
        f"💰 See.tg Free: <b>{SEETG_BILLING['free_remaining']}</b> | Баланс: <b>{SEETG_BILLING['balance_ton']} TON</b>\n"
        f"💾 Кэш see.tg: {len(SEETG_CACHE)} шт. | Профилей: {len(OWNER_CACHE)} шт.\n"
        f"🚫 В blacklist: {len(OWNERS_BLACKLIST)}\n"
        f"📤 Обработано лотов: {len(SENT_GIFTS)}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="👥 УПРАВЛЯТЬ АККАУНТАМИ", callback_data="accounts_menu")],
                [InlineKeyboardButton(text="🔄 ОБНОВИТЬ МОДЕЛИ", callback_data="reload_models")],
                [InlineKeyboardButton(text="🏠 ГЛАВНОЕ", callback_data="menu")],
            ]
        ),
    )
    await callback.answer()


@dp.callback_query(F.data == "reload_models")
async def reload_models_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.answer("Загрузка моделей...")
    await load_base_gifts()
    await callback.message.edit_text(f"✅ Модели обновлены. Доступно: {len(BASE_GIFTS)}", reply_markup=admin_main_keyboard())


# ============================================================
# MAIN
# ============================================================

async def main():
    global OWNERS_BLACKLIST, OWNER_CACHE, SEETG_CACHE, SENT_GIFTS, CLAIMED_LOGS, SUBSCRIPTIONS, http_session, trial_watcher_task, accounts_watcher_task

    validate_config()
    await init_db()

    saved_accounts = await db_get("telethon_accounts", [])
    if isinstance(saved_accounts, list):
        for item in saved_accounts:
            if isinstance(item, dict) and item.get("session"):
                await add_account_to_pool(item["session"], save=False)
        save_accounts_pool()

    old_single = await db_get("telethon_session", {})
    if isinstance(old_single, dict) and old_single.get("session"):
        await add_account_to_pool(old_single["session"], save=True)

    if TELETHON_SESSION:
        await add_account_to_pool(TELETHON_SESSION, save=True)

    OWNERS_BLACKLIST = await db_get("owner_blacklist", {})
    OWNER_CACHE = await db_get("owner_cache", {})
    SEETG_CACHE = await db_get("seetg_cache", {})
    SENT_GIFTS = await db_get("sent_gifts", {})
    CLAIMED_LOGS = await db_get("claimed_logs", {})

    saved_subs = await db_get("subscriptions", {})
    if isinstance(saved_subs, dict):
        SUBSCRIPTIONS["users"] = saved_subs.get("users") or {}
        SUBSCRIPTIONS["keys"] = saved_subs.get("keys") or {}
        SUBSCRIPTIONS["pending_payments"] = saved_subs.get("pending_payments") or {}

    saved_settings = await db_get("monitor_settings", {})
    apply_loaded_settings(saved_settings)

    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    http_session = aiohttp.ClientSession(connector=connector)
    trial_watcher_task = asyncio.create_task(trial_watcher_loop())
    accounts_watcher_task = asyncio.create_task(accounts_watcher_loop())

    # Если ключ Gemini уже задан, но список моделей ещё не проверялся — проверяем при старте
    active_gem_key = get_active_gemini_key()
    if active_gem_key and not MONITOR_SETTINGS.get("gemini_available_models"):
        ok, models, err = await discover_gemini_models_for_key(active_gem_key)
        if ok:
            MONITOR_SETTINGS["gemini_available_models"] = models
            if get_active_gemini_model() not in models:
                MONITOR_SETTINGS["gemini_model"] = models[0]
            save_monitor_settings()
            log.info("✅ Gemini ключ проверен! Доступные модели: %s (активна: %s)", ", ".join(models), get_active_gemini_model())
        else:
            log.warning("⚠️ Ключ Gemini не прошёл проверку при старте: %s", err)

    log.info(
        "Данные загружены: Аккаунтов в пуле=%s, ЧС=%s, кэш профилей=%s, кэш see.tg=%s, отправлено=%s, моделей=%s",
        len(ACCOUNTS_POOL), len(OWNERS_BLACKLIST), len(OWNER_CACHE), len(SEETG_CACHE), len(SENT_GIFTS), len(MONITOR_SETTINGS.get("model_ids") or [])
    )

    if ACCOUNTS_POOL:
        await ensure_models_loaded()
        if MONITOR_SETTINGS.get("enabled") and get_target_chat_id() and MONITOR_SETTINGS.get("model_ids"):
            await start_monitor()
    else:
        log.warning("В пуле нет аккаунтов Telethon. Добавь их через /set_session или кнопку в Админ-панели.")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    log.info("Бот запущен в режиме polling.")
    try:
        await dp.start_polling(bot)
    finally:
        await stop_monitor()
        if trial_watcher_task and not trial_watcher_task.done():
            trial_watcher_task.cancel()
        if accounts_watcher_task and not accounts_watcher_task.done():
            accounts_watcher_task.cancel()
        pending_saves = [t for t in db_save_tasks.values() if not t.done()]
        if pending_saves:
            await asyncio.gather(*pending_saves, return_exceptions=True)
        for w in ACCOUNTS_POOL:
            try:
                if w.client.is_connected():
                    await w.client.disconnect()
            except Exception:
                pass
        if db_pool is not None:
            await db_pool.close()
        if http_session and not http_session.closed:
            await http_session.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
