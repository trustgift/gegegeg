import asyncio
import html
import json
import logging
import os
import secrets
import socket
import time
from dataclasses import dataclass
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
    TelegramBadRequest,
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
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)
from telethon.tl import functions
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import PeerUser


# ============================================================
# CONFIG (БЕРЁТСЯ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ХОСТИНГА)
# ============================================================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TELETHON_SESSION = os.getenv("TELETHON_SESSION", "").strip()

# Админы через запятую в ENV: ADMIN_IDS=1602809044,123456789
ADMIN_IDS: Set[int] = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

# База данных PostgreSQL (переменная DATABASE_URL в ENV)
RAW_DB_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE_URL = RAW_DB_URL.replace("Postgresql://", "postgresql://").replace("postgres://", "postgresql://")

TON_WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS", "").strip()
SUBSCRIPTION_PRICE_TON = int(os.getenv("SUBSCRIPTION_PRICE_TON", "5"))
TRIAL_DURATION_SECONDS = 30 * 60  # 30 минут
MAX_KEY_TRANSFERS = 3             # Максимум переносов подписки по ключу

# Настройки API see.tg (kartoshka.free)
SEETG_TOKEN = os.getenv("SEETG_TOKEN", "").strip()
SEETG_BASE_URL = "https://kartoshka.free/v1"
SEETG_REQUEST_DELAY = 0.8

SESSION_NAME = "telethon_market_userbot"
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "0"))

# Скорость работы (сбалансированный быстрый режим без частых FloodWait)
MONITOR_INTERVAL = 15       # Пауза между кругами обхода (сек)
MONITOR_PAGE_LIMIT = 2      # Проверять 2 самых свежих лота с модели за круг
MONITOR_API_DELAY = 2.2     # Безопасная задержка запросов профилей в Telethon (сек)
MONITOR_SEND_DELAY = 0.35   # Пауза между отправкой сообщений в чат (сек)
MODEL_STEP_DELAY = 0.8      # Пауза при переходе к следующей модели (сек)

# Фильтры по умолчанию (обычные юзеры с 1-3 подарками)
DEFAULT_MAX_NFT_GIFTS = 3        # Максимум NFT-подарков в профиле
DEFAULT_MAX_TOTAL_GIFTS = 6      # Максимум ВСЕХ подарков в профиле (NFT + обычные)
DEFAULT_MAX_RATING = 5           # Порог рейтинга (строго < 5, включая -1 и 0)
DEFAULT_MIN_PRICE = 0
DEFAULT_MAX_PRICE = 0
DEFAULT_USERNAME_MODE = "required"

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
    """Автоповтор обработки апдейта при кратковременном сбросе TCP-соединения хостингом."""
    for attempt in range(3):
        try:
            return await handler(event, data)
        except TelegramNetworkError as e:
            if attempt == 2:
                raise
            log.warning("Сетевой сброс Telegram API (%s), повтор %s/3...", e, attempt + 1)
            await asyncio.sleep(0.5)


user_client = TelegramClient(
    StringSession(TELETHON_SESSION) if TELETHON_SESSION else SESSION_NAME,
    API_ID,
    API_HASH,
)
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
}

monitor_task: Optional[asyncio.Task] = None
trial_watcher_task: Optional[asyncio.Task] = None
monitor_running = False
monitor_first_scan_done = False
monitor_lock = asyncio.Lock()
market_api_lock = asyncio.Lock()
last_market_api_request = 0.0
profile_flood_until = 0.0


# ============================================================
# DATA CLASSES
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
    nft_gifts_count: int = 0
    total_gifts_count: int = 0
    rating_level: int = 0
    rating_stars: int = 0

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


# ============================================================
# FSM STATES
# ============================================================

class AuthState(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_string_session = State()


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
        await asyncio.sleep(1.2)
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
    if len(OWNER_CACHE) > 10000:
        for k in list(OWNER_CACHE.keys())[:-8000]:
            OWNER_CACHE.pop(k, None)
    schedule_db_save("owner_cache", OWNER_CACHE)


def save_seetg_cache() -> None:
    if len(SEETG_CACHE) > 15000:
        for k in list(SEETG_CACHE.keys())[:-12000]:
            SEETG_CACHE.pop(k, None)
    schedule_db_save("seetg_cache", SEETG_CACHE)


def save_sent_gifts() -> None:
    if len(SENT_GIFTS) > 4000:
        for k in list(SENT_GIFTS.keys())[:-3000]:
            SENT_GIFTS.pop(k, None)
    schedule_db_save("sent_gifts", SENT_GIFTS)


def save_subscriptions() -> None:
    schedule_db_save("subscriptions", SUBSCRIPTIONS, immediate=True)


def save_claimed_logs() -> None:
    if len(CLAIMED_LOGS) > 2000:
        for k in list(CLAIMED_LOGS.keys())[:-1500]:
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
            async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
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
                    retry_after = float(resp.headers.get("Retry-After", 2.0))
                    log.warning("SEE.TG: Лимит запросов (429), ждём %.1f сек.", retry_after)
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status == 404:
                    res_data = {"status": "not_found"}
                    SEETG_CACHE[slug] = res_data
                    save_seetg_cache()
                    return res_data

                if resp.status != 200:
                    log.warning("SEE.TG: HTTP %s для %s", resp.status, slug)
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
            await asyncio.sleep(1.5)

    return {"status": "error"}


# ============================================================
# TELETHON CLIENT & RATE LIMITER
# ============================================================

async def ensure_user_client_connected() -> None:
    if not user_client.is_connected():
        await user_client.connect()


async def is_user_client_authorized() -> bool:
    await ensure_user_client_connected()
    try:
        return await user_client.is_user_authorized()
    except (AuthKeyUnregisteredError, ValueError, ConnectionError) as e:
        log.warning("Сессия сброшена или не зарегистрирована: %s", e)
        return False


async def pace_market_api() -> None:
    global last_market_api_request
    async with market_api_lock:
        now = asyncio.get_running_loop().time()
        wait_for = MONITOR_API_DELAY - (now - last_market_api_request)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        last_market_api_request = asyncio.get_running_loop().time()


# ============================================================
# ТОЧНЫЙ ПОДСЧЁТ ТОЛЬКО NFT-ПОДАРКОВ И ПРОФИЛЯ
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


async def fetch_user_nft_gifts_count(user_id: int, max_to_check: int = 10) -> int:
    global profile_flood_until
    if time.time() < profile_flood_until:
        return 0

    try:
        await pace_market_api()
        peer = await user_client.get_input_entity(user_id)
        raw_gifts = []

        if hasattr(functions.payments, "GetSavedStarGiftsRequest"):
            try:
                res = await user_client(
                    functions.payments.GetSavedStarGiftsRequest(
                        peer=peer,
                        offset="",
                        limit=max_to_check + 5,
                        exclude_unlimited=True,
                    )
                )
                raw_gifts = getattr(res, "gifts", []) or []
            except TypeError:
                res = await user_client(
                    functions.payments.GetSavedStarGiftsRequest(
                        peer=peer,
                        offset="",
                        limit=30,
                    )
                )
                raw_gifts = getattr(res, "gifts", []) or []
        elif hasattr(functions.payments, "GetUserStarGiftsRequest"):
            res = await user_client(
                functions.payments.GetUserStarGiftsRequest(
                    user_id=peer,
                    offset="",
                    limit=30,
                )
            )
            raw_gifts = getattr(res, "gifts", []) or []

        return sum(1 for g in raw_gifts if is_nft_gift(g))

    except FloodWaitError as e:
        wait_time = int(e.seconds) + 1
        profile_flood_until = time.time() + wait_time
        log.warning("FloodWait на проверку NFT (%s сек). Ставим парсинг профилей на паузу.", wait_time)
        return 0
    except Exception as e:
        log.warning("Ошибка подсчёта NFT для %s: %s", user_id, e)
        return 0


async def resolve_owner_info(raw_gift: Any) -> Optional[OwnerInfo]:
    global profile_flood_until

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

    if MONITOR_SETTINGS.get("username_mode") == "required" and not seetg_username and owner_id:
        cached = OWNER_CACHE.get(str(owner_id))
        if cached and cached.get("username"):
            return None

    if owner_id:
        cache_key = str(owner_id)
        cached = OWNER_CACHE.get(cache_key)

        cache_is_corrupted = (
            cached
            and cached.get("v") != 2
            and cached.get("nft_gifts_count", 0) == 0
            and cached.get("total_gifts_count", 0) > 0
        )

        if cached and "nft_gifts_count" in cached and "rating_level" in cached and not cache_is_corrupted:
            username = cached.get("username") or seetg_username or None
            label = cached.get("label") or seetg_name or f"id:{owner_id}"
            key = f"username:{username.lower()}" if username else f"id:{owner_id}"
            return OwnerInfo(
                key=key,
                label=label,
                username=username,
                user_id=owner_id,
                nft_gifts_count=cached.get("nft_gifts_count", 0),
                total_gifts_count=cached.get("total_gifts_count", 0),
                rating_level=cached.get("rating_level", 0),
                rating_stars=cached.get("rating_stars", 0),
            )

        if time.time() < profile_flood_until:
            return None

        try:
            await ensure_user_client_connected()
            await pace_market_api()

            try:
                peer = await user_client.get_input_entity(owner_id)
            except Exception:
                if seetg_username:
                    peer = await user_client.get_input_entity(seetg_username)
                else:
                    raise

            full = await user_client(GetFullUserRequest(id=peer))
            full_user = full.full_user

            user_obj = next((u for u in full.users if u.id == owner_id), None) or (full.users[0] if full.users else None)
            username = getattr(user_obj, "username", None) or seetg_username
            first_name = getattr(user_obj, "first_name", "") or ""
            last_name = getattr(user_obj, "last_name", "") or ""
            name = f"{first_name} {last_name}".strip() or seetg_name or ""

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
            early_reject = (max_rating > 0 and rating_level >= max_rating) or (max_total > 0 and total_gifts > max_total)

            if early_reject:
                nft_gifts_count = 0
            else:
                max_limit = safe_int(MONITOR_SETTINGS.get("max_owner_nft_gifts"), DEFAULT_MAX_NFT_GIFTS)
                nft_gifts_count = await fetch_user_nft_gifts_count(owner_id, max_to_check=max_limit + 1)

            OWNER_CACHE[cache_key] = {
                "v": 2,
                "label": label,
                "username": username or "",
                "user_id": owner_id,
                "nft_gifts_count": nft_gifts_count,
                "total_gifts_count": total_gifts,
                "rating_level": rating_level,
                "rating_stars": rating_stars,
            }
            save_owner_cache()

            log.info(
                "Профиль %s: всего подарков=%s, NFT=%s, рейтинг=%s",
                label, total_gifts, "пропуск" if early_reject else nft_gifts_count, rating_level
            )

            return OwnerInfo(
                key=key,
                label=label,
                username=username,
                user_id=owner_id,
                nft_gifts_count=nft_gifts_count,
                total_gifts_count=total_gifts,
                rating_level=rating_level,
                rating_stars=rating_stars,
            )

        except FloodWaitError as e:
            wait_time = int(e.seconds) + 1
            profile_flood_until = time.time() + wait_time
            log.warning("FloodWait при запросе профиля %s (%s сек). Ставим парсинг новых лотов на паузу до разбана.", owner_id, wait_time)
            return None

        except Exception as e:
            log.warning("Telethon не открыл профиль %s (%s)", owner_id, e)
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
    while True:
        try:
            await pace_market_api()
            try:
                return await user_client(
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
                    return await user_client(
                        functions.payments.GetResaleStarGiftsRequest(
                            gift_id=gift_id,
                            offset=offset,
                            limit=limit,
                            sort_by_price=sort_by_price,
                            sort_by_num=False,
                        )
                    )
                raise

        except FloodWaitError as e:
            wait_time = int(e.seconds) + 1
            log.warning("FloodWait на запросе маркета: %s сек.", wait_time)
            await asyncio.sleep(min(wait_time, 60))
        except RpcMcgetFailError:
            log.warning("Временный сбой серверов Telegram (RpcMcgetFailError), ждём 5 сек...")
            await asyncio.sleep(5)


def is_stars_only_listing(raw_gift: Any) -> bool:
    if bool(get_field(raw_gift, "resale_ton_only", False)):
        return False
    price = extract_stars_amount(get_field(raw_gift, "resell_amount"))
    return price > 0


# ============================================================
# MONITORING LOGIC (ОБЩИЙ ЧАТ + КНОПКА "ЗАБРАТЬ ЛОГ")
# ============================================================

async def get_monitor_models() -> List[BaseGift]:
    await ensure_models_loaded()
    selected = {safe_int(x, 0) for x in (MONITOR_SETTINGS.get("model_ids") or [])}
    if not selected:
        return []
    return [g for g in BASE_GIFTS if g.gift_id in selected and g.availability_resale > 0]


async def send_monitor_gift(market_gift: MarketGift, base_gift: BaseGift) -> bool:
    target_chat_id = get_target_chat_id()
    if not target_chat_id:
        return False

    owner = market_gift.owner
    num_line = f" #{market_gift.num}" if market_gift.num else ""
    rating_display = str(owner.rating_level) if owner.rating_level > 0 else "0 / новый"
    profile_line = f"{owner.nft_gifts_count} NFT (всего: {owner.total_gifts_count} шт.)"

    public_text = (
        "🚨 <b>НОВЫЙ ЛОГ (STARS)</b>\n\n"
        f"🎁 <b>Подарок:</b> {escape_text(market_gift.title)}{num_line}\n"
        f"📦 <b>Модель:</b> {escape_text(base_gift.title)}\n"
        f"💰 <b>Цена:</b> {market_gift.price} ⭐\n"
        f"👑 <b>Рейтинг владельца:</b> {rating_display}\n"
        f"🖼 <b>Профиль:</b> {profile_line}\n\n"
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

    if not first_scan and time.time() < profile_flood_until:
        return 0

    try:
        result = await get_resale_page(
            gift_id=base_gift.gift_id,
            offset="",
            limit=MONITOR_PAGE_LIMIT,
            sort_by_price=False,
        )
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
                if time.time() >= profile_flood_until:
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

            sent = await send_monitor_gift(market_gift, base_gift)
            if sent:
                await asyncio.sleep(MONITOR_SEND_DELAY)
                sent_count += 1
            else:
                SENT_GIFTS.pop(slug, None)

    except FloodWaitError as e:
        wait_time = int(e.seconds) + 1
        log.warning("FloodWait в мониторе для %s: %s сек.", base_gift.title, wait_time)
        await asyncio.sleep(min(wait_time, 60))
    except Exception as e:
        log.exception("Ошибка проверки модели %s: %s", base_gift.title, e)

    return sent_count


async def monitor_cycle(first_scan: bool = False) -> int:
    async with monitor_lock:
        models = await get_monitor_models()
        if not models:
            return 0

        total_sent = 0
        for index, model in enumerate(models, 1):
            if not MONITOR_SETTINGS.get("enabled"):
                break
            if not first_scan and time.time() < profile_flood_until:
                break
            try:
                sent = await process_monitor_model(model, first_scan)
                total_sent += sent
                if sent:
                    save_sent_gifts()
                    log.info("Монитор: %s/%s | %s | отправлено=%s", index, len(models), model.title, sent)
                await asyncio.sleep(MODEL_STEP_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("Ошибка в очереди модели %s: %s", model.title, e)

        if total_sent > 0 or first_scan:
            save_sent_gifts()

        return total_sent


async def monitor_loop() -> None:
    global monitor_running, monitor_first_scan_done
    monitor_running = True
    monitor_first_scan_done = False

    log.info(
        "Монитор запущен (интервал: %s сек). Рейтинг <%s, Макс. NFT: %s, Макс. всего: %s",
        MONITOR_INTERVAL,
        MONITOR_SETTINGS.get("max_rating_level"),
        MONITOR_SETTINGS.get("max_owner_nft_gifts"),
        MONITOR_SETTINGS.get("max_owner_total_gifts"),
    )

    try:
        while MONITOR_SETTINGS.get("enabled"):
            first_scan = not monitor_first_scan_done
            selected_models = MONITOR_SETTINGS.get("model_ids") or []
            if not selected_models:
                MONITOR_SETTINGS["enabled"] = False
                save_monitor_settings()
                break

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
    if not get_target_chat_id() or not await is_user_client_authorized():
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
    try:
        await ensure_user_client_connected()
        if not await user_client.is_user_authorized():
            return []

        result = await user_client(functions.payments.GetStarGiftsRequest(hash=0))
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
        [InlineKeyboardButton(text="🚫 ЧЁРНЫЙ СПИСОК", callback_data="blacklist")],
        [InlineKeyboardButton(text="⚙️ АДМИН-ПАНЕЛЬ", callback_data="admin_panel")],
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
    nft_info = f"максимум {max_nft} шт." if max_nft > 0 else "выключен"
    total_info = f"максимум {max_total} шт." if max_total > 0 else "выключен"

    flood_rem = max(0, int(profile_flood_until - time.time()))
    flood_line = (
        f"\n⏸ <b>ПАУЗА (FloodWait профилей TG): ещё {flood_rem // 60} мин.</b>\n"
        "<i>(Смените аккаунт в Админ-панели, чтобы продолжить без ожидания)</i>"
        if flood_rem > 0
        else ""
    )

    return (
        "📡 <b>АВТОМАТИЧЕСКИЙ МОНИТОРИНГ (ОБЩИЙ ЧАТ)</b>\n\n"
        f"Статус: {'🟢 РАБОТАЕТ' if enabled else '🔴 ОСТАНОВЛЕН'}\n"
        f"База данных: <b>{'✅ PostgreSQL' if db_pool else '⚠️ Локально'}</b>\n"
        f"Целевая группа: <code>{escape_text(target_text)}</code>\n"
        f"Интервал круга: <b>{MONITOR_INTERVAL} сек.</b> (глубина: {MONITOR_PAGE_LIMIT} лота)\n"
        f"Диапазон цены: <b>{escape_text(price_text)}</b>\n"
        f"Фильтр рейтинга: <b>{escape_text(rating_info)}</b>\n"
        f"NFT в профиле: <b>{escape_text(nft_info)}</b>\n"
        f"Всего подарков в профиле: <b>{escape_text(total_info)}</b>\n"
        f"Фильтр юзернеймов: <b>{escape_text(username_text)}</b>\n"
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
        if not await is_user_client_authorized():
            await message.answer(
                "⚠️ Сессия Telethon не подключена.\nВыбери способ входа ниже:",
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
            "👑 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\nПарсер доступен только администраторам. Выбери раздел:",
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
# АДМИНСКИЕ КОМАНДЫ И УНИВЕРСАЛЬНАЯ АВТОРИЗАЦИЯ TELETHON (DC 1-5 + STRING)
# ============================================================

async def reset_and_prepare_new_session() -> None:
    """Останавливает монитор, разрывает старую сессию и создаёт чистый клиент под любой DC."""
    global user_client, profile_flood_until
    await stop_monitor()
    profile_flood_until = 0.0
    try:
        if user_client.is_connected():
            await user_client.disconnect()
    except Exception:
        pass
    user_client = TelegramClient(StringSession(), API_ID, API_HASH)
    await user_client.connect()


async def finalize_new_session(message: Message) -> None:
    """Сохраняет новую StringSession в PostgreSQL и выводит её админу."""
    session_str = user_client.session.save() if isinstance(user_client.session, StringSession) else ""
    if session_str:
        schedule_db_save("telethon_session", {"session": session_str}, immediate=True)

    me = await user_client.get_me()
    dc_id = getattr(user_client.session, "dc_id", "?")
    await message.answer(
        f"✅ <b>Сессия (DC {dc_id}) успешно подключена и сохранена в PostgreSQL!</b>\n"
        f"👤 Профиль: <b>{escape_text(me.first_name)}</b> (@{escape_text(me.username or 'без_юзернейма')})\n"
        "Загружаю модели подарков...",
        parse_mode="HTML",
    )
    await load_base_gifts()
    await message.answer(f"Готово. Доступно моделей: {len(BASE_GIFTS)}", reply_markup=admin_main_keyboard())


@dp.message(Command("set_target_chat"))
async def set_target_chat(message: Message):
    if not is_admin(message.from_user.id):
        return
    MONITOR_SETTINGS["target_chat_id"] = message.chat.id
    save_monitor_settings()
    await message.answer(f"✅ Целевая группа привязана!\nChat ID: <code>{message.chat.id}</code>", parse_mode="HTML")


@dp.message(Command("add_session"))
async def add_session_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(AuthState.waiting_phone)
    await message.answer(
        "📱 <b>Подключение / смена аккаунта (поддерживаются все DC 1–5)</b>\n\n"
        "Введи номер телефона (например: <code>+79991234567</code>):\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML",
    )


@dp.message(AuthState.waiting_phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw_phone = (message.text or "").strip()
    digits = "".join(ch for ch in raw_phone if ch.isdigit())
    if len(digits) < 10:
        await message.answer("❌ Некорректный номер. Введи номер с кодом страны (например: <code>+79991234567</code>):", parse_mode="HTML")
        return

    phone = "+" + digits
    try:
        await reset_and_prepare_new_session()
        try:
            sent_code = await user_client.send_code_request(phone)
        except PhoneMigrateError as e:
            log.info("Аккаунт %s находится на DC %s, переключаем дата-центр...", phone, e.new_dc)
            await user_client._switch_dc(e.new_dc)
            sent_code = await user_client.send_code_request(phone)

        await state.update_data(phone=phone, phone_code_hash=sent_code.phone_code_hash)
        await state.set_state(AuthState.waiting_code)
        dc_id = getattr(user_client.session, "dc_id", "?")
        await message.answer(
            f"✅ Код отправлен (<b>DC {dc_id}</b>).\n"
            "⚠️ Вводи код <b>с пробелом</b> (например: <code>12 345</code>), чтобы Telegram не заблокировал его:",
            parse_mode="HTML",
        )
    except Exception as e:
        await state.clear()
        await message.answer(f"❌ Ошибка отправки кода: {e}")


@dp.message(AuthState.waiting_code)
async def auth_code(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
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
        await user_client.sign_in(
            phone=data.get("phone"),
            code=code,
            phone_code_hash=data.get("phone_code_hash"),
        )
        await state.clear()
        await finalize_new_session(message)
    except SessionPasswordNeededError:
        await state.set_state(AuthState.waiting_password)
        await message.answer("🔐 На аккаунте включён 2FA. Введи облачный пароль:")
    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
        await message.answer(f"❌ Неверный или истёкший код ({e.__class__.__name__}). Попробуй ещё раз или нажми /cancel")
    except Exception as e:
        await message.answer(f"❌ Ошибка входа: {e}\nНажми /cancel для сброса.")


@dp.message(AuthState.waiting_password)
async def auth_password(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        await user_client.sign_in(password=(message.text or "").strip())
        await state.clear()
        await finalize_new_session(message)
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
        "🔑 <b>ИМПОРТ ГОТОВОЙ STRING SESSION</b>\n\n"
        "Отправь сюда строку сессии Telethon (начинается с <code>1...</code>).\n"
        "Бот сам заменит старую сессию, сохранит новую в PostgreSQL и подключится.\n\n"
        "❌ Отмена — /cancel"
    )
    if isinstance(event, CallbackQuery):
        await event.message.answer(text, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, parse_mode="HTML")


@dp.message(AuthState.waiting_string_session)
async def paste_session_input(message: Message, state: FSMContext):
    global user_client, profile_flood_until

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

    # Удаляем сообщение со строкой сессии из чата для безопасности
    try:
        await message.delete()
    except Exception:
        pass

    await stop_monitor()
    profile_flood_until = 0.0

    old_client = user_client
    try:
        test_client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
        await test_client.connect()

        if not await test_client.is_user_authorized():
            await test_client.disconnect()
            await message.answer("❌ Эта строка сессии не авторизована или уже сброшена в Telegram. Попробуй другую или /cancel.")
            return

        if old_client and old_client.is_connected():
            await old_client.disconnect()

        user_client = test_client
        schedule_db_save("telethon_session", {"session": session_str}, immediate=True)

        me = await user_client.get_me()
        dc_id = getattr(user_client.session, "dc_id", "?")
        await state.clear()

        await message.answer(
            f"✅ <b>Аккаунт подключён и сохранён в PostgreSQL!</b>\n"
            f"👤 Профиль: <b>{escape_text(me.first_name)}</b> (@{escape_text(me.username or 'без_юзернейма')})\n"
            f"🆔 ID: <code>{me.id}</code> | Дата-центр: <b>DC {dc_id}</b>\n\n"
            "Загружаю модели подарков...",
            parse_mode="HTML",
        )
        await load_base_gifts()
        await message.answer(f"Готово. Доступно моделей: {len(BASE_GIFTS)}", reply_markup=admin_main_keyboard())

    except Exception as e:
        await message.answer(f"❌ Ошибка проверки строки сессии: <code>{escape_text(e)}</code>\nПопробуй ещё раз или нажми /cancel", parse_mode="HTML")


# ============================================================
# АДМИН-МЕНЮ И НАСТРОЙКИ МОНИТОРИНГА
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
    if not await is_user_client_authorized():
        await callback.answer("Сессия не авторизована.", show_alert=True)
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
        "• <code>5</code> — только обычные юзеры с рейтингом <b>строго меньше 5</b>.\n"
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
        "• <code>6</code> — пропускать только юзеров, у которых всего не больше 6 подарков.\n"
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
        await message.answer("❌ Отправь число (например: <code>6</code> или <code>0</code>).", parse_mode="HTML")
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
    is_auth = await is_user_client_authorized()
    me = await user_client.get_me() if is_auth else None
    dc_id = getattr(user_client.session, "dc_id", "?")
    account = f"{me.first_name} (@{me.username}) [ID: {me.id} | DC {dc_id}]" if me else "—"

    lifetime_count = sum(1 for u in SUBSCRIPTIONS["users"].values() if u.get("plan") == "lifetime")
    trial_count = sum(1 for u in SUBSCRIPTIONS["users"].values() if u.get("plan") == "trial")

    await callback.message.edit_text(
        "⚙️ <b>АДМИН-ПАНЕЛЬ</b>\n\n"
        f"🔐 Сессия: {'✅ АКТИВНА' if is_auth else '❌ НЕ АКТИВНА'}\n"
        f"👤 Аккаунт: {escape_text(account)}\n"
        f"🗄 База данных: {'✅ PostgreSQL' if db_pool else '⚠️ Локальная'}\n"
        f"👑 Админов: {len([x for x in ADMIN_IDS if x > 0])}\n"
        f"💎 Подписок навсегда: <b>{lifetime_count}</b> | Триал: <b>{trial_count}</b>\n"
        f"📦 Моделей в базе: {len(BASE_GIFTS)}\n"
        f"💰 See.tg Free: <b>{SEETG_BILLING['free_remaining']}</b> | Баланс: <b>{SEETG_BILLING['balance_ton']} TON</b>\n"
        f"💾 Кэш see.tg: {len(SEETG_CACHE)} шт.\n"
        f"🚫 В blacklist: {len(OWNERS_BLACKLIST)}\n"
        f"📤 Обработано лотов: {len(SENT_GIFTS)}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔑 ВСТАВИТЬ STRING SESSION", callback_data="paste_session_btn")],
                [InlineKeyboardButton(text="📱 ВОЙТИ ПО НОМЕРУ (DC 1–5)", callback_data="add_session_btn")],
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


@dp.callback_query(F.data == "add_session_btn")
async def add_session_btn(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(AuthState.waiting_phone)
    await callback.message.answer(
        "📱 <b>Подключение / смена аккаунта (поддерживаются все DC 1–5)</b>\n\n"
        "Введи номер телефона (например: <code>+79991234567</code>):\n"
        "❌ Отмена — /cancel",
        parse_mode="HTML",
    )
    await callback.answer()


# ============================================================
# MAIN
# ============================================================

async def main():
    global OWNERS_BLACKLIST, OWNER_CACHE, SEETG_CACHE, SENT_GIFTS, CLAIMED_LOGS, SUBSCRIPTIONS, http_session, trial_watcher_task, user_client

    validate_config()
    await init_db()

    # Загружаем сохранённую StringSession из PostgreSQL (приоритет над ENV)
    saved_session_obj = await db_get("telethon_session", {})
    db_session_str = saved_session_obj.get("session", "").strip() if isinstance(saved_session_obj, dict) else ""
    active_session_str = db_session_str or TELETHON_SESSION
    if active_session_str:
        user_client = TelegramClient(StringSession(active_session_str), API_ID, API_HASH)

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

    log.info(
        "Данные загружены из БД: ЧС=%s, кэш профилей=%s, кэш see.tg=%s, отправлено=%s, подписок=%s, моделей в настройках=%s",
        len(OWNERS_BLACKLIST), len(OWNER_CACHE), len(SEETG_CACHE), len(SENT_GIFTS), len(SUBSCRIPTIONS["users"]), len(MONITOR_SETTINGS.get("model_ids") or [])
    )

    await ensure_user_client_connected()

    if await is_user_client_authorized():
        me = await user_client.get_me()
        dc_id = getattr(user_client.session, "dc_id", "?")
        log.info("Сессия Telethon активна: %s (@%s) [ID: %s | DC %s]", me.first_name, me.username, me.id, dc_id)
        await ensure_models_loaded()
        if MONITOR_SETTINGS.get("enabled") and get_target_chat_id() and MONITOR_SETTINGS.get("model_ids"):
            await start_monitor()
    else:
        log.warning("Сессия Telethon не авторизована. Используй /set_session или кнопку в Админ-панели.")

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
        pending_saves = [t for t in db_save_tasks.values() if not t.done()]
        if pending_saves:
            await asyncio.gather(*pending_saves, return_exceptions=True)
        if db_pool is not None:
            await db_pool.close()
        if http_session and not http_session.closed:
            await http_session.close()
        if user_client.is_connected():
            await user_client.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
