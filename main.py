#!/usr/bin/env python3
"""
Telegram Channel Monitor & Relay Bot
====================================
Architecture (two Pyrogram clients in one process):

  * USER client (SESSION_STRING)  -> listens to monitored channels (a user
    account can read any channel it is a member of) and relays matched posts
    to TARGET_CHANNEL_ID.
  * BOT client  (BOT_TOKEN)       -> serves the interactive inline-keyboard
    admin UI (only bots may send InlineKeyboardMarkup).

Persistence: SQLite (`bot_data.db`) — keywords, monitored channels, config.
Health check: aiohttp server on :8080 returning HTTP 200 "OK" at "/".
"""

import asyncio
import logging
import os
import re
import sqlite3
import sys
import threading
from datetime import datetime

import pytz
from aiohttp import web
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# --------------------------------------------------------------------------- #
#  Configuration & logging
# --------------------------------------------------------------------------- #

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("monitor")
logging.getLogger("pyrogram").setLevel(logging.WARNING)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        log.critical("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


API_ID = int(_require_env("API_ID"))
API_HASH = _require_env("API_HASH")
SESSION_STRING = _require_env("SESSION_STRING")
BOT_TOKEN = _require_env("BOT_TOKEN")
ADMIN_USER_ID = int(_require_env("ADMIN_USER_ID"))
TARGET_CHANNEL_ID = int(_require_env("TARGET_CHANNEL_ID"))

DB_PATH = os.getenv("DB_PATH", "bot_data.db")
HEALTH_PORT = int(os.getenv("PORT", "8080"))

DEFAULT_CONFIG = {
    "work_start_hour": "8",
    "work_end_hour": "23",
    "timezone": "Asia/Yerevan",
}

# --------------------------------------------------------------------------- #
#  SQLite layer
# --------------------------------------------------------------------------- #

_db_lock = threading.Lock()
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row


def init_db() -> None:
    with _db_lock, _db:
        _db.executescript(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT NOT NULL UNIQUE COLLATE NOCASE
            );
            CREATE TABLE IF NOT EXISTS monitored_channels (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id  INTEGER NOT NULL UNIQUE,
                username TEXT,
                title    TEXT
            );
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        for key, value in DEFAULT_CONFIG.items():
            _db.execute(
                "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)",
                (key, value),
            )


def get_config(key: str, default: str = "") -> str:
    with _db_lock:
        row = _db.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_config(key: str, value: str) -> None:
    with _db_lock, _db:
        _db.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def db_add_keyword(word: str) -> bool:
    try:
        with _db_lock, _db:
            _db.execute("INSERT INTO keywords (word) VALUES (?)", (word.strip(),))
        return True
    except sqlite3.IntegrityError:
        return False


def db_remove_keyword(kw_id: int) -> None:
    with _db_lock, _db:
        _db.execute("DELETE FROM keywords WHERE id = ?", (kw_id,))


def db_list_keywords() -> list[sqlite3.Row]:
    with _db_lock:
        return _db.execute("SELECT id, word FROM keywords ORDER BY word").fetchall()


def db_add_channel(chat_id: int, username: str | None, title: str) -> bool:
    try:
        with _db_lock, _db:
            _db.execute(
                "INSERT INTO monitored_channels (chat_id, username, title) VALUES (?, ?, ?)",
                (chat_id, username, title),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def db_remove_channel(row_id: int) -> None:
    with _db_lock, _db:
        _db.execute("DELETE FROM monitored_channels WHERE id = ?", (row_id,))


def db_list_channels() -> list[sqlite3.Row]:
    with _db_lock:
        return _db.execute(
            "SELECT id, chat_id, username, title FROM monitored_channels ORDER BY title"
        ).fetchall()


# --------------------------------------------------------------------------- #
#  In-memory caches (refreshed on every DB mutation — no per-message queries)
# --------------------------------------------------------------------------- #

KEYWORDS: set[str] = set()
CHANNEL_IDS: set[int] = set()


def refresh_caches() -> None:
    global KEYWORDS, CHANNEL_IDS
    KEYWORDS = {row["word"].lower() for row in db_list_keywords()}
    CHANNEL_IDS = {row["chat_id"] for row in db_list_channels()}
    log.info("Caches refreshed: %d keywords, %d channels", len(KEYWORDS), len(CHANNEL_IDS))


# --------------------------------------------------------------------------- #
#  Pyrogram clients
# --------------------------------------------------------------------------- #

user = Client(
    "monitor_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True,
)

bot = Client(
    "monitor_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)

admin_only = filters.private & filters.user(ADMIN_USER_ID)


async def safe_call(func, *args, max_retries: int = 5, **kwargs):
    """Run a Pyrogram coroutine, transparently absorbing FloodWait and
    retrying transient RPC errors with exponential backoff."""
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            wait = int(e.value) + 1
            log.warning("FloodWait: sleeping %ss (%s)", wait, func.__name__)
            await asyncio.sleep(wait)
        except (ConnectionError, TimeoutError, OSError) as e:
            log.warning("Connection issue on %s (attempt %d/%d): %s",
                        func.__name__, attempt, max_retries, e)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        except RPCError as e:
            log.error("RPC error on %s: %s", func.__name__, e)
            raise
    raise RuntimeError(f"safe_call: {func.__name__} failed after {max_retries} retries")


# --------------------------------------------------------------------------- #
#  Work-hours logic
# --------------------------------------------------------------------------- #

def within_work_hours() -> bool:
    tz_name = get_config("timezone", "Asia/Yerevan")
    try:
        tz = pytz.timezone(tz_name)
    except pytz.UnknownTimeZoneError:
        tz = pytz.timezone("Asia/Yerevan")
    now = datetime.now(tz)
    start = int(get_config("work_start_hour", "8"))
    end = int(get_config("work_end_hour", "23"))
    if start == end:                       # e.g. 0-0 → around the clock
        return True
    if start < end:
        return start <= now.hour < end
    return now.hour >= start or now.hour < end   # overnight window, e.g. 22-6


# --------------------------------------------------------------------------- #
#  Real-time listener & relay
# --------------------------------------------------------------------------- #

async def _is_monitored(_, __, m: Message) -> bool:
    return bool(m.chat) and m.chat.id in CHANNEL_IDS


monitored = filters.create(_is_monitored)


def find_matches(text: str) -> list[str]:
    lowered = text.lower()
    return sorted(kw for kw in KEYWORDS if kw in lowered)


def message_link(m: Message) -> str:
    if m.chat.username:
        return f"https://t.me/{m.chat.username}/{m.id}"
    return f"https://t.me/c/{str(m.chat.id).replace('-100', '', 1)}/{m.id}"


@user.on_message(filters.channel & monitored)
async def on_channel_post(client: Client, message: Message):
    text = message.text or message.caption or ""
    if not text or not KEYWORDS:
        return
    if not within_work_hours():
        log.debug("Outside work hours — ignoring post %s/%s", message.chat.id, message.id)
        return

    matches = find_matches(text)
    if not matches:
        return

    log.info("Match %s in %r (msg %s)", matches, message.chat.title, message.id)
    try:
        # 1) priority header
        header = await safe_call(
            client.send_message,
            TARGET_CHANNEL_ID,
            "🚨 **PROMOCODE / KEYWORD FOUND!**\n"
            f"🔎 Matched: `{', '.join(matches)}`\n"
            f"📢 Source: **{message.chat.title or message.chat.id}**",
        )
        # 2) the post itself — forward, or fall back to text + link if the
        #    source channel has protected content
        try:
            await safe_call(
                client.forward_messages,
                TARGET_CHANNEL_ID, message.chat.id, message.id,
            )
        except RPCError:
            snippet = text if len(text) <= 3800 else text[:3800] + "…"
            await safe_call(
                client.send_message,
                TARGET_CHANNEL_ID,
                f"{snippet}\n\n🔗 [Open original post]({message_link(message)})",
                disable_web_page_preview=True,
            )
        # 3) call to action
        await safe_call(client.send_message, TARGET_CHANNEL_ID, "🔔 **ACTIVATE NOW!**")

        # Pin the header WITH notification → priority sound for subscribers
        try:
            await safe_call(
                client.pin_chat_message,
                TARGET_CHANNEL_ID, header.id, disable_notification=False,
            )
        except RPCError as e:
            log.warning("Could not pin alert: %s", e)
    except Exception:
        log.exception("Failed to relay alert for msg %s", message.id)


# --------------------------------------------------------------------------- #
#  Admin UI — inline keyboards
# --------------------------------------------------------------------------- #

# One admin → simple pending-input state: None or an action tag
pending_action: str | None = None

CANCEL_KB = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Channels", callback_data="menu:channels"),
         InlineKeyboardButton("🔑 Keywords", callback_data="menu:keywords")],
        [InlineKeyboardButton("⏰ Work Hours", callback_data="menu:hours"),
         InlineKeyboardButton("📊 Status", callback_data="status")],
    ])


def channels_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel", callback_data="ch:add"),
         InlineKeyboardButton("❌ Remove Channel", callback_data="ch:remove")],
        [InlineKeyboardButton("📋 List Channels", callback_data="ch:list")],
        [InlineKeyboardButton("⬅️ Back", callback_data="menu:main")],
    ])


def keywords_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Word", callback_data="kw:add"),
         InlineKeyboardButton("❌ Remove Word", callback_data="kw:remove")],
        [InlineKeyboardButton("📋 List Words", callback_data="kw:list")],
        [InlineKeyboardButton("⬅️ Back", callback_data="menu:main")],
    ])


def hours_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("08 → 23", callback_data="hours:set:8:23"),
         InlineKeyboardButton("09 → 18", callback_data="hours:set:9:18")],
        [InlineKeyboardButton("🌍 24/7", callback_data="hours:set:0:0"),
         InlineKeyboardButton("✍️ Custom", callback_data="hours:custom")],
        [InlineKeyboardButton("⬅️ Back", callback_data="menu:main")],
    ])


def hours_text() -> str:
    start = get_config("work_start_hour")
    end = get_config("work_end_hour")
    tz = get_config("timezone")
    active = "🟢 ACTIVE now" if within_work_hours() else "🔴 SLEEPING now"
    window = "24/7" if start == end else f"{int(start):02d}:00 → {int(end):02d}:00"
    return (
        "⏰ **Work Hours**\n\n"
        f"Window: **{window}**\n"
        f"Timezone: `{tz}`\n"
        f"State: {active}\n\n"
        "Pick a preset or set a custom window:"
    )


def status_text() -> str:
    tz = pytz.timezone(get_config("timezone", "Asia/Yerevan"))
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")
    state = "🟢 monitoring" if within_work_hours() else "🔴 outside work hours"
    return (
        "📊 **Status**\n\n"
        f"• Channels monitored: **{len(CHANNEL_IDS)}**\n"
        f"• Active keywords: **{len(KEYWORDS)}**\n"
        f"• Work window: **{get_config('work_start_hour')}:00 → "
        f"{get_config('work_end_hour')}:00** ({get_config('timezone')})\n"
        f"• Local time: `{now}`\n"
        f"• Listener: {state}\n"
        f"• Target channel: `{TARGET_CHANNEL_ID}`"
    )


@bot.on_message(filters.private, group=-1)
async def _log_incoming(_, message: Message):
    """Diagnostic tap: logs every private message the bot receives so a
    wrong ADMIN_USER_ID or missing updates are immediately visible."""
    uid = message.from_user.id if message.from_user else None
    if uid != ADMIN_USER_ID:
        log.warning("Ignoring message from user %s — not ADMIN_USER_ID (%s)",
                    uid, ADMIN_USER_ID)
    else:
        log.info("Admin message received: %r", message.text)


@bot.on_message(filters.private & filters.command("ping"))
async def cmd_ping(_, message: Message):
    """Self-test: proves the bot receives updates end-to-end."""
    log.info("SELF-TEST OK: /ping received from user %s",
             message.from_user.id if message.from_user else "?")
    await safe_call(message.reply_text, "🏓 pong — updates are flowing!")


@bot.on_message(admin_only & filters.command(["start", "menu"]))
async def cmd_menu(_, message: Message):
    global pending_action
    pending_action = None
    await safe_call(
        message.reply_text,
        "🤖 **Channel Monitor — Control Panel**\nChoose a section:",
        reply_markup=main_menu_kb(),
    )


# ---------------------------- callback router ------------------------------ #

@bot.on_callback_query(filters.user(ADMIN_USER_ID))
async def on_callback(client: Client, cq: CallbackQuery):
    global pending_action
    data = cq.data

    async def show(text: str, kb: InlineKeyboardMarkup):
        try:
            await cq.message.edit_text(text, reply_markup=kb)
        except RPCError:
            # MESSAGE_NOT_MODIFIED and friends — safe to ignore
            pass

    if data == "cancel":
        pending_action = None
        await show("✅ Input cancelled.", main_menu_kb())

    elif data == "menu:main":
        pending_action = None
        await show("🤖 **Channel Monitor — Control Panel**\nChoose a section:", main_menu_kb())

    elif data == "menu:channels":
        await show("📢 **Channels** — manage monitored sources:", channels_menu_kb())

    elif data == "menu:keywords":
        await show("🔑 **Keywords** — manage trigger words:", keywords_menu_kb())

    elif data == "menu:hours":
        await show(hours_text(), hours_menu_kb())

    elif data == "status":
        await show(status_text(), InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔄 Refresh", callback_data="status"),
              InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]
        ))

    # ------------------------------ channels ------------------------------- #
    elif data == "ch:add":
        pending_action = "add_channel"
        await show(
            "➕ Send the channel **@username**, invite link, or numeric ID "
            "(e.g. `@durov` or `-1001234567890`).\n\n"
            "⚠️ The monitoring account must be a member of the channel.",
            CANCEL_KB,
        )

    elif data == "ch:list":
        rows = db_list_channels()
        if not rows:
            body = "📋 No channels monitored yet."
        else:
            body = "📋 **Monitored channels:**\n\n" + "\n".join(
                f"• **{r['title']}** — `{r['chat_id']}`"
                + (f" (@{r['username']})" if r["username"] else "")
                for r in rows
            )
        await show(body, channels_menu_kb())

    elif data == "ch:remove":
        rows = db_list_channels()
        if not rows:
            await show("Nothing to remove — the channel list is empty.", channels_menu_kb())
        else:
            kb = [[InlineKeyboardButton(f"❌ {r['title']}", callback_data=f"chdel:{r['id']}")]
                  for r in rows]
            kb.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:channels")])
            await show("Tap a channel to stop monitoring it:", InlineKeyboardMarkup(kb))

    elif data.startswith("chdel:"):
        db_remove_channel(int(data.split(":", 1)[1]))
        refresh_caches()
        await show("✅ Channel removed.", channels_menu_kb())

    # ------------------------------ keywords ------------------------------- #
    elif data == "kw:add":
        pending_action = "add_keyword"
        await show(
            "➕ Send the keyword or phrase to track (case-insensitive).\n"
            "You can send several — **one per line**.",
            CANCEL_KB,
        )

    elif data == "kw:list":
        rows = db_list_keywords()
        body = ("🔑 **Active keywords:**\n\n" + "\n".join(f"• `{r['word']}`" for r in rows)
                if rows else "🔑 No keywords configured yet.")
        await show(body, keywords_menu_kb())

    elif data == "kw:remove":
        rows = db_list_keywords()
        if not rows:
            await show("Nothing to remove — the keyword list is empty.", keywords_menu_kb())
        else:
            kb = [[InlineKeyboardButton(f"❌ {r['word']}", callback_data=f"kwdel:{r['id']}")]
                  for r in rows]
            kb.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:keywords")])
            await show("Tap a keyword to delete it:", InlineKeyboardMarkup(kb))

    elif data.startswith("kwdel:"):
        db_remove_keyword(int(data.split(":", 1)[1]))
        refresh_caches()
        await show("✅ Keyword removed.", keywords_menu_kb())

    # ------------------------------ work hours ----------------------------- #
    elif data.startswith("hours:set:"):
        _, _, start, end = data.split(":")
        set_config("work_start_hour", start)
        set_config("work_end_hour", end)
        await show(hours_text(), hours_menu_kb())

    elif data == "hours:custom":
        pending_action = "set_hours"
        await show(
            "✍️ Send the window as two hours `START END` (0-23), e.g. `8 23`.\n"
            "Send `0 0` for 24/7 monitoring.",
            CANCEL_KB,
        )

    await cq.answer()


# ------------------------- pending text input ------------------------------ #

@bot.on_message(admin_only & filters.text & ~filters.command(["start", "menu"]))
async def on_admin_text(_, message: Message):
    global pending_action
    if pending_action is None:
        return
    action, pending_action = pending_action, None
    raw = message.text.strip()

    if action == "add_keyword":
        added, dupes = [], []
        for line in filter(None, (l.strip() for l in raw.splitlines())):
            (added if db_add_keyword(line) else dupes).append(line)
        refresh_caches()
        parts = []
        if added:
            parts.append("✅ Added: " + ", ".join(f"`{w}`" for w in added))
        if dupes:
            parts.append("⚠️ Already existed: " + ", ".join(f"`{w}`" for w in dupes))
        await safe_call(message.reply_text, "\n".join(parts) or "Nothing added.",
                        reply_markup=keywords_menu_kb())

    elif action == "add_channel":
        ref: int | str = raw
        if re.fullmatch(r"-?\d+", raw):
            ref = int(raw)
        try:
            chat = await safe_call(user.get_chat, ref)
        except Exception as e:
            await safe_call(
                message.reply_text,
                f"❌ Could not resolve `{raw}`: {e}\n"
                "Make sure the monitoring account is a member of the channel.",
                reply_markup=channels_menu_kb(),
            )
            return
        title = chat.title or str(chat.id)
        if db_add_channel(chat.id, chat.username, title):
            refresh_caches()
            await safe_call(message.reply_text,
                            f"✅ Now monitoring **{title}** (`{chat.id}`).",
                            reply_markup=channels_menu_kb())
        else:
            await safe_call(message.reply_text,
                            f"⚠️ **{title}** is already monitored.",
                            reply_markup=channels_menu_kb())

    elif action == "set_hours":
        m = re.fullmatch(r"(\d{1,2})\s+(\d{1,2})", raw)
        if not m or not (0 <= int(m.group(1)) <= 23 and 0 <= int(m.group(2)) <= 23):
            pending_action = "set_hours"
            await safe_call(message.reply_text,
                            "❌ Invalid format. Send two hours 0-23, e.g. `8 23`.",
                            reply_markup=CANCEL_KB)
            return
        set_config("work_start_hour", m.group(1))
        set_config("work_end_hour", m.group(2))
        await safe_call(message.reply_text, hours_text(), reply_markup=hours_menu_kb())


# --------------------------------------------------------------------------- #
#  Health-check web server (Koyeb)
# --------------------------------------------------------------------------- #

async def start_health_server() -> web.AppRunner:
    async def ok(_):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info("Health-check server listening on :%d", HEALTH_PORT)
    return runner


# --------------------------------------------------------------------------- #
#  Entrypoint
# --------------------------------------------------------------------------- #

async def main() -> None:
    init_db()
    refresh_caches()

    runner = await start_health_server()
    await user.start()
    await bot.start()

    me = await user.get_me()
    bot_me = await bot.get_me()
    log.info("User client: %s (id %s) | Bot: @%s", me.first_name, me.id, bot_me.username)

    # Startup self-test: user account pings the bot; if updates flow, the
    # bot logs "SELF-TEST OK" within a few seconds.
    try:
        await safe_call(user.send_message, bot_me.username, "/ping")
        log.info("Self-test /ping sent to @%s — waiting for the bot to receive it...",
                 bot_me.username)
    except RPCError as e:
        log.warning("Could not send self-test ping: %s", e)

    try:
        await safe_call(
            bot.send_message, ADMIN_USER_ID,
            "✅ **Monitor is up.** Send /menu to open the control panel.",
        )
    except RPCError:
        log.warning("Could not DM admin — have they pressed Start on the bot?")

    await idle()

    await bot.stop()
    await user.stop()
    await runner.cleanup()


if __name__ == "__main__":
    # kurigram's Client captures the event loop at construction time, so we
    # must run on that same loop — asyncio.run() would create a new one and
    # crash with "attached to a different loop".
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
