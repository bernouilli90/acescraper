#!/usr/bin/env python3
"""
AceScraper Telegram Bot

Variables de entorno (.env):
  TELEGRAM_BOT_TOKEN      — Token del bot (obligatorio)
  ACESCRAPER_API          — URL base de la API (por defecto http://localhost:8000)
  TELEGRAM_ALLOWED_USERS  — IDs de Telegram separados por coma (vacío = todos)
"""
import asyncio
import html
import logging
import os

import httpx
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from app.docker_utils import docker_restart

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("acebot")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_BASE = os.getenv("ACESCRAPER_API", "http://localhost:8000").rstrip("/")
_raw_users = os.getenv("TELEGRAM_ALLOWED_USERS", "").strip()
ALLOWED = set(int(x) for x in _raw_users.split(",") if x.strip().isdigit())


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def _get(path: str, timeout: float = 30.0):
    async with httpx.AsyncClient(base_url=API_BASE, timeout=timeout) as c:
        r = await c.get(path)
        r.raise_for_status()
        return r.json()


async def _post(path: str, timeout: float = 600.0):
    async with httpx.AsyncClient(base_url=API_BASE, timeout=timeout) as c:
        r = await c.post(path)
        r.raise_for_status()
        return r.json()


# ── Auth ──────────────────────────────────────────────────────────────────────

def authorized(update: Update) -> bool:
    if not ALLOWED:
        return True
    return update.effective_user.id in ALLOWED


async def deny(update: Update):
    await update.effective_message.reply_text("⛔ No autorizado.")


# ── /start · /ayuda ───────────────────────────────────────────────────────────

HELP = (
    "🎬 *AceScraper Bot*\n\n"
    "/feeds — Actualizar todos los feeds\n"
    "/test\\_fail — Probar streams fallidos\n"
    "/test\\_ok — Probar streams OK\n"
    "/test\\_untested — Probar streams no probados\n"
    "/test\\_all — Probar todos los streams\n"
    "/buscar `<nombre>` — Buscar canal por nombre\n"
    "/reiniciar — Reiniciar contenedores Acestream/Acexy"
)


async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await update.message.reply_text(HELP, parse_mode="Markdown")


async def cmd_ayuda(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await update.message.reply_text(HELP, parse_mode="Markdown")


# ── Feed refresh ──────────────────────────────────────────────────────────────

async def cmd_feeds(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    msg = await update.message.reply_text("⏳ Actualizando todos los feeds…")

    async def _do():
        try:
            await _post("/api/feeds/refresh-all", timeout=300)
            await msg.edit_text("✅ Feeds actualizados correctamente.")
        except Exception as e:
            await msg.edit_text(f"❌ Error al actualizar feeds: {e}")

    asyncio.create_task(_do())


# ── Stream tests ──────────────────────────────────────────────────────────────

_BAR_FULL = "█"
_BAR_EMPTY = "░"
_BAR_LEN = 10


def _bar(current: int, total: int) -> str:
    if total == 0:
        return _BAR_EMPTY * _BAR_LEN
    filled = round(current / total * _BAR_LEN)
    return _BAR_FULL * filled + _BAR_EMPTY * (_BAR_LEN - filled)


async def _poll_progress(msg, label: str):
    """Edita el mensaje cada 5 s mientras haya pruebas en curso."""
    await asyncio.sleep(2)  # pequeña espera para que arranque el test
    while True:
        try:
            prog = await _get("/api/stream-test/progress", timeout=10)
            if not prog.get("running"):
                break
            current = prog.get("current", 0)
            total = prog.get("total", 0)
            pct = int(current / total * 100) if total else 0
            bar = _bar(current, total)
            await msg.edit_text(
                f"⏳ *{label}*\n\n"
                f"[{bar}] {current}/{total} ({pct}%)",
                parse_mode="Markdown",
            )
        except Exception:
            pass
        await asyncio.sleep(5)


async def _run_test(update: Update, endpoint: str, label: str):
    msg = await update.message.reply_text(f"⏳ Iniciando: *{label}*…", parse_mode="Markdown")

    async def _do():
        poll = asyncio.create_task(_poll_progress(msg, label))
        try:
            await _post(endpoint, timeout=600)
        except Exception as e:
            poll.cancel()
            await msg.edit_text(f"❌ Error en *{label}*: {e}", parse_mode="Markdown")
            return

        poll.cancel()
        try:
            await poll
        except asyncio.CancelledError:
            pass

        try:
            prog = await _get("/api/stream-test/progress", timeout=10)
            total = prog.get("total", "?")
        except Exception:
            total = "?"

        await msg.edit_text(
            f"✅ *{label}* completada.\nStreams probados: {total}",
            parse_mode="Markdown",
        )

    asyncio.create_task(_do())


async def cmd_test_fail(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await _run_test(update, "/api/stream-test/run-now/fail", "Prueba de streams fallidos")


async def cmd_test_ok(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await _run_test(update, "/api/stream-test/run-now/ok", "Prueba de streams OK")


async def cmd_test_untested(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await _run_test(update, "/api/stream-test/run-now/untested", "Prueba de streams no probados")


async def cmd_test_all(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)
    await _run_test(update, "/api/stream-test/run-now/all", "Prueba de todos los streams")


# ── Docker restart ────────────────────────────────────────────────────────────

_RESTART_TARGETS = {
    "acestream": "acestream",
    "acexy":     "acexy",
}


async def cmd_reiniciar(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)

    keyboard = [
        [
            InlineKeyboardButton("🔄 Acestream", callback_data="restart:acestream"),
            InlineKeyboardButton("🔄 Acexy",     callback_data="restart:acexy"),
        ],
        [InlineKeyboardButton("🔄 Ambos",         callback_data="restart:both")],
    ]
    await update.message.reply_text(
        "¿Qué contenedor quieres reiniciar?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cb_restart(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not authorized(update):
        await query.edit_message_text("⛔ No autorizado.")
        return

    target = query.data.split(":")[1]

    if target == "both":
        containers = list(_RESTART_TARGETS.values())
        label = "Acestream + Acexy"
    else:
        containers = [_RESTART_TARGETS[target]]
        label = target.capitalize()

    await query.edit_message_text(f"⏳ Reiniciando {label}…")

    lines = []
    for name in containers:
        ok, err = await docker_restart(name)
        if ok:
            lines.append(f"✅ <b>{name}</b> reiniciado")
        else:
            lines.append(f"❌ <b>{name}</b>: {html.escape(err)}")

    await query.edit_message_text("\n".join(lines), parse_mode="HTML")


# ── Channel search ────────────────────────────────────────────────────────────

MAX_RESULTS = 20

STATUS_ICON = {"ok": "✅", "fail": "❌", "untested": "❓"}


async def cmd_buscar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return await deny(update)

    query = " ".join(ctx.args).strip().lower() if ctx.args else ""
    if not query:
        await update.message.reply_text(
            "Uso: /buscar <nombre del canal>\nEjemplo: /buscar La 1"
        )
        return

    try:
        channels = await _get("/api/channels/")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al consultar canales: {e}")
        return

    matches = [
        ch for ch in channels
        if query in ch["name"].lower() and ch.get("sources")
    ]

    if not matches:
        await update.message.reply_text(
            f'🔍 Sin resultados para "{query}" con hashes asignados.'
        )
        return

    keyboard = [
        [InlineKeyboardButton(
            f"📺 {ch['name']}  ({len(ch['sources'])} hash{'es' if len(ch['sources']) != 1 else ''})",
            callback_data=f"ch:{ch['id']}",
        )]
        for ch in matches[:MAX_RESULTS]
    ]

    note = f" *(mostrando {MAX_RESULTS} de {len(matches)})*" if len(matches) > MAX_RESULTS else ""
    await update.message.reply_text(
        f'🔍 *{len(matches)} canal(es)* encontrados para "{query}"{note}:',
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def cb_channel(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not authorized(update):
        await query.edit_message_text("⛔ No autorizado.")
        return

    channel_id = int(query.data.split(":")[1])

    try:
        cfg = await _get("/api/config/")
        ip = cfg.get("acexy_ip", "127.0.0.1")
        port = cfg.get("acexy_port", 6878)
    except Exception:
        ip, port = "127.0.0.1", 6878

    try:
        channels = await _get("/api/channels/")
    except Exception as e:
        await query.edit_message_text(f"❌ Error: {e}")
        return

    channel = next((ch for ch in channels if ch["id"] == channel_id), None)
    if not channel:
        await query.edit_message_text("❌ Canal no encontrado.")
        return

    sources = channel.get("sources", [])
    if not sources:
        await query.edit_message_text(
            f"⚠️ <b>{html.escape(channel['name'])}</b> no tiene hashes asignados.",
            parse_mode="HTML",
        )
        return

    keyboard = []
    for s in sources:
        icon = STATUS_ICON.get(s.get("test_status", "untested"), "❓")
        label = s.get("label") or (s["ace_hash"][:12] + "…")
        ace_url = f"http://{ip}:{port}/ace/getstream?id={s['ace_hash']}"
        inactive = " 🚫" if not s.get("active", True) else ""
        btn_label = f"{icon} {label}{inactive}"
        if len(btn_label) > 60:
            btn_label = btn_label[:57] + "…"
        keyboard.append([InlineKeyboardButton(btn_label, url=ace_url)])

    await query.edit_message_text(
        f"📺 <b>{html.escape(channel['name'])}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ── Build / Main ──────────────────────────────────────────────────────────────

def build_app() -> Application:
    """Construye y devuelve la Application de Telegram sin arrancarla."""
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN no está definido en el entorno.")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("ayuda",        cmd_ayuda))
    app.add_handler(CommandHandler("feeds",        cmd_feeds))
    app.add_handler(CommandHandler("test_fail",    cmd_test_fail))
    app.add_handler(CommandHandler("test_ok",      cmd_test_ok))
    app.add_handler(CommandHandler("test_untested",cmd_test_untested))
    app.add_handler(CommandHandler("test_all",     cmd_test_all))
    app.add_handler(CommandHandler("buscar",       cmd_buscar))
    app.add_handler(CommandHandler("reiniciar",    cmd_reiniciar))
    app.add_handler(CallbackQueryHandler(cb_channel, pattern=r"^ch:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_restart, pattern=r"^restart:(acestream|acexy|both)$"))

    return app


if __name__ == "__main__":
    # Ejecución standalone (desarrollo)
    if not TOKEN:
        raise SystemExit("ERROR: TELEGRAM_BOT_TOKEN no está definido en el entorno.")
    logger.info("AceScraper Bot arrancado — polling activo (modo standalone)")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES)
