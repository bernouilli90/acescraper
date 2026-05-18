from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.database import init_db, AsyncSessionLocal
from app import crud
from app.routers import channels, sources, groups, feeds, export, config, scheduler as scheduler_router, xmltv as xmltv_router, stream_test as stream_test_router
from app.routers import epg as epg_router
from app.routers import watchdog as watchdog_router
from app.routers import status as status_router
from app.routers import export_profiles as export_profiles_router
from app.routers import stream_proxy as stream_proxy_router
from app.routers.scheduler import apply_config, _refresh_task, JOB_ID
from app.routers.stream_test import apply_test_config
from app.routers.epg import apply_epg_config
from app.routers.watchdog import apply_watchdog_config
import logging
import os

logger = logging.getLogger("acescraper")

_scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Read persisted scheduler config from DB
    async with AsyncSessionLocal() as db:
        cfg = await crud.get_config(db)

    interval_minutes = int(cfg.get("scheduler_interval_minutes", "360"))
    enabled = cfg.get("scheduler_enabled", "true") == "true"

    app.state.scheduler = _scheduler
    apply_config(_scheduler, interval_minutes, enabled)

    test_enabled = cfg.get("stream_test_enabled", "false") == "true"
    test_interval_fail = int(cfg.get("stream_test_interval_fail", "60"))
    test_interval_ok = int(cfg.get("stream_test_interval_ok", "360"))
    apply_test_config(_scheduler, test_interval_fail, test_interval_ok, test_enabled)

    epg_enabled = cfg.get("epg_enabled", "false") == "true"
    epg_interval_hours = int(cfg.get("epg_interval_hours", "24"))
    apply_epg_config(_scheduler, epg_interval_hours, epg_enabled)

    watchdog_enabled = cfg.get("watchdog_enabled", "false") == "true"
    watchdog_interval = int(cfg.get("watchdog_interval_minutes", "5"))
    apply_watchdog_config(_scheduler, watchdog_interval, watchdog_enabled)

    _scheduler.start()

    # Arrancar bot de Telegram si hay token configurado
    _bot_app = None
    if os.getenv("TELEGRAM_BOT_TOKEN"):
        try:
            from bot import build_app as _build_bot
            _bot_app = _build_bot()
            await _bot_app.initialize()
            await _bot_app.start()
            await _bot_app.updater.start_polling(allowed_updates=["message", "callback_query"])
            logger.info("Telegram bot arrancado")
        except Exception as e:
            logger.warning("No se pudo arrancar el bot de Telegram: %s", e)
            _bot_app = None

    yield

    if _bot_app is not None:
        await _bot_app.updater.stop()
        await _bot_app.stop()
        await _bot_app.shutdown()

    _scheduler.shutdown()


app = FastAPI(title="AceScraper", lifespan=lifespan)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGOS_DIR = os.getenv("LOGOS_DIR", os.path.join(BASE_DIR, "static", "logos"))
os.makedirs(LOGOS_DIR, exist_ok=True)
app.mount("/static/logos", StaticFiles(directory=LOGOS_DIR), name="logos")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

app.include_router(channels.router)
app.include_router(sources.router)
app.include_router(groups.router)
app.include_router(feeds.router)
app.include_router(export.router)
app.include_router(config.router)
app.include_router(scheduler_router.router)
app.include_router(xmltv_router.router)
app.include_router(stream_test_router.router)
app.include_router(epg_router.router)
app.include_router(watchdog_router.router)
app.include_router(status_router.router)
app.include_router(export_profiles_router.router)
app.include_router(stream_proxy_router.router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
