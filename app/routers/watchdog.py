from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.triggers.interval import IntervalTrigger
from app.database import get_db
from app import crud, schemas
from app.docker_utils import docker_restart, docker_list_containers
from datetime import datetime, timezone
import httpx

router = APIRouter(prefix="/api/watchdog", tags=["watchdog"])

JOB_ID = "watchdog_check"

_state: dict = {
    "acexy":      {"status": "unknown", "last_check": None, "last_restart": None},
    "acestream":  {"status": "unknown", "last_check": None, "last_restart": None},
}


def _get_scheduler(request: Request):
    return request.app.state.scheduler


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _check_url(url: str) -> bool:
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=5.0, follow_redirects=True)
            return r.status_code < 500
    except Exception:
        return False


async def _run_check(db=None):
    from app.database import AsyncSessionLocal
    from app import crud as _crud

    if db is None:
        async with AsyncSessionLocal() as db:
            await _run_check(db)
            return

    cfg = await _crud.get_config(db)
    auto_restart = cfg.get("watchdog_auto_restart", "true") == "true"

    acexy_url = cfg.get("watchdog_acexy_url", "").strip()
    if not acexy_url:
        acexy_ip   = cfg.get("acexy_ip", "127.0.0.1")
        acexy_port = cfg.get("acexy_port", "6878")
        acexy_url  = f"http://{acexy_ip}:{acexy_port}/"

    acestream_url       = cfg.get("watchdog_acestream_url", "").strip()
    acexy_container     = cfg.get("watchdog_acexy_container", "acexy")
    acestream_container = cfg.get("watchdog_acestream_container", "acestream")

    now = _now_iso()

    acexy_ok = await _check_url(acexy_url)
    _state["acexy"]["status"]     = "ok" if acexy_ok else "fail"
    _state["acexy"]["last_check"] = now
    if not acexy_ok and auto_restart:
        ok, _ = await docker_restart(acexy_container)
        if ok:
            _state["acexy"]["last_restart"] = now

    if acestream_url:
        acestream_ok = await _check_url(acestream_url)
        _state["acestream"]["status"]     = "ok" if acestream_ok else "fail"
        _state["acestream"]["last_check"] = now
        if not acestream_ok and auto_restart:
            ok, _ = await docker_restart(acestream_container)
            if ok:
                _state["acestream"]["last_restart"] = now
    else:
        _state["acestream"]["status"] = "unknown"


def apply_watchdog_config(scheduler, interval_minutes: int, enabled: bool):
    if scheduler.get_job(JOB_ID):
        scheduler.remove_job(JOB_ID)
    if enabled:
        scheduler.add_job(
            _run_check,
            IntervalTrigger(minutes=interval_minutes),
            id=JOB_ID,
            replace_existing=True,
        )


@router.get("/containers")
async def list_containers():
    return await docker_list_containers()


@router.get("/status")
async def get_status(db: AsyncSession = Depends(get_db)):
    cfg = await crud.get_config(db)
    return {
        "enabled":              cfg.get("watchdog_enabled", "false") == "true",
        "auto_restart":         cfg.get("watchdog_auto_restart", "true") == "true",
        "interval_minutes":     int(cfg.get("watchdog_interval_minutes", "5")),
        "acexy_container":      cfg.get("watchdog_acexy_container", "acexy"),
        "acexy_url":            cfg.get("watchdog_acexy_url", ""),
        "acestream_container":  cfg.get("watchdog_acestream_container", "acestream"),
        "acestream_url":        cfg.get("watchdog_acestream_url", ""),
        "services":             _state,
    }


@router.put("/config")
async def update_config(data: schemas.WatchdogConfig, request: Request, db: AsyncSession = Depends(get_db)):
    await crud.set_config(db, "watchdog_enabled",             str(data.enabled).lower())
    await crud.set_config(db, "watchdog_interval_minutes",    str(data.interval_minutes))
    await crud.set_config(db, "watchdog_auto_restart",        str(data.auto_restart).lower())
    await crud.set_config(db, "watchdog_acexy_container",     data.acexy_container)
    await crud.set_config(db, "watchdog_acexy_url",           data.acexy_url)
    await crud.set_config(db, "watchdog_acestream_container", data.acestream_container)
    await crud.set_config(db, "watchdog_acestream_url",       data.acestream_url)
    apply_watchdog_config(_get_scheduler(request), data.interval_minutes, data.enabled)
    return await get_status(db)


@router.post("/check-now")
async def check_now(db: AsyncSession = Depends(get_db)):
    await _run_check(db)
    return await get_status(db)


@router.post("/restart/{service}")
async def restart_service(service: str, db: AsyncSession = Depends(get_db)):
    if service not in ("acexy", "acestream"):
        raise HTTPException(status_code=400, detail="Servicio desconocido")
    cfg = await crud.get_config(db)
    key = "watchdog_acexy_container" if service == "acexy" else "watchdog_acestream_container"
    container = cfg.get(key, service)
    ok, error = await docker_restart(container)
    if ok:
        _state[service]["last_restart"] = _now_iso()
    return {"restarted": ok, "container": container, "error": error}
