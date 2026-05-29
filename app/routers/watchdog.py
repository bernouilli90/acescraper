from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.triggers.interval import IntervalTrigger
from app.database import get_db
from app import crud, schemas
from app.docker_utils import docker_restart, docker_list_containers, docker_inspect_container
from datetime import datetime, timezone

router = APIRouter(prefix="/api/watchdog", tags=["watchdog"])

JOB_ID = "watchdog_check"

_state: dict = {
    "acexy":     {"status": "unknown", "container_status": None, "health": None, "failing_streak": 0, "last_check": None, "last_restart": None},
    "acestream": {"status": "unknown", "container_status": None, "health": None, "failing_streak": 0, "last_check": None, "last_restart": None},
}


def _derive_status(info: dict) -> str:
    """Map Docker inspect result to a single display status."""
    cs = info.get("container_status")
    if cs == "not_found":
        return "not_found"
    if cs == "error":
        return "error"
    if not info.get("running"):
        return "stopped"
    health = info.get("health")
    if health == "unhealthy":
        return "unhealthy"
    # "starting" = healthcheck aún acumulando ciclos, el contenedor sí corre
    return "ok"


def _get_scheduler(request: Request):
    return request.app.state.scheduler


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _run_check(db=None):
    from app.database import AsyncSessionLocal
    from app import crud as _crud

    if db is None:
        async with AsyncSessionLocal() as db:
            await _run_check(db)
            return

    cfg = await _crud.get_config(db)
    auto_restart        = cfg.get("watchdog_auto_restart", "true") == "true"
    acexy_container     = cfg.get("watchdog_acexy_container", "acexy")
    acestream_container = cfg.get("watchdog_acestream_container", "acestream")

    now = _now_iso()

    for svc, container in [("acexy", acexy_container), ("acestream", acestream_container)]:
        info = await docker_inspect_container(container)
        status = _derive_status(info)
        _state[svc].update({
            "status":           status,
            "container_status": info.get("container_status"),
            "health":           info.get("health"),
            "failing_streak":   info.get("failing_streak", 0),
            "last_check":       now,
        })
        should_restart = status in ("stopped", "unhealthy")
        if should_restart and auto_restart:
            ok, _ = await docker_restart(container)
            if ok:
                _state[svc]["last_restart"] = now


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
        "acestream_container":  cfg.get("watchdog_acestream_container", "acestream"),
        "services":             _state,
    }


@router.put("/config")
async def update_config(data: schemas.WatchdogConfig, request: Request, db: AsyncSession = Depends(get_db)):
    await crud.set_configs(db, {
        "watchdog_enabled":             str(data.enabled).lower(),
        "watchdog_interval_minutes":    str(data.interval_minutes),
        "watchdog_auto_restart":        str(data.auto_restart).lower(),
        "watchdog_acexy_container":     data.acexy_container,
        "watchdog_acestream_container": data.acestream_container,
    })
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
