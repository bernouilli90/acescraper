from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.triggers.interval import IntervalTrigger
from app.database import get_db
from app import crud, schemas, scraper, models
from app.docker_utils import docker_restart
from datetime import timezone
import asyncio

router = APIRouter(prefix="/api/stream-test", tags=["stream-test"])

JOB_FAIL = "stream_test_fail"
JOB_OK   = "stream_test_ok"


def _get_scheduler(request: Request):
    return request.app.state.scheduler


def apply_test_config(scheduler, interval_fail: int, interval_ok: int, enabled: bool):
    for job_id in (JOB_FAIL, JOB_OK):
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
    if not enabled:
        return
    scheduler.add_job(
        _test_fail_task,
        IntervalTrigger(minutes=interval_fail),
        id=JOB_FAIL,
        replace_existing=True,
    )
    scheduler.add_job(
        _test_ok_task,
        IntervalTrigger(minutes=interval_ok),
        id=JOB_OK,
        replace_existing=True,
    )


async def _do_restart_containers(cfg: dict, wait: bool):
    containers_str = cfg.get("stream_test_restart_containers", "").strip()
    if not containers_str:
        return
    for name in [c.strip() for c in containers_str.split(",") if c.strip()]:
        await docker_restart(name)
    if wait:
        secs = int(cfg.get("stream_test_restart_wait_seconds", "15"))
        if secs > 0:
            await asyncio.sleep(secs)


async def _get_restart_cfg() -> dict:
    from app.database import AsyncSessionLocal
    from app import crud as _crud
    async with AsyncSessionLocal() as db:
        return await _crud.get_config(db)


async def _restart_before_if_configured():
    cfg = await _get_restart_cfg()
    if cfg.get("stream_test_restart_enabled", "false") == "true":
        await _do_restart_containers(cfg, wait=True)


async def _restart_after_if_configured():
    cfg = await _get_restart_cfg()
    if cfg.get("stream_test_restart_after_enabled", "false") == "true":
        await _do_restart_containers(cfg, wait=False)


async def _test_fail_task():
    from app import scraper as sc
    await _restart_before_if_configured()
    await sc.run_stream_tests(["untested", "fail"])
    await _restart_after_if_configured()


async def _test_ok_task():
    from app import scraper as sc
    await _restart_before_if_configured()
    await sc.run_stream_tests(["ok"])
    await _restart_after_if_configured()


def _next_run(scheduler, job_id: str):
    job = scheduler.get_job(job_id)
    if job and job.next_run_time:
        return job.next_run_time.astimezone(timezone.utc).replace(tzinfo=None)
    return None


@router.get("/progress")
async def get_progress():
    from app.scraper import test_progress
    return test_progress


@router.post("/cancel")
async def cancel_test():
    from app.scraper import test_progress
    test_progress["cancel"] = True
    return {"ok": True}


@router.get("/config", response_model=schemas.StreamTestStatus)
async def get_config(request: Request, db: AsyncSession = Depends(get_db)):
    cfg = await crud.get_config(db)
    scheduler = _get_scheduler(request)
    last_run_str = cfg.get("stream_test_last_run")
    last_run = None
    if last_run_str:
        from datetime import datetime
        last_run = datetime.fromisoformat(last_run_str).replace(tzinfo=None)
    return schemas.StreamTestStatus(
        enabled=cfg.get("stream_test_enabled", "false") == "true",
        interval_fail_minutes=int(cfg.get("stream_test_interval_fail", "60")),
        interval_ok_minutes=int(cfg.get("stream_test_interval_ok", "360")),
        concurrency=int(cfg.get("stream_test_concurrency", "5")),
        restart_before_test=cfg.get("stream_test_restart_enabled", "false") == "true",
        restart_after_test=cfg.get("stream_test_restart_after_enabled", "false") == "true",
        restart_containers=cfg.get("stream_test_restart_containers", ""),
        restart_wait_seconds=int(cfg.get("stream_test_restart_wait_seconds", "15")),
        auto_deactivate_enabled=cfg.get("stream_test_auto_deactivate_enabled", "false") == "true",
        auto_deactivate_hours=int(cfg.get("stream_test_auto_deactivate_hours", "48")),
        next_run_fail=_next_run(scheduler, JOB_FAIL),
        next_run_ok=_next_run(scheduler, JOB_OK),
        last_run=last_run,
    )


@router.put("/config", response_model=schemas.StreamTestStatus)
async def update_config(
    data: schemas.StreamTestConfig,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await crud.set_configs(db, {
        "stream_test_enabled":               str(data.enabled).lower(),
        "stream_test_interval_fail":         str(data.interval_fail_minutes),
        "stream_test_interval_ok":           str(data.interval_ok_minutes),
        "stream_test_concurrency":           str(data.concurrency),
        "stream_test_restart_enabled":       str(data.restart_before_test).lower(),
        "stream_test_restart_after_enabled": str(data.restart_after_test).lower(),
        "stream_test_restart_containers":    data.restart_containers,
        "stream_test_restart_wait_seconds":  str(data.restart_wait_seconds),
        "stream_test_auto_deactivate_enabled": str(data.auto_deactivate_enabled).lower(),
        "stream_test_auto_deactivate_hours": str(data.auto_deactivate_hours),
    })
    apply_test_config(_get_scheduler(request), data.interval_fail_minutes, data.interval_ok_minutes, data.enabled)
    return await get_config(request, db)


@router.post("/run-now/fail", response_model=schemas.StreamTestStatus)
async def run_now_fail(request: Request, db: AsyncSession = Depends(get_db)):
    await _restart_before_if_configured()
    from app import scraper as sc
    await sc.run_stream_tests(["untested", "fail"])
    await _restart_after_if_configured()
    return await get_config(request, db)


@router.post("/run-now/ok", response_model=schemas.StreamTestStatus)
async def run_now_ok(request: Request, db: AsyncSession = Depends(get_db)):
    await _restart_before_if_configured()
    from app import scraper as sc
    await sc.run_stream_tests(["ok"])
    await _restart_after_if_configured()
    return await get_config(request, db)


@router.post("/run-now/untested", response_model=schemas.StreamTestStatus)
async def run_now_untested(request: Request, db: AsyncSession = Depends(get_db)):
    await _restart_before_if_configured()
    from app import scraper as sc
    await sc.run_stream_tests(["untested"])
    await _restart_after_if_configured()
    return await get_config(request, db)


@router.post("/run-now/all", response_model=schemas.StreamTestStatus)
async def run_now_all(request: Request, db: AsyncSession = Depends(get_db)):
    await _restart_before_if_configured()
    from app import scraper as sc
    await sc.run_stream_tests(["untested", "fail", "ok"])
    await _restart_after_if_configured()
    return await get_config(request, db)


@router.post("/run-now/dead", response_model=schemas.StreamTestStatus)
async def run_now_dead(request: Request, db: AsyncSession = Depends(get_db)):
    await _restart_before_if_configured()
    from app import scraper as sc
    await sc.run_stream_tests(["dead"])
    await _restart_after_if_configured()
    return await get_config(request, db)


@router.post("/{source_id}", response_model=schemas.StreamTestResult)
async def test_one(source_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Source).where(models.Source.id == source_id))
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    cfg = await crud.get_config(db)
    ip = cfg.get("acexy_ip", "127.0.0.1")
    port = int(cfg.get("acexy_port", "6878"))
    ok = await scraper.test_stream(source.ace_hash, ip, port)
    status = "ok" if ok else "fail"
    await crud.update_source_test_result(db, source_id, status)
    return schemas.StreamTestResult(source_id=source_id, status=status)
