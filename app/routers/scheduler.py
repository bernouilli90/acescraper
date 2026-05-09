from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.triggers.interval import IntervalTrigger
from app.database import get_db
from app import crud, schemas
from datetime import timezone

router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])

JOB_ID = "feed_refresh"


def _get_scheduler(request: Request):
    return request.app.state.scheduler


def apply_config(scheduler, interval_minutes: int, enabled: bool):
    """Reschedule (or remove) the feed_refresh job according to config."""
    scheduler.remove_job(JOB_ID) if scheduler.get_job(JOB_ID) else None
    if enabled:
        scheduler.add_job(
            _refresh_task,
            IntervalTrigger(minutes=interval_minutes),
            id=JOB_ID,
            replace_existing=True,
        )


async def _refresh_task():
    """Wrapper used by APScheduler — updates last_run in DB."""
    from app.database import AsyncSessionLocal
    from app import scraper
    from datetime import datetime
    async with AsyncSessionLocal() as db:
        await crud.set_config(db, "scheduler_last_run", datetime.now(timezone.utc).isoformat())
        await scraper.refresh_all_feeds(db)
        await scraper.refresh_all_xmltv(db)


@router.get("/", response_model=schemas.SchedulerStatus)
async def get_status(request: Request, db: AsyncSession = Depends(get_db)):
    scheduler = _get_scheduler(request)
    cfg = await crud.get_config(db)
    job = scheduler.get_job(JOB_ID)

    next_run = None
    if job and job.next_run_time:
        next_run = job.next_run_time.astimezone(timezone.utc).replace(tzinfo=None)

    last_run_str = cfg.get("scheduler_last_run")
    last_run = None
    if last_run_str:
        from datetime import datetime
        last_run = datetime.fromisoformat(last_run_str).replace(tzinfo=None)

    return schemas.SchedulerStatus(
        enabled=cfg.get("scheduler_enabled", "true") == "true",
        interval_minutes=int(cfg.get("scheduler_interval_minutes", "360")),
        next_run=next_run,
        last_run=last_run,
        running=job is not None,
    )


@router.put("/", response_model=schemas.SchedulerStatus)
async def update_config(
    data: schemas.SchedulerConfig,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await crud.set_config(db, "scheduler_enabled", str(data.enabled).lower())
    await crud.set_config(db, "scheduler_interval_minutes", str(data.interval_minutes))
    scheduler = _get_scheduler(request)
    apply_config(scheduler, data.interval_minutes, data.enabled)
    return await get_status(request, db)


@router.post("/run-now", response_model=schemas.SchedulerStatus)
async def run_now(request: Request, db: AsyncSession = Depends(get_db)):
    await _refresh_task()
    return await get_status(request, db)
