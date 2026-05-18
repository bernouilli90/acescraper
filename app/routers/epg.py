import os
from datetime import datetime, timezone

import aiofiles
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app import crud, schemas, scraper

router = APIRouter(tags=["epg"])

JOB_ID = "epg_generate"

EPG_FILE = os.getenv("EPG_FILE", "./data/epg.xml")


def _get_scheduler(request: Request):
    return request.app.state.scheduler


def apply_epg_config(scheduler, interval_hours: int, enabled: bool):
    if scheduler.get_job(JOB_ID):
        scheduler.remove_job(JOB_ID)
    if enabled:
        scheduler.add_job(
            _epg_task,
            IntervalTrigger(hours=interval_hours),
            id=JOB_ID,
            replace_existing=True,
        )


async def _epg_task():
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        # Refresh XMLTV channel metadata first
        await scraper.refresh_all_xmltv(db)

        cfg = await crud.get_config(db)
        days = int(cfg.get("epg_days", "7"))
        channels = await crud.get_channels_for_export(db)
        tvg_ids = {ch.tvg_id for ch in channels}
        xmltv_urls_obj = await crud.get_xmltv_urls(db)
        urls = [x.url for x in xmltv_urls_obj]

    xml_bytes, ch_count, prog_count = await scraper.generate_epg(tvg_ids, days, urls)

    os.makedirs(os.path.dirname(EPG_FILE), exist_ok=True)
    async with aiofiles.open(EPG_FILE, "wb") as f:
        await f.write(xml_bytes)

    async with AsyncSessionLocal() as db:
        now = datetime.now(timezone.utc).isoformat()
        await crud.set_config(db, "epg_last_generated", now)
        await crud.set_config(db, "epg_channel_count", str(ch_count))
        await crud.set_config(db, "epg_programme_count", str(prog_count))


def _next_run(scheduler):
    job = scheduler.get_job(JOB_ID)
    if job and job.next_run_time:
        return job.next_run_time.astimezone(timezone.utc).replace(tzinfo=None)
    return None


async def _build_status(request: Request, db: AsyncSession) -> schemas.EpgStatus:
    cfg = await crud.get_config(db)
    scheduler = _get_scheduler(request)

    last_gen_str = cfg.get("epg_last_generated")
    last_gen = datetime.fromisoformat(last_gen_str).replace(tzinfo=None) if last_gen_str else None

    file_size_kb = 0.0
    if os.path.exists(EPG_FILE):
        file_size_kb = round(os.path.getsize(EPG_FILE) / 1024, 1)

    return schemas.EpgStatus(
        enabled=cfg.get("epg_enabled", "false") == "true",
        interval_hours=int(cfg.get("epg_interval_hours", "24")),
        days=int(cfg.get("epg_days", "7")),
        next_run=_next_run(scheduler),
        last_generated=last_gen,
        channel_count=int(cfg.get("epg_channel_count", "0")),
        programme_count=int(cfg.get("epg_programme_count", "0")),
        file_size_kb=file_size_kb,
    )


@router.get("/api/epg/config", response_model=schemas.EpgStatus)
async def get_config(request: Request, db: AsyncSession = Depends(get_db)):
    return await _build_status(request, db)


@router.put("/api/epg/config", response_model=schemas.EpgStatus)
async def update_config(
    data: schemas.EpgConfig,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await crud.set_config(db, "epg_enabled", str(data.enabled).lower())
    await crud.set_config(db, "epg_interval_hours", str(data.interval_hours))
    await crud.set_config(db, "epg_days", str(data.days))
    apply_epg_config(_get_scheduler(request), data.interval_hours, data.enabled)
    return await _build_status(request, db)


@router.post("/api/epg/generate", response_model=schemas.EpgStatus)
async def generate_now(request: Request, db: AsyncSession = Depends(get_db)):
    await _epg_task()
    return await _build_status(request, db)


@router.get("/epg.xml")
async def serve_epg():
    if not os.path.exists(EPG_FILE):
        raise HTTPException(status_code=404, detail="EPG no generado todavía")
    return FileResponse(EPG_FILE, media_type="application/xml", filename="epg.xml")
