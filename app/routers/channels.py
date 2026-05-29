import asyncio
import glob
import os
from datetime import datetime, timezone, timedelta
from lxml import etree

import aiofiles
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app import crud, schemas
from app.scraper import _parse_xmltv_dt

EPG_FILE = os.getenv("EPG_FILE", "./data/epg.xml")

router = APIRouter(prefix="/api/channels", tags=["channels"])

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
LOGOS_DIR = os.getenv("LOGOS_DIR", os.path.join(_ROOT, "static", "logos"))

_ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif", "image/svg+xml"}
_EXT_MAP = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/svg+xml": ".svg",
}


@router.get("/", response_model=list[schemas.ChannelOut])
async def list_channels(skip: int = 0, limit: int = 10000, db: AsyncSession = Depends(get_db)):
    return await crud.get_channels(db, skip=skip, limit=limit)


@router.post("/", response_model=schemas.ChannelOut, status_code=status.HTTP_201_CREATED)
async def create_channel(data: schemas.ChannelCreate, db: AsyncSession = Depends(get_db)):
    existing = await crud.get_channel_by_tvg_id(db, data.tvg_id)
    if existing:
        raise HTTPException(status_code=409, detail="tvg_id already exists")
    return await crud.create_channel(db, data)


@router.post("/{channel_id}/logo", response_model=schemas.ChannelOut)
async def upload_logo(
    channel_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")

    ct = (file.content_type or "").split(";")[0].strip()
    if ct not in _ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail=f"Tipo no soportado: {ct}")

    ext = _EXT_MAP.get(ct, ".jpg")
    filename = f"channel_{channel_id}{ext}"

    os.makedirs(LOGOS_DIR, exist_ok=True)
    for old in glob.glob(os.path.join(LOGOS_DIR, f"channel_{channel_id}.*")):
        os.remove(old)

    async with aiofiles.open(os.path.join(LOGOS_DIR, filename), "wb") as f:
        await f.write(await file.read())

    await crud.set_channel_logo(db, channel_id, filename)
    return await crud.get_channel(db, channel_id)


@router.delete("/{channel_id}/logo", status_code=status.HTTP_204_NO_CONTENT)
async def delete_logo(channel_id: int, db: AsyncSession = Depends(get_db)):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")

    if ch.custom_logo:
        path = os.path.join(LOGOS_DIR, ch.custom_logo)
        if os.path.exists(path):
            os.remove(path)

    await crud.set_channel_logo(db, channel_id, None)


@router.get("/{channel_id}", response_model=schemas.ChannelOut)
async def get_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    return ch


@router.patch("/{channel_id}", response_model=schemas.ChannelOut)
async def update_channel(channel_id: int, data: schemas.ChannelUpdate, db: AsyncSession = Depends(get_db)):
    ch = await crud.update_channel(db, channel_id, data)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    return ch


@router.delete("/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_channel(db, channel_id)


def _channel_schedule(tvg_id: str, hours_ahead: int = 12) -> list:
    """Programs for tvg_id in [now-1h, now+hours_ahead], sorted by start."""
    if not tvg_id or not os.path.exists(EPG_FILE):
        return []
    now    = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=hours_ahead)
    past   = now - timedelta(hours=1)
    progs  = []
    try:
        for _, elem in etree.iterparse(EPG_FILE, events=("end",), tag="programme", recover=True):
            if elem.get("channel", "") == tvg_id:
                start = _parse_xmltv_dt(elem.get("start", ""))
                stop  = _parse_xmltv_dt(elem.get("stop",  ""))
                if start and stop and stop >= past and start <= cutoff:
                    title_el = elem.find("title")
                    desc_el  = elem.find("desc")
                    progs.append({
                        "title": title_el.text if title_el is not None else "",
                        "start": start.isoformat(),
                        "stop":  stop.isoformat(),
                        "desc":  desc_el.text if desc_el is not None else None,
                    })
            parent = elem.getparent()
            elem.clear()
            if parent is not None:
                parent.remove(elem)
    except Exception:
        pass
    progs.sort(key=lambda p: p["start"])
    return progs


@router.get("/{channel_id}/guide")
async def channel_guide(channel_id: int, db: AsyncSession = Depends(get_db)):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Canal no encontrado")

    active_sources = [s for s in ch.sources if s.active and s.test_status == "ok" and not s.deleted]
    schedule = await asyncio.to_thread(_channel_schedule, ch.tvg_id)

    return {
        "id":    ch.id,
        "name":  ch.name,
        "logo":  f"/static/logos/{ch.custom_logo}" if ch.custom_logo else ch.logo,
        "tvg_id": ch.tvg_id,
        "sources": [
            {
                "id":          s.id,
                "ace_hash":    s.ace_hash,
                "label":       s.label,
                "test_status": s.test_status,
                "test_last_run": s.test_last_run.isoformat() if s.test_last_run else None,
            }
            for s in active_sources
        ],
        "schedule": schedule,
    }


@router.get("/{channel_id}/detail")
async def channel_detail(channel_id: int, db: AsyncSession = Depends(get_db)):
    """All sources (any status) + EPG schedule for the channel detail modal."""
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Canal no encontrado")

    schedule = await asyncio.to_thread(_channel_schedule, ch.tvg_id)

    return {
        "id":     ch.id,
        "name":   ch.name,
        "tvg_id": ch.tvg_id,
        "logo":   f"/static/logos/{ch.custom_logo}" if ch.custom_logo else ch.logo,
        "sources": [
            {
                "id":           s.id,
                "ace_hash":     s.ace_hash,
                "label":        s.label,
                "active":       s.active,
                "test_status":  s.test_status,
                "test_last_run": s.test_last_run.isoformat() if s.test_last_run else None,
                "fail_since":   s.fail_since.isoformat() if s.fail_since else None,
            }
            for s in sorted(ch.sources, key=lambda s: (not s.active, s.test_status != "ok", s.id))
            if not s.deleted
        ],
        "schedule": schedule,
    }
