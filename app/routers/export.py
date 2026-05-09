from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.database import get_db
from app import crud, models

router = APIRouter(prefix="/list", tags=["export"])


async def _build_m3u(db: AsyncSession, url_builder, base_url: str) -> str:
    result = await db.execute(
        select(models.Channel)
        .options(
            selectinload(models.Channel.sources),
            selectinload(models.Channel.groups),
        )
    )
    channels = result.scalars().all()

    lines = ["#EXTM3U"]
    for ch in channels:
        active_sources = [s for s in ch.sources if s.active and s.test_status == "ok"]
        if not active_sources:
            continue
        group_title = ch.groups[0].name if ch.groups else "Sin grupo"
        if ch.custom_logo:
            logo = f"{base_url}static/logos/{ch.custom_logo}"
        else:
            logo = ch.logo or ""
        for src in active_sources:
            label = src.label or ch.name
            lines.append(
                f'#EXTINF:-1 tvg-id="{ch.tvg_id}" tvg-name="{ch.name}" '
                f'tvg-logo="{logo}" group-title="{group_title}",{label}'
            )
            lines.append(url_builder(src.ace_hash))

    return "\n".join(lines)


@router.get("/native", response_class=PlainTextResponse)
async def export_native(
    request: Request,
    group: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    content = await _build_m3u(db, lambda h: f"acestream://{h}", str(request.base_url))
    return PlainTextResponse(content, media_type="application/x-mpegurl")


@router.get("/proxy", response_class=PlainTextResponse)
async def export_proxy(
    request: Request,
    group: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    config = await crud.get_config(db)
    ip = config.get("acexy_ip", "127.0.0.1")
    port = config.get("acexy_port", "6878")

    def proxy_url(h: str) -> str:
        return f"http://{ip}:{port}/ace/getstream?id={h}"

    content = await _build_m3u(db, proxy_url, str(request.base_url))
    return PlainTextResponse(content, media_type="application/x-mpegurl")
