from fastapi import APIRouter, Depends, HTTPException
from fastapi.requests import Request
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.database import get_db
from app import crud, models

router = APIRouter(prefix="/list", tags=["export"])


def _apply_name_template(template: str, ch, src) -> str:
    try:
        return template.format(
            canal=ch.name,
            etiqueta=src.label or "",
            etiqueta_o_canal=src.label or ch.name,
            hash4=src.ace_hash[:4],
            hash8=src.ace_hash[:8],
        )
    except (KeyError, ValueError):
        return src.label or ch.name


async def _build_m3u(db: AsyncSession, url_builder, base_url: str, name_template: str = "{canal}") -> str:
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
            label = _apply_name_template(name_template, ch, src)
            lines.append(
                f'#EXTINF:-1 tvg-id="{ch.tvg_id}" tvg-name="{ch.name}" '
                f'tvg-logo="{logo}" group-title="{group_title}",{label}'
            )
            lines.append(url_builder(src.ace_hash))

    return "\n".join(lines)


@router.get("/native", response_class=PlainTextResponse)
async def export_native(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    content = await _build_m3u(db, lambda h: f"acestream://{h}", str(request.base_url))
    return PlainTextResponse(content, media_type="application/x-mpegurl")


@router.get("/proxy", response_class=PlainTextResponse)
async def export_proxy(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    config = await crud.get_config(db)
    ip = config.get("acexy_ip", "127.0.0.1")
    port = config.get("acexy_port", "6878")

    def proxy_url(h: str) -> str:
        return f"http://{ip}:{port}/ace/getstream?id={h}"

    content = await _build_m3u(db, proxy_url, str(request.base_url))
    return PlainTextResponse(content, media_type="application/x-mpegurl")


@router.get("/p/{profile_id}", response_class=PlainTextResponse)
async def export_by_profile(
    profile_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    profile = await crud.get_export_profile(db, profile_id)
    if not profile:
        raise HTTPException(404, "Perfil no encontrado")

    if profile.url_type == "proxy":
        config = await crud.get_config(db)
        ip = config.get("acexy_ip", "127.0.0.1")
        port = config.get("acexy_port", "6878")
        url_builder = lambda h: f"http://{ip}:{port}/ace/getstream?id={h}"
    else:
        url_builder = lambda h: f"acestream://{h}"

    content = await _build_m3u(db, url_builder, str(request.base_url), profile.name_template)
    return PlainTextResponse(content, media_type="application/x-mpegurl")
