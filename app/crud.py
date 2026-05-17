from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update
from sqlalchemy.orm import selectinload
from typing import Optional
from app import models, schemas


# ── Channels ──────────────────────────────────────────────────────────────────

async def get_channels(db: AsyncSession, skip: int = 0, limit: int = 10000):
    result = await db.execute(
        select(models.Channel)
        .options(
            selectinload(models.Channel.groups),
            selectinload(models.Channel.sources),
            selectinload(models.Channel.xmltv_urls),
        )
        .order_by(models.Channel.id)
        .offset(skip).limit(limit)
    )
    return result.scalars().all()


async def get_channel(db: AsyncSession, channel_id: int):
    result = await db.execute(
        select(models.Channel)
        .options(
            selectinload(models.Channel.groups),
            selectinload(models.Channel.sources),
            selectinload(models.Channel.xmltv_urls),
        )
        .where(models.Channel.id == channel_id)
    )
    return result.scalar_one_or_none()


async def get_channel_by_tvg_id(db: AsyncSession, tvg_id: str):
    result = await db.execute(
        select(models.Channel).where(models.Channel.tvg_id == tvg_id)
    )
    return result.scalar_one_or_none()


async def create_channel(db: AsyncSession, data: schemas.ChannelCreate):
    groups = []
    if data.group_ids:
        res = await db.execute(
            select(models.Group).where(models.Group.id.in_(data.group_ids))
        )
        groups = res.scalars().all()
    channel = models.Channel(tvg_id=data.tvg_id, name=data.name, logo=data.logo, groups=groups)
    db.add(channel)
    await db.commit()
    await db.refresh(channel)
    return channel


async def update_channel(db: AsyncSession, channel_id: int, data: schemas.ChannelUpdate):
    channel = await get_channel(db, channel_id)
    if not channel:
        return None
    if data.name is not None:
        channel.name = data.name
    if data.logo is not None:
        channel.logo = data.logo
    if data.group_ids is not None:
        res = await db.execute(
            select(models.Group).where(models.Group.id.in_(data.group_ids))
        )
        channel.groups = res.scalars().all()
    await db.commit()
    await db.refresh(channel)
    return channel


async def delete_channel(db: AsyncSession, channel_id: int):
    await db.execute(delete(models.Channel).where(models.Channel.id == channel_id))
    await db.commit()


async def set_channel_logo(db: AsyncSession, channel_id: int, filename: Optional[str]):
    await db.execute(
        update(models.Channel)
        .where(models.Channel.id == channel_id)
        .values(custom_logo=filename)
    )
    await db.commit()


# ── Sources ───────────────────────────────────────────────────────────────────

async def get_sources(db: AsyncSession, skip: int = 0, limit: int = 1000, unmapped_only: bool = False):
    q = select(models.Source).options(selectinload(models.Source.channel))
    if unmapped_only:
        q = q.where(models.Source.channel_id.is_(None))
    result = await db.execute(q.offset(skip).limit(limit))
    return result.scalars().all()


async def get_source_by_hash(db: AsyncSession, ace_hash: str):
    result = await db.execute(
        select(models.Source).where(models.Source.ace_hash == ace_hash)
    )
    return result.scalar_one_or_none()


async def create_source(db: AsyncSession, data: schemas.SourceCreate):
    source = models.Source(**data.model_dump())
    db.add(source)
    await db.commit()
    await db.refresh(source)
    return source


async def bulk_create_sources(
    db: AsyncSession,
    entries: list[dict],
    feed_url_id: Optional[int] = None,
) -> dict:
    """
    entries: list of {ace_hash, tvg_id?, label?}
    - New hash: create it; if tvg_id resolves to a channel, assign it.
    - Existing hash without channel: assign channel from tvg_id if available.
    - Existing hash with channel already set: skip (never override).
    """
    new_count    = 0
    dup_count    = 0
    mapped_count = 0

    for entry in entries:
        h       = entry['ace_hash']
        tvg_id  = entry.get('tvg_id')
        label   = entry.get('label')

        # Resolve tvg_id → channel once per entry
        channel_id: Optional[int] = None
        if tvg_id:
            ch = await get_channel_by_tvg_id(db, tvg_id)
            if ch:
                channel_id = ch.id

        existing = await get_source_by_hash(db, h)
        if existing:
            dup_count += 1
            if existing.channel_id is None and channel_id is not None:
                existing.channel_id = channel_id
                mapped_count += 1
            if existing.label is None and label:
                existing.label = label
        else:
            db.add(models.Source(
                ace_hash=h,
                label=label,
                feed_url_id=feed_url_id,
                channel_id=channel_id,
            ))
            new_count += 1
            if channel_id is not None:
                mapped_count += 1

    await db.commit()
    return {"new": new_count, "duplicates": dup_count, "mapped": mapped_count}


async def update_source(db: AsyncSession, source_id: int, data: schemas.SourceUpdate):
    result = await db.execute(select(models.Source).where(models.Source.id == source_id))
    source = result.scalar_one_or_none()
    if not source:
        return None
    for field, val in data.model_dump(exclude_none=True).items():
        setattr(source, field, val)
    await db.commit()
    await db.refresh(source)
    return source


async def delete_source(db: AsyncSession, source_id: int):
    await db.execute(delete(models.Source).where(models.Source.id == source_id))
    await db.commit()


async def update_source_test_result(db: AsyncSession, source_id: int, status: str):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    result = await db.execute(select(models.Source).where(models.Source.id == source_id))
    source = result.scalar_one_or_none()
    if not source:
        return
    prev = source.test_status
    # a dead stream that still fails stays dead (not downgraded to fail)
    if status == "fail" and prev == "dead":
        status = "dead"
    # track when continuous failure streak started
    if status in ("fail", "dead") and prev not in ("fail", "dead"):
        source.fail_since = now
    elif status == "ok":
        source.fail_since = None
    source.test_status = status
    source.test_last_run = now
    await db.commit()


async def mark_long_failing_sources_dead(db: AsyncSession, threshold_hours: int) -> int:
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
    result = await db.execute(
        update(models.Source)
        .where(models.Source.active.is_(True))
        .where(models.Source.test_status == "fail")
        .where(models.Source.fail_since != None)  # noqa: E711
        .where(models.Source.fail_since <= cutoff)
        .values(test_status="dead")
    )
    await db.commit()
    return result.rowcount


async def get_channels_for_export(db: AsyncSession) -> list:
    """Channels that have at least one active+ok source (same logic as M3U export)."""
    result = await db.execute(
        select(models.Channel).options(selectinload(models.Channel.sources))
    )
    return [
        ch for ch in result.scalars().all()
        if any(s.active and s.test_status == "ok" for s in ch.sources)
    ]


async def get_sources_to_test(db: AsyncSession, statuses: list[str], limit: int = 500):
    result = await db.execute(
        select(models.Source)
        .where(models.Source.active.is_(True))
        .where(models.Source.test_status.in_(statuses))
        .order_by(models.Source.test_last_run.asc().nullsfirst())
        .limit(limit)
    )
    return result.scalars().all()


async def bulk_delete_sources(db: AsyncSession, ids: list[int]) -> int:
    result = await db.execute(delete(models.Source).where(models.Source.id.in_(ids)))
    await db.commit()
    return result.rowcount


# ── Groups ────────────────────────────────────────────────────────────────────

async def get_group_channel_ids_ordered(db: AsyncSession, group_id: int) -> list[int]:
    result = await db.execute(
        select(models.channel_group.c.channel_id)
        .where(models.channel_group.c.group_id == group_id)
        .order_by(models.channel_group.c.position, models.channel_group.c.channel_id)
    )
    return [row[0] for row in result.fetchall()]


async def update_group_channel_order(db: AsyncSession, group_id: int, channel_ids: list[int]):
    for pos, ch_id in enumerate(channel_ids):
        await db.execute(
            update(models.channel_group)
            .where(models.channel_group.c.group_id == group_id)
            .where(models.channel_group.c.channel_id == ch_id)
            .values(position=pos)
        )
    await db.commit()


async def get_groups(db: AsyncSession):
    result = await db.execute(select(models.Group))
    return result.scalars().all()


async def create_group(db: AsyncSession, data: schemas.GroupCreate):
    group = models.Group(name=data.name)
    db.add(group)
    await db.commit()
    await db.refresh(group)
    return group


async def delete_group(db: AsyncSession, group_id: int):
    await db.execute(delete(models.Group).where(models.Group.id == group_id))
    await db.commit()


# ── FeedUrls ──────────────────────────────────────────────────────────────────

async def get_feed_urls(db: AsyncSession):
    result = await db.execute(select(models.FeedUrl))
    return result.scalars().all()


async def get_feed_url(db: AsyncSession, feed_id: int):
    result = await db.execute(
        select(models.FeedUrl).where(models.FeedUrl.id == feed_id)
    )
    return result.scalar_one_or_none()


async def create_feed_url(db: AsyncSession, data: schemas.FeedUrlCreate):
    feed = models.FeedUrl(**data.model_dump())
    db.add(feed)
    await db.commit()
    await db.refresh(feed)
    return feed


async def delete_feed_url(db: AsyncSession, feed_id: int):
    await db.execute(delete(models.FeedUrl).where(models.FeedUrl.id == feed_id))
    await db.commit()


async def update_feed_url_stats(db: AsyncSession, feed_id: int, count: int):
    from datetime import datetime, timezone
    await db.execute(
        update(models.FeedUrl)
        .where(models.FeedUrl.id == feed_id)
        .values(last_fetched=datetime.now(timezone.utc), last_count=count)
    )
    await db.commit()


# ── XmltvUrls ─────────────────────────────────────────────────────────────────

async def get_xmltv_urls(db: AsyncSession):
    result = await db.execute(select(models.XmltvUrl))
    return result.scalars().all()


async def get_xmltv_url(db: AsyncSession, xmltv_id: int):
    result = await db.execute(
        select(models.XmltvUrl)
        .options(
            selectinload(models.XmltvUrl.channels)
            .selectinload(models.Channel.xmltv_urls)
        )
        .where(models.XmltvUrl.id == xmltv_id)
    )
    return result.scalar_one_or_none()


async def create_xmltv_url(db: AsyncSession, data: schemas.XmltvUrlCreate):
    xmltv = models.XmltvUrl(url=data.url, label=data.label)
    db.add(xmltv)
    await db.commit()
    await db.refresh(xmltv)
    return xmltv


async def delete_xmltv_url(db: AsyncSession, xmltv_id: int) -> Optional[dict]:
    xmltv = await get_xmltv_url(db, xmltv_id)
    if not xmltv:
        return None

    orphan_ids = [
        ch.id for ch in xmltv.channels
        if all(x.id == xmltv_id for x in ch.xmltv_urls)
    ]
    if orphan_ids:
        await db.execute(delete(models.Channel).where(models.Channel.id.in_(orphan_ids)))

    await db.execute(delete(models.XmltvUrl).where(models.XmltvUrl.id == xmltv_id))
    await db.commit()
    return {"deleted_channels": len(orphan_ids)}


async def import_xmltv_channels(db: AsyncSession, channels: list[dict], xmltv_id: int) -> dict:
    result = await db.execute(
        select(models.XmltvUrl).where(models.XmltvUrl.id == xmltv_id)
    )
    xmltv = result.scalar_one_or_none()
    if not xmltv:
        return {"created": 0, "updated": 0, "total": 0}

    created = updated = 0
    for ch in channels:
        existing = await get_channel_by_tvg_id(db, ch["tvg_id"])
        if existing:
            existing.name = ch["name"]
            if ch.get("logo"):
                existing.logo = ch["logo"]
            # Link to this xmltv if not already linked
            linked_ids_result = await db.execute(
                select(models.channel_xmltv.c.xmltv_id).where(
                    models.channel_xmltv.c.channel_id == existing.id,
                    models.channel_xmltv.c.xmltv_id == xmltv_id,
                )
            )
            if linked_ids_result.scalar_one_or_none() is None:
                await db.execute(
                    models.channel_xmltv.insert().values(channel_id=existing.id, xmltv_id=xmltv_id)
                )
            updated += 1
        else:
            new_ch = models.Channel(tvg_id=ch["tvg_id"], name=ch["name"], logo=ch.get("logo"))
            db.add(new_ch)
            await db.flush()
            await db.execute(
                models.channel_xmltv.insert().values(channel_id=new_ch.id, xmltv_id=xmltv_id)
            )
            created += 1

    from datetime import datetime, timezone
    await db.execute(
        update(models.XmltvUrl)
        .where(models.XmltvUrl.id == xmltv_id)
        .values(last_fetched=datetime.now(timezone.utc), channel_count=created + updated)
    )
    await db.commit()
    return {"created": created, "updated": updated, "total": created + updated}


# ── Config ────────────────────────────────────────────────────────────────────

async def get_config(db: AsyncSession) -> dict:
    result = await db.execute(select(models.AppConfig))
    rows = result.scalars().all()
    return {r.key: r.value for r in rows}


async def set_config(db: AsyncSession, key: str, value: str):
    result = await db.execute(
        select(models.AppConfig).where(models.AppConfig.key == key)
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.value = value
    else:
        db.add(models.AppConfig(key=key, value=value))
    await db.commit()
