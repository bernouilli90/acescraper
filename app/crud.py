from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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


async def set_channel_logo(db: AsyncSession, channel_id: int, filename: Optional[str], clear_logo_url: bool = False):
    values: dict = {"custom_logo": filename}
    if clear_logo_url:
        values["logo"] = None
    await db.execute(
        update(models.Channel)
        .where(models.Channel.id == channel_id)
        .values(**values)
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
    if not entries:
        return {"new": 0, "duplicates": 0, "mapped": 0}

    # Batch-fetch channels by tvg_id (1 query instead of N)
    tvg_ids = {e['tvg_id'] for e in entries if e.get('tvg_id')}
    channel_by_tvg: dict[str, int] = {}
    if tvg_ids:
        res = await db.execute(
            select(models.Channel.tvg_id, models.Channel.id)
            .where(models.Channel.tvg_id.in_(tvg_ids))
        )
        channel_by_tvg = {row[0]: row[1] for row in res.fetchall()}

    # Batch-fetch existing sources by hash (1 query instead of N)
    hashes = [e['ace_hash'] for e in entries]
    res = await db.execute(
        select(models.Source).where(models.Source.ace_hash.in_(hashes))
    )
    existing_by_hash: dict[str, models.Source] = {s.ace_hash: s for s in res.scalars().all()}

    new_count    = 0
    dup_count    = 0
    mapped_count = 0

    for entry in entries:
        h          = entry['ace_hash']
        tvg_id     = entry.get('tvg_id')
        label      = entry.get('label')
        channel_id = channel_by_tvg.get(tvg_id) if tvg_id else None

        existing = existing_by_hash.get(h)
        if existing:
            dup_count += 1
            if existing.channel_id is None and channel_id is not None:
                existing.channel_id = channel_id
                mapped_count += 1
            if existing.label is None and label:
                existing.label = label
        else:
            new_src = models.Source(
                ace_hash=h,
                label=label,
                feed_url_id=feed_url_id,
                channel_id=channel_id,
            )
            db.add(new_src)
            existing_by_hash[h] = new_src  # evitar dobles dentro del mismo batch
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
    await db.execute(
        update(models.Source)
        .where(models.Source.id == source_id)
        .values(deleted=True)
    )
    await db.commit()


async def restore_source(db: AsyncSession, source_id: int):
    await db.execute(
        update(models.Source)
        .where(models.Source.id == source_id)
        .values(deleted=False)
    )
    await db.commit()


async def bulk_restore_sources(db: AsyncSession, ids: list[int]) -> int:
    result = await db.execute(
        update(models.Source)
        .where(models.Source.id.in_(ids))
        .values(deleted=False)
    )
    await db.commit()
    return result.rowcount


async def update_source_test_result(db: AsyncSession, source_id: int, status: str) -> str:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    result = await db.execute(select(models.Source).where(models.Source.id == source_id))
    source = result.scalar_one_or_none()
    if not source:
        return status
    # a dead stream that still fails stays dead (not downgraded to fail)
    if status == "fail" and source.test_status == "dead":
        status = "dead"
    if status == "ok":
        source.fail_since = None
    elif status in ("fail", "dead") and source.fail_since is None:
        # Set fail_since on first failure, or fill NULL left by schema migration
        source.fail_since = now
    source.test_status = status
    source.test_last_run = now
    await db.commit()
    return status


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


async def get_all_tvg_ids(db: AsyncSession) -> set[str]:
    """tvg_ids of channels that have at least one source, for EPG generation."""
    from sqlalchemy import text
    result = await db.execute(
        text(
            "SELECT DISTINCT c.tvg_id FROM channels c "
            "INNER JOIN sources s ON s.channel_id = c.id "
            "WHERE c.tvg_id IS NOT NULL AND c.tvg_id != ''"
        )
    )
    return {row[0] for row in result.fetchall()}


async def get_sources_to_test(db: AsyncSession, statuses: list[str], limit: int = 500):
    result = await db.execute(
        select(models.Source)
        .where(models.Source.active.is_(True))
        .where(models.Source.deleted.is_(False))
        .where(models.Source.test_status.in_(statuses))
        .order_by(models.Source.test_last_run.asc().nullsfirst())
        .limit(limit)
    )
    return result.scalars().all()


async def bulk_delete_sources(db: AsyncSession, ids: list[int]) -> int:
    result = await db.execute(
        update(models.Source)
        .where(models.Source.id.in_(ids))
        .values(deleted=True)
    )
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


async def get_group_channels_with_active_sources(db: AsyncSession, group_id: int):
    """Channels in the group (ordered) that have at least one active source."""
    q = (
        select(models.Channel)
        .join(models.channel_group, models.Channel.id == models.channel_group.c.channel_id)
        .where(models.channel_group.c.group_id == group_id)
        .options(selectinload(models.Channel.sources))
        .order_by(models.channel_group.c.position, models.Channel.id)
    )
    result = await db.execute(q)
    channels = result.scalars().all()
    return [ch for ch in channels if any(s.active for s in ch.sources)]


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

    # Batch-fetch all existing channels by tvg_id (1 query instead of N)
    tvg_ids = [ch["tvg_id"] for ch in channels]
    res = await db.execute(
        select(models.Channel).where(models.Channel.tvg_id.in_(tvg_ids))
    )
    existing_by_tvg: dict[str, models.Channel] = {ch.tvg_id: ch for ch in res.scalars().all()}

    # Batch-fetch existing xmltv links for all known channels (1 query instead of N)
    existing_ch_ids = [ch.id for ch in existing_by_tvg.values()]
    linked_channel_ids: set[int] = set()
    if existing_ch_ids:
        res = await db.execute(
            select(models.channel_xmltv.c.channel_id)
            .where(models.channel_xmltv.c.xmltv_id == xmltv_id)
            .where(models.channel_xmltv.c.channel_id.in_(existing_ch_ids))
        )
        linked_channel_ids = {row[0] for row in res.fetchall()}

    created = updated = 0
    for ch in channels:
        existing = existing_by_tvg.get(ch["tvg_id"])
        if existing:
            existing.name = ch["name"]
            if ch.get("logo"):
                existing.logo = ch["logo"]
            if existing.id not in linked_channel_ids:
                await db.execute(
                    models.channel_xmltv.insert().values(channel_id=existing.id, xmltv_id=xmltv_id)
                )
                linked_channel_ids.add(existing.id)
            updated += 1
        else:
            new_ch = models.Channel(tvg_id=ch["tvg_id"], name=ch["name"], logo=ch.get("logo"))
            db.add(new_ch)
            await db.flush()
            await db.execute(
                models.channel_xmltv.insert().values(channel_id=new_ch.id, xmltv_id=xmltv_id)
            )
            existing_by_tvg[ch["tvg_id"]] = new_ch
            created += 1

    from datetime import datetime, timezone
    await db.execute(
        update(models.XmltvUrl)
        .where(models.XmltvUrl.id == xmltv_id)
        .values(last_fetched=datetime.now(timezone.utc), channel_count=created + updated)
    )
    await db.commit()
    return {"created": created, "updated": updated, "total": created + updated}


async def bulk_import_channels(db: AsyncSession, channels: list[dict]) -> dict:
    """Create or update channels from a parsed XMLTV file (no xmltv_id link).

    Batches SELECT queries at 500 ids to stay within SQLite's parameter limit.
    """
    if not channels:
        return {"created": 0, "updated": 0, "total": 0}

    all_tvg_ids = [ch["tvg_id"] for ch in channels]
    existing_by_tvg: dict[str, models.Channel] = {}
    BATCH = 500
    for i in range(0, len(all_tvg_ids), BATCH):
        res = await db.execute(
            select(models.Channel).where(models.Channel.tvg_id.in_(all_tvg_ids[i:i + BATCH]))
        )
        for ch in res.scalars().all():
            existing_by_tvg[ch.tvg_id] = ch

    created = updated = 0
    for ch in channels:
        existing = existing_by_tvg.get(ch["tvg_id"])
        if existing:
            existing.name = ch["name"]
            if ch.get("logo"):
                existing.logo = ch["logo"]
            updated += 1
        else:
            new_ch = models.Channel(tvg_id=ch["tvg_id"], name=ch["name"], logo=ch.get("logo"))
            db.add(new_ch)
            existing_by_tvg[ch["tvg_id"]] = new_ch
            created += 1

    await db.commit()
    return {"created": created, "updated": updated, "total": created + updated}


# ── Config ────────────────────────────────────────────────────────────────────

async def get_config(db: AsyncSession) -> dict:
    result = await db.execute(select(models.AppConfig))
    rows = result.scalars().all()
    return {r.key: r.value for r in rows}


async def set_config(db: AsyncSession, key: str, value: str):
    stmt = sqlite_insert(models.AppConfig).values(key=key, value=value)
    stmt = stmt.on_conflict_do_update(index_elements=["key"], set_={"value": value})
    await db.execute(stmt)
    await db.commit()


async def set_configs(db: AsyncSession, updates: dict):
    for key, value in updates.items():
        stmt = sqlite_insert(models.AppConfig).values(key=key, value=value)
        stmt = stmt.on_conflict_do_update(index_elements=["key"], set_={"value": value})
        await db.execute(stmt)
    await db.commit()


# ── Export Profiles ───────────────────────────────────────────────────────────

async def list_export_profiles(db: AsyncSession):
    r = await db.execute(select(models.ExportProfile).order_by(models.ExportProfile.id))
    return r.scalars().all()


async def get_export_profile(db: AsyncSession, profile_id: int):
    r = await db.execute(select(models.ExportProfile).where(models.ExportProfile.id == profile_id))
    return r.scalar_one_or_none()


async def create_export_profile(db: AsyncSession, data: schemas.ExportProfileIn):
    obj = models.ExportProfile(**data.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return obj


async def update_export_profile(db: AsyncSession, profile_id: int, data: schemas.ExportProfileIn):
    obj = await get_export_profile(db, profile_id)
    if not obj:
        return None
    for k, v in data.model_dump().items():
        setattr(obj, k, v)
    await db.commit()
    await db.refresh(obj)
    return obj


async def delete_export_profile(db: AsyncSession, profile_id: int):
    obj = await get_export_profile(db, profile_id)
    if obj:
        await db.delete(obj)
        await db.commit()
    return obj
