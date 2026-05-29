import asyncio
import gzip
import io
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from lxml import etree
from sqlalchemy.ext.asyncio import AsyncSession

ACE_HASH_RE = re.compile(r'\b([0-9a-fA-F]{40})\b')
TVG_ID_RE   = re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE)


def parse_m3u(text: str) -> list[dict]:
    """Parse M3U or plain text. Returns list of {ace_hash, tvg_id, label}.

    Handles both structured M3U (#EXTINF lines) and raw hash dumps.
    Deduplicates by hash, keeping the first occurrence (which carries tvg-id).
    """
    entries: list[dict] = []
    seen: set[str] = set()

    pending_tvg_id: Optional[str] = None
    pending_label:  Optional[str] = None

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.upper().startswith('#EXTINF'):
            m = TVG_ID_RE.search(line)
            pending_tvg_id = m.group(1).strip() or None if m else None
            comma = line.rfind(',')
            pending_label = line[comma + 1:].strip() or None if comma != -1 else None

        elif not line.startswith('#'):
            for h in ACE_HASH_RE.findall(line):
                h = h.lower()
                if h not in seen:
                    seen.add(h)
                    entries.append({
                        'ace_hash': h,
                        'tvg_id':   pending_tvg_id,
                        'label':    pending_label,
                    })
            # consume pending metadata regardless of whether hashes were found
            pending_tvg_id = None
            pending_label  = None

    return entries


def extract_hashes(text: str) -> list[str]:
    return [e['ace_hash'] for e in parse_m3u(text)]


async def fetch_and_parse(url: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    return parse_m3u(resp.text)


async def fetch_and_extract(url: str) -> list[str]:
    return [e['ace_hash'] for e in await fetch_and_parse(url)]


def parse_xmltv(content: bytes) -> list[dict]:
    """Return list of {tvg_id, name, logo} from XMLTV data.

    Uses iterparse + on-demand gzip to avoid loading the full DOM into memory.
    """
    channels = []
    try:
        buf = io.BytesIO(content)
        source = gzip.GzipFile(fileobj=buf) if content[:2] == b'\x1f\x8b' else buf
        for _, ch in etree.iterparse(source, events=("end",), tag="channel", recover=True):
            tvg_id = ch.get("id", "").strip()
            if tvg_id:
                name_el = ch.find("display-name")
                name = name_el.text.strip() if name_el is not None and name_el.text else tvg_id
                icon_el = ch.find("icon")
                logo = icon_el.get("src", "") if icon_el is not None else ""
                channels.append({"tvg_id": tvg_id, "name": name, "logo": logo or None})
            parent = ch.getparent()
            ch.clear()
            if parent is not None:
                parent.remove(ch)
    except etree.XMLSyntaxError:
        pass
    return channels


async def fetch_xmltv(url: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    return parse_xmltv(resp.content)


# ── Scheduled refresh ─────────────────────────────────────────────────────────

async def refresh_all_feeds(db: AsyncSession):
    from app import crud
    feeds = await crud.get_feed_urls(db)
    active = [f for f in feeds if f.active]
    feed_progress.update({"running": True, "current": 0, "total": len(active), "current_url": None})
    try:
        for feed in active:
            feed_progress["current_url"] = feed.url
            try:
                entries = await fetch_and_parse(feed.url)
                await crud.bulk_create_sources(db, entries, feed_url_id=feed.id)
                await crud.update_feed_url_stats(db, feed.id, len(entries))
            except Exception:
                pass
            feed_progress["current"] += 1
    finally:
        feed_progress.update({"running": False, "current_url": None})


async def test_stream(ace_hash: str, acexy_ip: str, acexy_port: int) -> bool:
    url = f"http://{acexy_ip}:{acexy_port}/ace/getstream?id={ace_hash}"
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                return r.status_code < 400
    except Exception:
        return False


test_progress: dict = {
    "running": False, "current": 0, "total": 0, "cancel": False,
    "current_hash": None, "current_label": None,
    "recent": [],  # [{hash, label, status}] últimos 5
}

feed_progress: dict = {
    "running": False, "current": 0, "total": 0, "current_url": None,
}

_test_lock = asyncio.Lock()


async def run_stream_tests(statuses: list[str]):
    if _test_lock.locked():
        return
    async with _test_lock:
        await _run_stream_tests(statuses)


async def _run_stream_tests(statuses: list[str]):
    from app import crud
    from app.database import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        cfg = await crud.get_config(db)
        ip = cfg.get("acexy_ip", "127.0.0.1")
        port = int(cfg.get("acexy_port", "6878"))
        concurrency = int(cfg.get("stream_test_concurrency", "5"))
        sources = await crud.get_sources_to_test(db, statuses)
        source_list = [(s.id, s.ace_hash, s.label) for s in sources]

    test_progress["running"] = True
    test_progress["current"] = 0
    test_progress["total"] = len(source_list)
    test_progress["cancel"] = False
    test_progress["current_hash"] = None
    test_progress["current_label"] = None

    sem = asyncio.Semaphore(concurrency)

    async def _test_one(source_id: int, ace_hash: str, label: Optional[str]):
        async with sem:
            if test_progress["cancel"]:
                return
            display = label or (ace_hash[:12] + "…")
            test_progress["current_hash"] = ace_hash[:12]
            test_progress["current_label"] = display
            ok = await test_stream(ace_hash, ip, port)
            raw_status = "ok" if ok else "fail"
            async with AsyncSessionLocal() as db:
                final_status = await crud.update_source_test_result(db, source_id, raw_status)
            test_progress["recent"] = (
                [{"hash": ace_hash[:12], "label": display, "status": final_status}]
                + test_progress["recent"]
            )[:5]
            test_progress["current"] += 1

    try:
        await asyncio.gather(*[_test_one(sid, h, lbl) for sid, h, lbl in source_list])
    finally:
        test_progress["running"] = False
        test_progress["current_hash"] = None
        test_progress["current_label"] = None

    from datetime import datetime, timezone
    async with AsyncSessionLocal() as db:
        await crud.set_config(db, "stream_test_last_run", datetime.now(timezone.utc).isoformat())

    if cfg.get("stream_test_auto_deactivate_enabled", "false") == "true":
        threshold = int(cfg.get("stream_test_auto_deactivate_hours", "48"))
        async with AsyncSessionLocal() as db:
            await crud.mark_long_failing_sources_dead(db, threshold)


def _parse_xmltv_dt(s: str) -> Optional[datetime]:
    try:
        s = s.strip()
        dt = datetime.strptime(s[:14], "%Y%m%d%H%M%S")
        tz_part = s[14:].strip()
        if len(tz_part) >= 5:
            sign = 1 if tz_part[0] == "+" else -1
            tz = timezone(timedelta(hours=sign * int(tz_part[1:3]), minutes=sign * int(tz_part[3:5])))
        else:
            tz = timezone.utc
        return dt.replace(tzinfo=tz)
    except Exception:
        return None


async def generate_epg(tvg_ids: set[str], days: int, xmltv_urls: list[str]) -> tuple[bytes, int, int]:
    """
    Fetch XMLTV URLs, filter to tvg_ids and [now, now+days] window.
    Returns (xml_bytes, channel_count, programme_count).

    Uses streaming HTTP download + iterparse to avoid loading multi-GB DOMs into memory.
    """
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days)
    past_cutoff = now - timedelta(hours=1)  # include currently-airing programmes

    channels_xml: dict[str, bytes] = {}   # tvg_id → serialized <channel> element
    programmes_xml: list[bytes] = []

    for url in xmltv_urls:
        try:
            # Stream download into a single buffer (avoids holding full content + decompressed copy)
            buf = io.BytesIO()
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.aiter_bytes(65536):
                        buf.write(chunk)
            buf.seek(0)

            # Wrap with on-demand gzip decompressor if needed (no full-decompress copy)
            magic = buf.read(2)
            buf.seek(0)
            source = gzip.GzipFile(fileobj=buf) if magic == b"\x1f\x8b" else buf

            # Stream-parse: never loads the full DOM into memory
            for _, elem in etree.iterparse(source, events=("end",), tag=("channel", "programme"), recover=True):
                if elem.tag == "channel":
                    cid = elem.get("id", "")
                    if cid in tvg_ids and cid not in channels_xml:
                        channels_xml[cid] = etree.tostring(elem, encoding="unicode").encode("utf-8")
                elif elem.tag == "programme":
                    if elem.get("channel", "") in tvg_ids:
                        start = _parse_xmltv_dt(elem.get("start", ""))
                        if start is not None and start <= cutoff:
                            stop = _parse_xmltv_dt(elem.get("stop", ""))
                            if stop is None or stop >= past_cutoff:
                                programmes_xml.append(
                                    etree.tostring(elem, encoding="unicode").encode("utf-8")
                                )
                # Free element and remove from parent to prevent DOM accumulation
                parent = elem.getparent()
                elem.clear()
                if parent is not None:
                    parent.remove(elem)
        except Exception:
            continue

    header = b'<?xml version="1.0" encoding="UTF-8"?>\n<tv generator-info-name="AceScraper">\n'
    body = b"\n".join(channels_xml.values()) + b"\n" + b"\n".join(programmes_xml)
    xml_bytes = header + body + b"\n</tv>\n"
    return xml_bytes, len(channels_xml), len(programmes_xml)


async def refresh_all_xmltv(_db: AsyncSession):
    from app import crud
    from app.database import AsyncSessionLocal
    # Use a fresh session to list XMLTVs, isolated from any feed session state
    async with AsyncSessionLocal() as db:
        xmltvs = await crud.get_xmltv_urls(db)
        xmltv_list = [(x.id, x.url) for x in xmltvs]
    for xmltv_id, xmltv_url in xmltv_list:
        try:
            channels = await fetch_xmltv(xmltv_url)
            async with AsyncSessionLocal() as db:
                await crud.import_xmltv_channels(db, channels, xmltv_id)
        except Exception:
            pass
