import re
import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud

router = APIRouter(tags=["stream-proxy"])

_HASH_RE = re.compile(r'^[0-9a-fA-F]{40}$')


@router.get("/api/stream/{ace_hash}")
async def proxy_stream(ace_hash: str, db: AsyncSession = Depends(get_db)):
    if not _HASH_RE.match(ace_hash):
        raise HTTPException(status_code=400, detail="hash inválido")

    cfg = await crud.get_config(db)
    ip   = cfg.get("acexy_ip",   "127.0.0.1")
    port = int(cfg.get("acexy_port", "6878"))
    url  = f"http://{ip}:{port}/ace/getstream?id={ace_hash}"

    async def _gen():
        timeout = httpx.Timeout(connect=10.0, read=None, write=None, pool=None)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", url) as resp:
                async for chunk in resp.aiter_bytes(65536):
                    yield chunk

    return StreamingResponse(_gen(), media_type="video/mp2t")
