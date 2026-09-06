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

    timeout = httpx.Timeout(connect=10.0, read=None, write=None, pool=None)
    client = httpx.AsyncClient(timeout=timeout)
    try:
        resp = await client.send(client.build_request("GET", url), stream=True)
    except httpx.HTTPError as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"No se pudo conectar con acexy: {e}")

    async def _gen():
        try:
            async for chunk in resp.aiter_bytes(65536):
                yield chunk
        except httpx.HTTPError:
            # acexy cortó la conexión a mitad de stream (normal si el canal cae) —
            # no hay forma de cambiar el status ya enviado, solo cerrar limpio.
            pass
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(_gen(), media_type="video/mp2t")
