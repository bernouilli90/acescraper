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

    async def _fail(detail: str):
        await resp.aclose()
        await client.aclose()
        raise HTTPException(status_code=502, detail=detail[:500])

    if resp.status_code >= 400:
        detail = (await resp.aread()).decode("utf-8", errors="replace").strip()
        await _fail(detail or f"acexy devolvió {resp.status_code}")

    # acexy a veces responde 200 igualmente pero con un mensaje de texto plano
    # en el cuerpo en vez del stream real (p.ej. "Failed to start stream: ...
    # timeout awaiting response headers" cuando el motor Acestream no consigue
    # arrancar la sesión). Antes esto se reenviaba tal cual — Content-Type
    # video/mp2t con 200 — el navegador no podía decodificar ese texto como
    # vídeo y el reproductor fallaba al instante con un "sin conexión"
    # genérico que ocultaba el motivo real. Los paquetes MPEG-TS siempre
    # empiezan por el byte de sincronismo 0x47, así que si el primer trozo no
    # lo tiene, es texto de error y lo tratamos como tal.
    body_iter = resp.aiter_bytes(65536)
    try:
        first_chunk = await anext(body_iter)
    except StopAsyncIteration:
        await _fail("acexy cerró la conexión sin enviar datos")
        return  # unreachable, _fail siempre lanza
    except httpx.HTTPError as e:
        await _fail(f"Error leyendo el stream de acexy: {e}")
        return

    if not first_chunk.startswith(b"\x47"):
        await _fail(first_chunk.decode("utf-8", errors="replace").strip() or "Respuesta de acexy no reconocida")

    async def _gen():
        try:
            yield first_chunk
            async for chunk in body_iter:
                yield chunk
        except httpx.HTTPError:
            # acexy cortó la conexión a mitad de stream (normal si el canal cae) —
            # no hay forma de cambiar el status ya enviado, solo cerrar limpio.
            pass
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(_gen(), media_type="video/mp2t")
