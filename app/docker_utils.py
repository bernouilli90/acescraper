import httpx

DOCKER_SOCKET = "/var/run/docker.sock"


async def docker_restart(container: str) -> tuple[bool, str]:
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            r = await client.post(f"/containers/{container}/restart", timeout=20.0)
            if r.status_code == 204:
                return True, ""
            return False, f"Docker devolvió {r.status_code}"
    except PermissionError:
        return False, "Sin acceso al socket Docker (/var/run/docker.sock)"
    except FileNotFoundError:
        return False, "Socket Docker no encontrado — monta /var/run/docker.sock"
    except Exception as e:
        return False, str(e)


async def docker_list_containers() -> list[dict]:
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
            r = await client.get("/containers/json?all=true", timeout=5.0)
            r.raise_for_status()
            return [
                {
                    "name":   c["Names"][0].lstrip("/"),
                    "id":     c["Id"][:12],
                    "image":  c["Image"],
                    "status": c["State"],  # "running", "exited", etc.
                }
                for c in r.json()
            ]
    except Exception:
        return []
