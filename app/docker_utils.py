import httpx

DOCKER_SOCKET = "/var/run/docker.sock"


def _transport():
    return httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)


async def docker_restart(container: str) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(transport=_transport(), base_url="http://localhost") as client:
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


async def docker_inspect_container(container: str) -> dict:
    """
    Returns container state via Docker API.
    {
      running: bool,
      container_status: str,   # "running", "exited", "paused", "not_found", "error"
      health: str|None,        # "healthy", "unhealthy", "starting", None (no healthcheck)
      failing_streak: int,
      error: str,              # only when container_status == "error"
    }
    """
    try:
        async with httpx.AsyncClient(transport=_transport(), base_url="http://localhost") as client:
            r = await client.get(f"/containers/{container}/json", timeout=5.0)
            if r.status_code == 404:
                return {"running": False, "container_status": "not_found", "health": None, "failing_streak": 0}
            r.raise_for_status()
            state = r.json().get("State", {})
            health_info = state.get("Health")
            return {
                "running":          state.get("Running", False),
                "container_status": state.get("Status", "unknown"),
                "health":           health_info.get("Status") if health_info else None,
                "failing_streak":   health_info.get("FailingStreak", 0) if health_info else 0,
            }
    except PermissionError:
        return {"running": False, "container_status": "error", "health": None, "failing_streak": 0,
                "error": "Sin acceso al socket Docker (/var/run/docker.sock)"}
    except FileNotFoundError:
        return {"running": False, "container_status": "error", "health": None, "failing_streak": 0,
                "error": "Socket Docker no encontrado — monta /var/run/docker.sock"}
    except Exception as e:
        return {"running": False, "container_status": "error", "health": None, "failing_streak": 0,
                "error": str(e)}


async def docker_list_containers() -> list[dict]:
    try:
        async with httpx.AsyncClient(transport=_transport(), base_url="http://localhost") as client:
            r = await client.get("/containers/json?all=true", timeout=5.0)
            r.raise_for_status()
            return [
                {
                    "name":   c["Names"][0].lstrip("/"),
                    "id":     c["Id"][:12],
                    "image":  c["Image"],
                    "status": c["State"],
                }
                for c in r.json()
            ]
    except Exception:
        return []
