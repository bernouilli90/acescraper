import os
import subprocess


def _get_version() -> str:
    v = os.getenv("APP_VERSION")
    if v:
        return v
    try:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=base,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "dev"


APP_VERSION = _get_version()
