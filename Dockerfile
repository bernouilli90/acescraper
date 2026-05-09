# ── Build stage: install deps ─────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Non-root user
RUN useradd -m -u 1000 -s /bin/sh appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY app/        app/
COPY templates/  templates/
COPY static/     static/
COPY bot.py      .

# Persistent data directory (SQLite lives here)
RUN mkdir -p /data && chown appuser:appuser /data

USER appuser

ENV DATABASE_URL=sqlite+aiosqlite:////data/acescraper.db \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_PORT=8000

EXPOSE ${APP_PORT}

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://localhost:{os.getenv(\"APP_PORT\",\"8000\")}/api/config/')"

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${APP_PORT}"]
