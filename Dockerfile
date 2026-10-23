FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && groupadd --system app \
    && useradd --system --gid app --no-create-home app

COPY --chown=app:app src ./src
USER app

EXPOSE 8000

# One process: each application worker starts its own RSS scheduler.
CMD ["python", "-m", "uvicorn", "src.interfaces.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-graceful-shutdown", "30"]
