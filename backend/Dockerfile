FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

COPY backend/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd -g 1000 appuser \
    && useradd -u 1000 -g appuser -s /bin/sh -m appuser

COPY --from=builder /install /usr/local

WORKDIR /app

COPY backend /app/backend
COPY data /app/data

RUN chown -R appuser:appuser /app

USER appuser
WORKDIR /app/backend

EXPOSE 8000

ENTRYPOINT ["sh", "entrypoint.sh"]
