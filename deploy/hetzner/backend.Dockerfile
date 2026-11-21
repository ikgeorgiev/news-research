FROM python:3.13-slim AS builder
WORKDIR /build
COPY backend/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home appuser \
    && mkdir -p /var/lib/cef-news/article-bodies /var/lib/cef-news/logs \
    && chown -R appuser:appuser /var/lib/cef-news
COPY --from=builder /install /usr/local
WORKDIR /app/backend
COPY --chown=appuser:appuser backend/ /app/backend/
COPY --chown=appuser:appuser data/cef_tickers.csv /app/data/cef_tickers.csv
# Keep Windows checkout CRLF out of the Linux entrypoint.
RUN sed -i 's/\r$//' entrypoint.sh
USER appuser
EXPOSE 8000
ENTRYPOINT ["sh", "entrypoint.sh"]
