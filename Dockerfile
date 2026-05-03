FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libimage-exiftool-perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data

ENV PHOTO_TAGGER_DB_PATH=/data/indexing.db \
    PHOTO_TAGGER_SETTINGS_PATH=/data/tagger_settings.json \
    PHOTO_TAGGER_DASHBOARD_ROOT=/mnt/synology \
    PHOTO_TAGGER_HOST=0.0.0.0 \
    PHOTO_TAGGER_PORT=5000 \
    PHOTO_TAGGER_MAX_WORKERS=2

VOLUME ["/data"]
EXPOSE 5000

CMD ["python", "app.py"]
