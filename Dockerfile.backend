# Backend Dockerfile for ConversionCentral API
FROM python:3.11-slim-bullseye AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    DEBIAN_FRONTEND=noninteractive

# Install build dependencies for the API
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        ca-certificates \
        gcc \
        g++ \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . /code

# Entrypoint script handles migrations before starting uvicorn
RUN chmod +x docker/backend/start.sh

EXPOSE 8000

CMD ["docker/backend/start.sh"]
