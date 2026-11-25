FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 SNIPY_HEADLESS=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libcurl4 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY snipy ./snipy
RUN useradd -m sniper && mkdir -p /data && chown -R sniper /data /app
USER sniper

EXPOSE 8080
CMD ["python", "-m", "snipy", "--headless", "--serve"]
