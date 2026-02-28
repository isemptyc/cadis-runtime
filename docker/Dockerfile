FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CADIS_CACHE_DIR=/opt/cadis/cache
ENV CADIS_BOOTSTRAP_STATE_PATH=/tmp/cadis_bootstrap_state.json
ENV PORT=5000

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY cadis_runtime /app/cadis_runtime
COPY cadis_runtime_app /app/cadis_runtime_app
COPY docker/docker-entrypoint.sh /app/docker-entrypoint.sh

RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
