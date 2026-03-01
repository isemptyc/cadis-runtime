FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CADIS_CACHE_DIR=/opt/cadis/cache
ENV CADIS_BOOTSTRAP_STATE_PATH=/tmp/cadis_bootstrap_state.json
ENV PORT=5000

WORKDIR /app

ARG CADIS_RUNTIME_VERSION=0.1.2
ARG CADIS_RUNTIME_INSTALL_MODE=pypi

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md
COPY LICENSE /app/LICENSE
COPY cadis_runtime /app/cadis_runtime
COPY cadis_runtime_app /app/cadis_runtime_app
COPY docker/docker-entrypoint.sh /app/docker-entrypoint.sh

RUN if [ "${CADIS_RUNTIME_INSTALL_MODE}" = "pypi" ]; then \
      pip install --no-cache-dir "cadis-runtime[bootstrap]==${CADIS_RUNTIME_VERSION}"; \
    elif [ "${CADIS_RUNTIME_INSTALL_MODE}" = "local" ]; then \
      pip install --no-cache-dir "/app[bootstrap]"; \
    else \
      echo "Unsupported CADIS_RUNTIME_INSTALL_MODE: ${CADIS_RUNTIME_INSTALL_MODE}" >&2; \
      exit 1; \
    fi

RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
