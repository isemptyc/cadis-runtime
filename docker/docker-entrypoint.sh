#!/usr/bin/env sh
set -eu

python -m cadis_runtime.runtime_startup
exec gunicorn --bind 0.0.0.0:"${PORT:-5000}" --workers "${GUNICORN_WORKERS:-1}" cadis_runtime.app:app
