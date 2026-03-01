
# cadis-runtime

Cadis Runtime is a deterministic execution layer for ISO 3166-1–scoped administrative hierarchy lookup.

It interprets pre-built datasets and composes final hierarchy results strictly according to dataset-declared policies. The runtime itself does not construct datasets, ingest raw OpenStreetMap data, or perform global routing.

Cadis Runtime is designed to operate offline once a dataset has been bootstrapped and cached.

---

## Design Principles

- **Lightweight execution layer**  
  Runtime interprets datasets but does not build them.

- **Deterministic behavior**  
  All hierarchy supplementation and structural behavior are defined by dataset manifests.

- **Offline-first operation**  
  Once a dataset is cached, the runtime does not require network access.

- **Immutable dataset releases**  
  Datasets are versioned and validated independently from the runtime.

---

## Project Layout

- `packages/cadis-core/` — Country-agnostic structural engine package (`cadis-core`)
- `packages/cadis-cdn/` — Dataset transport/bootstrap/integrity package (`cadis-cdn`)
- `cadis_runtime/` — Runtime library package (`cadis-runtime`)
- `cadis_runtime_app/` — Standalone Docker/API application code (not shipped in `cadis-runtime` wheel)
- `pyproject.toml` — Packaging metadata for `cadis-runtime` (library distribution)
- `docker/` — Container build and entrypoint assets
- `README.md` — User-facing documentation

---

## Library Vs App

- **Library (`cadis-runtime`)**: use in Python code via `CadisRuntime`.
- **App (`cadis_runtime_app`)**: standalone production-ready Docker service code in this repo.

## Internal Components

`cadis-core` and `cadis-cdn` are low-level Cadis components intended for internal composition.
For stable public integration, prefer `cadis-runtime` (and `cadis-global` when available) instead of directly coupling to internal packages.

---

## Python API (Library Entrypoint)

Install options:

```bash
# Deterministic runtime only (dataset_dir mode)
pip install cadis-runtime

# Include bootstrap helpers (from_iso2 / bootstrap_dataset)
pip install "cadis-runtime[bootstrap]"
```

Choose by use case:

- `pip install cadis-runtime` for deterministic runtime execution with local `dataset_dir`.
- `pip install "cadis-runtime[bootstrap]"` when you need `CadisRuntime.from_iso2(...)` or bootstrap helpers.

Use `CadisRuntime` as the public runtime contract:

```python
from cadis_runtime import CadisRuntime

runtime = CadisRuntime(dataset_dir="/path/to/dataset")
response = runtime.lookup(25.033, 121.5654)
```

Or use the convenience bootstrap constructor:

`from_iso2(...)` requires `cadis-runtime[bootstrap]` and a writable cache path.
On local/macOS environments, prefer paths like `/tmp/cadis-cache` instead of `/opt/cadis/cache`.

```python
from cadis_runtime import CadisRuntime

runtime = CadisRuntime.from_iso2(
    "TW",
    cache_dir="/tmp/cadis-cache",
    update_to_latest=False,
)
response = runtime.lookup(25.033, 121.5654)
```

`cadis-core` provides structural engine logic.
`cadis-cdn` provides bootstrap/transport/integrity primitives and is only needed for bootstrap helpers.
Base `cadis-runtime` depends on `cadis-core` only, while bootstrap helpers use optional extra `cadis-runtime[bootstrap]`.

---

## Docker Runtime (Single ISO 3166-1 Entity per Container)

This section is app/service mode (`cadis_runtime_app`) and not the Python library return contract.

Each container instance serves exactly one ISO 3166-1 alpha-2 dataset.

At startup, the runtime performs:

1. If a valid cached dataset exists and update is not requested → use it.
2. If no dataset exists → resolve latest version and download it.
3. Verify dataset checksum.
4. Unpack into cache directory.
5. Start Flask API.

After a dataset has been successfully cached, the runtime can operate fully offline.

Container startup entrypoint:

```bash
python -m cadis_runtime_app.app_startup
```

---

## Build Image

Release-like build (default, installs pinned package from PyPI):

```bash
docker build -f docker/Dockerfile -t cadis-runtime:latest .
```

---

## Run Container

```bash
docker run \
  --name cadis-tw \
  -p 5000:5000 \
  -e CADIS_COUNTRY_ISO2=TW \
  cadis-runtime:latest
```

---

## Persistent Cache (Offline Mode)

To preserve the dataset cache across container restarts, mount a Docker volume:

```bash
docker run \
  --name cadis-tw \
  -p 5000:5000 \
  -e CADIS_COUNTRY_ISO2=TW \
  -v cadis_cache_tw:/opt/cadis/cache \
  cadis-runtime:latest
```

Without a mounted volume, the container filesystem is ephemeral and the dataset will be re-downloaded when a new container instance is created.

---

## Environment Variables

* `CADIS_COUNTRY_ISO2` (required)
  ISO 3166-1 alpha-2 code to serve (e.g., TW, JP, HK).

* `CADIS_CACHE_DIR` (default: `/opt/cadis/cache`)
  Directory where datasets are stored.

* `UPDATE_TO_LATEST` (default: `FALSE`)

  * `FALSE` — Deterministic/offline-first mode.
    If a valid cached dataset exists, use it without checking for remote updates.
  * `TRUE` — Startup-time update check.
    Resolve the latest dataset version and upgrade if newer.

* `PORT` (default: `5000`)
  Flask application port.

* `GUNICORN_WORKERS` (default: `1`)
  Number of Gunicorn worker processes.

---

## API

### Health Check

```
GET /health
```

Response:

```json
{
  "country_iso2": "TW",
  "dataset_dir": "/opt/cadis/cache/TW/tw.admin/v1.0.2",
  "dataset_id": "tw.admin",
  "dataset_version": "v1.0.2",
  "status": "ok"
}
```

---

### Administrative Lookup

```
POST /lookup
```

Request body:

```json
{
  "lat": 25.033,
  "lon": 121.5654
}
```

```bash
curl -sS -X POST "http://127.0.0.1:5000/lookup" \
  -H "Content-Type: application/json" \
  -d '{"lat":25.033,"lon":121.5654}'
```

Response (example):

```json
{
  "engine": "cadis",
  "lookup_status": "ok",
  "summary_text": "臺北市, 信義區",
  "iso_context": {
    "iso2": "TW",
    "name": "Taiwan"
  },  
  "result": {
    "admin_hierarchy": [
      {
        "level": 4,
        "name": "臺北市",
        "osm_id": "tw_r1293250",
        "rank": 0,
        "source": "polygon"
      },
      {
        "level": 7,
        "name": "信義區",
        "osm_id": "tw_r2881027",
        "rank": 1,
        "source": "polygon"
      }
    ]
  },
  "version": "0.1.2"
}
```

The `version` field reflects the installed `cadis-runtime` package version.

---

## Dataset Separation

Cadis Runtime does **not** bundle or redistribute OpenStreetMap data.

Datasets are:

* Built separately
* Versioned independently
* Distributed via the cadis-dataset repository

Runtime validates dataset integrity via checksum and manifest metadata.

---

## ISO Code Policy

Cadis Runtime uses ISO 3166-1 alpha-2 codes as technical routing identifiers.

These codes are interpreted strictly according to the ISO 3166 standard and are used solely for data partitioning and administrative dataset selection.

Cadis does not interpret ISO codes as political statements or sovereignty declarations.

---

## Supported ISO 3166-1 Entities

| ISO2 | Name   | Dataset ID | Package Size (tar.gz) | Unpacked Size | Release Date (UTC) |
|:-----|:-------|:-----------|----------------------:|--------------:|-------------------:|
| TW   | Taiwan | tw.admin   | 1.8 MB                | 2.0 MB        | 2026-02-28         |
| JP   | Japan  | jp.admin   | 20.4 MB               | 21.3 MB       | 2026-03-01         |

Additional ISO 3166-1 entity datasets will be published as they become available.
