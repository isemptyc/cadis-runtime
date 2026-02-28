
# cadis-runtime

Cadis Runtime is a lightweight, deterministic, dataset-driven execution engine for ISO 3166-1 administrative hierarchy lookup.

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

- `cadis_runtime/` — Core runtime package (lookup engine + dataset interpretation)
- `cadis_runtime_app/` — Docker/API application layer (bootstrap + Flask endpoints)
- `docker/` — Container build and entrypoint assets
- `README.md` — User-facing documentation

---

## Docker Runtime (Single ISO 3166-1 Entity per Container)

Each container instance serves exactly one ISO 3166-1 alpha-2 dataset.

At startup, the runtime performs:

1. If a valid cached dataset exists and update is not requested → use it.
2. If no dataset exists → resolve latest version and download it.
3. Verify dataset checksum.
4. Unpack into cache directory.
5. Start Flask API.

After a dataset has been successfully cached, the runtime can operate fully offline.

---

## Build Image

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
  "version": "0.1.0"
}
```

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

| ISO2 | Name   | Dataset ID | Package Size (tar.gz) | Unpacked Size | Release Date |
|:-----|:-------|:-----------|----------------------:|--------------:|-------------:|
| TW   | Taiwan | tw.admin   | 1.9 MB                | 2.9 MB        | 2026-02-26   |

Additional ISO 3166-1 entity datasets will be published as they become available.
