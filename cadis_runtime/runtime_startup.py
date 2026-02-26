from __future__ import annotations

import json
import os
import sys

from cadis_runtime.runtime_bootstrap import (
    DEFAULT_DATASET_MANIFEST_URL,
    bootstrap_country_dataset,
    write_bootstrap_state,
)


def main() -> int:
    iso2 = os.getenv("CADIS_REGION_ISO2", "").strip()
    if not iso2:
        print("CADIS_REGION_ISO2 is required.", file=sys.stderr)
        return 2

    dataset_manifest_url = os.getenv("CADIS_DATASET_MANIFEST_URL", DEFAULT_DATASET_MANIFEST_URL).strip()
    cache_dir = os.getenv("CADIS_CACHE_DIR", "/opt/cadis/cache").strip()
    state_path = os.getenv("CADIS_BOOTSTRAP_STATE_PATH", "/tmp/cadis_bootstrap_state.json").strip()
    update_raw = os.getenv("UPDATE_TO_LATEST", "FALSE").strip().lower()
    update_to_latest = update_raw in {"1", "true", "yes", "on"}
    dataset_version = os.getenv("CADIS_DATASET_VERSION", "").strip() or None

    state = bootstrap_country_dataset(
        country_iso2=iso2,
        dataset_manifest_url=dataset_manifest_url,
        cache_dir=cache_dir,
        update_to_latest=update_to_latest,
        dataset_version=dataset_version,
    )
    state["region_iso2"] = state.pop("country_iso2")
    write_bootstrap_state(state_path, state)
    print(json.dumps({"status": "bootstrapped", **state}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
