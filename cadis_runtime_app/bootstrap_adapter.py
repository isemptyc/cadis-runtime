from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cadis_cdn.bootstrap import bootstrap_country_dataset as cdn_bootstrap_country_dataset
from cadis_cdn.runtime_compat import validate_manifest_runtime_compatibility
from cadis_runtime.dataset.loader import load_runtime_policy
from cadis_runtime.version import __version__ as CADIS_VERSION

DEFAULT_DATASET_MANIFEST_URL = (
    "https://raw.githubusercontent.com/isemptyc/cadis-dataset/main/releases/dataset_manifest.json"
)


def bootstrap_country_dataset(
    *,
    country_iso2: str,
    dataset_manifest_url: str = DEFAULT_DATASET_MANIFEST_URL,
    cache_dir: str | Path = "/opt/cadis/cache",
    timeout_sec: int = 30,
    update_to_latest: bool = False,
    dataset_version: str | None = None,
) -> dict[str, Any]:
    return cdn_bootstrap_country_dataset(
        country_iso2=country_iso2,
        dataset_manifest_url=dataset_manifest_url,
        cache_dir=cache_dir,
        timeout_sec=timeout_sec,
        update_to_latest=update_to_latest,
        dataset_version=dataset_version,
        validate_release_manifest_compatibility=lambda manifest: validate_manifest_runtime_compatibility(
            manifest,
            runtime_version=CADIS_VERSION,
        ),
        validate_dataset_dir=load_runtime_policy,
    )


def write_bootstrap_state(path: str | Path, state: dict[str, Any]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def read_bootstrap_state(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
