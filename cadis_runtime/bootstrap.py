from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cadis_cdn.bootstrap import (
    bootstrap_country_dataset as cdn_bootstrap_country_dataset,
    bootstrap_release_dataset,
)
from cadis_cdn.runtime_compat import validate_manifest_runtime_compatibility

from cadis_runtime.dataset.loader import load_runtime_policy
from cadis_runtime.version import __version__ as CADIS_VERSION

MANIFEST_NAME = "dataset_release_manifest.json"
MANIFEST_PROFILE = "cadis.dataset.release"
RUNTIME_POLICY_FILE = "runtime_policy.json"
DEFAULT_DATASET_MANIFEST_URL = (
    "https://raw.githubusercontent.com/isemptyc/cadis-dataset/main/releases/dataset_manifest.json"
)


def _validate_runtime_dataset(dataset_dir: Path) -> None:
    policy_path = dataset_dir / RUNTIME_POLICY_FILE
    policy_obj = json.loads(policy_path.read_text(encoding="utf-8"))
    layers = policy_obj.get("layers")
    if not isinstance(layers, dict):
        raise ValueError("runtime_policy.json missing layers object.")
    hierarchy_required = layers.get("hierarchy_required")
    repair_required = layers.get("repair_required")
    if not isinstance(hierarchy_required, bool):
        raise ValueError("runtime_policy.json layers.hierarchy_required must be boolean.")
    if not isinstance(repair_required, bool):
        raise ValueError("runtime_policy.json layers.repair_required must be boolean.")

    loaded_policy = load_runtime_policy(dataset_dir)
    if hierarchy_required and not (dataset_dir / "hierarchy.json").exists():
        raise ValueError("runtime_policy requires hierarchy.json but it is missing.")
    if repair_required and not (dataset_dir / "repair.json").exists():
        raise ValueError("runtime_policy requires repair.json but it is missing.")
    for overlay in loaded_policy.optional_layers:
        if not (dataset_dir / overlay.file).exists():
            raise ValueError(
                f"runtime_policy optional overlay file missing after bootstrap download: {overlay.file}"
            )


def bootstrap_dataset(
    dataset_base: str,
    country: str,
    *,
    cache_dir: str | Path | None = None,
    timeout_sec: int = 15,
) -> dict[str, Any]:
    """
    Download + verify release-manifest governed dataset into runtime cache.
    """
    return bootstrap_release_dataset(
        dataset_base,
        country,
        runtime_version=CADIS_VERSION,
        validate_dataset_dir=_validate_runtime_dataset,
        cache_dir=cache_dir,
        timeout_sec=timeout_sec,
        manifest_name=MANIFEST_NAME,
        manifest_profile=MANIFEST_PROFILE,
        runtime_policy_file=RUNTIME_POLICY_FILE,
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
    """
    Bootstrap country dataset from dataset-manifest routing entry.
    """
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
        validate_dataset_dir=_validate_runtime_dataset,
    )
