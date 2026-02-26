from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
import urllib.request

from cadis_runtime.dataset.loader import load_runtime_policy
from cadis_runtime.version import __version__ as CADIS_VERSION

MANIFEST_NAME = "dataset_release_manifest.json"
MANIFEST_PROFILE = "cadis.dataset.release"
RUNTIME_POLICY_FILE = "runtime_policy.json"


def _parse_semver(raw: str, *, field: str) -> tuple[int, ...]:
    value = raw.strip()
    if value.startswith("v"):
        value = value[1:]
    parts = value.split(".")
    if not parts or any(not p.isdigit() for p in parts):
        raise ValueError(f"Manifest invalid {field} (expected semver-like digits, e.g. 2.0.0).")
    return tuple(int(p) for p in parts)


def _validate_manifest_runtime_compatibility(manifest: dict[str, Any]) -> tuple[str, str]:
    runtime_compat = manifest.get("runtime_compat")
    if not isinstance(runtime_compat, dict):
        raise ValueError("Manifest missing runtime_compat object.")

    min_cadis_version = runtime_compat.get("min")
    if not isinstance(min_cadis_version, str) or not min_cadis_version.strip():
        raise ValueError("Manifest missing runtime_compat.min.")

    max_cadis_version_exclusive = runtime_compat.get("max_exclusive")
    if not isinstance(max_cadis_version_exclusive, str) or not max_cadis_version_exclusive.strip():
        raise ValueError("Manifest missing runtime_compat.max_exclusive.")

    runtime_v = _parse_semver(CADIS_VERSION, field="cadis runtime version")
    min_v = _parse_semver(min_cadis_version, field="min_cadis_version")
    max_v = _parse_semver(max_cadis_version_exclusive, field="max_cadis_version_exclusive")

    if min_v >= max_v:
        raise ValueError("Manifest has invalid runtime range: min_cadis_version must be < max_cadis_version_exclusive.")
    if runtime_v < min_v:
        raise ValueError(
            f"Cadis runtime {CADIS_VERSION} is lower than required min_cadis_version {min_cadis_version}."
        )
    if runtime_v >= max_v:
        raise ValueError(
            f"Cadis runtime {CADIS_VERSION} is not supported (>= max_cadis_version_exclusive {max_cadis_version_exclusive})."
        )

    return min_cadis_version.strip(), max_cadis_version_exclusive.strip()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _bundle_checksum_from_files(checksums: dict[str, str]) -> str:
    h = hashlib.sha256()
    for rel in sorted(checksums.keys()):
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(checksums[rel].encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


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
    iso2 = country.strip().upper()
    if not iso2:
        raise ValueError("country must be a non-empty ISO2 code")

    dataset_url = dataset_base.rstrip("/")
    manifest_url = f"{dataset_url}/{MANIFEST_NAME}"

    cache_root = Path(cache_dir).expanduser() if cache_dir else (Path.home() / ".cache" / "cadis")

    with urllib.request.urlopen(manifest_url, timeout=timeout_sec) as r:
        manifest = json.loads(r.read().decode("utf-8"))

    if manifest.get("profile") != MANIFEST_PROFILE:
        raise ValueError(f"Invalid manifest profile: {manifest.get('profile')!r}")
    if manifest.get("schema_version") != 2:
        raise ValueError(f"Unsupported schema version: {manifest.get('schema_version')!r}")
    manifest_country = manifest.get("country_iso")
    if not isinstance(manifest_country, str) or manifest_country.strip().upper() != iso2:
        raise ValueError(
            f"Manifest country mismatch: expected={iso2} actual={manifest.get('country_iso')!r}"
        )
    dataset_id = manifest.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ValueError("Manifest missing dataset_id.")
    dataset_version = manifest.get("dataset_version")
    if not isinstance(dataset_version, str) or not dataset_version.strip():
        raise ValueError("Manifest missing dataset_version.")

    target_dir = cache_root / iso2 / dataset_id.strip() / dataset_version.strip()
    target_dir.mkdir(parents=True, exist_ok=True)
    if manifest.get("checksum_algo") != "sha256":
        raise ValueError(f"Unsupported checksum algorithm: {manifest.get('checksum_algo')!r}")
    min_cadis_version, max_cadis_version_exclusive = _validate_manifest_runtime_compatibility(manifest)

    checksums = manifest.get("checksums")
    if not isinstance(checksums, dict):
        raise ValueError("Manifest missing checksums object.")
    files = checksums.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("Manifest checksums.files must be a non-empty object")
    if RUNTIME_POLICY_FILE not in files:
        raise ValueError("Manifest files must include runtime_policy.json.")
    runtime_policy_entry = files[RUNTIME_POLICY_FILE]
    if not isinstance(runtime_policy_entry, dict):
        raise ValueError("Manifest checksums.files.runtime_policy.json must be an object.")
    runtime_policy_checksum = runtime_policy_entry.get("sha256")
    if not isinstance(runtime_policy_checksum, str) or not runtime_policy_checksum.strip():
        raise ValueError("Manifest checksums.files.runtime_policy.json missing sha256.")

    verified: dict[str, str] = {}
    downloaded = []
    for rel, entry in files.items():
        if not isinstance(entry, dict):
            raise ValueError(f"Manifest checksums.files[{rel!r}] must be an object.")
        expected_sha = entry.get("sha256")
        expected_size = entry.get("size")
        if not isinstance(expected_sha, str) or not expected_sha.strip():
            raise ValueError(f"Manifest checksums.files[{rel!r}] missing sha256.")
        if not isinstance(expected_size, int):
            raise ValueError(f"Manifest checksums.files[{rel!r}] missing integer size.")

        url = f"{dataset_url}/{rel}"
        out = target_dir / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(url, timeout=timeout_sec) as r:
            out.write_bytes(r.read())

        actual_sha = _sha256_file(out)
        if actual_sha != expected_sha:
            raise ValueError(f"Checksum mismatch for {rel}: expected={expected_sha} actual={actual_sha}")
        if out.stat().st_size != expected_size:
            raise ValueError(f"Size mismatch for {rel}: expected={expected_size} actual={out.stat().st_size}")

        verified[rel] = actual_sha
        downloaded.append(url)

    expected_bundle = manifest.get("manifest_bundle_checksum") or manifest.get("bundle_checksum")
    if expected_bundle:
        actual_bundle = _bundle_checksum_from_files(verified)
        if actual_bundle != expected_bundle:
            raise ValueError(
                f"Bundle checksum mismatch: expected={expected_bundle} actual={actual_bundle}"
            )

    runtime_policy_path = target_dir / RUNTIME_POLICY_FILE
    if not runtime_policy_path.exists():
        raise ValueError("runtime_policy.json missing after bootstrap download.")
    actual_policy_sha = _sha256_file(runtime_policy_path)
    if actual_policy_sha != runtime_policy_checksum:
        raise ValueError(
            "runtime_policy checksum mismatch: "
            f"expected={runtime_policy_checksum} actual={actual_policy_sha}"
        )
    policy_raw = json.loads(runtime_policy_path.read_text(encoding="utf-8"))
    layers = policy_raw.get("layers")
    if not isinstance(layers, dict):
        raise ValueError("runtime_policy.json missing layers object.")
    hierarchy_required = layers.get("hierarchy_required")
    repair_required = layers.get("repair_required")
    if not isinstance(hierarchy_required, bool):
        raise ValueError("runtime_policy.json layers.hierarchy_required must be boolean.")
    if not isinstance(repair_required, bool):
        raise ValueError("runtime_policy.json layers.repair_required must be boolean.")
    if hierarchy_required and "hierarchy.json" not in files:
        raise ValueError("runtime_policy requires hierarchy.json but manifest files missing it.")
    if repair_required and "repair.json" not in files:
        raise ValueError("runtime_policy requires repair.json but manifest files missing it.")

    # Overlay contract alignment: policy-declared optional layer files are mandatory
    # dataset contract files and must exist in both manifest and downloaded cache.
    loaded_policy = load_runtime_policy(target_dir)
    for overlay in loaded_policy.optional_layers:
        if overlay.file not in files:
            raise ValueError(
                f"runtime_policy optional overlay requires {overlay.file}, but manifest files missing it."
            )
        if not (target_dir / overlay.file).exists():
            raise ValueError(
                f"runtime_policy optional overlay file missing after bootstrap download: {overlay.file}"
            )

    local_manifest = target_dir / MANIFEST_NAME
    local_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "country": iso2,
        "dataset_url": dataset_url,
        "manifest_url": manifest_url,
        "cache_dir": str(target_dir),
        "min_cadis_version": min_cadis_version,
        "max_cadis_version_exclusive": max_cadis_version_exclusive,
        "downloaded_urls": downloaded,
        "manifest": manifest,
    }
