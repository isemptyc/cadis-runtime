from __future__ import annotations

import hashlib
import json
import tarfile
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
import urllib.request

from cadis_runtime.bootstrap import _validate_manifest_runtime_compatibility
from cadis_runtime.dataset.loader import load_runtime_policy

DEFAULT_DATASET_MANIFEST_URL = (
    "https://raw.githubusercontent.com/isemptyc/cadis-dataset/main/releases/dataset_manifest.json"
)


def _repo_relative_url(base_url: str, relative_path: str) -> str:
    rel_raw = relative_path.strip()
    if rel_raw.startswith(("http://", "https://", "file://")):
        return rel_raw
    rel = rel_raw.lstrip("/")
    if rel.startswith("releases/"):
        parsed = urlparse(base_url)
        marker = "/releases/"
        if marker in parsed.path:
            prefix = parsed.path.split(marker, 1)[0].rstrip("/") + "/"
            return urlunparse(parsed._replace(path=prefix + rel))
    return urljoin(base_url, rel)


def _read_json_url(url: str, *, timeout_sec: int) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def _read_text_url(url: str, *, timeout_sec: int) -> str:
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
        return response.read().decode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_sha256_file(raw: str) -> str:
    token = raw.strip().split()[0] if raw.strip() else ""
    if len(token) != 64 or any(c not in "0123456789abcdefABCDEF" for c in token):
        raise ValueError("Invalid sha256 file content for dataset package.")
    return token.lower()


def _safe_extract_tar_gz(archive_path: Path, target_dir: Path) -> None:
    target_resolved = target_dir.resolve()
    with tarfile.open(archive_path, mode="r:gz") as tar:
        for member in tar.getmembers():
            member_path = (target_dir / member.name).resolve()
            if not str(member_path).startswith(str(target_resolved)):
                raise ValueError(f"Unsafe tar entry path: {member.name!r}")
        tar.extractall(path=target_dir)


def _required_files_present(dataset_dir: Path) -> list[str]:
    required = [
        "dataset_release_manifest.json",
        "geometry.ffsf",
        "geometry_meta.json",
        "runtime_policy.json",
    ]
    return [name for name in required if not (dataset_dir / name).exists()]


def _parse_version_for_sort(raw: str) -> tuple[int, ...]:
    value = raw.strip()
    if value.startswith("v"):
        value = value[1:]
    parts = value.split(".")
    if not parts or any(not p.isdigit() for p in parts):
        return tuple()
    return tuple(int(p) for p in parts)


def _validate_cached_dataset_dir(dataset_dir: Path) -> bool:
    if _required_files_present(dataset_dir):
        return False
    load_runtime_policy(dataset_dir)
    return True


def _find_local_cached_dataset(*, iso2: str, cache_root: Path, dataset_id: str) -> dict[str, Any] | None:
    versions_root = cache_root / iso2 / dataset_id
    if not versions_root.exists():
        return None

    candidates: list[tuple[tuple[int, ...], str, Path]] = []
    for entry in versions_root.iterdir():
        if not entry.is_dir():
            continue
        parsed = _parse_version_for_sort(entry.name)
        if parsed:
            candidates.append((parsed, entry.name, entry))
    candidates.sort(reverse=True)

    for _, version, path in candidates:
        if _validate_cached_dataset_dir(path):
            return {
                "country_iso2": iso2,
                "dataset_id": dataset_id,
                "dataset_version": version,
                "dataset_dir": str(path),
                "used_cached_dataset": True,
            }
    return None


def _resolve_latest_release(
    *,
    country_iso2: str,
    dataset_manifest_url: str,
    timeout_sec: int,
) -> dict[str, Any]:
    root_manifest = _read_json_url(dataset_manifest_url, timeout_sec=timeout_sec)
    countries = root_manifest.get("countries")
    if not isinstance(countries, dict):
        raise ValueError("dataset_manifest.json missing countries object.")

    iso2 = country_iso2.strip().upper()
    country_block = countries.get(iso2)
    if not isinstance(country_block, dict):
        raise ValueError(f"dataset_manifest.json does not include country {iso2}.")

    dataset_id = f"{iso2.lower()}.admin"
    release_entry = country_block.get(dataset_id)
    if not isinstance(release_entry, dict):
        raise ValueError(f"dataset_manifest.json missing dataset entry {dataset_id} for {iso2}.")

    latest = release_entry.get("latest")
    manifest_rel = release_entry.get("manifest")
    if not isinstance(latest, str) or not latest.strip():
        raise ValueError("dataset_manifest latest is missing/invalid.")
    if not isinstance(manifest_rel, str) or not manifest_rel.strip():
        raise ValueError("dataset_manifest manifest path is missing/invalid.")

    release_manifest_url = _repo_relative_url(dataset_manifest_url, manifest_rel)
    release_manifest = _read_json_url(release_manifest_url, timeout_sec=timeout_sec)
    manifest_country = str(release_manifest.get("country_iso", "")).strip().upper()
    if manifest_country != iso2:
        raise ValueError(
            f"Release manifest country mismatch: expected={iso2} actual={manifest_country!r}."
        )

    release_dataset_id = release_manifest.get("dataset_id")
    release_version = release_manifest.get("dataset_version")
    if not isinstance(release_dataset_id, str) or not release_dataset_id.strip():
        raise ValueError("Release manifest missing dataset_id.")
    if not isinstance(release_version, str) or not release_version.strip():
        raise ValueError("Release manifest missing dataset_version.")
    if release_version.strip() != latest.strip():
        raise ValueError(f"Release version mismatch: latest={latest!r} manifest={release_version!r}.")

    _validate_manifest_runtime_compatibility(release_manifest)
    base_release_url = release_manifest_url.rsplit("/", 1)[0] + "/"
    package_url = urljoin(base_release_url, "dataset_package.tar.gz")
    package_sha_url = urljoin(base_release_url, "dataset_package.tar.gz.sha256")

    return {
        "country_iso2": iso2,
        "dataset_manifest_url": dataset_manifest_url,
        "release_manifest_url": release_manifest_url,
        "dataset_id": release_dataset_id.strip(),
        "dataset_version": release_version.strip(),
        "package_url": package_url,
        "package_sha_url": package_sha_url,
    }


def _resolve_pinned_release(
    *,
    country_iso2: str,
    dataset_manifest_url: str,
    dataset_version: str,
    timeout_sec: int,
) -> dict[str, Any]:
    iso2 = country_iso2.strip().upper()
    version = dataset_version.strip()
    if not version:
        raise ValueError("CADIS_DATASET_VERSION must be non-empty when set.")

    dataset_id = f"{iso2.lower()}.admin"
    release_manifest_rel = f"releases/{iso2}/{dataset_id}/{version}/dataset_release_manifest.json"
    release_manifest_url = _repo_relative_url(dataset_manifest_url, release_manifest_rel)
    release_manifest = _read_json_url(release_manifest_url, timeout_sec=timeout_sec)

    manifest_country = str(release_manifest.get("country_iso", "")).strip().upper()
    if manifest_country != iso2:
        raise ValueError(
            f"Release manifest country mismatch: expected={iso2} actual={manifest_country!r}."
        )
    release_dataset_id = release_manifest.get("dataset_id")
    release_version = release_manifest.get("dataset_version")
    if not isinstance(release_dataset_id, str) or not release_dataset_id.strip():
        raise ValueError("Release manifest missing dataset_id.")
    if not isinstance(release_version, str) or not release_version.strip():
        raise ValueError("Release manifest missing dataset_version.")
    if release_version.strip() != version:
        raise ValueError(
            f"Pinned release mismatch: requested={version!r} manifest={release_version!r}."
        )

    _validate_manifest_runtime_compatibility(release_manifest)
    base_release_url = release_manifest_url.rsplit("/", 1)[0] + "/"
    package_url = urljoin(base_release_url, "dataset_package.tar.gz")
    package_sha_url = urljoin(base_release_url, "dataset_package.tar.gz.sha256")

    return {
        "country_iso2": iso2,
        "dataset_manifest_url": dataset_manifest_url,
        "release_manifest_url": release_manifest_url,
        "dataset_id": release_dataset_id.strip(),
        "dataset_version": release_version.strip(),
        "package_url": package_url,
        "package_sha_url": package_sha_url,
    }


def _download_and_extract_release(
    *,
    cache_root: Path,
    release: dict[str, Any],
    timeout_sec: int,
) -> dict[str, Any]:
    target_dir = (
        cache_root
        / release["country_iso2"]
        / release["dataset_id"]
        / release["dataset_version"]
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="cadis_pkg_") as tmp:
        tmp_path = Path(tmp)
        archive_path = tmp_path / "dataset_package.tar.gz"
        with urllib.request.urlopen(release["package_url"], timeout=timeout_sec) as response:
            archive_path.write_bytes(response.read())

        expected_sha = _parse_sha256_file(
            _read_text_url(release["package_sha_url"], timeout_sec=timeout_sec)
        )
        actual_sha = _sha256_file(archive_path)
        if actual_sha != expected_sha:
            raise ValueError(f"Package checksum mismatch: expected={expected_sha} actual={actual_sha}")

        _safe_extract_tar_gz(archive_path, target_dir)

    missing = _required_files_present(target_dir)
    if missing:
        raise ValueError(f"Extracted package missing required files: {missing}")
    load_runtime_policy(target_dir)

    return {
        **release,
        "dataset_dir": str(target_dir),
        "used_cached_dataset": False,
        "update_checked": True,
    }


def bootstrap_country_dataset(
    *,
    country_iso2: str,
    dataset_manifest_url: str = DEFAULT_DATASET_MANIFEST_URL,
    cache_dir: str | Path = "/opt/cadis/cache",
    timeout_sec: int = 30,
    update_to_latest: bool = False,
    dataset_version: str | None = None,
) -> dict[str, Any]:
    iso2 = country_iso2.strip().upper()
    if len(iso2) != 2:
        raise ValueError("country_iso2 must be a 2-letter ISO2 code.")

    cache_root = Path(cache_dir).expanduser()
    dataset_id = f"{iso2.lower()}.admin"
    pinned_version = dataset_version.strip() if isinstance(dataset_version, str) else ""

    if pinned_version:
        pinned_dir = cache_root / iso2 / dataset_id / pinned_version
        if pinned_dir.exists() and _validate_cached_dataset_dir(pinned_dir):
            return {
                "country_iso2": iso2,
                "dataset_id": dataset_id,
                "dataset_version": pinned_version,
                "dataset_dir": str(pinned_dir),
                "used_cached_dataset": True,
                "dataset_manifest_url": dataset_manifest_url,
                "update_checked": False,
                "version_pinned": True,
            }

        release = _resolve_pinned_release(
            country_iso2=iso2,
            dataset_manifest_url=dataset_manifest_url,
            dataset_version=pinned_version,
            timeout_sec=timeout_sec,
        )
        downloaded = _download_and_extract_release(
            cache_root=cache_root,
            release=release,
            timeout_sec=timeout_sec,
        )
        downloaded["version_pinned"] = True
        return downloaded

    cached = _find_local_cached_dataset(iso2=iso2, cache_root=cache_root, dataset_id=dataset_id)

    if cached and not update_to_latest:
        return {
            **cached,
            "dataset_manifest_url": dataset_manifest_url,
            "update_checked": False,
        }

    release = _resolve_latest_release(
        country_iso2=iso2,
        dataset_manifest_url=dataset_manifest_url,
        timeout_sec=timeout_sec,
    )
    latest_target = cache_root / iso2 / release["dataset_id"] / release["dataset_version"]
    if latest_target.exists() and _validate_cached_dataset_dir(latest_target):
        return {
            **release,
            "dataset_dir": str(latest_target),
            "used_cached_dataset": True,
            "update_checked": True,
        }

    return _download_and_extract_release(cache_root=cache_root, release=release, timeout_sec=timeout_sec)


def write_bootstrap_state(path: str | Path, state: dict[str, Any]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def read_bootstrap_state(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
