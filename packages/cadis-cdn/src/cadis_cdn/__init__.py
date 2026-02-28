"""cadis-cdn: dataset transport, integrity, and extraction primitives."""

from cadis_cdn.archive import safe_extract_tar_gz
from cadis_cdn.bootstrap import (
    bootstrap_release_dataset,
    bootstrap_country_dataset,
    download_and_extract_release,
    find_local_cached_dataset,
    parse_version_for_sort,
    required_files_present,
    resolve_latest_release,
    resolve_pinned_release,
    validate_cached_dataset_dir,
)
from cadis_cdn.hashing import bundle_checksum_from_files, parse_sha256_file, sha256_file
from cadis_cdn.runtime_compat import parse_semver, validate_manifest_runtime_compatibility
from cadis_cdn.transport import read_bytes_url, read_json_url, read_text_url, repo_relative_url
from cadis_cdn.version import __version__

__all__ = [
    "__version__",
    "bootstrap_release_dataset",
    "bootstrap_country_dataset",
    "bundle_checksum_from_files",
    "download_and_extract_release",
    "find_local_cached_dataset",
    "parse_version_for_sort",
    "parse_semver",
    "parse_sha256_file",
    "required_files_present",
    "read_bytes_url",
    "read_json_url",
    "read_text_url",
    "repo_relative_url",
    "resolve_latest_release",
    "resolve_pinned_release",
    "safe_extract_tar_gz",
    "sha256_file",
    "validate_manifest_runtime_compatibility",
    "validate_cached_dataset_dir",
]
