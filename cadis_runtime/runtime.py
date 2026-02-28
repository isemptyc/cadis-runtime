from __future__ import annotations

from pathlib import Path
from typing import cast

from cadis_runtime.bootstrap import DEFAULT_DATASET_MANIFEST_URL, bootstrap_country_dataset
from cadis_runtime.execution.pipeline import CadisLookupPipeline
from cadis_runtime.types import LookupResponse


class CadisRuntime:
    """Stable public runtime entrypoint for country-level lookup execution."""

    def __init__(self, *, dataset_dir: str | Path, country_name: str | None = None):
        self._pipeline = CadisLookupPipeline(dataset_dir=dataset_dir, country_name=country_name)

    @classmethod
    def from_iso2(
        cls,
        country_iso2: str,
        *,
        cache_dir: str | Path = "/opt/cadis/cache",
        dataset_manifest_url: str = DEFAULT_DATASET_MANIFEST_URL,
        timeout_sec: int = 30,
        update_to_latest: bool = False,
        dataset_version: str | None = None,
        country_name: str | None = None,
    ) -> "CadisRuntime":
        state = bootstrap_country_dataset(
            country_iso2=country_iso2,
            dataset_manifest_url=dataset_manifest_url,
            cache_dir=cache_dir,
            timeout_sec=timeout_sec,
            update_to_latest=update_to_latest,
            dataset_version=dataset_version,
        )
        return cls(dataset_dir=state["dataset_dir"], country_name=country_name)

    def lookup(self, lat: float, lon: float) -> LookupResponse:
        return cast(LookupResponse, self._pipeline.lookup(lat, lon))
