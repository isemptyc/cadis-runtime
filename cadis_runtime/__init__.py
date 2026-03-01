"""Cadis runtime public API."""

from __future__ import annotations

from warnings import warn

from cadis_runtime.runtime import CadisRuntime
from cadis_runtime.types import AdminHierarchyNode, CountryInfo, LookupResponse, LookupResult, LookupStatus
from cadis_runtime.version import __version__

__all__ = [
    "__version__",
    "bootstrap_dataset",
    "bootstrap_country_dataset",
    "CadisRuntime",
    "LookupStatus",
    "CountryInfo",
    "AdminHierarchyNode",
    "LookupResult",
    "LookupResponse",
]

_DEPRECATED_IMPORTS = {"CadisLookupPipeline", "RuntimeLookupPipeline"}


def bootstrap_dataset(*args, **kwargs):
    from cadis_runtime.bootstrap import bootstrap_dataset as _bootstrap_dataset

    return _bootstrap_dataset(*args, **kwargs)


def bootstrap_country_dataset(*args, **kwargs):
    from cadis_runtime.bootstrap import bootstrap_country_dataset as _bootstrap_country_dataset

    return _bootstrap_country_dataset(*args, **kwargs)


def __getattr__(name: str):
    if name in _DEPRECATED_IMPORTS:
        from cadis_runtime.execution.pipeline import CadisLookupPipeline, RuntimeLookupPipeline

        resolved = {
            "CadisLookupPipeline": CadisLookupPipeline,
            "RuntimeLookupPipeline": RuntimeLookupPipeline,
        }
        warn(
            f"`cadis_runtime.{name}` is deprecated and will be removed in a future release. "
            "Use `cadis_runtime.CadisRuntime` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        globals()[name] = resolved[name]
        return resolved[name]
    raise AttributeError(f"module 'cadis_runtime' has no attribute {name!r}")
