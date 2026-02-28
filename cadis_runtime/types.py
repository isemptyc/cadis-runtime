from __future__ import annotations

from typing import Any, Literal, TypedDict

LookupStatus = Literal["ok", "partial", "failed"]


class CountryInfo(TypedDict):
    level: int
    name: str


class AdminHierarchyNode(TypedDict):
    rank: int
    osm_id: str | None
    level: int
    name: str
    source: str


class LookupResult(TypedDict, total=False):
    country: CountryInfo
    admin_hierarchy: list[AdminHierarchyNode]
    source: str
    context_anchor: dict[str, Any]


class LookupResponse(TypedDict):
    lookup_status: LookupStatus
    engine: str
    version: str
    result: LookupResult
