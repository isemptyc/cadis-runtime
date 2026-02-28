from __future__ import annotations

import json
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
import urllib.request


def repo_relative_url(base_url: str, relative_path: str) -> str:
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


def read_json_url(url: str, *, timeout_sec: int) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def read_text_url(url: str, *, timeout_sec: int) -> str:
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
        return response.read().decode("utf-8")


def read_bytes_url(url: str, *, timeout_sec: int) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
        return response.read()
