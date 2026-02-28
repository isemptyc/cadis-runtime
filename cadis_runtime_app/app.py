from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, jsonify, request

from cadis_runtime import CadisRuntime
from cadis_runtime_app.bootstrap_adapter import read_bootstrap_state
from cadis_runtime.errors import DatasetNotBootstrappedError, RuntimePolicyInvalidError

BOOTSTRAP_STATE_PATH = os.getenv("CADIS_BOOTSTRAP_STATE_PATH", "/tmp/cadis_bootstrap_state.json")


def create_app() -> Flask:
    app = Flask(__name__)
    app.json.ensure_ascii = False

    state = read_bootstrap_state(BOOTSTRAP_STATE_PATH)
    dataset_dir = state["dataset_dir"]
    country_iso2 = state["country_iso2"]
    runtime = CadisRuntime(dataset_dir=Path(dataset_dir))

    @app.get("/health")
    def health() -> tuple[dict, int]:
        return (
            {
                "status": "ok",
                "country_iso2": country_iso2,
                "dataset_id": state.get("dataset_id"),
                "dataset_version": state.get("dataset_version"),
                "dataset_dir": dataset_dir,
            },
            200,
        )

    @app.post("/lookup")
    def lookup() -> tuple[dict, int]:
        payload = request.get_json(silent=True) or {}
        if "lat" not in payload or "lon" not in payload:
            return (
                {
                    "error_code": "INVALID_REQUEST",
                    "error_message": "Request must include numeric lat and lon.",
                },
                400,
            )
        try:
            lat = float(payload["lat"])
            lon = float(payload["lon"])
        except (TypeError, ValueError):
            return (
                {
                    "error_code": "INVALID_REQUEST",
                    "error_message": "lat and lon must be numeric.",
                },
                400,
            )

        try:
            response = runtime.lookup(lat, lon)
            result = response.get("result")
            country_name = ""
            if isinstance(result, dict):
                country = result.get("country")
                if isinstance(country, dict):
                    name = country.get("name")
                    if isinstance(name, str):
                        country_name = name
                result.pop("country", None)
            nodes = response.get("result", {}).get("admin_hierarchy", [])
            names = []
            for node in nodes:
                name = node.get("name")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
            response["summary_text"] = ", ".join(names)
            response["iso_context"] = {"iso2": country_iso2, "name": country_name}
            return jsonify(response), 200
        except DatasetNotBootstrappedError as exc:
            return (
                {
                    "error_code": "DATASET_NOT_BOOTSTRAPPED",
                    "error_message": "Dataset cache is missing required files.",
                    "details": {
                        "dataset_dir": exc.dataset_dir,
                        "missing_files": exc.missing_files,
                    },
                },
                500,
            )
        except RuntimePolicyInvalidError as exc:
            return (
                {
                    "error_code": "RUNTIME_POLICY_MISSING_OR_INVALID",
                    "error_message": "Runtime policy file is missing or invalid.",
                    "details": {"dataset_dir": exc.dataset_dir, "reason": exc.reason},
                },
                500,
            )
        except Exception as exc:  # pragma: no cover - safety boundary
            return (
                {
                    "error_code": "LOOKUP_RUNTIME_ERROR",
                    "error_message": "Runtime lookup failed.",
                    "details": {"exception": str(exc)},
                },
                500,
            )

    return app


app = create_app()
