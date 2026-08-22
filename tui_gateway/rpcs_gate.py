"""RPCS pre-dispatch contract for Hermes' shared interactive gateway."""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class RPCSGateError(RuntimeError):
    pass


@dataclass(frozen=True)
class RPCSGateConfig:
    enabled: bool
    control_url: str
    actor_id: str
    project_id: str = "default"
    timeout_seconds: float = 5.0


def load_gate_config(config: dict[str, Any]) -> RPCSGateConfig:
    raw = config.get("rpcs_control") if isinstance(config, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    enabled = bool(raw.get("enabled", False))
    control_url = str(raw.get("url") or "").strip().rstrip("/")
    actor_id = str(raw.get("actor_id") or "").strip()
    project_id = str(raw.get("project_id") or "default").strip() or "default"
    try:
        timeout = float(raw.get("timeout_seconds", 5.0))
    except (TypeError, ValueError):
        timeout = 5.0
    if enabled and (not control_url or not actor_id):
        raise RPCSGateError("rpcs_control requires url and actor_id when enabled")
    return RPCSGateConfig(enabled, control_url, actor_id, project_id, max(0.1, timeout))


def _post(cfg: RPCSGateConfig, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    key = os.environ.get("RPCS_HERMES_INGRESS_KEY", "").strip()
    if not key:
        raise RPCSGateError("RPCS_HERMES_INGRESS_KEY is not configured")
    request = urllib.request.Request(
        f"{cfg.control_url}{path}",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "X-RPCS-Actor-ID": cfg.actor_id,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=cfg.timeout_seconds) as response:
            body = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise RPCSGateError(f"RPCS control rejected dispatch ({exc.code}): {detail}") from exc
    except (OSError, ValueError) as exc:
        raise RPCSGateError(f"RPCS control unavailable: {exc}") from exc
    if not isinstance(body, dict):
        raise RPCSGateError("RPCS control returned an invalid response")
    return body


def plan_dispatch(
    cfg: RPCSGateConfig,
    *,
    surface: str,
    surface_session_id: str,
    turn_id: str,
    text: str,
    current_session_id: str | None = None,
) -> dict[str, Any] | None:
    if not cfg.enabled:
        return None
    payload: dict[str, Any] = {
        "surface": surface,
        "surface_session_id": surface_session_id,
        "turn_id": turn_id,
        "text": text,
        "project_id": cfg.project_id,
    }
    if current_session_id:
        payload["current_session_id"] = current_session_id
    result = _post(cfg, "/internal/hermes/dispatches", payload)
    if result.get("state") != "planned" or not result.get("dispatch_id"):
        raise RPCSGateError(f"RPCS dispatch blocked: {result.get('state', 'unknown')}")
    return result


def resolve_dispatch(
    cfg: RPCSGateConfig, dispatch_id: str, route: dict[str, Any]
) -> dict[str, Any] | None:
    if not cfg.enabled:
        return None
    result = _post(cfg, f"/internal/hermes/dispatches/{dispatch_id}/resolve", route)
    if result.get("state") != "resolved":
        raise RPCSGateError(f"RPCS route was not resolved: {result.get('state', 'unknown')}")
    return result


def credential_fingerprint(value: str | None) -> str | None:
    """Return a non-reversible display fingerprint, never the credential."""
    if not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def route_markdown(dispatch: dict[str, Any]) -> str:
    route = dispatch.get("resolved_route") or dispatch.get("route") or {}
    planned = dispatch.get("planned_profile") or dispatch.get("profile") or "—"
    reasons = ", ".join(dispatch.get("reason_codes") or []) or "—"
    rows = [
        ("Policy profile", planned),
        ("Runtime", route.get("runtime") or "—"),
        ("Provider", route.get("provider") or "—"),
        ("Model", route.get("model") or "—"),
        ("Reasoning", route.get("reasoning_effort") or "default"),
        ("Credential", route.get("credential_alias") or route.get("credential_fingerprint") or "not exposed"),
        ("Reason", reasons),
        ("Attempt", str(dispatch.get("attempt") or 1)),
    ]
    body = "\n".join(f"| {key} | {value} |" for key, value in rows)
    return f"**RPCS route resolved**\n\n| Field | Value |\n|---|---|\n{body}"
