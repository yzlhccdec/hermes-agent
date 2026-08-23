import json
import urllib.error

import pytest

from tui_gateway.rpcs_gate import (
    RPCSGateError,
    interactive_route,
    load_gate_config,
    plan_dispatch,
    resolve_dispatch,
    route_markdown,
)


class _Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return json.dumps(self.body).encode()


def test_disabled_gate_is_noop():
    cfg = load_gate_config({})
    assert cfg.enabled is False
    assert plan_dispatch(
        cfg, surface="hermes-native", surface_session_id="s1", turn_id="1", text="hi"
    ) is None


def test_enabled_gate_requires_behavioral_config():
    with pytest.raises(RPCSGateError, match="requires url and actor_id"):
        load_gate_config({"rpcs_control": {"enabled": True}})


def test_plan_and_resolve_send_authenticated_contract(monkeypatch):
    monkeypatch.setenv("RPCS_HERMES_INGRESS_KEY", "secret")
    calls = []

    def open_request(request, timeout):
        calls.append((request, timeout, json.loads(request.data)))
        if request.full_url.endswith("/resolve"):
            return _Response({
                "dispatch_id": "dispatch_1", "state": "resolved", "profile": "codex-fast",
                "route": {"runtime": "codex-app-server", "provider": "openai", "model": "gpt-5.4",
                          "reasoning_effort": "low", "credential_alias": "openai-default"},
                "reason_codes": ["mode:fast"], "attempt": 1,
            })
        return _Response({
            "dispatch_id": "dispatch_1", "state": "planned", "profile": "codex-fast",
            "session_id": "sess_1", "reason_codes": ["mode:fast"], "attempt": 1,
        })

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    cfg = load_gate_config({"rpcs_control": {
        "enabled": True, "url": "http://control:8787/", "actor_id": "user-1",
    }})
    planned = plan_dispatch(
        cfg, surface="hermes-native", surface_session_id="ui-1", turn_id="rpc-1", text="build it"
    )
    resolved = resolve_dispatch(cfg, planned["dispatch_id"], {
        "profile": "codex-fast", "runtime": "codex-app-server", "provider": "openai",
        "model": "gpt-5.4", "reasoning_effort": "low", "credential_alias": "openai-default",
    })

    assert calls[0][0].headers["Authorization"] == "Bearer secret"
    assert calls[0][0].headers["X-rpcs-actor-id"] == "user-1"
    assert calls[0][2]["surface_session_id"] == "ui-1"
    assert calls[1][0].full_url.endswith("/internal/hermes/dispatches/dispatch_1/resolve")
    rendered = route_markdown(resolved)
    assert "gpt-5.4" in rendered
    assert "openai-default" in rendered
    assert "mode:fast" in rendered


def test_enabled_gate_fails_closed_without_secret(monkeypatch):
    monkeypatch.delenv("RPCS_HERMES_INGRESS_KEY", raising=False)
    cfg = load_gate_config({"rpcs_control": {
        "enabled": True, "url": "http://control:8787", "actor_id": "user-1",
    }})
    with pytest.raises(RPCSGateError, match="INGRESS_KEY"):
        plan_dispatch(
            cfg, surface="hermes-native", surface_session_id="ui-1", turn_id="rpc-1", text="hi"
        )


def test_interactive_route_requires_truthful_hermes_runtime():
    route = interactive_route({"route": {
        "runtime": "hermes-loop", "provider": "openai-codex", "model": "gpt-5.4",
        "reasoning_effort": "high",
    }})
    assert route["provider"] == "openai-codex"
    with pytest.raises(RPCSGateError, match="requires hermes-loop"):
        interactive_route({"route": {
            "runtime": "codex-app-server", "provider": "openai-codex", "model": "gpt-5.4",
            "reasoning_effort": "high",
        }})
