import json

from tools import rpcs_client_tool
from tools.tool_search import is_deferrable_tool_name


def test_rpcs_client_tool_waits_for_cli_and_returns_asset(monkeypatch):
    monkeypatch.setenv("RPCS_CONTROL_URL", "http://control")
    monkeypatch.setenv("RPCS_HERMES_INGRESS_KEY", "secret")
    monkeypatch.setenv("RPCS_ACTOR_ID", "actor")
    replies = iter([
        {"action_id": "action_1", "state": "pending", "project_id": "project"},
        {"action_id": "action_1", "state": "claimed", "project_id": "project"},
        {"action_id": "action_1", "state": "completed", "project_id": "project", "result": {
            "asset_id": "host_1", "alias": "prod", "host": "10.0.0.8", "port": 22,
        }},
    ])
    calls = []

    def fake_request(method, path, payload=None):
        calls.append((method, path, payload))
        return next(replies)

    monkeypatch.setattr(rpcs_client_tool, "_request", fake_request)
    monkeypatch.setattr(rpcs_client_tool.time, "sleep", lambda _seconds: None)
    result = json.loads(rpcs_client_tool.request_ssh_host_onboarding(
        "prod", "10.0.0.8", "project", "cli_1"
    ))
    assert result["asset_id"] == "host_1"
    assert result["success"] is True
    assert calls[0][2]["project_id"] == "project"
    assert calls[0][2]["client_id"] == "cli_1"


def test_rpcs_client_tool_is_hidden_without_bridge_configuration(monkeypatch):
    for name in ("RPCS_CONTROL_URL", "RPCS_HERMES_INGRESS_KEY", "RPCS_ACTOR_ID"):
        monkeypatch.delenv(name, raising=False)
    assert rpcs_client_tool.check_rpcs_client_requirements() is False


def test_rpcs_client_tool_is_core_and_never_deferred():
    assert is_deferrable_tool_name("rpcs_onboard_ssh_host") is False
