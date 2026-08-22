import json

from tools import rpcs_client_tool


def test_rpcs_client_tool_waits_for_cli_and_returns_asset(monkeypatch):
    monkeypatch.setenv("RPCS_CONTROL_URL", "http://control")
    monkeypatch.setenv("RPCS_HERMES_INGRESS_KEY", "secret")
    monkeypatch.setenv("RPCS_ACTOR_ID", "actor")
    monkeypatch.setenv("RPCS_PROJECT_ID", "project")
    replies = iter([
        {"action_id": "action_1", "state": "pending", "project_id": "project"},
        {"action_id": "action_1", "state": "claimed", "project_id": "project"},
        {"action_id": "action_1", "state": "completed", "project_id": "project", "result": {
            "asset_id": "host_1", "alias": "prod", "host": "10.0.0.8", "port": 22,
        }},
    ])
    monkeypatch.setattr(rpcs_client_tool, "_request", lambda *args, **kwargs: next(replies))
    monkeypatch.setattr(rpcs_client_tool.time, "sleep", lambda _seconds: None)
    result = json.loads(rpcs_client_tool.request_ssh_host_onboarding("prod", "10.0.0.8"))
    assert result["asset_id"] == "host_1"
    assert result["success"] is True


def test_rpcs_client_tool_is_hidden_without_bridge_configuration(monkeypatch):
    for name in ("RPCS_CONTROL_URL", "RPCS_HERMES_INGRESS_KEY", "RPCS_ACTOR_ID", "RPCS_PROJECT_ID"):
        monkeypatch.delenv(name, raising=False)
    assert rpcs_client_tool.check_rpcs_client_requirements() is False
