"""RPCS bridge for operations that must use capabilities on the user's CLI machine."""

import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from tools.registry import registry, tool_error


def check_rpcs_client_requirements() -> bool:
    return all(os.environ.get(name, "").strip() for name in (
        "RPCS_CONTROL_URL", "RPCS_HERMES_INGRESS_KEY", "RPCS_ACTOR_ID"
    ))


def _request(method: str, path: str, payload=None) -> dict:
    base = os.environ["RPCS_CONTROL_URL"].rstrip("/")
    headers = {
        "Authorization": f"Bearer {os.environ['RPCS_HERMES_INGRESS_KEY']}",
        "X-RPCS-Actor-ID": os.environ["RPCS_ACTOR_ID"],
    }
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload, separators=(",", ":")).encode()
    request = Request(f"{base}{path}", data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode())
    except HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode()).get("error", f"http_{exc.code}")
        except (ValueError, AttributeError):
            detail = f"http_{exc.code}"
        raise RuntimeError(f"RPCS Control rejected client action: {detail}") from exc
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"RPCS Control unavailable: {exc}") from exc
    if not isinstance(result, dict):
        raise RuntimeError("RPCS Control returned an invalid client action")
    return result


def request_ssh_host_onboarding(
    alias: str, host: str, project_id: str, client_id: str, port=22, bootstrap_user="root"
) -> str:
    """Wait for the connected RPCS CLI to onboard an existing SSH server."""
    try:
        action = _request("POST", "/internal/client-actions/ssh-host-onboarding", {
            "project_id": project_id,
            "client_id": client_id,
            "alias": alias,
            "host": host,
            "port": int(port),
            "bootstrap_user": bootstrap_user,
        })
        deadline = time.monotonic() + min(
            max(int(os.environ.get("RPCS_CLIENT_ACTION_TIMEOUT_SECONDS", "86400")), 60), 86400
        )
        while action.get("state") in {"pending", "claimed"}:
            if time.monotonic() >= deadline:
                return tool_error("The local RPCS CLI did not complete SSH onboarding before timeout.")
            time.sleep(1)
            action = _request("GET", f"/internal/client-actions/{quote(action['action_id'], safe='')}")
        if action.get("state") != "completed" or not action.get("result", {}).get("asset_id"):
            return tool_error(action.get("error") or "The local RPCS CLI could not onboard the SSH host.")
        return json.dumps({
            "success": True,
            "project_id": action["project_id"],
            "asset_id": action["result"]["asset_id"],
            "alias": action["result"]["alias"],
            "host": action["result"]["host"],
            "port": action["result"]["port"],
            "instruction": "Bind this asset_id in the Spec SSH data_access entry.",
        }, ensure_ascii=False)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return tool_error(str(exc))


RPCS_SSH_ONBOARD_SCHEMA = {
    "name": "rpcs_onboard_ssh_host",
    "description": (
        "Ask the currently connected RPCS CLI to use the user's existing local SSH access to "
        "onboard a server, then wait for its project-scoped asset_id. Use this whenever a user "
        "assigns an existing host for diagnosis, deployment, restart, or other SSH work and no "
        "trusted asset_id is already known. Do not ask the user to run a separate onboarding "
        "command. After success, put the returned asset_id into the Spec's SSH data_access entry."
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "alias": {"type": "string", "description": "Short stable name used in the task Spec."},
            "host": {"type": "string", "description": "Existing server hostname or IP address."},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 22},
            "bootstrap_user": {
                "type": "string",
                "description": "Existing SSH login user available through the local SSH config/agent.",
                "default": "root",
            },
            "client_id": {
                "type": "string",
                "description": (
                    "Opaque client binding supplied by the RPCS system instruction. Copy it exactly; "
                    "never invent or reuse a value from another conversation."
                ),
            },
            "project_id": {
                "type": "string",
                "description": "Project binding supplied by the RPCS system instruction. Copy it exactly.",
            },
        },
        "required": ["alias", "host", "project_id", "client_id"],
    },
}


registry.register(
    name="rpcs_onboard_ssh_host",
    toolset="terminal",
    schema=RPCS_SSH_ONBOARD_SCHEMA,
    handler=lambda args, **kw: request_ssh_host_onboarding(
        alias=args.get("alias", ""),
        host=args.get("host", ""),
        project_id=args.get("project_id", ""),
        client_id=args.get("client_id", ""),
        port=args.get("port", 22),
        bootstrap_user=args.get("bootstrap_user", "root"),
    ),
    check_fn=check_rpcs_client_requirements,
    emoji="🔐",
)
