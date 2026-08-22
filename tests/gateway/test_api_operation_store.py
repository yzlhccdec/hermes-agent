import json

import pytest

from gateway.api_operation_store import OperationConflict, OperationStore
from hermes_cli import kanban_db as kb


def _prepare(store, operation_id="op_1", request_hash="hash_1", object_id="run_1"):
    return store.prepare(
        operation_id=operation_id,
        actor_id="actor_1",
        endpoint_kind="runs.create",
        request_hash=request_hash,
        profile="default",
        spec_hash="spec_1",
        object_id=object_id,
        initial_status={"run_id": object_id, "status": "queued"},
    )


def test_prepare_replay_claim_and_restart(tmp_path):
    path = tmp_path / "operations.db"
    first_store = OperationStore(path)
    first = _prepare(first_store)
    assert first.replayed is False
    first_store.close()

    reopened = OperationStore(path)
    replay = _prepare(reopened, object_id="run_discarded")
    assert replay.replayed is True
    assert replay.object_id == "run_1"
    assert reopened.claim_dispatch("op_1") is True
    assert reopened.claim_dispatch("op_1") is False
    reopened.update_status("run_1", {"run_id": "run_1", "status": "completed"})
    assert reopened.get_status("run_1")["status"] == "completed"
    reopened.close()


def test_changed_binding_conflicts(tmp_path):
    store = OperationStore(tmp_path / "operations.db")
    _prepare(store)
    try:
        _prepare(store, request_hash="different")
        assert False, "expected immutable binding conflict"
    except OperationConflict:
        pass
    finally:
        store.close()


def test_worker_mutation_is_bound_atomic_and_replayed(tmp_path):
    path = tmp_path / "kanban.db"
    kb.init_db(path)
    conn = kb.connect(path)
    store = OperationStore(path, connection=conn)
    complete_id = kb.create_task(
        conn, title="worker", body=json.dumps({"spec_hash": "spec_1"}),
        assignee="codex-standard", created_by="rpcs-control", tenant="actor_1",
        initial_status="running",
    )
    payload = {"attempt_id": "attempt_1", "attempt": 1, "spec_hash": "spec_1",
               "capsule_sha256": "a" * 64, "summary": "done"}
    kwargs = dict(operation_id="worker-complete:attempt_1", actor_id="actor_1",
                  request_hash="request_1", profile="codex-standard", spec_hash="spec_1",
                  task_id=complete_id, action="complete", payload=payload)
    prepared, status = store.mutate_worker_task(**kwargs)
    assert prepared.replayed is False
    assert status["status"] == "completed"
    assert kb.get_task(conn, complete_id).status == "done"
    replay, _ = store.mutate_worker_task(**kwargs)
    assert replay.replayed is True

    foreign_id = kb.create_task(
        conn, title="foreign", body=json.dumps({"spec_hash": "spec_1"}),
        assignee="codex-standard", created_by="rpcs-control", tenant="actor_2",
        initial_status="running",
    )
    with pytest.raises(ValueError, match="worker task not found"):
        store.mutate_worker_task(**{**kwargs, "operation_id": "foreign",
                                  "task_id": foreign_id, "action": "block",
                                  "payload": {"spec_hash": "spec_1", "reason": "wait",
                                              "kind": "needs_input"}})
    store.close()
