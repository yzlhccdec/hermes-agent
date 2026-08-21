from gateway.api_operation_store import OperationConflict, OperationStore


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
