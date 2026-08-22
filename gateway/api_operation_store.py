"""Durable operation identities for API requests dispatched in the background."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


class OperationConflict(ValueError):
    """The operation id already exists with a different immutable binding."""


@dataclass(frozen=True)
class PreparedOperation:
    operation_id: str
    object_id: str
    state: str
    replayed: bool


class OperationStore:
    """SQLite-backed prepare/claim ledger.

    Preparing the stable upstream object and its immutable binding is one
    transaction.  Claiming dispatch is a second compare-and-set transaction;
    concurrent retries can therefore create neither a second object nor a
    second in-process dispatch.
    """

    def __init__(self, db_path: str | Path, *, connection=None):
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = connection or sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_operations (
                operation_id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                endpoint_kind TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                profile TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                object_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                status_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()

    def prepare_kanban_root(
        self, *, operation_id: str, actor_id: str, request_hash: str,
        profile: str, spec_hash: str, title: str, body: str,
    ) -> PreparedOperation:
        """Atomically create a blocked Kanban root and its operation binding.

        The connection must point at the active Kanban database.  ``create_task``
        uses a nested savepoint, so the task row/event and operation ledger row
        commit or roll back together under this outer transaction.
        """
        from hermes_cli import kanban_db as kb

        endpoint_kind = "kanban.roots.create"
        binding = (actor_id, endpoint_kind, request_hash, profile, spec_hash)
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if row is not None:
                existing = tuple(row[key] for key in (
                    "actor_id", "endpoint_kind", "request_hash", "profile", "spec_hash"
                ))
                if existing != binding:
                    raise OperationConflict(operation_id)
                return PreparedOperation(operation_id, row["object_id"], row["state"], True)
            task_id = kb.create_task(
                self._conn,
                title=title,
                body=body,
                assignee=None,
                created_by="rpcs-control",
                tenant=actor_id,
                idempotency_key=operation_id,
                initial_status="blocked",
            )
            status = {
                "object": "hermes.kanban.root",
                "task_id": task_id,
                "status": "blocked",
                "spec_hash": spec_hash,
            }
            self._conn.execute(
                """INSERT INTO api_operations
                   (operation_id, actor_id, endpoint_kind, request_hash, profile,
                    spec_hash, object_id, state, status_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?)""",
                (*((operation_id,) + binding + (task_id,)),
                 json.dumps(status, sort_keys=True, separators=(",", ":")), now, now),
            )
        return PreparedOperation(operation_id, task_id, "completed", False)

    def prepare_kanban_graph(
        self, *, operation_id: str, actor_id: str, request_hash: str,
        profile: str, spec_hash: str, root_task_id: str, nodes: list[dict[str, Any]],
    ) -> tuple[PreparedOperation, dict[str, Any]]:
        """Atomically compile workers, blocked validation barriers and links."""
        from hermes_cli import kanban_db as kb

        endpoint_kind = "kanban.graphs.create"
        binding = (actor_id, endpoint_kind, request_hash, profile, spec_hash)
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if row is not None:
                existing = tuple(row[key] for key in (
                    "actor_id", "endpoint_kind", "request_hash", "profile", "spec_hash"
                ))
                if existing != binding:
                    raise OperationConflict(operation_id)
                status = json.loads(row["status_json"])
                return PreparedOperation(operation_id, row["object_id"], row["state"], True), status
            if kb.get_task(self._conn, root_task_id) is None:
                raise ValueError("root task not found")
            graph_id = f"graph_{uuid.uuid4().hex}"
            task_ids: dict[str, str] = {}
            barrier_ids: dict[str, str] = {}
            for node in nodes:
                parents = [barrier_ids[key] for key in node.get("depends_on", [])]
                task_id = kb.create_task(
                    self._conn,
                    title=node["title"],
                    body=json.dumps({"objective": node["objective"], "spec_hash": spec_hash},
                                    sort_keys=True, ensure_ascii=False),
                    assignee=node["profile"],
                    created_by="rpcs-control",
                    tenant=actor_id,
                    parents=parents,
                    idempotency_key=f"{operation_id}:worker:{node['key']}",
                    initial_status="running",
                )
                barrier_id = kb.create_task(
                    self._conn,
                    title=f"Validate: {node['title']}",
                    body=json.dumps({"worker_task_id": task_id, "spec_hash": spec_hash},
                                    sort_keys=True, ensure_ascii=False),
                    assignee=None,
                    created_by="rpcs-control",
                    tenant=actor_id,
                    parents=[task_id],
                    idempotency_key=f"{operation_id}:validate:{node['key']}",
                    initial_status="blocked",
                )
                task_ids[node["key"]] = task_id
                barrier_ids[node["key"]] = barrier_id
            depended_on = {dep for node in nodes for dep in node.get("depends_on", [])}
            final_keys = [node["key"] for node in nodes if node["key"] not in depended_on]
            for key in final_keys:
                self._conn.execute(
                    "INSERT OR IGNORE INTO task_links(parent_id,child_id) VALUES(?,?)",
                    (barrier_ids[key], root_task_id),
                )
            status = {
                "object": "hermes.kanban.graph", "graph_id": graph_id,
                "root_task_id": root_task_id, "status": "compiled",
                "spec_hash": spec_hash, "tasks": task_ids, "barriers": barrier_ids,
            }
            self._conn.execute(
                """INSERT INTO api_operations
                   (operation_id, actor_id, endpoint_kind, request_hash, profile,
                    spec_hash, object_id, state, status_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?)""",
                (*((operation_id,) + binding + (graph_id,)),
                 json.dumps(status, sort_keys=True, separators=(",", ":")), now, now),
            )
        return PreparedOperation(operation_id, graph_id, "completed", False), status

    def complete_validation_barrier(
        self, *, operation_id: str, actor_id: str, request_hash: str,
        profile: str, spec_hash: str, barrier_task_id: str, validation: dict[str, Any],
    ) -> tuple[PreparedOperation, dict[str, Any]]:
        from hermes_cli import kanban_db as kb

        endpoint_kind = "kanban.barriers.complete"
        binding = (actor_id, endpoint_kind, request_hash, profile, spec_hash)
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is not None:
                existing = tuple(row[key] for key in (
                    "actor_id", "endpoint_kind", "request_hash", "profile", "spec_hash"
                ))
                if existing != binding:
                    raise OperationConflict(operation_id)
                status = json.loads(row["status_json"])
                return PreparedOperation(operation_id, row["object_id"], row["state"], True), status
            task = kb.get_task(self._conn, barrier_task_id)
            if task is None or task.tenant != actor_id:
                raise ValueError("validation barrier not found")
            try:
                task_body = json.loads(task.body or "{}")
            except json.JSONDecodeError as exc:
                raise ValueError("invalid validation barrier") from exc
            if task_body.get("spec_hash") != spec_hash or not task_body.get("worker_task_id"):
                raise ValueError("validation barrier binding mismatch")
            if not kb.complete_task(
                self._conn, barrier_task_id, result="validated",
                summary="RPCS Contract Guard passed",
                metadata={"validation_record": validation}, fire_lifecycle_hook=False,
            ):
                raise ValueError("validation barrier is not completable")
            validation_object_id = f"validation_{uuid.uuid4().hex}"
            status = {"object": "hermes.kanban.validation", "validation_id": validation_object_id,
                      "barrier_task_id": barrier_task_id, "status": "completed",
                      "spec_hash": spec_hash}
            self._conn.execute(
                """INSERT INTO api_operations
                   (operation_id, actor_id, endpoint_kind, request_hash, profile,
                    spec_hash, object_id, state, status_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?)""",
                (*((operation_id,) + binding + (validation_object_id,)),
                 json.dumps(status, sort_keys=True, separators=(",", ":")), now, now),
            )
        return PreparedOperation(operation_id, validation_object_id, "completed", False), status

    def mutate_worker_task(
        self, *, operation_id: str, actor_id: str, request_hash: str,
        profile: str, spec_hash: str, task_id: str, action: str,
        payload: dict[str, Any],
    ) -> tuple[PreparedOperation, dict[str, Any]]:
        """Atomically complete or block exactly one control-bound worker task."""
        from hermes_cli import kanban_db as kb

        if action not in {"complete", "block"}:
            raise ValueError("unsupported worker action")
        endpoint_kind = f"kanban.tasks.{action}"
        binding = (actor_id, endpoint_kind, request_hash, profile, spec_hash)
        timestamp = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is not None:
                existing = tuple(row[key] for key in (
                    "actor_id", "endpoint_kind", "request_hash", "profile", "spec_hash"
                ))
                if existing != binding or row["object_id"] != task_id:
                    raise OperationConflict(operation_id)
                status = json.loads(row["status_json"])
                return PreparedOperation(operation_id, task_id, row["state"], True), status
            task = kb.get_task(self._conn, task_id)
            if task is None or task.tenant != actor_id or task.assignee != profile:
                raise ValueError("worker task not found")
            try:
                task_body = json.loads(task.body or "{}")
            except json.JSONDecodeError as exc:
                raise ValueError("invalid worker task") from exc
            if task_body.get("spec_hash") != spec_hash or payload.get("spec_hash") != spec_hash:
                raise ValueError("worker task binding mismatch")
            if action == "complete":
                if not payload.get("capsule_sha256"):
                    raise ValueError("capsule_sha256 is required")
                changed = kb.complete_task(
                    self._conn, task_id, result="capsule submitted",
                    summary=payload.get("summary") or "RPCS worker completed",
                    metadata={"rpcs_attempt_id": payload.get("attempt_id"),
                              "capsule_sha256": payload["capsule_sha256"],
                              "spec_hash": spec_hash}, fire_lifecycle_hook=False,
                )
                state = "completed"
            else:
                reason = payload.get("reason")
                kind = payload.get("kind")
                if not isinstance(reason, str) or not reason.strip():
                    raise ValueError("block reason is required")
                changed = kb.block_task(self._conn, task_id, reason=reason.strip(), kind=kind)
                state = "blocked"
            if not changed:
                # kanban_db lifecycle helpers may commit their state transition
                # before an API-ledger write is interrupted.  Converge only
                # when the same bound task already reached the requested class.
                current = kb.get_task(self._conn, task_id)
                terminal = ({"done"} if action == "complete"
                            else {"blocked", "todo", "triage"})
                if current is None or current.status not in terminal:
                    raise ValueError(f"worker task is not {action}able")
            status = {"object": "hermes.kanban.worker_task", "task_id": task_id,
                      "status": state, "spec_hash": spec_hash,
                      "attempt_id": payload.get("attempt_id")}
            self._conn.execute(
                """INSERT INTO api_operations
                   (operation_id, actor_id, endpoint_kind, request_hash, profile,
                    spec_hash, object_id, state, status_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?)""",
                (*((operation_id,) + binding + (task_id,)),
                 json.dumps(status, sort_keys=True, separators=(",", ":")),
                 timestamp, timestamp),
            )
        return PreparedOperation(operation_id, task_id, "completed", False), status

    def prepare(
        self, *, operation_id: str, actor_id: str, endpoint_kind: str,
        request_hash: str, profile: str, spec_hash: str, object_id: str,
        initial_status: dict[str, Any],
    ) -> PreparedOperation:
        binding = (actor_id, endpoint_kind, request_hash, profile, spec_hash)
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if row is not None:
                existing = tuple(row[key] for key in (
                    "actor_id", "endpoint_kind", "request_hash", "profile", "spec_hash"
                ))
                if existing != binding:
                    raise OperationConflict(operation_id)
                return PreparedOperation(operation_id, row["object_id"], row["state"], True)
            self._conn.execute(
                """INSERT INTO api_operations
                   (operation_id, actor_id, endpoint_kind, request_hash, profile,
                    spec_hash, object_id, state, status_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (*((operation_id,) + binding + (object_id,)),
                 json.dumps(initial_status, sort_keys=True, separators=(",", ":")), now, now),
            )
        return PreparedOperation(operation_id, object_id, "queued", False)

    def claim_dispatch(self, operation_id: str) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE api_operations SET state = 'running', updated_at = ? "
                "WHERE operation_id = ? AND state = 'queued'",
                (time.time(), operation_id),
            )
            return cursor.rowcount == 1

    def update_status(self, object_id: str, status: dict[str, Any]) -> None:
        state = str(status.get("status", "running"))
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE api_operations SET state = ?, status_json = ?, updated_at = ? "
                "WHERE object_id = ?",
                (state, json.dumps(status, sort_keys=True, separators=(",", ":")),
                 time.time(), object_id),
            )

    def get_by_operation(self, operation_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM api_operations WHERE operation_id = ?", (operation_id,)
            ).fetchone()
        return self._as_dict(row)

    def get_status(self, object_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT status_json FROM api_operations WHERE object_id = ?", (object_id,)
            ).fetchone()
        return json.loads(row["status_json"]) if row else None

    @staticmethod
    def _as_dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        result = dict(row)
        result["status"] = json.loads(result.pop("status_json"))
        return result

    def close(self) -> None:
        with self._lock:
            self._conn.close()
