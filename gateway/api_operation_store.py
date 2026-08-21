"""Durable operation identities for API requests dispatched in the background."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
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

    def __init__(self, db_path: str | Path):
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
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
