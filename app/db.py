from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


CREATE_MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    thread_id TEXT,
    from_email TEXT,
    from_name TEXT,
    subject TEXT,
    snippet TEXT,
    date_text TEXT,
    internal_date INTEGER,
    label_ids TEXT,
    is_unread INTEGER,
    has_attachments INTEGER,
    size_estimate INTEGER,
    processed_at TEXT
);
"""

CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT,
    finished_at TEXT,
    mode TEXT,
    total_scanned INTEGER,
    total_saved INTEGER,
    total_errors INTEGER
);
"""


class SQLiteInventory:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(CREATE_MESSAGES_TABLE)
        conn.execute(CREATE_RUNS_TABLE)
        conn.commit()

    def create_run(self, conn: sqlite3.Connection, started_at: str, mode: str) -> int:
        cur = conn.execute(
            """
            INSERT INTO runs (started_at, mode, total_scanned, total_saved, total_errors)
            VALUES (?, ?, 0, 0, 0)
            """,
            (started_at, mode),
        )
        conn.commit()
        return int(cur.lastrowid)

    def finalize_run(
        self,
        conn: sqlite3.Connection,
        run_id: int,
        finished_at: str,
        total_scanned: int,
        total_saved: int,
        total_errors: int,
    ) -> None:
        conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, total_scanned = ?, total_saved = ?, total_errors = ?
            WHERE run_id = ?
            """,
            (finished_at, total_scanned, total_saved, total_errors, run_id),
        )
        conn.commit()

    def upsert_message(self, conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO messages (
                message_id, thread_id, from_email, from_name, subject, snippet,
                date_text, internal_date, label_ids, is_unread, has_attachments,
                size_estimate, processed_at
            ) VALUES (
                :message_id, :thread_id, :from_email, :from_name, :subject, :snippet,
                :date_text, :internal_date, :label_ids, :is_unread, :has_attachments,
                :size_estimate, :processed_at
            )
            ON CONFLICT(message_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                from_email = excluded.from_email,
                from_name = excluded.from_name,
                subject = excluded.subject,
                snippet = excluded.snippet,
                date_text = excluded.date_text,
                internal_date = excluded.internal_date,
                label_ids = excluded.label_ids,
                is_unread = excluded.is_unread,
                has_attachments = excluded.has_attachments,
                size_estimate = excluded.size_estimate,
                processed_at = excluded.processed_at
            """,
            payload,
        )
