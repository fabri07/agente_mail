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

CREATE_MESSAGE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON messages(thread_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_from_email ON messages(from_email);",
    "CREATE INDEX IF NOT EXISTS idx_messages_internal_date ON messages(internal_date);",
    "CREATE INDEX IF NOT EXISTS idx_messages_processed_at ON messages(processed_at);",
)

CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT,
    finished_at TEXT,
    mode TEXT,
    status TEXT DEFAULT 'running',
    total_scanned INTEGER,
    total_saved INTEGER,
    total_errors INTEGER,
    fatal_error TEXT
);
"""

CREATE_RUNS_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at);",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
)

UPSERT_MESSAGE_SQL = """
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
"""


class SQLiteInventory:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def init_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(CREATE_MESSAGES_TABLE)
        conn.execute(CREATE_RUNS_TABLE)
        self._ensure_runs_columns(conn)
        for statement in CREATE_MESSAGE_INDEXES:
            conn.execute(statement)
        for statement in CREATE_RUNS_INDEXES:
            conn.execute(statement)
        conn.commit()

    def _ensure_runs_columns(self, conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(runs)")
        }
        if "status" not in columns:
            conn.execute("ALTER TABLE runs ADD COLUMN status TEXT DEFAULT 'running'")
        if "fatal_error" not in columns:
            conn.execute("ALTER TABLE runs ADD COLUMN fatal_error TEXT")

    def create_run(self, conn: sqlite3.Connection, started_at: str, mode: str) -> int:
        cur = conn.execute(
            """
            INSERT INTO runs (started_at, mode, status, total_scanned, total_saved, total_errors)
            VALUES (?, ?, 'running', 0, 0, 0)
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
        status: str,
        fatal_error: str | None = None,
    ) -> None:
        conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, total_scanned = ?, total_saved = ?, total_errors = ?, status = ?, fatal_error = ?
            WHERE run_id = ?
            """,
            (finished_at, total_scanned, total_saved, total_errors, status, fatal_error, run_id),
        )
        conn.commit()

    def upsert_message(self, conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
        self.upsert_messages(conn, [payload])

    def upsert_messages(self, conn: sqlite3.Connection, payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        conn.executemany(UPSERT_MESSAGE_SQL, payloads)
