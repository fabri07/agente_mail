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
    processed_at TEXT,
    classification TEXT,
    urgency TEXT,
    agent_notes TEXT,
    agent_processed_at TEXT,
    is_trashed INTEGER DEFAULT 0
);
"""

CREATE_MESSAGE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON messages(thread_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_from_email ON messages(from_email);",
    "CREATE INDEX IF NOT EXISTS idx_messages_internal_date ON messages(internal_date);",
    "CREATE INDEX IF NOT EXISTS idx_messages_processed_at ON messages(processed_at);",
    "CREATE INDEX IF NOT EXISTS idx_messages_classification ON messages(classification);",
    "CREATE INDEX IF NOT EXISTS idx_messages_urgency ON messages(urgency);",
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

# Tabla de acciones del agente: registro de todo lo que hace
CREATE_AGENT_ACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS agent_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    message_id TEXT,
    details TEXT,
    performed_at TEXT NOT NULL,
    success INTEGER DEFAULT 1,
    error TEXT
);
"""

CREATE_AGENT_ACTIONS_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_agent_actions_session ON agent_actions(session_id);",
    "CREATE INDEX IF NOT EXISTS idx_agent_actions_type ON agent_actions(action_type);",
    "CREATE INDEX IF NOT EXISTS idx_agent_actions_message ON agent_actions(message_id);",
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
        conn.execute(CREATE_AGENT_ACTIONS_TABLE)
        self._ensure_columns(conn)
        for statement in CREATE_MESSAGE_INDEXES:
            conn.execute(statement)
        for statement in CREATE_RUNS_INDEXES:
            conn.execute(statement)
        for statement in CREATE_AGENT_ACTIONS_INDEXES:
            conn.execute(statement)
        conn.commit()

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        """Agrega columnas nuevas a tablas existentes si no están presentes."""
        runs_columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)")}
        if "status" not in runs_columns:
            conn.execute("ALTER TABLE runs ADD COLUMN status TEXT DEFAULT 'running'")
        if "fatal_error" not in runs_columns:
            conn.execute("ALTER TABLE runs ADD COLUMN fatal_error TEXT")

        msg_columns = {row["name"] for row in conn.execute("PRAGMA table_info(messages)")}
        new_msg_columns = {
            "classification": "TEXT",
            "urgency": "TEXT",
            "agent_notes": "TEXT",
            "agent_processed_at": "TEXT",
            "is_trashed": "INTEGER DEFAULT 0",
        }
        for col, col_type in new_msg_columns.items():
            if col not in msg_columns:
                conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {col_type}")

    # -------------------------------------------------------------------------
    # Runs
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Messages
    # -------------------------------------------------------------------------

    def upsert_message(self, conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
        self.upsert_messages(conn, [payload])

    def upsert_messages(self, conn: sqlite3.Connection, payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        conn.executemany(UPSERT_MESSAGE_SQL, payloads)

    def update_classification(
        self,
        conn: sqlite3.Connection,
        message_id: str,
        classification: str,
        urgency: str,
        notes: str,
        processed_at: str,
    ) -> None:
        conn.execute(
            """
            UPDATE messages
            SET classification = ?, urgency = ?, agent_notes = ?, agent_processed_at = ?
            WHERE message_id = ?
            """,
            (classification, urgency, notes, processed_at, message_id),
        )
        conn.commit()

    def mark_trashed(self, conn: sqlite3.Connection, message_id: str) -> None:
        conn.execute(
            "UPDATE messages SET is_trashed = 1 WHERE message_id = ?",
            (message_id,),
        )
        conn.commit()

    def search_messages(
        self,
        conn: sqlite3.Connection,
        from_email: str | None = None,
        subject_contains: str | None = None,
        label: str | None = None,
        unread_only: bool = False,
        has_attachments: bool | None = None,
        classification: str | None = None,
        urgency: str | None = None,
        include_trashed: bool = False,
        order_by: str = "date_desc",
        limit: int = 20,
    ) -> list[dict]:
        conditions = []
        params: list[Any] = []

        if not include_trashed:
            conditions.append("(is_trashed IS NULL OR is_trashed = 0)")

        if from_email:
            conditions.append("from_email LIKE ?")
            params.append(f"%{from_email}%")

        if subject_contains:
            conditions.append("subject LIKE ?")
            params.append(f"%{subject_contains}%")

        if label:
            conditions.append("label_ids LIKE ?")
            params.append(f"%{label}%")

        if unread_only:
            conditions.append("is_unread = 1")

        if has_attachments is not None:
            conditions.append("has_attachments = ?")
            params.append(1 if has_attachments else 0)

        if classification:
            conditions.append("classification = ?")
            params.append(classification)

        if urgency:
            conditions.append("urgency = ?")
            params.append(urgency)

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        order_map = {
            "date_desc": "internal_date DESC",
            "date_asc": "internal_date ASC",
            "size_desc": "size_estimate DESC",
        }
        order_sql = order_map.get(order_by, "internal_date DESC")

        query = f"""
            SELECT message_id, thread_id, from_email, from_name, subject, snippet,
                   date_text, internal_date, label_ids, is_unread, has_attachments,
                   size_estimate, classification, urgency, agent_notes
            FROM messages
            {where_clause}
            ORDER BY {order_sql}
            LIMIT ?
        """
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_inbox_stats(self, conn: sqlite3.Connection) -> dict[str, Any]:
        total = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE (is_trashed IS NULL OR is_trashed = 0)"
        ).fetchone()[0]

        unread = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE is_unread = 1 AND (is_trashed IS NULL OR is_trashed = 0)"
        ).fetchone()[0]

        classified = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE classification IS NOT NULL AND (is_trashed IS NULL OR is_trashed = 0)"
        ).fetchone()[0]

        urgent = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE urgency = 'alta' AND (is_trashed IS NULL OR is_trashed = 0)"
        ).fetchone()[0]

        top_senders = conn.execute(
            """
            SELECT from_email, COUNT(*) as cnt
            FROM messages
            WHERE (is_trashed IS NULL OR is_trashed = 0) AND from_email IS NOT NULL
            GROUP BY from_email ORDER BY cnt DESC LIMIT 10
            """
        ).fetchall()

        by_classification = conn.execute(
            """
            SELECT classification, COUNT(*) as cnt
            FROM messages
            WHERE classification IS NOT NULL AND (is_trashed IS NULL OR is_trashed = 0)
            GROUP BY classification ORDER BY cnt DESC
            """
        ).fetchall()

        last_sync = conn.execute(
            "SELECT MAX(processed_at) FROM messages"
        ).fetchone()[0]

        return {
            "total_messages": total,
            "unread_messages": unread,
            "classified_messages": classified,
            "urgent_messages": urgent,
            "top_senders": [{"email": r[0], "count": r[1]} for r in top_senders],
            "by_classification": [{"category": r[0], "count": r[1]} for r in by_classification],
            "last_sync": last_sync,
        }

    # -------------------------------------------------------------------------
    # Agent actions log
    # -------------------------------------------------------------------------

    def log_action(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        action_type: str,
        performed_at: str,
        message_id: str | None = None,
        details: str | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_actions (session_id, action_type, message_id, details, performed_at, success, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (session_id, action_type, message_id, details, performed_at, 1 if success else 0, error),
        )
        conn.commit()

    def get_session_actions(self, conn: sqlite3.Connection, session_id: str) -> list[dict]:
        rows = conn.execute(
            """
            SELECT action_type, message_id, details, performed_at, success, error
            FROM agent_actions WHERE session_id = ?
            ORDER BY performed_at ASC
            """,
            (session_id,),
        ).fetchall()
        return [dict(row) for row in rows]
