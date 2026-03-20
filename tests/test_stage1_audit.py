from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from app.config import load_settings
from app.db import SQLiteInventory
from app.extractor import detect_attachments, normalize_message, update_summary_counts


class ConfigTests(unittest.TestCase):
    def test_progress_every_must_be_positive(self) -> None:
        with patch.dict(os.environ, {"PROGRESS_EVERY": "0"}, clear=False):
            with self.assertRaises(ValueError):
                load_settings()


class ExtractorTests(unittest.TestCase):
    def test_normalize_message_requires_message_id(self) -> None:
        with self.assertRaises(ValueError):
            normalize_message({"payload": {"headers": []}}, processed_at="2026-01-01T00:00:00+00:00")

    def test_detect_attachments_handles_nested_parts(self) -> None:
        payload = {
            "parts": [
                {
                    "parts": [
                        {
                            "filename": "invoice.pdf",
                            "body": {"attachmentId": "abc123"},
                        }
                    ]
                }
            ]
        }
        self.assertEqual(detect_attachments(payload), 1)

    def test_update_summary_counts_tracks_domains_and_labels(self) -> None:
        record = {
            "from_email": "alerts@example.com",
            "label_ids": '["INBOX", "UNREAD"]',
        }
        domains: Counter[str] = Counter()
        labels: Counter[str] = Counter()

        update_summary_counts(record, domains, labels)

        self.assertEqual(domains["example.com"], 1)
        self.assertEqual(labels["INBOX"], 1)
        self.assertEqual(labels["UNREAD"], 1)


class SQLiteInventoryTests(unittest.TestCase):
    def test_schema_adds_indexes_and_run_status_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "gmail_agent.db"
            inventory = SQLiteInventory(db_path)
            conn = inventory.connect()
            self.addCleanup(conn.close)

            inventory.init_schema(conn)

            run_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(runs)")
            }
            self.assertIn("status", run_columns)
            self.assertIn("fatal_error", run_columns)

            indexes = {
                row["name"]
                for row in conn.execute("PRAGMA index_list(messages)")
            }
            self.assertIn("idx_messages_thread_id", indexes)
            self.assertIn("idx_messages_from_email", indexes)
            self.assertIn("idx_messages_internal_date", indexes)

    def test_upsert_messages_updates_existing_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "gmail_agent.db"
            inventory = SQLiteInventory(db_path)
            conn = inventory.connect()
            self.addCleanup(conn.close)
            inventory.init_schema(conn)

            payload = {
                "message_id": "m1",
                "thread_id": "t1",
                "from_email": "a@example.com",
                "from_name": "A",
                "subject": "first",
                "snippet": "hello",
                "date_text": "Fri, 01 Jan 2026 10:00:00 +0000",
                "internal_date": 1,
                "label_ids": '["INBOX"]',
                "is_unread": 1,
                "has_attachments": 0,
                "size_estimate": 10,
                "processed_at": "2026-01-01T10:00:00+00:00",
            }

            inventory.upsert_messages(conn, [payload])
            conn.commit()

            payload["subject"] = "updated"
            inventory.upsert_messages(conn, [payload])
            conn.commit()

            row = conn.execute(
                "SELECT subject FROM messages WHERE message_id = ?",
                ("m1",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["subject"], "updated")


if __name__ == "__main__":
    unittest.main()
