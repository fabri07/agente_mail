from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from app.utils import extract_domain, header_lookup, parse_from_header, top_n


def detect_attachments(payload: dict[str, Any] | None) -> int:
    if not payload:
        return 0

    stack = [payload]
    while stack:
        part = stack.pop()
        filename = part.get("filename")
        body = part.get("body", {})
        if filename or body.get("attachmentId"):
            return 1
        stack.extend(part.get("parts", []))
    return 0


def normalize_message(message: dict[str, Any], processed_at: str) -> dict[str, Any]:
    headers = message.get("payload", {}).get("headers", [])
    from_name, from_email = parse_from_header(header_lookup(headers, "From"))

    labels = message.get("labelIds", [])
    return {
        "message_id": message.get("id", ""),
        "thread_id": message.get("threadId", ""),
        "from_email": from_email,
        "from_name": from_name,
        "subject": header_lookup(headers, "Subject"),
        "snippet": message.get("snippet", ""),
        "date_text": header_lookup(headers, "Date"),
        "internal_date": int(message.get("internalDate", 0) or 0),
        "label_ids": json.dumps(labels, ensure_ascii=False),
        "is_unread": int("UNREAD" in labels),
        "has_attachments": detect_attachments(message.get("payload")),
        "size_estimate": int(message.get("sizeEstimate", 0) or 0),
        "processed_at": processed_at,
    }


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def summarize_top_domains(records: list[dict[str, Any]], n: int = 10) -> list[tuple[str, int]]:
    domains = [extract_domain(record.get("from_email", "")) for record in records]
    return top_n(domains, n=n)


def summarize_top_labels(records: list[dict[str, Any]], n: int = 10) -> list[tuple[str, int]]:
    expanded: list[str] = []
    for record in records:
        try:
            labels = json.loads(record.get("label_ids", "[]"))
        except json.JSONDecodeError:
            labels = []
        expanded.extend([label for label in labels if label])
    return top_n(expanded, n=n)


def to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(records)
