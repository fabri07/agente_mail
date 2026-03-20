from __future__ import annotations

import email.utils
import re
from collections import Counter
from typing import Iterable


EMAIL_PATTERN = re.compile(r"([^@\s]+@[^@\s]+)")


def parse_from_header(from_header: str | None) -> tuple[str, str]:
    if not from_header:
        return "", ""

    name, addr = email.utils.parseaddr(from_header)
    if addr:
        return name.strip(), addr.strip().lower()

    match = EMAIL_PATTERN.search(from_header)
    if match:
        return name.strip(), match.group(1).strip().lower()

    return name.strip(), ""


def header_lookup(headers: list[dict[str, str]], key: str) -> str:
    target = key.lower()
    for item in headers:
        if item.get("name", "").lower() == target:
            return item.get("value", "")
    return ""


def top_n(items: Iterable[str], n: int = 10) -> list[tuple[str, int]]:
    counts = Counter(filter(None, items))
    return counts.most_common(n)


def extract_domain(email_address: str) -> str:
    if "@" not in email_address:
        return ""
    return email_address.split("@", 1)[1].lower()
