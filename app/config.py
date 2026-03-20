from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    credentials_file: Path
    token_file: Path
    db_path: Path
    log_file: Path
    log_level: str
    progress_every: int


SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def load_settings() -> Settings:
    load_dotenv()
    credentials_file = Path(os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json"))
    token_file = Path(os.getenv("GMAIL_TOKEN_FILE", "token.json"))
    db_path = Path(os.getenv("DB_PATH", "db/gmail_agent.db"))
    log_file = Path(os.getenv("LOG_FILE", "logs/run.log"))
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    progress_every = int(os.getenv("PROGRESS_EVERY", "100"))

    return Settings(
        credentials_file=credentials_file,
        token_file=token_file,
        db_path=db_path,
        log_file=log_file,
        log_level=log_level,
        progress_every=progress_every,
    )
