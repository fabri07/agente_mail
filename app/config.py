from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback solo para entornos incompletos
    def load_dotenv() -> bool:
        return False


@dataclass(frozen=True)
class Settings:
    credentials_file: Path
    token_file: Path
    db_path: Path
    log_file: Path
    log_level: str
    progress_every: int


SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default)).expanduser()


def _env_positive_int(name: str, default: str) -> int:
    raw_value = os.getenv(name, default)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} debe ser un entero positivo. Valor recibido: {raw_value!r}") from exc

    if value <= 0:
        raise ValueError(f"{name} debe ser un entero positivo. Valor recibido: {raw_value!r}")
    return value


def load_settings() -> Settings:
    load_dotenv()
    credentials_file = _env_path("GMAIL_CREDENTIALS_FILE", "credentials.json")
    token_file = _env_path("GMAIL_TOKEN_FILE", "token.json")
    db_path = _env_path("DB_PATH", "db/gmail_agent.db")
    log_file = _env_path("LOG_FILE", "logs/run.log")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    progress_every = _env_positive_int("PROGRESS_EVERY", "100")

    return Settings(
        credentials_file=credentials_file,
        token_file=token_file,
        db_path=db_path,
        log_file=log_file,
        log_level=log_level,
        progress_every=progress_every,
    )
