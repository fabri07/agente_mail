from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import Resource
else:  # pragma: no cover - solo para typing en runtime sin dependencias
    Credentials = Any
    Resource = Any


class GmailClient:
    """Read-only Gmail API client."""

    def __init__(self, credentials_file: Path, token_file: Path, scopes: list[str]) -> None:
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.scopes = scopes
        self.logger = logging.getLogger("gmail_agent")

    def _load_google_dependencies(self) -> tuple[Any, Any, Any, Any]:
        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials as CredentialsClass
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Faltan dependencias de Gmail API. Ejecuta `pip install -r requirements.txt` antes de correr el agente."
            ) from exc

        return Request, CredentialsClass, InstalledAppFlow, build

    def authenticate(self) -> Credentials:
        Request, CredentialsClass, InstalledAppFlow, _ = self._load_google_dependencies()

        if not self.credentials_file.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de credenciales OAuth: {self.credentials_file}. "
                "Coloca credentials.json en la raíz del proyecto o ajusta GMAIL_CREDENTIALS_FILE."
            )

        creds: Credentials | None = None
        if self.token_file.exists():
            try:
                creds = CredentialsClass.from_authorized_user_file(str(self.token_file), self.scopes)
            except (OSError, ValueError) as exc:
                self.logger.warning(
                    "Token OAuth inválido o corrupto en %s. Se solicitará una nueva autenticación. detalle=%s",
                    self.token_file,
                    exc,
                )

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    self.logger.info("Refrescando token OAuth read-only.")
                    creds.refresh(Request())
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning(
                        "No fue posible refrescar el token OAuth. Se solicitará una nueva autenticación. detalle=%s",
                        exc,
                    )
                    creds = None

            if not creds or not creds.valid:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_file),
                    self.scopes,
                )
                self.logger.info("Abriendo flujo OAuth local para autenticar Gmail read-only.")
                creds = flow.run_local_server(port=0)

            self._persist_token(creds)

        return creds

    def _persist_token(self, creds: Credentials) -> None:
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        self.token_file.write_text(creds.to_json(), encoding="utf-8")
        try:
            os.chmod(self.token_file, 0o600)
        except OSError:
            self.logger.warning("No fue posible ajustar permisos restrictivos para %s", self.token_file)

    def build_service(self) -> Resource:
        _, _, _, build = self._load_google_dependencies()
        creds = self.authenticate()
        return build("gmail", "v1", credentials=creds, cache_discovery=False)

    def iter_message_ids(
        self,
        service: Resource,
        max_results: int | None = None,
        page_size: int = 500,
    ) -> Iterator[str]:
        fetched = 0
        page_token: str | None = None

        while True:
            request_size = page_size
            if max_results is not None:
                remaining = max_results - fetched
                if remaining <= 0:
                    break
                request_size = min(page_size, remaining)

            response = (
                service.users()
                .messages()
                .list(
                    userId="me",
                    maxResults=request_size,
                    pageToken=page_token,
                    fields="messages/id,nextPageToken,resultSizeEstimate",
                )
                .execute(num_retries=3)
            )

            messages = response.get("messages", [])
            if not messages:
                break

            for message in messages:
                yield message["id"]
                fetched += 1
                if max_results is not None and fetched >= max_results:
                    return

            page_token = response.get("nextPageToken")
            if not page_token:
                break

    def get_message_metadata(self, service: Resource, message_id: str) -> dict:
        return (
            service.users()
            .messages()
            .get(
                userId="me",
                id=message_id,
                format="metadata",
                metadataHeaders=["From", "Subject", "Date"],
                fields=(
                    "id,threadId,labelIds,snippet,internalDate,sizeEstimate,"
                    "payload/headers,payload/parts,payload/filename,payload/body/attachmentId"
                ),
            )
            .execute(num_retries=3)
        )
