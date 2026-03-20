from __future__ import annotations

from pathlib import Path
from typing import Iterator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build


class GmailClient:
    """Read-only Gmail API client."""

    def __init__(self, credentials_file: Path, token_file: Path, scopes: list[str]) -> None:
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.scopes = scopes

    def authenticate(self) -> Credentials:
        if not self.credentials_file.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de credenciales OAuth: {self.credentials_file}. "
                "Coloca credentials.json en la raíz del proyecto o ajusta GMAIL_CREDENTIALS_FILE."
            )

        creds: Credentials | None = None
        if self.token_file.exists():
            creds = Credentials.from_authorized_user_file(str(self.token_file), self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_file),
                    self.scopes,
                )
                creds = flow.run_local_server(port=0)

            self.token_file.write_text(creds.to_json(), encoding="utf-8")

        return creds

    def build_service(self) -> Resource:
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
                .execute()
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
            .execute()
        )
