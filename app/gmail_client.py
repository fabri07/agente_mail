from __future__ import annotations

import base64
import logging
import os
from email.mime.text import MIMEText
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import Resource


class GmailClient:
    """Gmail API client con soporte de lectura y escritura."""

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
                    self.logger.info("Refrescando token OAuth.")
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
                self.logger.info("Abriendo flujo OAuth local para autenticar Gmail.")
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

    # -------------------------------------------------------------------------
    # Operaciones de LECTURA
    # -------------------------------------------------------------------------

    def iter_message_ids(
        self,
        service: Resource,
        max_results: int | None = None,
        page_size: int = 500,
        label_ids: list[str] | None = None,
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

            kwargs: dict[str, Any] = dict(
                userId="me",
                maxResults=request_size,
                pageToken=page_token,
                fields="messages/id,nextPageToken,resultSizeEstimate",
            )
            if label_ids:
                kwargs["labelIds"] = label_ids

            response = service.users().messages().list(**kwargs).execute(num_retries=3)

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

    def get_full_message(self, service: Resource, message_id: str) -> dict[str, Any]:
        """Obtiene el mensaje completo incluyendo cuerpo y todos los headers."""
        raw = (
            service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute(num_retries=3)
        )

        # Extraer cuerpo del mensaje
        body_text = self._extract_body(raw.get("payload", {}))

        # Extraer todos los headers relevantes
        headers = {}
        for h in raw.get("payload", {}).get("headers", []):
            headers[h["name"].lower()] = h["value"]

        return {
            "message_id": raw["id"],
            "thread_id": raw.get("threadId"),
            "subject": headers.get("subject", "(sin asunto)"),
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "date": headers.get("date", ""),
            "reply_to": headers.get("reply-to", headers.get("from", "")),
            "list_unsubscribe": headers.get("list-unsubscribe", ""),
            "list_unsubscribe_post": headers.get("list-unsubscribe-post", ""),
            "body": body_text,
            "snippet": raw.get("snippet", ""),
            "label_ids": raw.get("labelIds", []),
            "size_estimate": raw.get("sizeEstimate", 0),
        }

    def _extract_body(self, payload: dict) -> str:
        """Extrae el texto del cuerpo del mensaje recursivamente."""
        mime_type = payload.get("mimeType", "")

        if mime_type == "text/plain":
            data = payload.get("body", {}).get("data", "")
            if data:
                return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")

        if mime_type == "text/html":
            data = payload.get("body", {}).get("data", "")
            if data:
                html = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
                # Retornar HTML limpio (Claude puede interpretarlo)
                return html

        # Recorrer partes recursivamente, preferir text/plain
        parts = payload.get("parts", [])
        plain_text = ""
        html_text = ""
        for part in parts:
            result = self._extract_body(part)
            if part.get("mimeType") == "text/plain" and result:
                plain_text = result
            elif part.get("mimeType") == "text/html" and result:
                html_text = result
            elif result and not plain_text:
                plain_text = result

        return plain_text or html_text or ""

    # -------------------------------------------------------------------------
    # Operaciones de ESCRITURA
    # -------------------------------------------------------------------------

    def trash_message(self, service: Resource, message_id: str) -> dict:
        """Mueve un mensaje a la papelera (NO elimina permanentemente)."""
        return (
            service.users()
            .messages()
            .trash(userId="me", id=message_id)
            .execute(num_retries=3)
        )

    def modify_labels(
        self,
        service: Resource,
        message_id: str,
        add_label_ids: list[str] | None = None,
        remove_label_ids: list[str] | None = None,
    ) -> dict:
        """Agrega o quita etiquetas de un mensaje."""
        body: dict[str, Any] = {}
        if add_label_ids:
            body["addLabelIds"] = add_label_ids
        if remove_label_ids:
            body["removeLabelIds"] = remove_label_ids
        return (
            service.users()
            .messages()
            .modify(userId="me", id=message_id, body=body)
            .execute(num_retries=3)
        )

    def mark_as_read(self, service: Resource, message_id: str) -> dict:
        """Marca un mensaje como leído."""
        return self.modify_labels(service, message_id, remove_label_ids=["UNREAD"])

    def create_label_if_not_exists(self, service: Resource, label_name: str) -> str:
        """Crea una etiqueta si no existe, devuelve su ID."""
        existing = service.users().labels().list(userId="me").execute(num_retries=3)
        for label in existing.get("labels", []):
            if label["name"].lower() == label_name.lower():
                return label["id"]

        new_label = (
            service.users()
            .labels()
            .create(userId="me", body={"name": label_name})
            .execute(num_retries=3)
        )
        return new_label["id"]

    def create_draft(
        self,
        service: Resource,
        to: str,
        subject: str,
        body: str,
        reply_to_message_id: str | None = None,
    ) -> dict:
        """Crea un borrador de email en Gmail."""
        message = MIMEText(body, "plain", "utf-8")
        message["to"] = to
        message["subject"] = subject

        if reply_to_message_id:
            # Obtener thread_id para la respuesta
            try:
                original = (
                    service.users()
                    .messages()
                    .get(userId="me", id=reply_to_message_id, format="metadata",
                         metadataHeaders=["Message-ID", "Subject"])
                    .execute(num_retries=3)
                )
                thread_id = original.get("threadId")
                headers = {h["name"].lower(): h["value"]
                           for h in original.get("payload", {}).get("headers", [])}
                orig_msg_id = headers.get("message-id", "")
                if orig_msg_id:
                    message["In-Reply-To"] = orig_msg_id
                    message["References"] = orig_msg_id
            except Exception:  # noqa: BLE001
                thread_id = None
        else:
            thread_id = None

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        draft_body: dict[str, Any] = {"message": {"raw": raw}}
        if thread_id:
            draft_body["message"]["threadId"] = thread_id

        return (
            service.users()
            .drafts()
            .create(userId="me", body=draft_body)
            .execute(num_retries=3)
        )

    def send_email(
        self,
        service: Resource,
        to: str,
        subject: str,
        body: str,
        reply_to_message_id: str | None = None,
    ) -> dict:
        """Envía un email directamente."""
        message = MIMEText(body, "plain", "utf-8")
        message["to"] = to
        message["subject"] = subject

        thread_id = None
        if reply_to_message_id:
            try:
                original = (
                    service.users()
                    .messages()
                    .get(userId="me", id=reply_to_message_id, format="metadata",
                         metadataHeaders=["Message-ID"])
                    .execute(num_retries=3)
                )
                thread_id = original.get("threadId")
                headers = {h["name"].lower(): h["value"]
                           for h in original.get("payload", {}).get("headers", [])}
                orig_msg_id = headers.get("message-id", "")
                if orig_msg_id:
                    message["In-Reply-To"] = orig_msg_id
                    message["References"] = orig_msg_id
            except Exception:  # noqa: BLE001
                pass

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        send_body: dict[str, Any] = {"raw": raw}
        if thread_id:
            send_body["threadId"] = thread_id

        return (
            service.users()
            .messages()
            .send(userId="me", body=send_body)
            .execute(num_retries=3)
        )
