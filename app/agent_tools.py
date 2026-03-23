"""
Implementación de las herramientas que Claude puede usar durante la sesión del agente.
Cada función corresponde a un tool definido en AGENT_TOOLS.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from googleapiclient.discovery import Resource

    from app.db import SQLiteInventory
    from app.gmail_client import GmailClient

logger = logging.getLogger("gmail_agent")


# ---------------------------------------------------------------------------
# Definición de herramientas para Claude
# ---------------------------------------------------------------------------

AGENT_TOOLS: list[dict[str, Any]] = [
    {
        "name": "get_inbox_summary",
        "description": (
            "Obtiene estadísticas generales de la bandeja de entrada: total de mensajes, "
            "no leídos, urgentes, top remitentes, distribución por clasificación y fecha "
            "de última sincronización. Útil para planificar la sesión."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "sync_recent_emails",
        "description": (
            "Sincroniza los emails más recientes desde Gmail hacia la base de datos local. "
            "Usar al inicio de la sesión para tener los mensajes más nuevos disponibles."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "max_results": {
                    "type": "integer",
                    "description": "Máximo de emails a sincronizar (default: 200)",
                    "default": 200,
                }
            },
            "required": [],
        },
    },
    {
        "name": "search_emails",
        "description": (
            "Busca emails en la base de datos local con filtros. "
            "Devuelve lista de mensajes con id, remitente, asunto, fecha y clasificación."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "from_email": {"type": "string", "description": "Filtrar por dirección o dominio del remitente"},
                "subject_contains": {"type": "string", "description": "Texto a buscar en el asunto"},
                "label": {"type": "string", "description": "Filtrar por etiqueta Gmail (ej: INBOX, UNREAD, SPAM)"},
                "unread_only": {"type": "boolean", "description": "Solo emails no leídos"},
                "has_attachments": {"type": "boolean", "description": "Filtrar por presencia de adjuntos"},
                "classification": {
                    "type": "string",
                    "enum": ["empleo", "urgente", "personal", "newsletter", "notificacion", "spam", "factura", "trabajo", "otro"],
                    "description": "Filtrar por clasificación previa del agente",
                },
                "urgency": {
                    "type": "string",
                    "enum": ["alta", "media", "baja"],
                    "description": "Filtrar por urgencia",
                },
                "limit": {"type": "integer", "description": "Máximo de resultados (default: 30)", "default": 30},
                "order_by": {
                    "type": "string",
                    "enum": ["date_desc", "date_asc", "size_desc"],
                    "default": "date_desc",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_email_full_content",
        "description": (
            "Obtiene el contenido completo de un email desde Gmail: cuerpo, todos los headers, "
            "incluyendo List-Unsubscribe. Usar para emails que necesitan revisión antes de actuar."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "string", "description": "ID del mensaje Gmail"}
            },
            "required": ["message_id"],
        },
    },
    {
        "name": "classify_emails",
        "description": (
            "Guarda la clasificación de uno o más emails en la base de datos. "
            "Categorías: empleo, urgente, personal, newsletter, notificacion, spam, factura, trabajo, otro."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "classifications": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "message_id": {"type": "string"},
                            "category": {
                                "type": "string",
                                "enum": ["empleo", "urgente", "personal", "newsletter", "notificacion", "spam", "factura", "trabajo", "otro"],
                            },
                            "urgency": {"type": "string", "enum": ["alta", "media", "baja"]},
                            "notes": {"type": "string", "description": "Notas o razón de la clasificación"},
                        },
                        "required": ["message_id", "category", "urgency"],
                    },
                    "description": "Lista de clasificaciones a guardar",
                }
            },
            "required": ["classifications"],
        },
    },
    {
        "name": "trash_emails",
        "description": (
            "Mueve uno o más emails a la papelera de Gmail. NO elimina permanentemente. "
            "Usar para newsletters viejas, notificaciones irrelevantes, spam confirmado."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "message_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Lista de IDs de mensajes a mover a papelera",
                },
                "reason": {"type": "string", "description": "Razón para eliminar (para el log de sesión)"},
            },
            "required": ["message_ids", "reason"],
        },
    },
    {
        "name": "handle_unsubscribe",
        "description": (
            "Gestiona la desuscripción de una lista de correo. "
            "Extrae el header List-Unsubscribe y envía email de desuscripción o reporta el link."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "string", "description": "ID de un email de la newsletter"},
            },
            "required": ["message_id"],
        },
    },
    {
        "name": "create_draft",
        "description": (
            "Crea un borrador de email en Gmail. Usar para redactar emails de búsqueda de empleo, "
            "respuestas importantes, o cualquier email que necesite revisión antes de enviar."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Destinatario (email)"},
                "subject": {"type": "string", "description": "Asunto del email"},
                "body": {"type": "string", "description": "Cuerpo del email en texto plano"},
                "reply_to_message_id": {
                    "type": "string",
                    "description": "ID del mensaje al que se responde (opcional, para mantener el hilo)",
                },
            },
            "required": ["to", "subject", "body"],
        },
    },
    {
        "name": "mark_emails_read",
        "description": "Marca uno o más emails como leídos en Gmail.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Lista de IDs de mensajes a marcar como leídos",
                }
            },
            "required": ["message_ids"],
        },
    },
    {
        "name": "apply_label",
        "description": (
            "Aplica una etiqueta personalizada de Gmail a un mensaje. "
            "Crea la etiqueta automáticamente si no existe."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "string"},
                "label_name": {
                    "type": "string",
                    "description": "Nombre de la etiqueta (ej: 'EMPLEO', 'URGENTE', 'SEGUIMIENTO')",
                },
            },
            "required": ["message_id", "label_name"],
        },
    },
    {
        "name": "get_session_log",
        "description": (
            "Devuelve el log de todas las acciones realizadas en la sesión actual: "
            "cuántos emails clasificados, eliminados, borradores creados, desuscripciones, etc."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
]


# ---------------------------------------------------------------------------
# Ejecutor de herramientas
# ---------------------------------------------------------------------------

class ToolExecutor:
    """Ejecuta las herramientas del agente conectando Gmail API y SQLite."""

    def __init__(
        self,
        gmail_client: GmailClient,
        service: Resource,
        db: SQLiteInventory,
        conn: sqlite3.Connection,
        session_id: str,
    ) -> None:
        self.gmail = gmail_client
        self.service = service
        self.db = db
        self.conn = conn
        self.session_id = session_id

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def execute(self, tool_name: str, tool_input: dict[str, Any]) -> str:
        """Despacha la llamada al tool correcto y devuelve resultado como string JSON."""
        try:
            method = getattr(self, f"_tool_{tool_name}", None)
            if method is None:
                return json.dumps({"error": f"Tool desconocido: {tool_name}"})
            result = method(**tool_input)
            return json.dumps(result, ensure_ascii=False, default=str)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error ejecutando tool %s: %s", tool_name, exc)
            return json.dumps({"error": str(exc)})

    # -----------------------------------------------------------------------
    # Implementaciones
    # -----------------------------------------------------------------------

    def _tool_get_inbox_summary(self) -> dict:
        stats = self.db.get_inbox_stats(self.conn)
        return {"status": "ok", "stats": stats}

    def _tool_sync_recent_emails(self, max_results: int = 50) -> dict:
        from app.extractor import normalize_message

        synced = 0
        errors = 0
        batch: list[dict] = []

        now = self._now()
        for msg_id in self.gmail.iter_message_ids(self.service, max_results=max_results):
            try:
                raw = self.gmail.get_message_metadata(self.service, msg_id)
                payload = normalize_message(raw, now)
                batch.append(payload)
                if len(batch) >= 100:
                    self.db.upsert_messages(self.conn, batch)
                    self.conn.commit()
                    synced += len(batch)
                    batch = []
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.warning("Error sincronizando mensaje %s: %s", msg_id, exc)

        if batch:
            self.db.upsert_messages(self.conn, batch)
            self.conn.commit()
            synced += len(batch)

        self.db.log_action(
            self.conn, self.session_id, "sync",
            self._now(), details=f"Sincronizados {synced} emails, {errors} errores"
        )
        return {"status": "ok", "synced": synced, "errors": errors}

    def _tool_search_emails(
        self,
        from_email: str | None = None,
        subject_contains: str | None = None,
        label: str | None = None,
        unread_only: bool = False,
        has_attachments: bool | None = None,
        classification: str | None = None,
        urgency: str | None = None,
        limit: int = 30,
        order_by: str = "date_desc",
    ) -> dict:
        results = self.db.search_messages(
            self.conn,
            from_email=from_email,
            subject_contains=subject_contains,
            label=label,
            unread_only=unread_only,
            has_attachments=has_attachments,
            classification=classification,
            urgency=urgency,
            order_by=order_by,
            limit=limit,
        )
        return {"status": "ok", "count": len(results), "emails": results}

    def _tool_get_email_full_content(self, message_id: str) -> dict:
        content = self.gmail.get_full_message(self.service, message_id)
        # Truncar cuerpo si es muy largo para no saturar el contexto
        body = content.get("body", "")
        if len(body) > 8000:
            content["body"] = body[:8000] + "\n\n[... contenido truncado ...]"
        return {"status": "ok", "email": content}

    def _tool_classify_emails(self, classifications: list[dict]) -> dict:
        classified = 0
        for item in classifications:
            msg_id = item["message_id"]
            category = item["category"]
            urgency = item.get("urgency", "baja")
            notes = item.get("notes", "")
            self.db.update_classification(
                self.conn, msg_id, category, urgency, notes, self._now()
            )
            self.db.log_action(
                self.conn, self.session_id, "classify", self._now(),
                message_id=msg_id,
                details=json.dumps({"category": category, "urgency": urgency, "notes": notes}),
            )
            classified += 1
        return {"status": "ok", "classified": classified}

    def _tool_trash_emails(self, message_ids: list[str], reason: str) -> dict:
        trashed = 0
        errors = []
        for msg_id in message_ids:
            try:
                self.gmail.trash_message(self.service, msg_id)
                self.db.mark_trashed(self.conn, msg_id)
                self.db.log_action(
                    self.conn, self.session_id, "trash", self._now(),
                    message_id=msg_id, details=reason,
                )
                trashed += 1
            except Exception as exc:  # noqa: BLE001
                errors.append({"message_id": msg_id, "error": str(exc)})
                logger.warning("Error al mover a papelera %s: %s", msg_id, exc)
        return {"status": "ok", "trashed": trashed, "errors": errors}

    def _tool_handle_unsubscribe(self, message_id: str) -> dict:
        content = self.gmail.get_full_message(self.service, message_id)
        unsub_header = content.get("list_unsubscribe", "")
        unsub_post = content.get("list_unsubscribe_post", "")

        if not unsub_header:
            return {
                "status": "no_header",
                "message": "Este email no tiene header List-Unsubscribe. Desuscripción manual requerida.",
                "from": content.get("from", ""),
                "subject": content.get("subject", ""),
            }

        # Extraer email de desuscripción del header
        email_match = re.search(r"<mailto:([^>]+)>", unsub_header)
        link_match = re.search(r"<(https?://[^>]+)>", unsub_header)

        result: dict[str, Any] = {
            "status": "ok",
            "from": content.get("from", ""),
            "subject": content.get("subject", ""),
            "list_unsubscribe_header": unsub_header,
        }

        if email_match:
            unsub_email = email_match.group(1)
            # Determinar subject para el email de desuscripción
            unsub_subject = "unsubscribe"
            if "?" in unsub_email:
                unsub_email_addr, unsub_subject = unsub_email.split("?", 1)
                unsub_subject = unsub_subject.replace("subject=", "")
            else:
                unsub_email_addr = unsub_email

            if unsub_post and "List-Unsubscribe=One-Click" in unsub_post:
                # One-click: enviar email vacío
                try:
                    self.gmail.send_email(
                        self.service,
                        to=unsub_email_addr,
                        subject=unsub_subject,
                        body="",
                    )
                    result["action"] = "email_sent"
                    result["unsubscribe_email"] = unsub_email_addr
                    self.db.log_action(
                        self.conn, self.session_id, "unsubscribe", self._now(),
                        message_id=message_id,
                        details=f"Email enviado a {unsub_email_addr}",
                    )
                except Exception as exc:  # noqa: BLE001
                    result["action"] = "email_failed"
                    result["error"] = str(exc)
            else:
                result["action"] = "email_required"
                result["unsubscribe_email"] = unsub_email_addr
                result["instruction"] = f"Enviar email vacío a {unsub_email_addr} con asunto '{unsub_subject}'"
        elif link_match:
            result["action"] = "link_required"
            result["unsubscribe_link"] = link_match.group(1)
            result["instruction"] = f"Visitar este link para desuscribirse: {link_match.group(1)}"
        else:
            result["action"] = "manual_required"
            result["instruction"] = "No se pudo parsear el header. Desuscripción manual requerida."

        return result

    def _tool_create_draft(
        self,
        to: str,
        subject: str,
        body: str,
        reply_to_message_id: str | None = None,
    ) -> dict:
        draft = self.gmail.create_draft(
            self.service, to=to, subject=subject, body=body,
            reply_to_message_id=reply_to_message_id,
        )
        draft_id = draft.get("id", "")
        self.db.log_action(
            self.conn, self.session_id, "draft_created", self._now(),
            details=json.dumps({"to": to, "subject": subject, "draft_id": draft_id}),
        )
        return {"status": "ok", "draft_id": draft_id, "to": to, "subject": subject}

    def _tool_mark_emails_read(self, message_ids: list[str]) -> dict:
        marked = 0
        errors = []
        for msg_id in message_ids:
            try:
                self.gmail.mark_as_read(self.service, msg_id)
                marked += 1
            except Exception as exc:  # noqa: BLE001
                errors.append({"message_id": msg_id, "error": str(exc)})
        self.db.log_action(
            self.conn, self.session_id, "mark_read", self._now(),
            details=f"{marked} emails marcados como leídos",
        )
        return {"status": "ok", "marked": marked, "errors": errors}

    def _tool_apply_label(self, message_id: str, label_name: str) -> dict:
        label_id = self.gmail.create_label_if_not_exists(self.service, label_name)
        self.gmail.modify_labels(self.service, message_id, add_label_ids=[label_id])
        self.db.log_action(
            self.conn, self.session_id, "label_applied", self._now(),
            message_id=message_id, details=label_name,
        )
        return {"status": "ok", "message_id": message_id, "label": label_name, "label_id": label_id}

    def _tool_get_session_log(self) -> dict:
        actions = self.db.get_session_actions(self.conn, self.session_id)

        summary: dict[str, int] = {}
        for action in actions:
            action_type = action["action_type"]
            summary[action_type] = summary.get(action_type, 0) + 1

        return {
            "status": "ok",
            "session_id": self.session_id,
            "total_actions": len(actions),
            "summary": summary,
            "actions": actions[-20:],  # últimas 20 para no saturar el contexto
        }
