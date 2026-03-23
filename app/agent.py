"""
Loop principal del agente Claude para gestión de emails.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from googleapiclient.discovery import Resource

    from app.agent_tools import ToolExecutor
    from app.config import Settings

logger = logging.getLogger("gmail_agent")

SYSTEM_PROMPT = """Eres un agente personal de gestión de email. Trabajas de forma autónoma durante una sesión de tiempo limitado.

TUS OBJETIVOS en cada sesión (en orden de prioridad):
1. SINCRONIZAR: Obtén los emails más recientes al inicio.
2. DETECTAR URGENCIAS: Identifica emails urgentes que requieren atención inmediata.
3. BUSCAR EMPLEO: Revisa y gestiona emails relacionados con búsqueda de trabajo. Redacta borradores de respuesta profesionales para ofertas de empleo, entrevistas o contactos de RRHH.
4. CLASIFICAR: Organiza los emails no clasificados por categoría (empleo, urgente, personal, newsletter, notificacion, spam, factura, trabajo, otro).
5. LIMPIAR: Mueve a papelera newsletters viejas, notificaciones irrelevantes y spam confirmado. Prioriza eliminar emails masivos del mismo remitente.
6. DESUSCRIBIR: Gestiona desuscripciones de listas de correo no deseadas.
7. MARCAR LEÍDOS: Marca como leídos los emails procesados que no requieren acción.

REGLAS IMPORTANTES:
- Nunca elimines permanentemente — siempre usa la papelera (trash_emails).
- Para emails de empleo: redacta borradores completos, profesionales y personalizados.
- Antes de eliminar un email, verifica que realmente es irrelevante.
- Trabaja en lotes: busca grupos de emails similares para procesarlos juntos.
- Cuando detectes emails urgentes, descríbelos claramente en tu respuesta.
- Si encuentras emails que el usuario debería ver YA, resáltalos al principio.
- Gestiona tu tiempo: tienes una hora. Prioriza lo importante sobre lo voluminoso.
- Al final de la sesión, usa get_session_log para dar un resumen completo.

PARA EMAILS DE BÚSQUEDA DE EMPLEO:
- Redacta borradores en español a menos que el email original sea en otro idioma.
- Sé profesional, conciso y personalizado según el contexto del email.
- Incluye el nombre de la empresa y el puesto en el asunto.
- Usa apply_label con 'EMPLEO' para estos emails.

Comienza siempre con get_inbox_summary y sync_recent_emails para tener el contexto completo."""


def build_time_warning(elapsed_seconds: int, total_seconds: int) -> str:
    remaining = total_seconds - elapsed_seconds
    minutes_remaining = remaining // 60
    if minutes_remaining <= 10:
        return f"\n\n⚠️ TIEMPO RESTANTE: {minutes_remaining} minutos. Finaliza las tareas más importantes y prepara el resumen final."
    return ""


class GmailAgent:
    """Agente Claude con loop de tool use para gestión de emails."""

    def __init__(
        self,
        settings: Settings,
        tool_executor: ToolExecutor,
        session_id: str,
        max_duration_seconds: int = 3600,
    ) -> None:
        self.settings = settings
        self.executor = tool_executor
        self.session_id = session_id
        self.max_duration_seconds = max_duration_seconds
        self.start_time = time.time()
        self.messages: list[dict[str, Any]] = []

        try:
            import anthropic
            self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        except ImportError as exc:
            raise RuntimeError(
                "Falta el paquete 'anthropic'. Ejecuta: pip install anthropic"
            ) from exc

    def _elapsed(self) -> int:
        return int(time.time() - self.start_time)

    def _time_remaining(self) -> int:
        return max(0, self.max_duration_seconds - self._elapsed())

    def _is_time_up(self) -> bool:
        return self._elapsed() >= self.max_duration_seconds

    def _initial_prompt(self) -> str:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return (
            f"Sesión iniciada: {now}\n"
            f"Duración máxima: {self.max_duration_seconds // 60} minutos\n\n"
            "Comienza la sesión de gestión de emails. "
            "Sigue el orden de prioridades del sistema: sincronizar, urgencias, empleo, "
            "clasificar, limpiar, desuscribir."
        )

    def run(self) -> str:
        """Ejecuta el loop del agente hasta que Claude termine o se acabe el tiempo."""
        from app.agent_tools import AGENT_TOOLS
        import anthropic

        logger.info("Iniciando sesión del agente [%s]", self.session_id)

        initial_message = self._initial_prompt()
        self.messages = [{"role": "user", "content": initial_message}]

        final_summary = ""

        while not self._is_time_up():
            try:
                response = self.client.messages.create(
                    model=self.settings.claude_model,
                    max_tokens=4096,
                    thinking={"type": "adaptive"},
                    system=SYSTEM_PROMPT,
                    tools=AGENT_TOOLS,
                    messages=self.messages,
                )
            except anthropic.APIError as exc:
                logger.error("Error de API de Anthropic: %s", exc)
                break

            # Acumular texto de respuesta para el resumen final
            text_content = ""
            for block in response.content:
                if hasattr(block, "type") and block.type == "text":
                    text_content += block.text
                    print(block.text, flush=True)

            if text_content:
                final_summary = text_content

            # Si Claude terminó sin más tool calls
            if response.stop_reason == "end_turn":
                logger.info("Agente terminó normalmente (end_turn)")
                break

            # Extraer tool calls
            tool_use_blocks = [
                b for b in response.content
                if hasattr(b, "type") and b.type == "tool_use"
            ]

            if not tool_use_blocks:
                logger.info("Sin tool calls, terminando")
                break

            # Agregar respuesta del asistente al historial
            self.messages.append({"role": "assistant", "content": response.content})

            # Ejecutar cada tool call
            tool_results = []
            for tool_block in tool_use_blocks:
                tool_name = tool_block.name
                tool_input = tool_block.input
                tool_use_id = tool_block.id

                logger.info("Ejecutando tool: %s", tool_name)
                result_str = self.executor.execute(tool_name, tool_input)

                # Agregar advertencia de tiempo si queda poco
                time_warning = build_time_warning(self._elapsed(), self.max_duration_seconds)
                if time_warning:
                    result_str = result_str.rstrip("}") + f', "time_warning": "{time_warning.strip()}"' + "}"

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": result_str,
                })

            # Agregar resultados de tools al historial
            self.messages.append({"role": "user", "content": tool_results})

        if self._is_time_up():
            logger.info("Sesión terminada por límite de tiempo (%d segundos)", self.max_duration_seconds)
            # Notificar a Claude que el tiempo terminó y pedir resumen
            self.messages.append({
                "role": "user",
                "content": "⏰ TIEMPO AGOTADO. La sesión ha terminado. Por favor proporciona un resumen final de todo lo que hiciste en esta sesión.",
            })
            try:
                final_response = self.client.messages.create(
                    model=self.settings.claude_model,
                    max_tokens=2048,
                    system=SYSTEM_PROMPT,
                    tools=AGENT_TOOLS,
                    messages=self.messages,
                )
                for block in final_response.content:
                    if hasattr(block, "type") and block.type == "text":
                        final_summary = block.text
                        print(block.text, flush=True)
            except Exception as exc:  # noqa: BLE001
                logger.warning("No se pudo obtener el resumen final: %s", exc)

        return final_summary
