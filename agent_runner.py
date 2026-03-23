"""
Punto de entrada del agente de email.

Uso:
    python agent_runner.py                  # sesión de 1 hora
    python agent_runner.py --duration 30    # sesión de 30 minutos
    python agent_runner.py --duration 60    # sesión de 60 minutos (default)
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Asegurar que el directorio del proyecto esté en el path
sys.path.insert(0, str(Path(__file__).parent))

from app.agent import GmailAgent
from app.agent_tools import ToolExecutor
from app.config import SCOPES, load_settings
from app.db import SQLiteInventory
from app.gmail_client import GmailClient
from app.logger import setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Agente autónomo de gestión de emails con Claude"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duración máxima de la sesión en minutos (default: valor de AGENT_SESSION_DURATION en .env o 60)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings()
    setup_logger(settings.log_file, settings.log_level)
    logger = logging.getLogger("gmail_agent")

    # Validar API key de Anthropic
    if not settings.anthropic_api_key or settings.anthropic_api_key.startswith("sk-ant-..."):
        print("❌ ERROR: ANTHROPIC_API_KEY no configurada en .env")
        print("   Obtén tu API key en: https://console.anthropic.com")
        return 1

    # Calcular duración de sesión
    if args.duration:
        session_duration = args.duration * 60
    else:
        session_duration = settings.agent_session_duration

    session_id = str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*60}")
    print(f"  AGENTE DE EMAIL — Sesión {session_id}")
    print(f"  Inicio: {started_at}")
    print(f"  Duración máxima: {session_duration // 60} minutos")
    print(f"  Modelo: {settings.claude_model}")
    print(f"{'='*60}\n")

    logger.info("Iniciando sesión %s (duración: %d s)", session_id, session_duration)

    # Inicializar componentes
    gmail_client = GmailClient(
        credentials_file=settings.credentials_file,
        token_file=settings.token_file,
        scopes=SCOPES,
    )

    db = SQLiteInventory(settings.db_path)

    try:
        # Autenticar con Gmail
        print("🔐 Autenticando con Gmail...")
        service = gmail_client.build_service()
        print("✅ Gmail conectado\n")

        # Conectar base de datos
        conn = db.connect()
        db.init_schema(conn)

        # Construir el ejecutor de herramientas
        executor = ToolExecutor(
            gmail_client=gmail_client,
            service=service,
            db=db,
            conn=conn,
            session_id=session_id,
        )

        # Iniciar el agente
        agent = GmailAgent(
            settings=settings,
            tool_executor=executor,
            session_id=session_id,
            max_duration_seconds=session_duration,
        )

        print("🤖 Agente iniciado. Trabajando...\n")
        print("-" * 60)

        summary = agent.run()

        print("\n" + "=" * 60)
        print("  SESIÓN COMPLETADA")
        print(f"  Session ID: {session_id}")
        print("=" * 60)

        # Mostrar resumen de acciones
        actions = db.get_session_actions(conn, session_id)
        if actions:
            from collections import Counter
            counts = Counter(a["action_type"] for a in actions)
            print("\n📊 Acciones realizadas:")
            action_labels = {
                "sync": "Emails sincronizados",
                "classify": "Emails clasificados",
                "trash": "Emails a papelera",
                "draft_created": "Borradores creados",
                "unsubscribe": "Desuscripciones",
                "label_applied": "Etiquetas aplicadas",
                "mark_read": "Marcados como leídos",
            }
            for action_type, count in counts.most_common():
                label = action_labels.get(action_type, action_type)
                print(f"   • {label}: {count}")

        conn.close()
        return 0

    except FileNotFoundError as exc:
        print(f"\n❌ ERROR: {exc}")
        print("   Asegúrate de tener credentials.json en el directorio del proyecto.")
        return 1
    except KeyboardInterrupt:
        print("\n\n⚠️  Sesión interrumpida por el usuario")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error fatal en la sesión del agente: %s", exc)
        print(f"\n❌ Error fatal: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
