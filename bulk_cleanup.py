"""
bulk_cleanup.py — Limpieza masiva de la bandeja de entrada (Fase 1).

Pasos:
  1. sync     — Descarga TODOS los emails de Gmail a SQLite (sin IA, gratis)
  2. classify — Clasifica emails sin clasificar via Batches API + prompt caching
  3. apply    — Aplica acciones en Gmail (trash, labels) según clasificación

Uso:
  python bulk_cleanup.py              # los 3 pasos en secuencia
  python bulk_cleanup.py --step sync
  python bulk_cleanup.py --step classify
  python bulk_cleanup.py --step apply
  python bulk_cleanup.py --step apply --dry-run   # previsualizar sin tocar Gmail
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.config import SCOPES, load_settings
from app.db import SQLiteInventory
from app.extractor import normalize_message
from app.gmail_client import GmailClient
from app.logger import setup_logger

# ─────────────────────────────────────────────────────────────
# Prompt de clasificación (se cachea en todas las requests)
# ─────────────────────────────────────────────────────────────

CLASSIFY_PROMPT = """Clasifica este email. Responde SOLO con una de estas palabras:
urgente, empleo, personal, newsletter, notificacion, spam, factura, trabajo, otro

Criterios:
- urgente: requiere acción inmediata (citas médicas, alertas seguridad, vencimientos, IEFP, exámenes)
- empleo: ofertas de trabajo, reclutadores, RRHH, entrevistas, LinkedIn jobs
- personal: familia, amigos, contactos directos conocidos
- newsletter: boletines, marketing, promociones, listas de correo, Booking, webs comerciales
- notificacion: confirmaciones automáticas, alertas de sistemas, redes sociales, GitHub notifs
- spam: no deseado, sospechoso, phishing, Amway, MLM
- factura: facturas, recibos, comprobantes de pago
- trabajo: proyectos laborales, clientes, compañeros de trabajo
- otro: no encaja claramente en ninguna de las anteriores"""

# Categorías a mover a papelera automáticamente
TRASH_CATEGORIES = {"spam", "newsletter"}
# Etiquetas a aplicar
EMPLEO_CATEGORY = "empleo"

BATCH_ID_FILE = Path("db/bulk_batch_id.txt")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────
# PASO 1: Sincronizar todos los emails a SQLite
# ─────────────────────────────────────────────────────────────

def step_sync(gmail: GmailClient, service, db: SQLiteInventory, conn) -> int:
    logger = logging.getLogger("bulk_cleanup")
    print("\n📥 PASO 1: Sincronizando TODOS los emails de Gmail a SQLite...")
    print("   (sin tokens de IA — solo Gmail API)")

    synced = 0
    errors = 0
    batch: list[dict] = []
    now = now_iso()

    for msg_id in gmail.iter_message_ids(service):  # sin límite — todos
        try:
            raw = gmail.get_message_metadata(service, msg_id)
            payload = normalize_message(raw, now)
            batch.append(payload)

            if len(batch) >= 200:
                db.upsert_messages(conn, batch)
                conn.commit()
                synced += len(batch)
                batch = []
                print(f"   → {synced} emails guardados...", end="\r", flush=True)

        except Exception as exc:  # noqa: BLE001
            errors += 1
            logger.debug("Error procesando %s: %s", msg_id, exc)

    if batch:
        db.upsert_messages(conn, batch)
        conn.commit()
        synced += len(batch)

    print(f"\n✅ Sync completo: {synced} emails guardados, {errors} errores")
    logger.info("Sync completo: %d guardados, %d errores", synced, errors)
    return synced


# ─────────────────────────────────────────────────────────────
# PASO 2: Clasificar con Batches API + prompt caching
# ─────────────────────────────────────────────────────────────

def step_classify(db: SQLiteInventory, conn, api_key: str) -> None:
    import anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    logger = logging.getLogger("bulk_cleanup")
    client = anthropic.Anthropic(api_key=api_key)

    rows = conn.execute(
        """SELECT message_id, from_email, from_name, subject, snippet
           FROM messages
           WHERE classification IS NULL AND (is_trashed IS NULL OR is_trashed = 0)
           ORDER BY internal_date DESC"""
    ).fetchall()

    if not rows:
        print("\n✅ Todos los emails ya están clasificados. Pasa al paso apply.")
        return

    print(f"\n🤖 PASO 2: Clasificando {len(rows)} emails sin clasificar...")
    print("   Modelo: claude-haiku-4-5-20251001 + Batches API + prompt caching")

    # Retomar batch previo si fue interrumpido
    batch_id: str | None = None
    if BATCH_ID_FILE.exists():
        batch_id = BATCH_ID_FILE.read_text().strip()
        print(f"   ♻️  Retomando batch previo: {batch_id}")

    if not batch_id:
        requests = []
        valid_categories = {"urgente", "empleo", "personal", "newsletter", "notificacion",
                            "spam", "factura", "trabajo", "otro"}

        for row in rows:
            snippet = (row["snippet"] or "")[:250]
            subject = (row["subject"] or "(sin asunto)")[:120]
            from_info = f"{row['from_name'] or ''} <{row['from_email'] or ''}>".strip()
            user_content = f"De: {from_info}\nAsunto: {subject}\nSnippet: {snippet}"

            requests.append(Request(
                custom_id=row["message_id"],
                params=MessageCreateParamsNonStreaming(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=15,
                    system=[{
                        "type": "text",
                        "text": CLASSIFY_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_content}],
                ),
            ))

        print(f"   Enviando {len(requests)} requests...")
        batch = client.messages.batches.create(requests=requests)
        batch_id = batch.id
        BATCH_ID_FILE.write_text(batch_id)
        print(f"   ✅ Batch creado: {batch_id}")
        print("   (Si se interrumpe, el script retomará este batch automáticamente)")

    # Polling hasta completar
    print("   ⏳ Esperando resultados (puede tardar entre 15 y 60 minutos)...\n")
    while True:
        status = client.messages.batches.retrieve(batch_id)
        c = status.request_counts
        total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
        done = c.succeeded + c.errored + c.canceled + c.expired
        pct = int(done / total * 100) if total else 0
        print(f"   Progreso: {done}/{total} ({pct}%) — en proceso: {c.processing}   ", end="\r", flush=True)

        if status.processing_status == "ended":
            print(f"\n   ✅ Batch finalizado: {c.succeeded} OK, {c.errored} errores")
            break
        time.sleep(30)

    # Guardar resultados en DB
    now = now_iso()
    valid_categories = {"urgente", "empleo", "personal", "newsletter", "notificacion",
                        "spam", "factura", "trabajo", "otro"}
    saved = 0
    errors = 0

    for result in client.messages.batches.results(batch_id):
        if result.result.type == "succeeded":
            raw_text = result.result.message.content[0].text.strip().lower()
            # Limpiar respuesta (a veces el modelo añade puntuación)
            category = raw_text.split()[0].rstrip(".,;:") if raw_text else "otro"
            if category not in valid_categories:
                category = "otro"

            conn.execute(
                "UPDATE messages SET classification = ?, agent_processed_at = ? WHERE message_id = ?",
                (category, now, result.custom_id),
            )
            saved += 1
        else:
            errors += 1
            logger.warning("Batch error para %s: %s", result.custom_id, result.result.type)

    conn.commit()
    BATCH_ID_FILE.unlink(missing_ok=True)

    # Resumen de clasificación
    stats = conn.execute(
        "SELECT classification, COUNT(*) n FROM messages WHERE classification IS NOT NULL "
        "GROUP BY classification ORDER BY n DESC"
    ).fetchall()
    print(f"\n   💾 {saved} clasificaciones guardadas, {errors} errores\n")
    print("   Distribución:")
    for row in stats:
        print(f"     {row['classification']:15s}: {row['n']:>5}")


# ─────────────────────────────────────────────────────────────
# PASO 3: Aplicar acciones en Gmail
# ─────────────────────────────────────────────────────────────

def step_apply(gmail: GmailClient, service, db: SQLiteInventory, conn, dry_run: bool = False) -> None:
    logger = logging.getLogger("bulk_cleanup")
    mode_label = "DRY-RUN (sin cambios reales)" if dry_run else "REAL"
    print(f"\n⚡ PASO 3: Aplicando acciones en Gmail [{mode_label}]...")

    # ── Papelera: spam + newsletter ──────────────────────────
    trash_rows = conn.execute(
        """SELECT message_id FROM messages
           WHERE classification IN ('spam', 'newsletter')
           AND (is_trashed IS NULL OR is_trashed = 0)"""
    ).fetchall()
    trash_ids = [r["message_id"] for r in trash_rows]
    print(f"\n   🗑️  A papelera (spam + newsletter): {len(trash_ids)} emails")

    if not dry_run and trash_ids:
        moved = 0
        for i in range(0, len(trash_ids), 1000):
            chunk = trash_ids[i : i + 1000]
            try:
                service.users().messages().batchModify(
                    userId="me",
                    body={
                        "ids": chunk,
                        "addLabelIds": ["TRASH"],
                        "removeLabelIds": ["INBOX", "UNREAD"],
                    },
                ).execute()
                conn.executemany(
                    "UPDATE messages SET is_trashed = 1 WHERE message_id = ?",
                    [(mid,) for mid in chunk],
                )
                conn.commit()
                moved += len(chunk)
                print(f"      → {moved}/{len(trash_ids)} movidos a papelera...", end="\r", flush=True)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error batchModify trash: %s", exc)
        print(f"\n   ✅ {moved} emails movidos a papelera")

    # ── Etiqueta EMPLEO ─────────────────────────────────────
    empleo_rows = conn.execute(
        """SELECT message_id FROM messages
           WHERE classification = 'empleo'
           AND (is_trashed IS NULL OR is_trashed = 0)"""
    ).fetchall()
    empleo_ids = [r["message_id"] for r in empleo_rows]
    print(f"   💼 Etiqueta EMPLEO: {len(empleo_ids)} emails")

    if not dry_run and empleo_ids:
        try:
            label_id = gmail.create_label_if_not_exists(service, "EMPLEO")
            for i in range(0, len(empleo_ids), 1000):
                chunk = empleo_ids[i : i + 1000]
                service.users().messages().batchModify(
                    userId="me",
                    body={"ids": chunk, "addLabelIds": [label_id]},
                ).execute()
            print(f"   ✅ {len(empleo_ids)} emails etiquetados como EMPLEO")
        except Exception as exc:  # noqa: BLE001
            logger.error("Error etiquetando empleo: %s", exc)

    # ── Marcar leídos: notificaciones y newsletters ─────────
    unread_rows = conn.execute(
        """SELECT message_id FROM messages
           WHERE classification IN ('notificacion', 'newsletter', 'spam')
           AND is_unread = 1
           AND (is_trashed IS NULL OR is_trashed = 0)"""
    ).fetchall()
    unread_ids = [r["message_id"] for r in unread_rows]
    print(f"   ✉️  Marcar leídos (notif/newsletter/spam): {len(unread_ids)} emails")

    if not dry_run and unread_ids:
        for i in range(0, len(unread_ids), 1000):
            chunk = unread_ids[i : i + 1000]
            try:
                service.users().messages().batchModify(
                    userId="me",
                    body={"ids": chunk, "removeLabelIds": ["UNREAD"]},
                ).execute()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error marcando leídos: %s", exc)
        print(f"   ✅ {len(unread_ids)} emails marcados como leídos")

    # ── Resumen final ────────────────────────────────────────
    total_inbox = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE (is_trashed IS NULL OR is_trashed = 0)"
    ).fetchone()[0]
    total_trashed = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE is_trashed = 1"
    ).fetchone()[0]

    print(f"\n📊 Resultado final:")
    print(f"   Emails en inbox (conservados): {total_inbox}")
    print(f"   Emails a papelera (total):     {total_trashed}")

    stats = conn.execute(
        """SELECT classification, COUNT(*) n FROM messages
           WHERE classification IS NOT NULL AND (is_trashed IS NULL OR is_trashed = 0)
           GROUP BY classification ORDER BY n DESC"""
    ).fetchall()
    print("\n   Por categoría (inbox):")
    for row in stats:
        print(f"     {row['classification']:15s}: {row['n']:>5}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Limpieza masiva de Gmail — Fase 1")
    parser.add_argument(
        "--step",
        choices=["sync", "classify", "apply"],
        help="Ejecutar solo un paso (default: los 3 en secuencia)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="En el paso apply: previsualizar acciones sin modificar Gmail",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings()
    setup_logger(settings.log_file, settings.log_level)

    if not settings.anthropic_api_key or settings.anthropic_api_key.startswith("sk-ant-..."):
        print("❌ ANTHROPIC_API_KEY no configurada en .env")
        return 1

    print("\n" + "=" * 60)
    print("  BULK CLEANUP — Limpieza masiva de Gmail")
    print("=" * 60)

    gmail = GmailClient(settings.credentials_file, settings.token_file, SCOPES)
    db = SQLiteInventory(settings.db_path)

    print("\n🔐 Autenticando con Gmail...")
    service = gmail.build_service()
    print("✅ Gmail conectado\n")

    conn = db.connect()
    db.init_schema(conn)
    run_all = args.step is None

    try:
        if run_all or args.step == "sync":
            step_sync(gmail, service, db, conn)

        if run_all or args.step == "classify":
            step_classify(db, conn, settings.anthropic_api_key)

        if run_all or args.step == "apply":
            step_apply(gmail, service, db, conn, dry_run=args.dry_run)

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrumpido. Puedes retomar desde cualquier paso con --step.")
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("bulk_cleanup").exception("Error fatal: %s", exc)
        print(f"\n❌ Error fatal: {exc}")
        return 2
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("  ✅ BULK CLEANUP COMPLETADO")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
