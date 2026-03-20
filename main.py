from __future__ import annotations

import argparse
import sqlite3
from collections import Counter

from app.config import SCOPES, load_settings
from app.db import SQLiteInventory
from app.extractor import (
    normalize_message,
    now_utc_iso,
    update_summary_counts,
)
from app.gmail_client import GmailClient
from app.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="gmail_agent ETAPA 1: inventario local read-only de Gmail en SQLite"
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Cantidad máxima de emails a escanear (ideal para pruebas).",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Escanea todos los mensajes disponibles en la casilla.",
    )
    return parser


def run() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.max_results is not None and args.max_results <= 0:
        parser.error("--max-results debe ser un entero positivo.")
    if args.full_scan and args.max_results is not None:
        parser.error("Usa solo uno: --full-scan o --max-results.")

    try:
        settings = load_settings()
    except ValueError as exc:
        parser.error(str(exc))

    logger = setup_logger(settings.log_file, settings.log_level)

    if args.full_scan:
        max_results = None
        mode = "full_scan"
    else:
        max_results = args.max_results
        mode = f"limited_{max_results}" if max_results else "default_scan"

    logger.info("Iniciando run modo=%s", mode)
    logger.info("Seguridad: ejecución estrictamente read-only (scope gmail.readonly)")

    gmail = GmailClient(settings.credentials_file, settings.token_file, SCOPES)
    inventory = SQLiteInventory(settings.db_path)

    try:
        service = gmail.build_service()
    except (FileNotFoundError, RuntimeError) as exc:
        logger.error(str(exc))
        print(f"ERROR: {exc}")
        return 2

    try:
        conn = inventory.connect()
        inventory.init_schema(conn)
        started_at = now_utc_iso()
        run_id = inventory.create_run(conn, started_at=started_at, mode=mode)
    except sqlite3.Error as exc:
        logger.error("No fue posible inicializar SQLite: %s", exc)
        print(f"ERROR: No fue posible inicializar SQLite: {exc}")
        return 2

    scanned = 0
    saved = 0
    errors = 0
    run_status = "completed"
    fatal_error: str | None = None

    batch_size = 100
    pending_records: list[dict] = []
    domain_counts: Counter[str] = Counter()
    label_counts: Counter[str] = Counter()

    try:
        for message_id in gmail.iter_message_ids(service, max_results=max_results):
            scanned += 1
            try:
                raw = gmail.get_message_metadata(service, message_id)
                normalized = normalize_message(raw, processed_at=now_utc_iso())
                update_summary_counts(normalized, domain_counts, label_counts)
                pending_records.append(normalized)
                if len(pending_records) >= batch_size:
                    inventory.upsert_messages(conn, pending_records)
                    conn.commit()
                    saved += len(pending_records)
                    pending_records.clear()
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.exception("Error procesando message_id=%s: %s", message_id, exc)

            if scanned % settings.progress_every == 0:
                logger.info(
                    "Progreso run_id=%s escaneados=%s guardados=%s pendientes=%s errores=%s",
                    run_id,
                    scanned,
                    saved,
                    len(pending_records),
                    errors,
                )
    except Exception as exc:  # noqa: BLE001
        run_status = "failed"
        fatal_error = str(exc)
        logger.exception("Fallo fatal durante el inventario run_id=%s: %s", run_id, exc)
    finally:
        try:
            if pending_records:
                inventory.upsert_messages(conn, pending_records)
                conn.commit()
                saved += len(pending_records)
                pending_records.clear()
        except Exception as exc:  # noqa: BLE001
            run_status = "failed"
            fatal_error = fatal_error or f"Error persistiendo batch final: {exc}"
            logger.exception("No fue posible persistir el batch final run_id=%s: %s", run_id, exc)
        finally:
            if run_status != "failed" and errors > 0:
                run_status = "completed_with_errors"

            finished_at = now_utc_iso()
            try:
                inventory.finalize_run(
                    conn,
                    run_id,
                    finished_at,
                    scanned,
                    saved,
                    errors,
                    status=run_status,
                    fatal_error=fatal_error,
                )
            except sqlite3.Error as exc:
                run_status = "failed"
                fatal_error = fatal_error or f"No fue posible registrar el cierre en SQLite: {exc}"
                logger.exception("No fue posible registrar el cierre de la run_id=%s: %s", run_id, exc)
            finally:
                conn.close()

    top_domains = domain_counts.most_common(10)
    top_labels = label_counts.most_common(10)

    print("\n===== RESUMEN RUN =====")
    print(f"Run ID: {run_id}")
    print(f"Estado: {run_status}")
    print(f"Total escaneados: {scanned}")
    print(f"Total guardados SQLite: {saved}")
    print(f"Total errores: {errors}")
    if fatal_error:
        print(f"Error fatal: {fatal_error}")

    print("\nTop 10 dominios remitentes:")
    for domain, count in top_domains:
        print(f"- {domain}: {count}")

    print("\nTop 10 labels encontradas:")
    for label, count in top_labels:
        print(f"- {label}: {count}")

    logger.info(
        "Run finalizado run_id=%s status=%s scanned=%s saved=%s errors=%s",
        run_id,
        run_status,
        scanned,
        saved,
        errors,
    )
    return 0 if run_status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(run())
