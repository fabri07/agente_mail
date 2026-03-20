from __future__ import annotations

import argparse

from app.config import SCOPES, load_settings
from app.db import SQLiteInventory
from app.extractor import (
    normalize_message,
    now_utc_iso,
    summarize_top_domains,
    summarize_top_labels,
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

    settings = load_settings()
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
    except FileNotFoundError as exc:
        logger.error(str(exc))
        print(f"ERROR: {exc}")
        return 2

    conn = inventory.connect()
    inventory.init_schema(conn)

    started_at = now_utc_iso()
    run_id = inventory.create_run(conn, started_at=started_at, mode=mode)

    scanned = 0
    saved = 0
    errors = 0
    normalized_records: list[dict] = []

    batch_size = 100
    pending_writes = 0

    try:
        for message_id in gmail.iter_message_ids(service, max_results=max_results):
            scanned += 1
            try:
                raw = gmail.get_message_metadata(service, message_id)
                normalized = normalize_message(raw, processed_at=now_utc_iso())
                inventory.upsert_message(conn, normalized)
                saved += 1
                normalized_records.append(normalized)
                pending_writes += 1
                if pending_writes >= batch_size:
                    conn.commit()
                    pending_writes = 0
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.exception("Error procesando message_id=%s: %s", message_id, exc)

            if scanned % settings.progress_every == 0:
                logger.info("Progreso: escaneados=%s guardados=%s errores=%s", scanned, saved, errors)
    finally:
        if pending_writes > 0:
            conn.commit()
        finished_at = now_utc_iso()
        inventory.finalize_run(conn, run_id, finished_at, scanned, saved, errors)
        conn.close()

    top_domains = summarize_top_domains(normalized_records, n=10)
    top_labels = summarize_top_labels(normalized_records, n=10)

    print("\n===== RESUMEN RUN =====")
    print(f"Run ID: {run_id}")
    print(f"Total escaneados: {scanned}")
    print(f"Total guardados SQLite: {saved}")
    print(f"Total errores: {errors}")

    print("\nTop 10 dominios remitentes:")
    for domain, count in top_domains:
        print(f"- {domain}: {count}")

    print("\nTop 10 labels encontradas:")
    for label, count in top_labels:
        print(f"- {label}: {count}")

    logger.info("Run finalizado run_id=%s scanned=%s saved=%s errors=%s", run_id, scanned, saved, errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
