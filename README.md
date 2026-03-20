# gmail_agent — ETAPA 1 (Read-only + Inventario SQLite)

MVP local y auditable para conectarse a Gmail con OAuth, leer metadata de correos de forma paginada y construir un inventario local en SQLite.

> **Importante:** esta etapa es estrictamente de solo lectura. No modifica emails, no archiva, no borra, no etiqueta, no envía y no altera el inbox.

## Alcance de ETAPA 1

- Autenticación OAuth contra Gmail API.
- Cliente Gmail **read-only** (`gmail.readonly`).
- Lectura paginada de IDs de mensajes.
- Extracción de metadata por mensaje (sin descargar cuerpo completo).
- Persistencia en SQLite:
  - tabla `messages`
  - tabla `runs`
- Logs en archivo y consola.
- Resumen final de ejecución:
  - escaneados / guardados / errores
  - top 10 dominios remitentes
  - top 10 labels

## Estructura

```text
gmail_agent/
  main.py
  requirements.txt
  .env.example
  README.md
  /app
    __init__.py
    config.py
    gmail_client.py
    extractor.py
    db.py
    logger.py
    utils.py
  /db
  /logs
  /data
```

## Prerrequisitos

- Python 3.11+
- Cuenta de Google con Gmail
- Proyecto en Google Cloud con Gmail API habilitada

## 1) Crear y activar entorno virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 2) Instalar dependencias

```bash
pip install -r requirements.txt
```

## 3) Habilitar Gmail API y obtener `credentials.json`

1. Ir a Google Cloud Console.
2. Crear/seleccionar proyecto.
3. Habilitar **Gmail API**.
4. Configurar pantalla OAuth (External o Internal según tu cuenta).
5. Crear credenciales OAuth tipo **Desktop app**.
6. Descargar el archivo JSON.
7. Guardarlo como `credentials.json` en la raíz del proyecto.

Ruta esperada por defecto:

```text
./credentials.json
```

> Si falta este archivo, el script falla con mensaje claro y no continúa.

## 4) Configuración de variables de entorno

Copiar ejemplo:

```bash
cp .env.example .env
```

Variables principales:

- `GMAIL_CREDENTIALS_FILE` (default: `credentials.json`)
- `GMAIL_TOKEN_FILE` (default: `token.json`)
- `DB_PATH` (default: `db/gmail_agent.db`)
- `LOG_FILE` (default: `logs/run.log`)
- `PROGRESS_EVERY` (default: `100`)

> `PROGRESS_EVERY` debe ser un entero positivo. El token OAuth se guarda con permisos restrictivos cuando el sistema operativo lo permite.

## 5) Primera ejecución (autenticación OAuth + prueba)

Comando recomendado para prueba inicial:

```bash
python main.py --max-results 100
```

Qué pasa en esta ejecución:

- Se abre navegador para consentimiento OAuth (primera vez).
- Se genera `token.json` al autenticar correctamente.
- Se crea la base SQLite `db/gmail_agent.db` si no existe.
- Se crean tablas `messages` y `runs` si no existen.
- La tabla `runs` registra estado final (`completed`, `completed_with_errors`, `failed`) y error fatal si aplica.
- Se recorre Gmail de forma paginada y read-only.
- El guardado en SQLite se hace en lotes para mejorar rendimiento.

## 6) Escaneo completo (full scan)

```bash
python main.py --full-scan
```

## Modos soportados

- Prueba limitada:
  - `python main.py --max-results 100`
- Full scan:
  - `python main.py --full-scan`
- Por defecto (sin flags):
  - `python main.py`

> `--full-scan` y `--max-results` son excluyentes.

## Seguridad (estrictamente read-only)

Implementado con:

- Scope OAuth único: `https://www.googleapis.com/auth/gmail.readonly`
- Uso de endpoints de lectura (`messages.list`, `messages.get` metadata)
- Exclusión recomendada en git para `credentials.json`, `token.json`, `.env`, bases SQLite y logs.
- No existen llamadas a:
  - `modify`
  - `trash`
  - `delete`
  - `send`
  - `insert`

## Limitaciones de ETAPA 1

- No descarga cuerpo completo del email.
- No procesa adjuntos (solo detecta si existen por metadata).
- No realiza clasificación, acciones ni automatizaciones.
- No incluye interfaz web ni frontend.
- No incluye LLM ni features avanzadas.

## Troubleshooting

- **Error por `credentials.json` faltante**:
  - Verificar archivo en raíz o ajustar `GMAIL_CREDENTIALS_FILE`.
- **No abre browser para OAuth**:
  - Verificar entorno local con GUI o configurar autorización manual según Google OAuth.
- **Errores temporales de API**:
  - Revisar `logs/run.log`; el proceso continúa y contabiliza errores.

## Salidas generadas

- Token OAuth: `token.json`
- Base inventario: `db/gmail_agent.db`
- Logs: `logs/run.log`

---

Este repositorio implementa únicamente ETAPA 1: **autenticación + lectura + inventario + SQLite + logs**.
