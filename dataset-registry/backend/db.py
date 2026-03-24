import json
import os
from contextlib import contextmanager
from uuid import UUID

import psycopg2
import psycopg2.extras

psycopg2.extras.register_uuid()

DB_HOST = os.getenv("DB_HOST", "registry-db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "registry")
DB_PASSWORD = os.getenv("DB_PASSWORD", "registry")
DB_NAME = os.getenv("DB_NAME", "registry")

INIT_SQL = """\
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS datasets (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    source        TEXT NOT NULL UNIQUE,
    ol_namespace  TEXT NOT NULL,
    ol_name       TEXT NOT NULL,
    description   TEXT,
    schema_fields JSONB,
    tags          TEXT[],
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ol_namespace, ol_name)
);
"""


def _dsn() -> str:
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


@contextmanager
def get_conn():
    conn = psycopg2.connect(_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
    print("Database initialized")


def _row_to_dict(row, columns) -> dict:
    d = dict(zip(columns, row))
    if d.get("schema_fields") and isinstance(d["schema_fields"], str):
        d["schema_fields"] = json.loads(d["schema_fields"])
    if d.get("tags") is None:
        d["tags"] = []
    return d


COLUMNS = "id, name, source, ol_namespace, ol_name, description, schema_fields, tags, created_at, updated_at"


def create_dataset(name: str, source: str, ol_namespace: str, ol_name: str,
                   description: str | None, schema_fields: list[dict] | None,
                   tags: list[str] | None) -> dict:
    sf_json = json.dumps(schema_fields) if schema_fields else None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO datasets (name, source, ol_namespace, ol_name, description, schema_fields, tags) "
                f"VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING {COLUMNS}",
                (name, source, ol_namespace, ol_name, description, sf_json, tags),
            )
            row = cur.fetchone()
            cols = [desc[0] for desc in cur.description]
    return _row_to_dict(row, cols)


def get_dataset_by_id(dataset_id: UUID) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM datasets WHERE id = %s", (dataset_id,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
    return _row_to_dict(row, cols)


def get_dataset_by_source(source: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM datasets WHERE source = %s", (source,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
    return _row_to_dict(row, cols)


def list_datasets(tag: str | None = None) -> list[dict]:
    clauses = []
    params: list = []
    if tag:
        clauses.append("%s = ANY(tags)")
        params.append(tag)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {COLUMNS} FROM datasets {where} ORDER BY created_at DESC",
                params,
            )
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    return [_row_to_dict(r, cols) for r in rows]


def update_dataset(dataset_id: UUID, name: str | None, description: str | None,
                   schema_fields: list[dict] | None, tags: list[str] | None) -> dict | None:
    sets = ["updated_at = now()"]
    params: list = []

    if name is not None:
        sets.append("name = %s")
        params.append(name)
    if description is not None:
        sets.append("description = %s")
        params.append(description)
    if schema_fields is not None:
        sets.append("schema_fields = %s")
        params.append(json.dumps(schema_fields))
    if tags is not None:
        sets.append("tags = %s")
        params.append(tags)

    params.append(dataset_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE datasets SET {', '.join(sets)} WHERE id = %s RETURNING {COLUMNS}",
                params,
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
    return _row_to_dict(row, cols)


def delete_dataset(dataset_id: UUID) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM datasets WHERE id = %s", (dataset_id,))
            return cur.rowcount > 0
