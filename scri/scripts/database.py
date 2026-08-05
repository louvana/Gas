import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = None

for _ in range(5):
    temp_path = os.path.join(current_dir, ".env")
    if os.path.exists(temp_path):
        dotenv_path = temp_path
        break
    current_dir = os.path.dirname(current_dir)

if dotenv_path:
    load_dotenv(dotenv_path)
else:
    load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )


def upsert(conn, table, columns, rows, conflict_cols):
    if not rows:
        return

    cols_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_cols)

    update_cols = [c for c in columns if c not in conflict_cols]

    if update_cols:
        update_sql = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in update_cols)
        conflict_action = f"DO UPDATE SET {update_sql}"
    else:
        conflict_action = "DO NOTHING"

    query = f"""
        INSERT INTO {table} ({cols_sql})
        VALUES %s
        ON CONFLICT ({conflict_sql}) {conflict_action}
    """

    with conn.cursor() as cur:
        execute_values(cur, query, rows)
    conn.commit()