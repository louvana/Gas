import os
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv
#pour monter parent par parent
current_dir = Path(__file__).resolve().parent
dotenv_path = None

#on monte max 5 niveaux pour trouver .env
for _ in range(5):
    temp_path = current_dir / ".env"
    if temp_path.exists():
        dotenv_path = str(temp_path)
        break
    current_dir = current_dir.parent
#charger les mots de passe et les variables depuis le fichier .env
if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path)
    print(f".env chargé depuis : {dotenv_path}")
else:
    #tente de la chercher 
    load_dotenv()
    print(".env chargé depuis le répertoire par défaut ou variables système.")


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )


def upsert(conn, table, columns, rows, conflict_cols):
    #si la liste des lignes est vide on retourne
    if not rows:
        return
#on transforme la liste en chaine sql
    cols_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_cols)
    #on choisit la colonne qui ne cause pas de conflit
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
    