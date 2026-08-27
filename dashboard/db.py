import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")


@st.cache_resource(show_spinner=False)
def get_engine():
    
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


def test_connection() -> tuple[bool, str]:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connected"
    except Exception as e:
        return False, str(e)


@st.cache_data(ttl=300, show_spinner=False)
def list_tables(schema: str = "public") -> list[str]:
    
    query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(query, {"schema": schema}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def get_row_count(table: str, schema: str = "public") -> int:
    query = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    with get_engine().connect() as conn:
        return conn.execute(query).scalar_one()


@st.cache_data(ttl=120, show_spinner=True)
def load_table(table: str, schema: str = "public", limit: int | None = None) -> pd.DataFrame:
    """
    Loads a table into a DataFrame. `limit=None` loads everything —
    fine for scraped-data-sized tables, but if a table gets huge later,
    call this with a limit from the UI instead of removing the cap here.
    """
    sql = f'SELECT * FROM "{schema}"."{table}"'
    if limit:
        sql += f" LIMIT {int(limit)}"
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def run_query(sql: str) -> pd.DataFrame:
    """For the free-form SQL box — deliberately NOT cached, always fresh."""
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)
