import os
import time
import logging
from datetime import datetime, timedelta
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gasoil_intelligence',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Configuration Constants
EIA_BASE = os.environ.get("EIA_BASE_URL", "https://api.eia.gov/v2")
EIA_KEY = os.environ.get("EIA_API_KEY", "")
NHTSA_BASE = os.environ.get("NHTSA_BASE_URL", "https://api.nhtsa.gov")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
FUEL_KEYWORDS = ["FUEL", "GAS", "ENGINE"]



# Helper Functions (API Request & Database Upsert Logic)


def get_pg_connection():
    """Retrieves standard psycopg2 connection object from Airflow Connection."""
    hook = PostgresHook(postgres_conn_id="postgres_gasoil_db")
    return hook.get_conn()


def upsert_rows(conn, table, columns, rows, conflict_cols):
    """Executes SQL ON CONFLICT UPSERT using psycopg2 execute_values."""
    if not rows:
        logger.warning(f"No rows provided to upsert into {table}.")
        return

    cols_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_cols)
    update_cols = [c for c in columns if c not in conflict_cols]

    if update_cols:
        update_sql = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
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
    logger.info(f"Successfully upserted {len(rows)} rows into {table}.")


def get_with_retries(url, params, max_retries=MAX_RETRIES):
    """Fetches HTTP data with retry backoff for rate-limiting (429)."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                logger.warning("Rate limited (429), retrying in %ss", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts") from last_exc


def extract_eia_payload(resp):
    payload = resp.json()
    if "response" not in payload or "data" not in payload["response"]:
        raise RuntimeError(f"Unexpected EIA response shape: {payload}")
    return payload["response"]["data"]


def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None



# Airflow DAG Definition

@dag(
    dag_id='gasoil_intelligence_etl_v2',
    default_args=default_args,
    description="Live Pipeline: Ingest EIA spot/retail prices and NHTSA vehicle complaints into Postgres",
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gasoil', 'eia', 'nhtsa', 'production']
)
def gasoil_intelligence_pipeline():

    @task(execution_timeout=timedelta(minutes=10))
    def fetch_eia_crude_prices():
        """Fetches daily Brent & WTI Crude spot prices from EIA API."""
        logger.info("Starting EIA Crude Spot Prices ingestion...")
        url = f"{EIA_BASE}/petroleum/pri/spt/data/"
        params = {
            "api_key": EIA_KEY,
            "frequency": "daily",
            "data[0]": "value",
            "facets[series][]": ["RWTC", "RBRTE"],
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 100,
        }

        resp = get_with_retries(url, params)
        data = extract_eia_payload(resp)

        series_map = {"RWTC": "WTI", "RBRTE": "Brent"}
        rows = []

        for row in data:
            price = safe_float(row.get("value"))
            if price is None:
                continue
            crude_type = series_map.get(row.get("series"), row.get("series"))
            rows.append((row["period"], crude_type, price, "EIA"))

        conn = get_pg_connection()
        try:
            upsert_rows(
                conn=conn,
                table="crude_spot_prices",
                columns=["date", "crude_type", "price_usd_per_barrel", "source"],
                rows=rows,
                conflict_cols=["date", "crude_type", "source"],
            )
        finally:
            conn.close()

    @task(execution_timeout=timedelta(minutes=10))
    def fetch_eia_retail_gasoil():
        """Fetches weekly Retail Gasoline & Diesel prices from EIA API."""
        logger.info("Starting EIA Retail Gasoil Prices ingestion...")
        url = f"{EIA_BASE}/petroleum/pri/gnd/data/"
        params = {
            "api_key": EIA_KEY,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": ["EPMR", "EPD2D"],
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 200,
        }

        resp = get_with_retries(url, params)
        data = extract_eia_payload(resp)

        product_map = {"EPMR": "Regular Gasoline", "EPD2D": "Diesel"}
        rows = []

        for row in data:
            price = safe_float(row.get("value"))
            if price is None:
                continue

            rows.append((
                row["period"],
                product_map.get(row.get("product"), row.get("product")),
                row.get("area-name", "US"),
                row.get("process-name", "Retail"),
                price,
                "EIA",
            ))

        conn = get_pg_connection()
        try:
            upsert_rows(
                conn=conn,
                table="gasoil_prices",
                columns=["date", "product_type", "region", "grade", "price_usd_per_gallon", "source"],
                rows=rows,
                conflict_cols=["date", "product_type", "region", "grade", "source"],
            )
        finally:
            conn.close()

    @task(execution_timeout=timedelta(minutes=10))
    def fetch_nhtsa_complaints():
        """Fetches fuel & engine vehicle complaints from NHTSA API."""
        logger.info("Starting NHTSA Complaints ingestion...")
        vehicles = [("Toyota", "Camry", 2023), ("Ford", "F-150", 2023)]
        
        columns = [
            "complaint_id", "date", "make", "model", "year",
            "component", "fuel_related_flag", "description",
        ]
        rows = []

        for make, model, year in vehicles:
            url = f"{NHTSA_BASE}/complaints/complaintsByVehicle"
            query_params = {"make": make, "model": model, "modelYear": year}
            try:
                resp = requests.get(url, params=query_params, timeout=30)
                resp.raise_for_status()
                complaints = resp.json().get("results", [])
            except Exception as e:
                logger.error(f"Skipping {make} {model} {year}: {e}")
                continue

            for c in complaints:
                component = c.get("components", "")
                is_fuel_related = any(k in component.upper() for k in FUEL_KEYWORDS)
                date_str = c.get("dateComplaintFiled")
                
                try:
                    complaint_date = datetime.strptime(date_str, "%m/%d/%Y").date() if date_str else None
                except (ValueError, TypeError):
                    complaint_date = None

                rows.append((
                    c.get("odiNumber"),
                    complaint_date,
                    make,
                    model,
                    year,
                    component,
                    is_fuel_related,
                    (c.get("summary") or "")[:5000],
                ))

        conn = get_pg_connection()
        try:
            upsert_rows(
                conn=conn,
                table="nhtsa_complaints",
                columns=columns,
                rows=rows,
                conflict_cols=["complaint_id"],
            )
        finally:
            conn.close()

    # Define parallel task execution sequence
    t1 = fetch_eia_crude_prices()
    t2 = fetch_eia_retail_gasoil()
    t3 = fetch_nhtsa_complaints()

# Instantiate DAG
dag_instance = gasoil_intelligence_pipeline()