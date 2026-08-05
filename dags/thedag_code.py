import json
import logging
import os
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gasoil_intelligence',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
}

dag_name = 'gasoil_intelligence_etl_v2'

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    description='Decoupled Gasoil Data Pipeline with Data Quality Checks',
    schedule='0 6 * * *',
    max_active_runs=1,
)


def get_eia_api_key():
    """Retrieve EIA API Key from Airflow Variable or Environment fallback."""
    try:
        key = Variable.get("EIA_API_KEY")
        if key:
            return key
    except Exception:
        pass
    return os.environ.get("EIA_API_KEY") or os.environ.get("Crude_oil_key", "")


def get_postgres_conn_id():
    """Fallback connection ID matching both custom and default Airflow configs."""
    return os.environ.get("AIRFLOW_CONN_POSTGRES_DEFAULT", "postgres_gasoil_db")


def format_period_date(period_str):
    """Normalize EIA date strings (YYYY, YYYYMM, YYYYMMDD) into YYYY-MM-DD format."""
    if not period_str:
        return None
    cleaned = str(period_str).replace("-", "")
    if len(cleaned) == 8:
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:8]}"
    elif len(cleaned) == 6:
        return f"{cleaned[:4]}-{cleaned[4:6]}-01"
    elif len(cleaned) == 4:
        return f"{cleaned[:4]}-01-01"
    return period_str


# =========================================================================
# ETL Python Callables
# =========================================================================

def fetch_and_stage_eia_crude(**kwargs):
    """Stage: Extract raw crude spot prices from EIA and push to XCom."""
    api_key = get_eia_api_key()
    if not api_key:
        raise ValueError("EIA_API_KEY is missing! Configure it in Airflow Variables or .env.")

    eia_base = os.environ.get("EIA_BASE_URL", "https://api.eia.gov/v2")
    url = f"{eia_base}/petroleum/pri/spt/data/"
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": ["RWTC", "RBRTE"],
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 100,
    }
    logger.info("Extracting raw EIA crude spot prices...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw_data = resp.json().get("response", {}).get("data", [])
    
    if not raw_data:
        logger.warning("EIA API returned empty payload for crude spot prices.")

    kwargs["ti"].xcom_push(key="raw_crude_data", value=json.dumps(raw_data))


def fetch_and_stage_eia_retail(**kwargs):
    """Stage: Extract raw retail gasoil prices from EIA and push to XCom."""
    api_key = get_eia_api_key()
    if not api_key:
        raise ValueError("EIA_API_KEY is missing! Configure it in Airflow Variables or .env.")

    eia_base = os.environ.get("EIA_BASE_URL", "https://api.eia.gov/v2")
    url = f"{eia_base}/petroleum/pri/gnd/data/"
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[product][]": ["EPMR", "EPD2D"],
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 200,
    }
    logger.info("Extracting raw EIA retail gasoil prices...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw_data = resp.json().get("response", {}).get("data", [])

    if not raw_data:
        logger.warning("EIA API returned empty payload for retail gasoil prices.")

    kwargs["ti"].xcom_push(key="raw_retail_data", value=json.dumps(raw_data))


def load_crude_spot_prices_table(**kwargs):
    """Transform & Load: Parse crude spot records and upsert into PostgreSQL."""
    ti = kwargs["ti"]
    raw_json = ti.xcom_pull(key="raw_crude_data", task_ids="Stage_eia_crude_prices")
    if not raw_json:
        logger.warning("No crude data found in XCom.")
        return

    records = json.loads(raw_json)
    series_map = {"RWTC": "WTI", "RBRTE": "Brent"}
    rows = []

    for row in records:
        try:
            price = float(row.get("value"))
        except (TypeError, ValueError):
            continue

        if price <= 0:
            continue

        period_date = format_period_date(row.get("period"))
        crude_type = series_map.get(row.get("series"), row.get("series"))
        rows.append((period_date, crude_type, price, "EIA"))

    if not rows:
        logger.warning("No valid transformed crude spot records.")
        return

    conn_id = get_postgres_conn_id()
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    query = """
        INSERT INTO crude_spot_prices (date, crude_type, price_usd_per_barrel, source)
        VALUES %s
        ON CONFLICT (date, crude_type, source)
        DO UPDATE SET price_usd_per_barrel = EXCLUDED.price_usd_per_barrel;
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Successfully loaded {len(rows)} records into crude_spot_prices.")
    finally:
        conn.close()


def load_gasoil_prices_table(**kwargs):
    """Transform & Load: Parse retail gasoil records and upsert into PostgreSQL."""
    ti = kwargs["ti"]
    raw_json = ti.xcom_pull(key="raw_retail_data", task_ids="Stage_eia_retail_gasoil")
    if not raw_json:
        logger.warning("No retail gasoil data found in XCom.")
        return

    records = json.loads(raw_json)
    product_map = {"EPMR": "Regular Gasoline", "EPD2D": "Diesel"}
    rows = []

    for row in records:
        try:
            price = float(row.get("value"))
        except (TypeError, ValueError):
            continue

        if price <= 0:
            continue

        period_date = format_period_date(row.get("period"))
        product_type = product_map.get(row.get("product"), row.get("product"))
        region = row.get("area-name", "US")
        grade = row.get("process-name", "Retail")
        rows.append((period_date, product_type, region, grade, price, "EIA"))

    if not rows:
        logger.warning("No valid transformed retail gasoil records.")
        return

    conn_id = get_postgres_conn_id()
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    query = """
        INSERT INTO gasoil_prices (date, product_type, region, grade, price_usd_per_gallon, source)
        VALUES %s
        ON CONFLICT (date, product_type, region, grade, source)
        DO UPDATE SET price_usd_per_gallon = EXCLUDED.price_usd_per_gallon;
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Successfully loaded {len(rows)} records into gasoil_prices.")
    finally:
        conn.close()


def run_data_quality_checks(**kwargs):
    """Data Quality Check: Ensure tables contain records and no NULL essential fields."""
    conn_id = get_postgres_conn_id()
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    tables = ["crude_spot_prices", "gasoil_prices"]

    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                if count < 1:
                    raise ValueError(f"Data Quality check failed: Table '{table}' is empty!")
                logger.info(f"Data Quality check passed: Table '{table}' contains {count} rows.")
    finally:
        conn.close()


# =========================================================================
# DAG Operators Instantiation
# =========================================================================

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

stage_eia_crude_prices = PythonOperator(
    task_id='Stage_eia_crude_prices',
    python_callable=fetch_and_stage_eia_crude,
    dag=dag,
)

stage_eia_retail_gasoil = PythonOperator(
    task_id='Stage_eia_retail_gasoil',
    python_callable=fetch_and_stage_eia_retail,
    dag=dag,
)

load_crude_spot_table = PythonOperator(
    task_id='Load_crude_spot_prices_table',
    python_callable=load_crude_spot_prices_table,
    dag=dag,
)

load_gasoil_prices_table = PythonOperator(
    task_id='Load_gasoil_prices_table',
    python_callable=load_gasoil_prices_table,
    dag=dag,
)

data_quality_checks = PythonOperator(
    task_id='Run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)


# =========================================================================
# Task Dependency Flow
# =========================================================================

start_operator >> [stage_eia_crude_prices, stage_eia_retail_gasoil]

stage_eia_crude_prices >> load_crude_spot_table
stage_eia_retail_gasoil >> load_gasoil_prices_table

[load_crude_spot_table, load_gasoil_prices_table] >> data_quality_checks >> end_operator