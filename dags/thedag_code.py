import json
import logging
from datetime import datetime, timedelta
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'dag_me',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

@dag(
    dag_id='gasoil_intelligence_etl_1',
    default_args=default_args,
    description="Pipeline ETL d'ingestion des prix du carburant vers PostgreSQL",
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gasoil', 'production', 'postgres']
)
def gasoil_etl_pipeline():

    @task()
    def extract_raw_data() -> str:
        logger.info("Extraction des données...")
        mock_raw_data = [
            {"station_id": 101, "ville": "Agadir", "produit": "Gasoil", "prix": "11.50", "date": "2026-07-30"},
            {"station_id": 102, "ville": "Casablanca", "produit": "Gasoil", "prix": "11.80", "date": "2026-07-30"},
            {"station_id": 103, "ville": "Marrakech", "produit": "Gasoil", "prix": None, "date": "2026-07-30"},
        ]
        return json.dumps(mock_raw_data)

    @task()
    def transform_data(raw_data_json: str) -> str:
        logger.info("Transformation des données...")
        data = json.loads(raw_data_json)
        df = pd.DataFrame(data)

        # 1. Drop missing prices and convert types
        df = df.dropna(subset=['prix']).copy()
        df['prix'] = df['prix'].astype(float)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 2. Rename columns to match PostgreSQL table schema exactly
        df = df.rename(columns={
            'ville': 'region',
            'produit': 'product_type',
            'prix': 'price_usd_per_gallon'
        })

        # 3. Add default values for composite Primary Keys (grade, source)
        df['grade'] = 'Standard'
        df['source'] = 'Mock_API'

        # 4. Drop columns that do NOT exist in the SQL table
        df = df.drop(columns=['station_id'])

        return df.to_json(orient='records')

    @task(execution_timeout=timedelta(minutes=5))
    def load_to_postgres(cleaned_data_json: str):
        logger.info("Chargement dans PostgreSQL...")
        df = pd.read_json(cleaned_data_json, orient='records')

        if df.empty:
            logger.warning("Aucune donnée à charger.")
            return

        # Fetch Postgres hook and get SQLAlchemy engine
        hook = PostgresHook(postgres_conn_id="postgres_gasoil_db")
        engine = hook.get_sqlalchemy_engine()

        # Execute using a context manager to auto-commit and prevent socket hanging
        with engine.begin() as connection:
            df.to_sql('gasoil_prices', con=connection, if_exists='append', index=False)

        # Cleanup engine connection pool
        engine.dispose()
        logger.info("Données chargées avec succès dans gasoil_prices !")

    # Pipeline task dependencies
    raw_json = extract_raw_data()
    cleaned_json = transform_data(raw_json)
    load_to_postgres(cleaned_json)

# Instantiate the DAG
gasoil_pipeline_dag = gasoil_etl_pipeline()