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

        # Drop missing prices and cast types
        df = df.dropna(subset=['prix']).copy()
        df['prix'] = df['prix'].astype(float)
        df['station_id'] = df['station_id'].astype(int)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df['ingested_at'] = datetime.now().isoformat()

        return df.to_json(orient='records')

    @task()
    def load_to_postgres(cleaned_data_json: str):
        logger.info("Chargement dans PostgreSQL...")
        df = pd.read_json(cleaned_data_json, orient='records')

        if df.empty:
            logger.warning("Aucune donnée à charger.")
            return

        postgres_hook = PostgresHook(postgres_conn_id='postgres_gasoil_db')
        engine = postgres_hook.get_sqlalchemy_engine()

        # Open connection block to ensure clean transaction commit
        with engine.begin() as conn:
            df.to_sql(
                name='stg_fact_gasoil_prices',
                con=conn,
                if_exists='append',
                index=False
            )
        logger.info("Insertion réussie !")

    raw_json = extract_raw_data()
    cleaned_json = transform_data(raw_json)
    load_to_postgres(cleaned_json)

gasoil_pipeline_dag = gasoil_etl_pipeline()