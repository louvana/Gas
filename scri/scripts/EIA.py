import logging
import os
import time
import requests
from dotenv import load_dotenv
import json  
from database import get_conn, upsert

dotenv_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(dotenv_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EIA_KEY = os.environ.get("EIA_API_KEY") or os.environ.get("Crude_oil_key")
EIA_BASE = os.environ.get("EIA_BASE_URL", "https://api.eia.gov/v2")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


def _get_with_retries(url, params, max_retries=MAX_RETRIES):
    if not params.get("api_key"):
        raise ValueError("EIA API Key is missing. Ensure EIA_API_KEY is set in .env")
        
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
            logger.warning("Request failed (attempt %d/%d): %s. Retrying in %ss...", attempt, max_retries, exc, wait)
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts") from last_exc


def _extract_data(resp):
    payload = resp.json()
    if "response" not in payload or "data" not in payload["response"]:
        raise RuntimeError(f"Unexpected EIA response shape: {payload}")
    return payload["response"]["data"]


def _safe_float(value):
    if value is None:
        return None
    try:
        val = float(value)
        return val if val > 0 else None
    except (TypeError, ValueError):
        return None


def _run_upsert(table, columns, rows, conflict_cols):
    if not rows:
        logger.warning("No rows to upsert into %s", table)
        return
    conn = get_conn()
    try:
        upsert(conn, table=table, columns=columns, rows=rows, conflict_cols=conflict_cols)
    finally:
        conn.close()
    logger.info("Upserted %s rows into %s", len(rows), table)


def fetch_crude_spot_prices():
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

    resp = _get_with_retries(url, params)
    data = _extract_data(resp)

    series_map = {"RWTC": "WTI", "RBRTE": "Brent"}
    rows = []

    for row in data:
        price = _safe_float(row.get("value"))
        if price is None:
            continue
        crude_type = series_map.get(row.get("series"), row.get("series"))
        rows.append((row["period"], crude_type, price, "EIA"))

    _run_upsert(
        table="crude_spot_prices",
        columns=["date", "crude_type", "price_usd_per_barrel", "source"],
        rows=rows,
        conflict_cols=["date", "crude_type", "source"],
    )


def fetch_gasoil_retail_prices():
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

    resp = _get_with_retries(url, params)
    data = _extract_data(resp)

    product_map = {"EPMR": "Regular Gasoline", "EPD2D": "Diesel"}
    rows = []

    for row in data:
        price = _safe_float(row.get("value"))
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

    _run_upsert(
        table="gasoil_prices",
        columns=["date", "product_type", "region", "grade", "price_usd_per_gallon", "source"],
        rows=rows,
        conflict_cols=["date", "product_type", "region", "grade", "source"],
    )


def export_to_jsonl(filename="train.jsonl"):
    """Récupère les prix de la base de données et les exporte au format JSONL pour Mistral AI."""
    conn = get_conn()
    cursor = conn.cursor()
    
    
    cursor.execute("SELECT date, product_type, region, price_usd_per_gallon FROM gasoil_prices ORDER BY date DESC LIMIT 200;")
    rows = cursor.fetchall()
    
    with open(filename, "w", encoding="utf-8") as f:
        for row in rows:
            date, product_type, region, price = row
            
            # Formulation naturelle des Q/A pour l'entraînement du LLM
            question = f"Quel était le prix du {product_type} dans la région {region} à la date du {date} ?"
            reponse = f"Le prix du {product_type} dans la région {region} le {date} était de {price} USD par gallon."
            
            # Structure Mistral AI
            structure_mistral = {
                "messages": [
                    {"role": "user", "content": question},
                    {"role": "assistant", "content": price} 
                ]
            }
           
            f.write(json.dumps(structure_mistral, ensure_ascii=False) + "\n")
            
    cursor.close()
    conn.close()
    logger.info("Données exportées avec succès dans %s au format JSONL", filename)


if __name__ == "__main__":
    fetch_crude_spot_prices()
    fetch_gasoil_retail_prices()
    export_to_jsonl()
