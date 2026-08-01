import logging
import os
import time
import xml.etree.ElementTree as ET

import requests
from database import get_conn, upsert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE = os.environ["FUELECONOMY_BASE_URL"]

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
BATCH_SIZE = 500
REQUEST_DELAY_SECONDS = 0.1  # be polite to the public API


def _get_xml(url, params=None, max_retries=MAX_RETRIES):
    """GET a URL and parse it as XML, with retries and clear error surfacing."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                logger.warning("Rate limited on %s, retrying in %ss", url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except requests.RequestException as exc:
            last_exc = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning("Request to %s failed (%s), retrying in %ss", url, exc, wait)
            time.sleep(wait)
        except ET.ParseError as exc:
            # Not worth retrying identical malformed content
            raise RuntimeError(f"Malformed XML from {url}: {exc}") from exc
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts") from last_exc


def _menu_values(root):
    """Extract 'value' text from each menuItem, skipping any that are malformed."""
    values = []
    for el in root.findall("menuItem"):
        value_el = el.find("value")
        if value_el is None or value_el.text is None:
            logger.warning("menuItem missing value, skipping: %s", ET.tostring(el, encoding="unicode"))
            continue
        values.append(value_el.text)
    return values


def get_years():
    root = _get_xml(f"{BASE}/vehicle/menu/year")
    return _menu_values(root)


def get_makes(year):
    root = _get_xml(f"{BASE}/vehicle/menu/make", params={"year": year})
    return _menu_values(root)


def get_models(year, make):
    root = _get_xml(f"{BASE}/vehicle/menu/model", params={"year": year, "make": make})
    return _menu_values(root)


def get_vehicle_ids(year, make, model):
    root = _get_xml(f"{BASE}/vehicle/menu/options", params={"year": year, "make": make, "model": model})
    return _menu_values(root)


def get_vehicle_detail(vehicle_id):
    root = _get_xml(f"{BASE}/vehicle/{vehicle_id}")

    def g(tag):
        el = root.find(tag)
        return el.text if el is not None else None

    def g_float(tag):
        val = g(tag)
        if val is None:
            return None
        try:
            return float(val)
        except ValueError:
            logger.warning("Non-numeric value for %s on vehicle %s: %r", tag, vehicle_id, val)
            return None

    def g_int(tag):
        val = g(tag)
        if val is None:
            return None
        try:
            return int(val)
        except ValueError:
            logger.warning("Non-numeric value for %s on vehicle %s: %r", tag, vehicle_id, val)
            return None

    return {
        "epa_id": int(vehicle_id),
        "make": g("make"),
        "model": g("model"),
        "year": g_int("year"),
        "fuel_type": g("fuelType"),
        "mpg_city": g_float("city08"),
        "mpg_highway": g_float("highway08"),
        "mpg_combined": g_float("comb08"),
        "annual_fuel_cost_usd": g_float("fuelCost08"),
        "co2_tailpipe_gpm": g_float("co2TailpipeGpm"),
        "barrels_per_year": g_float("barrels08"),
        "vehicle_class": g("VClass"),
    }


def _flush(conn, columns, rows, label=""):
    if not rows:
        return
    upsert(conn, "vehicle_fuel_economy", columns, rows, ["epa_id"])
    logger.info("Upserted %s vehicles%s", len(rows), f" ({label})" if label else "")
    rows.clear()



def run(years_to_pull=None):
    years_to_pull = years_to_pull or ["2023", "2024", "2025"]
    columns = [
        "epa_id", "make", "model", "year", "fuel_type", "mpg_city",
        "mpg_highway", "mpg_combined", "annual_fuel_cost_usd",
        "co2_tailpipe_gpm", "barrels_per_year", "vehicle_class"
    ]
    
    conn = get_conn()
    try:
        rows = []
        for year in years_to_pull:
            try:
                makes = get_makes(year)
            except RuntimeError as e:
                logger.error("Skipping year %s, could not fetch makes: %s", year, e)
                continue
                
            for make in makes:
                try:
                    models = get_models(year, make)
                except RuntimeError as e:
                    logger.warning("Skipping make %s for year %s: %s", make, year, e)
                    continue
                
                for model in models:
                    try:
                        vehicle_ids = get_vehicle_ids(year, make, model)
                    except RuntimeError as e:
                        logger.warning("Skipping model %s %s: %s", make, model, e)
                        continue
                    
                    for vehicle_id in vehicle_ids:
                        try:
                            # Respectful delay between API calls
                            time.sleep(REQUEST_DELAY_SECONDS)
                            
                            detail = get_vehicle_detail(vehicle_id)
                            if detail:
                                # Convert the dictionary to a list matching 'columns' order
                                row_data = [detail[col] for col in columns]
                                rows.append(row_data)
                                
                                # Batch flush to DB
                                if len(rows) >= BATCH_SIZE:
                                    _flush(conn, columns, rows, label=f"Batch {year}")
                                    
                        except Exception as e:
                            logger.error("Failed to process vehicle %s: %s", vehicle_id, e)
                            continue
                            
            # Flush any remaining rows for the year
            if rows:
                _flush(conn, columns, rows, label=f"Finalizing {year}")
                
    finally:
        conn.close()