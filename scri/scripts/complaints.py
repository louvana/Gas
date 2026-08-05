import os
import requests
from datetime import datetime
from database import get_conn, upsert

BASE = os.environ.get("NHTSA_BASE_URL", "https://api.nhtsa.gov")
FUEL_KEYWORDS = ["FUEL", "GAS", "ENGINE"]


def fetch_complaints(make, model, year):
    url = f"{BASE}/complaints/complaintsByVehicle"
    query_params = {"make": make, "model": model, "modelYear": year}
    resp = requests.get(url, params=query_params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("results", [])


def run(vehicles):
    """vehicles: list of (make, model, year) tuples"""
    conn = get_conn()
    columns = [
        "complaint_id", "date", "make", "model", "year",
        "component", "fuel_related_flag", "description",
    ]
    rows = []
    for make, model, year in vehicles:
        try:
            complaints = fetch_complaints(make, model, year)
        except Exception as e:
            print(f"Skipping {make} {model} {year}: {e}")
            continue

        for c in complaints:
            component = c.get("components", "")
            is_fuel_related = any(k in component.upper() for k in FUEL_KEYWORDS)
            date_str = c.get("dateComplaintFiled")
            try:
                complaint_date = datetime.strptime(date_str, "%m/%d/%Y").date() if date_str else None
            except ValueError:
                complaint_date = None

            odi = c.get("odiNumber")
            if not odi:
                continue

            rows.append((
                int(odi),
                complaint_date,
                make,
                model,
                int(year),
                component,
                is_fuel_related,
                (c.get("summary") or "")[:5000],
            ))

    upsert(conn, "nhtsa_complaints", columns, rows, ["complaint_id"])
    conn.close()
    print(f"Upserted {len(rows)} complaints")


if __name__ == "__main__":
    run([("Toyota", "Camry", 2023), ("Ford", "F-150", 2023)])