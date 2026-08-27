from flask import Flask, render_template, jsonify
from psycopg2.extras import RealDictCursor
from database import get_conn

app = Flask(__name__)

def execute_query(query, params=None):
    """Exécute une requête SQL et retourne le résultat sous forme de liste de dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()
    finally:
        conn.close()

# --- ROUTES DASHBOARD ---
@app.route('/')
def dashboard():
    return render_template('index.html')

# --- API ENDPOINTS ---
@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Cartes d'indicateurs clés (KPIs)."""
    stats_query = """
        SELECT 
            (SELECT COUNT(*) FROM gasoil_prices) AS total_gasoil_records,
            (SELECT COUNT(*) FROM crude_spot_prices) AS total_crude_records,
            (SELECT COUNT(*) FROM vehicle_fuel_economy) AS total_vehicles,
            (SELECT COUNT(*) FROM nhtsa_complaints WHERE fuel_related_flag = TRUE) AS fuel_complaints;
    """
    res = execute_query(stats_query)
    return jsonify({'status': 'success', 'data': res[0] if res else {}})

@app.route('/api/gasoil-prices', methods=['GET'])
def get_gasoil_prices():
    """Données des prix du gasoil."""
    query = """
        SELECT date, product_type, region, grade, price_usd_per_gallon, source 
        FROM gasoil_prices 
        ORDER BY date DESC LIMIT 500;
    """
    res = execute_query(query)
    return jsonify({'status': 'success', 'data': res})

@app.route('/api/vehicles', methods=['GET'])
def get_vehicles():
    """Données sur l'économie de carburant des véhicules."""
    query = """
        SELECT epa_id, make, model, year, fuel_type, mpg_combined, annual_fuel_cost_usd, co2_tailpipe_gpm 
        FROM vehicle_fuel_economy 
        ORDER BY year DESC, make ASC LIMIT 500;
    """
    res = execute_query(query)
    return jsonify({'status': 'success', 'data': res})

if __name__ == '__main__':
    app.run(debug=True, port=5000)