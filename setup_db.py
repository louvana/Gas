import os
from database import get_conn

def setup_and_verify():
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        #  Création de la table price_forecasts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_forecasts (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                product VARCHAR(100) NOT NULL,
                region VARCHAR(100) NOT NULL,
                predicted_price NUMERIC(8, 4) NOT NULL,
                yhat_lower NUMERIC(8, 4),
                yhat_upper NUMERIC(8, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, product, region)
            );
        """)
        
        #  tableau price_anomalies
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_anomalies (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                product VARCHAR(100) NOT NULL,
                region VARCHAR(100) NOT NULL,
                actual_price NUMERIC(8, 4) NOT NULL,
                price_change_pct NUMERIC(6, 4),
                is_anomaly BOOLEAN NOT NULL,
                anomaly_score NUMERIC(8, 4),
                anomaly_type VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, product, region)
            );
        """)
        
        conn.commit()
        print(" Tableaux SQL créés")
        
        # Vérification des tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name IN ('price_forecasts', 'price_anomalies');
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        print("\n Statut des tables dans la base :")
        for t in ['price_forecasts', 'price_anomalies']:
            if t in tables:
                print(f"  - {t} : PRÊTE")
            else:
                print(f"  - {t} : MANQUANTE")
                
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f" Erreur lors de la configuration : {e}")

if __name__ == "__main__":
    setup_and_verify()