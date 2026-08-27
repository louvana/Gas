import pandas as pd
from database import get_conn

def verify_output():
    conn = get_conn()
    
    # Vérifier les prédictions
    df_forecast = pd.read_sql("SELECT * FROM price_forecasts ORDER BY date ASC LIMIT 5;", conn)
    print("Forecasts:")
    print(df_forecast[['date', 'product', 'region', 'predicted_price', 'yhat_lower', 'yhat_upper']])
    
    print("\n" + "="*50 + "\n")
    
    # Vérifier les anomalies détectées
    df_anomalies = pd.read_sql("SELECT * FROM price_anomalies WHERE is_anomaly = TRUE ORDER BY date DESC LIMIT 5;", conn)
    print("Aperçu des anomalies détectées :")
    if not df_anomalies.empty:
        print(df_anomalies[['date', 'product', 'region', 'actual_price', 'price_change_pct', 'anomaly_type']])
    else:
        print("Aucune anomalie critique détectée dans l'historique.")
        
    conn.close()

if __name__ == "__main__":
    verify_output()