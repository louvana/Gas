import os
import pandas as pd
from prophet import Prophet
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine

# Connexion PostgreSQL
DB_URI = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/airflow")
engine = create_engine(DB_URI)

def fetch_raw_prices():
    query = """
        SELECT 
            date, 
            product_type AS product, 
            region, 
            price_usd_per_gallon AS price 
        FROM gasoil_prices 
        ORDER BY region, product_type, date ASC
    """
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

#  FORECASTING
def run_forecasting(df, forecast_days=30):
    forecast_results = []
    grouped = df.groupby(['product', 'region'])
    
    print(f" Traitement de {len(grouped)} groupes (Product/Region)")
    
    for (product, region), group in grouped:
        if len(group) < 5:  # Baissé pour s'assurer que même un petit historique est traité
            continue
            
        df_prophet = group[['date', 'price']].rename(columns={'date': 'ds', 'price': 'y'})
        max_date = df_prophet['ds'].max()
        
        # Désactivation des saisonnalités trop strictes si peu de données
        model = Prophet(daily_seasonality=False, yearly_seasonality=False, weekly_seasonality=False)
        model.fit(df_prophet)
        
        future = model.make_future_dataframe(periods=forecast_days, freq='D')
        forecast = model.predict(future)
        
        # Filtrage explicite des dates futures
        future_forecast = forecast[forecast['ds'] > max_date].copy()
        
        for _, row in future_forecast.iterrows():
            forecast_results.append({
                'date': row['ds'].strftime('%Y-%m-%d'),
                'product': product,
                'region': region,
                'predicted_price': round(float(row['yhat']), 4),
                'yhat_lower': round(float(row['yhat_lower']), 4),
                'yhat_upper': round(float(row['yhat_upper']), 4)
            })
            
    df_forecast = pd.DataFrame(forecast_results)
    
    if not df_forecast.empty:
        with engine.begin() as connection:
            df_forecast.to_sql(
                'price_forecasts', 
                con=connection, 
                if_exists='append', 
                index=False,
                method='multi'
            )
        print(f"SUCCESS: {len(df_forecast)} prédictions insérées dans `price_forecasts`.")
    else:
        print(" Aucune prédiction générée (vérifie tes filtres de dates).")

# ANOMALY DETECTION
def detect_anomalies(df):
    anomalies_results = []
    grouped = df.groupby(['product', 'region'])
    
    for (product, region), group in grouped:
        if len(group) < 5:
            continue
            
        group = group.sort_values('date').copy()
        group['price_change_pct'] = group['price'].pct_change().fillna(0)
        
        iso_model = IsolationForest(contamination=0.05, random_state=42)
        X = group[['price_change_pct']]
        
        group['anomaly_flag'] = iso_model.fit_predict(X)
        group['anomaly_score'] = iso_model.score_samples(X)
        
        for _, row in group.iterrows():
            is_anomaly = bool(row['anomaly_flag'] == -1)
            change = row['price_change_pct']
            
            if is_anomaly and change > 0:
                anomaly_type = 'SPIKE'
            elif is_anomaly and change < 0:
                anomaly_type = 'DROP'
            else:
                anomaly_type = 'NORMAL'
                
            anomalies_results.append({
                'date': row['date'].strftime('%Y-%m-%d'),
                'product': product,
                'region': region,
                'actual_price': round(float(row['price']), 4),
                'price_change_pct': round(float(change), 4),
                'is_anomaly': is_anomaly,
                'anomaly_score': round(float(row['anomaly_score']), 4),
                'anomaly_type': anomaly_type
            })
            
    df_anomalies = pd.DataFrame(anomalies_results)
    
    if not df_anomalies.empty:
        with engine.begin() as connection:
            df_anomalies.to_sql(
                'price_anomalies', 
                con=connection, 
                if_exists='append', 
                index=False,
                method='multi'
            )
        print(f" SUCCESS: {len(df_anomalies)} enregistrements d'anomalies insérés.")

if __name__ == "__main__":
    print("Démarrage du pipeline ML;")
    raw_data = fetch_raw_prices()
    if not raw_data.empty:
        run_forecasting(raw_data, forecast_days=30)
        detect_anomalies(raw_data)
    else:
        print(" Aucune donnée dans `gasoil_prices`.")