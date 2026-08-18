import numpy as np
from sklearn.linear_model import LinearRegression
from database import get_conn
import pandas as pd

def predire_prochain_prix():
    conn = get_conn()
    cursor = conn.cursor()
    
    # 1. Extraire l'historique des prix 
    cursor.execute("SELECT price_usd_per_gallon FROM gasoil_prices ORDER BY date ASC;")
    prix = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    if len(prix) < 15:
        return "not enough data for prediction."
    
    df = pd.DataFrame(prix, columns=["prix_actuel"])  
    #lags
    df["Lag_1"] = df["prix_actuel"].shift(1)
    df["Lag_2"] = df["prix_actuel"].shift(2)  
    #rimedummy
    df["time_step"] = range(len(df))
    df_clean = df.dropna().copy()

    X = df_clean[["Lag_1", "Lag_2", "time_step"]].values
    Y = df_clean["prix_actuel"].values
    #model training
    model = LinearRegression()
    model.fit(X, Y)


    dernier_prix = df["prix_actuel"].iloc[-1]
    avant_dernier_prix = df["prix_actuel"].iloc[-2]
    prochain_time_step = len(df)
    prochaine_ligne = np.array([[dernier_prix, avant_dernier_prix, prochain_time_step]])
    prix_predit = model.predict(prochaine_ligne)[0]
    

    
    
    
    return round(float(prix_predit), 2)

if __name__ == "__main__":
    prediction = predire_prochain_prix()
    print(f"Prix prédit pour la prochaine période : {prediction} USD/gallon")
