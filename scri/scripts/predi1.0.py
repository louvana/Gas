import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from database import get_conn

def predire_prochain_prix(region_cible="WASHINGTON", produit_cible="Regular Gasoline"):

    os.environ["POSTGRES_HOST"] = "localhost"

    conn = get_conn()
    
    
    query = """
        SELECT date, price_usd_per_gallon 
        FROM gasoil_prices 
        WHERE region = %s AND product_type = %s
        ORDER BY date ASC;
    """
    df = pd.read_sql(query, conn, params=(region_cible, produit_cible))
    conn.close()
    
    if len(df) < 9:
        return {"erreur": f"Pas assez de données pour la région {region_cible}."}
    
    # time step +lags features
    df["Lag_1"] = df["price_usd_per_gallon"].shift(1)
    df["Lag_2"] = df["price_usd_per_gallon"].shift(2)
    df["time_step"] = range(len(df))
    
    df_clean = df.dropna().copy()

    X = df_clean[["Lag_1", "Lag_2", "time_step"]].values
    Y = df_clean["price_usd_per_gallon"].values
    
    # model training
    model = LinearRegression()
    model.fit(X, Y)

    dernier_prix = df["price_usd_per_gallon"].iloc[-1]
    avant_dernier_prix = df["price_usd_per_gallon"].iloc[-2]
    prochain_time_step = len(df)
    
    prochaine_ligne = np.array([[dernier_prix, avant_dernier_prix, prochain_time_step]])
    prix_predit = model.predict(prochaine_ligne)[0]
    
  
    derniere_date_db = pd.to_datetime(df["date"].iloc[-1])
    
    # verify if we have data weekely or daily 
    # Si l'écart moyen est de 7 jours, on ajoute 7 jours. Sinon, on ajoute 1 jour.
    if len(df) >= 2:
        ecart_jours = (pd.to_datetime(df["date"].iloc[-1]) - pd.to_datetime(df["date"].iloc[-2])).days
        prochaine_date = premiere_date_futur = derniere_date_db + pd.Timedelta(days=ecart_jours)
    else:
        prochaine_date = derniere_date_db + pd.Timedelta(days=7) # Par défaut +7 jours pour l'EIA
        
    # Formatage de la date
    date_formatee = prochaine_date.strftime("%d/%m/%Y")

    return {
        "date": date_formatee,
        "region": region_cible,
        "produit": produit_cible,
        "prix_predit": round(float(prix_predit), 2)
    }

if __name__ == "__main__":
    # Exec
    resultat = predire_prochain_prix(region_cible="WASHINGTON", produit_cible="Regular Gasoline")
    
    if "erreur" in resultat:
        print(resultat["erreur"])
    else:
        print(f" Résultat de Gasoil Intelligence :")
        print(f"Le prix prédit pour le {resultat['date']} dans la région '{resultat['region']}' ({resultat['produit']}) est de : {resultat['prix_predit']} USD/gallon")
