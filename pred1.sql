-- Table pour stocker les prédictions
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

-- Table pour stocker les anomalies
CREATE TABLE IF NOT EXISTS price_anomalies (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    product VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    actual_price NUMERIC(8, 4) NOT NULL,
    price_change_pct NUMERIC(6, 4),
    is_anomaly BOOLEAN NOT NULL,
    anomaly_score NUMERIC(8, 4),
    anomaly_type VARCHAR(20), -- 'SPIKE', 'DROP' ou 'NORMAL'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, product, region)
);