
CREATE TABLE IF NOT EXISTS gasoil_prices (
    date DATE,
    product_type VARCHAR(60),
    region VARCHAR(100),
    grade VARCHAR(60),
    price_usd_per_gallon NUMERIC(10, 4),
    source VARCHAR(50),
    PRIMARY KEY (date, product_type, region, grade, source)
);

CREATE TABLE IF NOT EXISTS crude_spot_prices (
    date DATE,
    crude_type VARCHAR(50),
    price_usd_per_barrel NUMERIC(10, 4),
    source VARCHAR(50),
    PRIMARY KEY (date, crude_type, source)
);


CREATE TABLE IF NOT EXISTS vehicle_fuel_economy (
    epa_id INT PRIMARY KEY,
    make VARCHAR(100),
    model VARCHAR(100),
    year INT,
    fuel_type VARCHAR(50),
    mpg_city NUMERIC(5, 2),
    mpg_highway NUMERIC(5, 2),
    mpg_combined NUMERIC(5, 2),
    annual_fuel_cost_usd NUMERIC(10, 2),
    co2_tailpipe_gpm NUMERIC(10, 2),
    barrels_per_year NUMERIC(10, 2),
    vehicle_class VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS nhtsa_complaints (
    complaint_id INT PRIMARY KEY,
    date DATE,
    make VARCHAR(100),
    model VARCHAR(100),
    year INT,
    component VARCHAR(255),
    fuel_related_flag BOOLEAN,
    description TEXT
);


CREATE TABLE IF NOT EXISTS price_forecasts (
    forecast_date DATE,
    target_date DATE,
    product_type VARCHAR(60),
    predicted_price NUMERIC(10, 4),
    confidence_interval VARCHAR(100),
    model_version VARCHAR(50),
    PRIMARY KEY (forecast_date, target_date, product_type, model_version)
);


CREATE TABLE IF NOT EXISTS anomaly_log (
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    product_type VARCHAR(60),
    region VARCHAR(100),
    actual_price NUMERIC(10, 4),
    expected_price NUMERIC(10, 4),
    anomaly_score NUMERIC(5, 4),
    flag VARCHAR(50)
);