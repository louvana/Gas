-- Gasoil Prices Table
CREATE TABLE IF NOT EXISTS gasoil_prices (
    date DATE NOT NULL,
    product_type VARCHAR(60) NOT NULL,
    region VARCHAR(100) NOT NULL,
    grade VARCHAR(60) NOT NULL,
    price_usd_per_gallon NUMERIC(10, 4),
    source VARCHAR(50) NOT NULL,
    PRIMARY KEY (date, product_type, region, grade, source)
);

-- Crude Spot Prices Table
CREATE TABLE IF NOT EXISTS crude_spot_prices (
    date DATE NOT NULL,
    crude_type VARCHAR(50) NOT NULL,
    price_usd_per_barrel NUMERIC(10, 4),
    source VARCHAR(50) NOT NULL,
    PRIMARY KEY (date, crude_type, source)
);

-- Vehicle Fuel Economy Table
CREATE TABLE IF NOT EXISTS vehicle_fuel_economy (
    epa_id INT PRIMARY KEY,
    make VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INT NOT NULL,
    fuel_type VARCHAR(50),
    mpg_city INT,
    mpg_highway INT,
    mpg_combined INT,
    annual_fuel_cost_usd NUMERIC(10, 2),
    co2_tailpipe_gpm NUMERIC(10, 2),
    barrels_per_year NUMERIC(10, 2),
    vehicle_class VARCHAR(100)
);

-- NHTSA Complaints Table
CREATE TABLE IF NOT EXISTS nhtsa_complaints (
    complaint_id INT PRIMARY KEY,
    date DATE,
    make VARCHAR(100),
    model VARCHAR(100),
    year INT,
    component VARCHAR(255),
    fuel_related_flag BOOLEAN DEFAULT FALSE,
    description TEXT
);

-- Price Forecasts Table
CREATE TABLE IF NOT EXISTS price_forecasts (
    forecast_id SERIAL PRIMARY KEY,
    forecast_date DATE NOT NULL,
    target_date DATE NOT NULL,
    product_type VARCHAR(60) NOT NULL,
    predicted_price NUMERIC(10, 4) NOT NULL,
    confidence_interval JSONB,
    model_version VARCHAR(50) NOT NULL,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (forecast_date, target_date, product_type, model_version)
);

-- Anomaly Log Table
CREATE TABLE IF NOT EXISTS anomaly_log (
    anomaly_id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    product_type VARCHAR(60) NOT NULL,
    region VARCHAR(100) NOT NULL,
    actual_price NUMERIC(10, 4) NOT NULL,
    expected_price NUMERIC(10, 4),
    anomaly_score NUMERIC(8, 4),
    flag VARCHAR(50)
);

-- Query Log Table
CREATE TABLE IF NOT EXISTS query_log (
    query_id SERIAL PRIMARY KEY,
    query_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_query TEXT NOT NULL,
    retrieved_context TEXT,
    model_response TEXT,
    latency_ms INT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_gasoil_prices_date ON gasoil_prices(date);
CREATE INDEX IF NOT EXISTS idx_crude_spot_date ON crude_spot_prices(date);
CREATE INDEX IF NOT EXISTS idx_vehicle_make_model ON vehicle_fuel_economy(make, model, year);
CREATE INDEX IF NOT EXISTS idx_nhtsa_complaints_vehicle ON nhtsa_complaints(make, model, year);