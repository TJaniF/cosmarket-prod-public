CREATE TABLE IF NOT EXISTS {tbl_manifests} (
    manifest_id           VARCHAR(36) PRIMARY KEY,  -- UUID
    shipment_id           VARCHAR(36) NOT NULL,     -- UUID
    spacecraft_id         INTEGER NOT NULL,         -- FK to spacecraft.spacecraft_id
    origin_hub            VARCHAR(50),
    destination_planet_id INTEGER NOT NULL,         -- FK to planets.planet_id
    departure_ts          TIMESTAMP NOT NULL,
    total_weight_kg       DECIMAL(10,2),
    total_volume_m3       DECIMAL(8,2),
    cargo_fill_pct        DECIMAL(5,2),             -- percent of cargo bay used (0-100)
    cargo_value_usd       DECIMAL(12,2),
    priority_class        VARCHAR(20),              -- standard, expedited, critical
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {stg_manifests} (
    manifest_id           VARCHAR(36) PRIMARY KEY,
    shipment_id           VARCHAR(36) NOT NULL,
    spacecraft_id         INTEGER NOT NULL,
    origin_hub            VARCHAR(50),
    destination_planet_id INTEGER NOT NULL,
    departure_ts          TIMESTAMP NOT NULL,
    total_weight_kg       DECIMAL(10,2),
    total_volume_m3       DECIMAL(8,2),
    cargo_fill_pct        DECIMAL(5,2),
    cargo_value_usd       DECIMAL(12,2),
    priority_class        VARCHAR(20),
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
