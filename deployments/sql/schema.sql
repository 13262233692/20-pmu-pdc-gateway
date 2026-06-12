CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS pmu_stations (
    pmu_id      INTEGER PRIMARY KEY,
    station_name VARCHAR(128) NOT NULL,
    voltage_kv  DOUBLE PRECISION,
    region      VARCHAR(64),
    phasor_count INTEGER DEFAULT 10,
    analog_count INTEGER DEFAULT 0,
    digital_count INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pmu_data (
    time         TIMESTAMPTZ       NOT NULL,
    pmu_id       INTEGER           NOT NULL,
    freq         DOUBLE PRECISION  NOT NULL,
    dfreq        DOUBLE PRECISION  NOT NULL,
    stat         INTEGER           NOT NULL,
    phasor_reals DOUBLE PRECISION[] NOT NULL,
    phasor_imags DOUBLE PRECISION[] NOT NULL,
    analogs      DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
    digitals     INTEGER[]         NOT NULL DEFAULT '{}'
);

SELECT create_hypertable('pmu_data', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_pmu_data_pmu_id_time ON pmu_data (pmu_id, time DESC);

ALTER TABLE pmu_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pmu_id',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('pmu_data', INTERVAL '7 days', if_not_exists => TRUE);

SELECT add_retention_policy('pmu_data', INTERVAL '365 days', if_not_exists => TRUE);

CREATE VIEW IF NOT EXISTS pmu_1s_avg AS
SELECT
    time_bucket('1 second', time) AS bucket,
    pmu_id,
    AVG(freq) AS avg_freq,
    AVG(dfreq) AS avg_dfreq,
    COUNT(*) AS sample_count
FROM pmu_data
GROUP BY bucket, pmu_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS pmu_1min_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    pmu_id,
    AVG(freq) AS avg_freq,
    MIN(freq) AS min_freq,
    MAX(freq) AS max_freq,
    AVG(dfreq) AS avg_dfreq,
    MIN(dfreq) AS min_dfreq,
    MAX(dfreq) AS max_dfreq,
    COUNT(*) AS sample_count
FROM pmu_data
GROUP BY bucket, pmu_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('pmu_1min_stats',
    start_offset => INTERVAL '5 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

INSERT INTO pmu_stations (pmu_id, station_name, voltage_kv, region, phasor_count) VALUES
    (1,  'Station_A_500kV', 500.0, 'North', 10),
    (2,  'Station_B_220kV', 220.0, 'North', 10),
    (3,  'Station_C_500kV', 500.0, 'South', 10),
    (4,  'Station_D_220kV', 220.0, 'South', 10),
    (5,  'Station_E_330kV', 330.0, 'East',  10),
    (6,  'Station_F_500kV', 500.0, 'East',  10),
    (7,  'Station_G_220kV', 220.0, 'West',  10),
    (8,  'Station_H_500kV', 500.0, 'West',  10),
    (9,  'Station_I_220kV', 220.0, 'Central', 10),
    (10, 'Station_J_330kV', 330.0, 'Central', 10)
ON CONFLICT (pmu_id) DO NOTHING;
