
-- 1) Tabla cruda: el mensaje de Kafka tal como llego, con su estado de procesamiento
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    kafka_topic       TEXT      NOT NULL,
    kafka_partition   INTEGER   NOT NULL,
    kafka_offset      BIGINT    NOT NULL,
    kafka_key         TEXT,
    raw_payload       TEXT      NOT NULL,               -- mensaje original, sin modificar
    event_id          TEXT,                             -- se extrae del mensaje; puede faltar si es invalido
    received_at       TIMESTAMP NOT NULL DEFAULT now(),
    processing_status TEXT      NOT NULL DEFAULT 'RECEIVED',
    status_detail     TEXT,                             -- motivo cuando el evento es invalido o falla
    CONSTRAINT chk_raw_status CHECK (processing_status IN
        ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    CONSTRAINT uq_raw_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- 2) Dimension de paises: un registro por pais
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- 3) Dimension de fechas: un registro por dia (date_id con formato YYYYMMDD)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER  PRIMARY KEY,
    full_date DATE     NOT NULL UNIQUE,
    year      SMALLINT NOT NULL,
    quarter   SMALLINT NOT NULL,
    month     SMALLINT NOT NULL,
    day       SMALLINT NOT NULL
);

-- 4) Tabla de hechos: una fila por prediccion
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT   NOT NULL UNIQUE REFERENCES raw_happiness_events (raw_event_id),
    country_id           INTEGER  NOT NULL REFERENCES dim_country (country_id),
    date_id              INTEGER  NOT NULL REFERENCES dim_date (date_id),
    observation_year     SMALLINT NOT NULL,             -- anio del dato (2015-2019), para la tendencia
    actual_score         NUMERIC(6, 3) NOT NULL,
    predicted_score      NUMERIC(6, 3) NOT NULL,
    prediction_error     NUMERIC(6, 3) NOT NULL,        -- predicho - real (con signo)
    event_time           TIMESTAMP NOT NULL,            -- cuando el producer emitio el evento
    prediction_timestamp TIMESTAMP NOT NULL DEFAULT now()
);