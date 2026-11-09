"""Consumer: lee eventos de Kafka, los valida, predice con model.pkl y guarda todo en PostgreSQL."""
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from confluent_kafka import Consumer

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"

BOOTSTRAP = "localhost:9092"
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5434")),
    "dbname": os.getenv("DB_NAME", "happiness"),
    "user": os.getenv("DB_USER", "happiness"),
    "password": os.getenv("DB_PASSWORD", "happiness"),
}

# ---- Contrato del evento ----
FEATURE_FIELDS = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
STRING_FIELDS = ["event_id", "country"]
NUMERIC_FIELDS = FEATURE_FIELDS + ["actual_happiness_score"]
REQUIRED_FIELDS = STRING_FIELDS + ["year"] + NUMERIC_FIELDS + ["event_time"]

FEATURE_RANGE = (0.0, 3.0)
SCORE_RANGE = (0.0, 10.0)
YEAR_RANGE = (2015, 2019)


# ---------------------------------------------------------------- validacion
def is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_schema(event):
    """Campos requeridos y tipos de dato. Devuelve (es_valido, motivo)."""
    if not isinstance(event, dict):
        return False, "El evento no es un objeto JSON"
    for field in REQUIRED_FIELDS:
        if field not in event:
            return False, f"Falta el campo: {field}"
    for field in STRING_FIELDS:
        if not isinstance(event[field], str) or not event[field].strip():
            return False, f"{field} debe ser texto no vacio"
    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return False, "year debe ser entero"
    for field in NUMERIC_FIELDS:
        if not is_number(event[field]):
            return False, f"{field} debe ser numerico"
    try:
        datetime.fromisoformat(event["event_time"])
    except (TypeError, ValueError):
        return False, "event_time no es un timestamp ISO-8601 valido"
    return True, "ok"


def validate_values(event):
    """Valores con sentido de negocio. Devuelve (es_valido, motivo)."""
    if not YEAR_RANGE[0] <= event["year"] <= YEAR_RANGE[1]:
        return False, f"year fuera de rango {YEAR_RANGE}: {event['year']}"
    for field in NUMERIC_FIELDS:
        if not math.isfinite(event[field]):
            return False, f"{field} no es un numero finito"
    lo, hi = FEATURE_RANGE
    for field in FEATURE_FIELDS:
        if not lo <= event[field] <= hi:
            return False, f"{field} fuera de rango [{lo}, {hi}]: {event[field]}"
    lo, hi = SCORE_RANGE
    if not lo <= event["actual_happiness_score"] <= hi:
        return False, f"actual_happiness_score fuera de rango [{lo}, {hi}]: {event['actual_happiness_score']}"
    return True, "ok"


# ------------------------------------------------------------ base de datos
def store_raw(conn, msg, payload):
    """Guarda el mensaje original. Devuelve (raw_event_id, ya_procesado)."""
    key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO raw_happiness_events
                       (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload, received_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
                   RETURNING raw_event_id""",
                (msg.topic(), msg.partition(), msg.offset(), key, payload, datetime.now()),
            )
            row = cur.fetchone()
            if row is not None:
                return row[0], False
            # El mensaje ya estaba guardado (se reprocesa): solo se retoma si quedo sin terminar
            cur.execute(
                """SELECT raw_event_id, processing_status FROM raw_happiness_events
                   WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s""",
                (msg.topic(), msg.partition(), msg.offset()),
            )
            raw_event_id, status = cur.fetchone()
            return raw_event_id, status != "RECEIVED"


def set_status(conn, raw_event_id, status, detail=None, event_id=None):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE raw_happiness_events
                   SET processing_status = %s, status_detail = %s,
                       event_id = COALESCE(%s, event_id)
                   WHERE raw_event_id = %s""",
                (status, detail, event_id, raw_event_id),
            )


def save_prediction(conn, raw_event_id, event, predicted):
    """Guarda pais, fecha y prediccion en una sola transaccion (todo o nada)."""
    event_dt = datetime.fromisoformat(event["event_time"]).replace(tzinfo=None)
    date_id = event_dt.year * 10000 + event_dt.month * 100 + event_dt.day
    actual = event["actual_happiness_score"]

    with conn:   # commit si todo sale bien, rollback si algo falla
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO dim_country (country_name) VALUES (%s)
                   ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
                   RETURNING country_id""",
                (event["country"].strip(),),
            )
            country_id = cur.fetchone()[0]

            cur.execute(
                """INSERT INTO dim_date (date_id, full_date, year, quarter, month, day)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (date_id) DO NOTHING""",
                (date_id, event_dt.date(), event_dt.year,
                 (event_dt.month - 1) // 3 + 1, event_dt.month, event_dt.day),
            )

            cur.execute(
                """INSERT INTO fact_predictions
                       (raw_event_id, country_id, date_id, observation_year,
                        actual_score, predicted_score, prediction_error,
                        event_time, prediction_timestamp)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (raw_event_id, country_id, date_id, event["year"],
                 actual, predicted, round(predicted - actual, 3),
                 event_dt, datetime.now()),
            )

            cur.execute(
                """UPDATE raw_happiness_events
                   SET processing_status = 'VALID', status_detail = NULL, event_id = %s
                   WHERE raw_event_id = %s""",
                (event["event_id"], raw_event_id),
            )


# --------------------------------------------------------------- prediccion
def predict(model, features, event) -> float:
    row = pd.DataFrame([[event[f] for f in features]], columns=features)
    value = float(model.predict(row)[0])
    if not math.isfinite(value):
        raise ValueError("el modelo devolvio un valor no finito")
    return value


# ------------------------------------------------------------- procesamiento
def reject(conn, raw_event_id, status, reason, event_id, msg):
    set_status(conn, raw_event_id, status, reason, event_id)
    print(f"[{status}] particion={msg.partition()} offset={msg.offset()} | {reason}")
    return status


def process_message(conn, model, features, msg):
    raw = (msg.value() or b"").decode("utf-8", errors="replace").replace("\x00", "\ufffd")

    # 1) Guardar el mensaje original antes de validar o transformar nada
    raw_event_id, already_done = store_raw(conn, msg, raw)
    if already_done:
        print(f"[SKIP] particion={msg.partition()} offset={msg.offset()} ya fue procesado")
        return "SKIPPED"

    # 2) Interpretar el JSON
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as exc:
        return reject(conn, raw_event_id, "INVALID_SCHEMA", f"JSON invalido: {exc}", None, msg)

    event_id = event.get("event_id") if isinstance(event, dict) else None
    event_id = event_id if isinstance(event_id, str) else None

    # 3) Validar contra el contrato
    ok, reason = validate_schema(event)
    if not ok:
        return reject(conn, raw_event_id, "INVALID_SCHEMA", reason, event_id, msg)
    ok, reason = validate_values(event)
    if not ok:
        return reject(conn, raw_event_id, "INVALID_VALUES", reason, event_id, msg)

    # 4) Predecir y guardar el resultado
    try:
        predicted = round(predict(model, features, event), 3)
        save_prediction(conn, raw_event_id, event, predicted)
    except Exception as exc:
        set_status(conn, raw_event_id, "PREDICTION_ERROR", str(exc)[:500], event_id)
        print(f"[PREDICTION_ERROR] {event_id} | {exc}")
        return "PREDICTION_ERROR"

    actual = event["actual_happiness_score"]
    print(f"[VALID] {event['event_id']} | {event['country']} {event['year']} | "
          f"real={actual:.3f} predicho={predicted:.3f} error={predicted - actual:+.3f}")
    return "VALID"


def main() -> None:
    model = joblib.load(MODEL_PATH)
    features = list(model.feature_names_in_)      # orden exacto que uso el entrenamiento
    missing = set(features) - set(FEATURE_FIELDS)
    if missing:
        sys.exit(f"El modelo requiere variables que el contrato no define: {sorted(missing)}")

    conn = psycopg2.connect(**DB_CONFIG)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,              # se confirma cada mensaje al terminar de procesarlo
    })
    consumer.subscribe([TOPIC])

    stats = Counter()
    print(f"Esperando eventos en '{TOPIC}' (grupo {GROUP_ID}). Ctrl+C para detener.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error del consumer: {msg.error()}")
                continue
            stats[process_message(conn, model, features, msg)] += 1
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        consumer.close()
        conn.close()
        print("Resumen:", dict(stats))


if __name__ == "__main__":
    main()