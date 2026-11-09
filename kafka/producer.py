"""Producer: publica cada fila del dataset de streaming como un evento en Kafka."""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "streaming" / "happiness_stream.csv"
BOOTSTRAP = "localhost:9092"
TOPIC = "happiness-events"
EXPECTED_PARTITIONS = 3


def check_topic() -> None:
    """Falla con un mensaje claro si Kafka no responde o el topic no está bien creado."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        meta = admin.list_topics(timeout=10)
    except Exception as exc:
        sys.exit(f"No se pudo conectar con Kafka en {BOOTSTRAP}: {exc}")

    topic = meta.topics.get(TOPIC)
    if topic is None:
        sys.exit(f"El topic '{TOPIC}' no existe. Créelo antes de ejecutar el producer.")
    if len(topic.partitions) != EXPECTED_PARTITIONS:
        sys.exit(f"El topic tiene {len(topic.partitions)} particiones y se esperaban {EXPECTED_PARTITIONS}.")


def build_event(n: int, row: dict) -> dict:
    """Arma el evento con el formato exacto del contrato."""
    return {
        "event_id": f"E{n:03d}",
        "country": row["country"],
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().replace(microsecond=0).isoformat(),
    }


def on_delivery(err, msg) -> None:
    if err is not None:
        print(f"   ERROR de entrega: {err}")
    else:
        print(f"   -> {msg.key().decode()} | partición {msg.partition()} | offset {msg.offset()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer de eventos de felicidad")
    parser.add_argument("--delay", type=float, default=0.5, help="segundos entre eventos")
    parser.add_argument("--limit", type=int, default=None, help="enviar solo los primeros N eventos")
    args = parser.parse_args()

    check_topic()

    df = pd.read_csv(CSV_PATH)
    if args.limit:
        df = df.head(args.limit)

    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    for n, row in enumerate(df.to_dict("records"), start=1):
        event = build_event(n, row)
        producer.produce(
            topic=TOPIC,
            key=event["country"],               # el país es la llave del evento
            value=json.dumps(event),
            callback=on_delivery,
        )
        producer.poll(0)
        print(f"[{n}/{len(df)}] {event['event_id']} | {event['country']} {event['year']}")
        time.sleep(args.delay)

    producer.flush()
    print("Todos los eventos fueron enviados.")


if __name__ == "__main__":
    main()