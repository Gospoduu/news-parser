import os

from faststream.kafka import KafkaBroker

KAFKA_URL = os.getenv("KAFKA_URL")
if not KAFKA_URL:
    raise RuntimeError("KAFKA_URL is not configured")

broker = KafkaBroker(KAFKA_URL, acks="all", enable_idempotence=True)
