
import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sales-events"

products = [
    {"product": "Laptop", "unit_price": 55000.00},
    {"product": "Mouse", "unit_price": 750.00},
    {"product": "Keyboard", "unit_price": 1800.00},
    {"product": "Monitor", "unit_price": 12000.00},
    {"product": "Headphones", "unit_price": 2500.00},
]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=3,
)

print("Retail sales producer started. Press Ctrl+C to stop.")

transaction_id = int(time.time())

try:
    while True:
        item = random.choice(products)
        quantity = random.randint(1, 5)

        sale = {
            "transaction_id": transaction_id,
            "store_id": random.randint(1, 5),
            "product": item["product"],
            "quantity": quantity,
            "unit_price": item["unit_price"],
            "total_amount": round(quantity * item["unit_price"], 2),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            future = producer.send(
                TOPIC,
                key=transaction_id,
                value=sale,
            )
            metadata = future.get(timeout=10)

            print(
                f"Sent transaction {transaction_id} | "
                f"{sale['product']} | "
                f"Amount: {sale['total_amount']:.2f} | "
                f"Partition: {metadata.partition} | "
                f"Offset: {metadata.offset}"
            )

            transaction_id += 1
            time.sleep(2)

        except KafkaError as error:
            print(f"Failed to publish transaction: {error}")
            time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.close()
    print("Producer stopped.")
