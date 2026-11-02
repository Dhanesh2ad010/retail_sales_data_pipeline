
import json
import os
from decimal import Decimal, InvalidOperation

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sales-events"
GROUP_ID = "retail-postgres-group"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "retail_sales",
    "user": "kafka_user",
    "password": os.getenv("PGPASSWORD", "kafka_pass"),
}


def normalize_sale(sale):
    """Validate and normalize a Kafka sales event."""

    required_fields = [
        "transaction_id",
        "product",
        "quantity",
        "unit_price",
        "store_id",
    ]

    missing = [
        field for field in required_fields
        if field not in sale or sale[field] is None
    ]

    if missing:
        raise ValueError(
            f"Missing required fields: {', '.join(missing)}"
        )

    quantity = int(sale["quantity"])
    store_id = int(sale["store_id"])
    transaction_id = int(sale["transaction_id"])
    unit_price = Decimal(str(sale["unit_price"]))

    if quantity <= 0:
        raise ValueError("quantity must be greater than zero")

    if store_id <= 0:
        raise ValueError("store_id must be greater than zero")

    if unit_price < 0:
        raise ValueError("unit_price cannot be negative")

    # Older events might not contain total_amount.
    total_amount = Decimal(
        str(sale.get("total_amount", quantity * unit_price))
    )

    if total_amount < 0:
        raise ValueError("total_amount cannot be negative")

    return {
        "transaction_id": transaction_id,
        "product": str(sale["product"]),
        "quantity": quantity,
        "unit_price": unit_price,
        "store_id": store_id,
        "total_amount": total_amount,
        "event_timestamp": sale.get("event_timestamp"),
    }


def insert_sale(cursor, sale):
    cursor.execute(
        """
        INSERT INTO sales_transactions (
            transaction_id,
            product,
            quantity,
            unit_price,
            store_id,
            total_amount,
            event_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
        """,
        (
            sale["transaction_id"],
            sale["product"],
            sale["quantity"],
            sale["unit_price"],
            sale["store_id"],
            sale["total_amount"],
            sale["event_timestamp"],
        ),
    )

    return cursor.rowcount == 1


def main():
    connection = None
    consumer = None

    try:
        connection = psycopg2.connect(**DB_CONFIG)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda key: (
                key.decode("utf-8") if key else None
            ),
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

        print("Kafka -> PostgreSQL consumer started.")
        print(f"Topic: {TOPIC}")
        print(f"Consumer group: {GROUP_ID}")
        print("Press Ctrl+C to stop.\n")

        for message in consumer:
            try:
                sale = normalize_sale(message.value)

                with connection.cursor() as cursor:
                    inserted = insert_sale(cursor, sale)

                # First persist the database transaction.
                connection.commit()

                # Then commit the Kafka offset.
                consumer.commit()

                if inserted:
                    print(
                        f"INSERTED | Transaction: "
                        f"{sale['transaction_id']} | "
                        f"Product: {sale['product']} | "
                        f"Quantity: {sale['quantity']} | "
                        f"Total: {sale['total_amount']:.2f} | "
                        f"Partition: {message.partition} | "
                        f"Offset: {message.offset}"
                    )
                else:
                    print(
                        f"DUPLICATE SKIPPED | Transaction: "
                        f"{sale['transaction_id']} | "
                        f"Partition: {message.partition} | "
                        f"Offset: {message.offset}"
                    )

            except (
                psycopg2.Error,
                ValueError,
                TypeError,
                KeyError,
                InvalidOperation,
                json.JSONDecodeError,
            ) as error:
                connection.rollback()

                print("\nEVENT PROCESSING FAILED")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")
                print(f"Error: {error}")
                print(
                    "Kafka offset was not committed for this event. "
                    "Stopping to prevent silently skipping it."
                )
                break

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    except (KafkaError, psycopg2.Error) as error:
        print(f"Connection or Kafka error: {error}")

    finally:
        if consumer is not None:
            consumer.close()

        if connection is not None:
            connection.close()

        print("Consumer stopped.")


if __name__ == "__main__":
    main()

