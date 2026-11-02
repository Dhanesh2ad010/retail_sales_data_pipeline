# Retail Sales Data Pipeline — Kafka + PostgreSQL

A real-time retail sales data pipeline built with **Apache Kafka** and **PostgreSQL**.  
The producer continuously generates simulated sales transaction events; the consumer reads those events, validates them, and persists them into a PostgreSQL database with exactly-once-style guarantees.

---

## Architecture

```
producer.py  ──►  Kafka Topic (sales-events)  ──►  consumer.py  ──►  PostgreSQL
```

| Component | Description |
|---|---|
| **Producer** | Generates random retail sales events and publishes them to the `sales-events` Kafka topic |
| **Kafka Broker** | Message broker running on `localhost:9092` |
| **Consumer** | Reads events from the topic, validates & normalises data, then inserts into PostgreSQL |
| **PostgreSQL** | Stores all sales transactions in the `sales_transactions` table |

---

## Project Structure

```
kafka-retail-project/
├── producer.py          # Kafka producer — generates and sends sales events
├── consumer.py          # Kafka consumer — reads events and writes to PostgreSQL
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

---

## Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Python | 3.9+ |
| Apache Kafka | 3.x |
| PostgreSQL | 13+ |

### 1. Clone the Repository

```bash
git clone https://github.com/Dhanesh2ad010/retail_sales_data_pipeline.git
cd retail_sales_data_pipeline
```

### 2. Create and Activate a Virtual Environment

```bash
# Create venv
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux / macOS)
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Kafka

Ensure a Kafka broker is running locally on `localhost:9092`.

```bash
# Start ZooKeeper (if not using KRaft)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

Create the topic:

```bash
kafka-topics.sh --create \
  --topic sales-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 5. Set Up PostgreSQL

Connect to PostgreSQL and run the following:

```sql
-- Create the database
CREATE DATABASE retail_sales;

-- Create a dedicated user
CREATE USER kafka_user WITH PASSWORD 'kafka_pass';
GRANT ALL PRIVILEGES ON DATABASE retail_sales TO kafka_user;

-- Connect to the database, then create the table
\c retail_sales

CREATE TABLE sales_transactions (
    id               SERIAL PRIMARY KEY,
    transaction_id   BIGINT        NOT NULL UNIQUE,
    product          VARCHAR(100)  NOT NULL,
    quantity         INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    store_id         INTEGER       NOT NULL CHECK (store_id > 0),
    total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    event_timestamp  TIMESTAMPTZ,
    inserted_at      TIMESTAMPTZ   DEFAULT NOW()
);
```

### 6. Configure the Database Password (optional)

The consumer reads the PostgreSQL password from the `PGPASSWORD` environment variable (defaults to `kafka_pass`):

```bash
# Windows
set PGPASSWORD=your_password

# Linux / macOS
export PGPASSWORD=your_password
```

---

## Running the Pipeline

Open **two separate terminals**.

**Terminal 1 — Start the Consumer first:**

```bash
python consumer.py
```

Expected output:
```
Kafka -> PostgreSQL consumer started.
Topic: sales-events
Consumer group: retail-postgres-group
Press Ctrl+C to stop.
```

**Terminal 2 — Start the Producer:**

```bash
python producer.py
```

Expected output:
```
Retail sales producer started. Press Ctrl+C to stop.
Sent transaction 1234567890 | Laptop | Amount: 110000.00 | Partition: 0 | Offset: 0
Sent transaction 1234567891 | Mouse  | Amount: 1500.00   | Partition: 1 | Offset: 0
...
```

---

## Kafka Event Schema

Each message published to `sales-events` is a JSON object with the following fields:

| Field | Type | Description |
|---|---|---|
| `transaction_id` | `int` | Unique transaction identifier |
| `store_id` | `int` | Store identifier (1–5) |
| `product` | `str` | Product name |
| `quantity` | `int` | Number of units sold (1–5) |
| `unit_price` | `float` | Price per unit (INR) |
| `total_amount` | `float` | `quantity × unit_price` |
| `event_timestamp` | `str` | ISO 8601 UTC timestamp |

### Sample Event

```json
{
  "transaction_id": 1735000001,
  "store_id": 3,
  "product": "Laptop",
  "quantity": 2,
  "unit_price": 55000.00,
  "total_amount": 110000.00,
  "event_timestamp": "2026-11-02T06:00:00+00:00"
}
```

---

## Sample Products

| Product | Unit Price (INR) |
|---|---|
| Laptop | ₹55,000 |
| Mouse | ₹750 |
| Keyboard | ₹1,800 |
| Monitor | ₹12,000 |
| Headphones | ₹2,500 |

---

## Data Integrity & Reliability

- **At-least-once delivery**: Producer uses `acks="all"` with `retries=3`.
- **Duplicate prevention**: Consumer uses `ON CONFLICT (transaction_id) DO NOTHING` in PostgreSQL.
- **Manual offset commit**: Kafka offset is committed only **after** the DB transaction is committed, preventing data loss on failure.
- **Validation**: Every incoming event is validated via `normalize_sale()` before writing to the database.
- **Error isolation**: If a single event fails processing, the consumer stops cleanly rather than silently skipping it.

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `kafka-python` | 3.0.12 | Kafka producer / consumer client |
| `psycopg2-binary` | 2.9.13 | PostgreSQL adapter for Python |

---

## Verifying Data in PostgreSQL

```sql
-- Total transactions ingested
SELECT COUNT(*) FROM sales_transactions;

-- Revenue by product
SELECT product,
       SUM(quantity)     AS total_units_sold,
       SUM(total_amount) AS total_revenue
FROM   sales_transactions
GROUP  BY product
ORDER  BY total_revenue DESC;

-- Transactions per store
SELECT store_id, COUNT(*) AS transactions
FROM   sales_transactions
GROUP  BY store_id
ORDER  BY store_id;
```

---

## Stopping the Pipeline

Press `Ctrl + C` in each terminal. Both the producer and consumer handle `KeyboardInterrupt` gracefully and close their connections cleanly.

---

## License

This project is open-source and available under the [MIT License](LICENSE).