# Streaming Data Pipeline PoC

This project implements a real-time streaming pipeline using **Apache Flink**, **Kafka**, **PostgreSQL**, **Redis**, and **ClickHouse**.

## Architecture
1.  **Source**: PostgreSQL (`engagement_events` table).
2.  **Ingestion**: Flink CDC reads Postgres WAL and writes to Kafka (`engagement_events` topic).
3.  **Processing**: Flink reads from Kafka, performs a Lookup Join with Postgres (`content` table), and calculates engagement metrics.
4.  **Sinks**:
    -   **Redis**: Stores the "Most Engaging Content" leaderboard (Sliding Window Aggregation).
    -   **ClickHouse**: Stores the enriched event stream for analysis.
    -   **External System**: Receives enriched events via HTTP (simulated).

## Prerequisites
-   Docker & Docker Compose

## Setup & Run

### 1. Start Infrastructure
Run the following command in this directory:
```bash
docker-compose up -d --build
```
This will start Postgres, Kafka, Flink Cluster, Redis, ClickHouse, Data Generator, and the Mock External System.

### 2. Submit Flink Jobs
Once the containers are up (wait ~30 seconds for Flink to initialize), submit the jobs:

**Job 1: Ingestion (CDC -> Kafka)**
```bash
docker exec jobmanager flink run -py /opt/flink/usrlib/ingest.py -d
```

**Job 2: Processing (Kafka -> Sinks)**
```bash
docker exec jobmanager flink run -py /opt/flink/usrlib/process.py -d
```

### 3. Verify Data

**Check Data Generator Logs:**
```bash
docker logs -f datagen
```

**Check Mock External System:**
```bash
docker logs -f external-system
```

**Check Redis Leaderboard:**
```bash
docker exec redis redis-cli ZRANGE leaderboard 0 -1 WITHSCORES
```

**Check ClickHouse:**
```bash
docker exec clickhouse clickhouse-client --query "SELECT * FROM engagement_enriched LIMIT 10"
```

## Project Structure
-   `src/`: Python source code (Flink jobs, datagen, mock external).
-   `sql/`: Database initialization scripts.
-   `docker-compose.yml`: Infrastructure definition.
-   `Dockerfile.flink`: Custom Flink image with connectors.
