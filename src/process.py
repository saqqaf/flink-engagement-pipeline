import logging
import json
import requests
import redis
import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common import RowKind

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_clickhouse():
    """Initialize ClickHouse table using HTTP API"""
    ddl = """
    CREATE TABLE IF NOT EXISTS engagement_enriched (
        id Int64,
        content_id String,
        user_id String,
        event_type String,
        event_ts DateTime,
        duration_ms Nullable(Int32),
        device String,
        engagement_seconds Nullable(Float64),
        engagement_pct Nullable(Float64),
        content_type String,
        length_seconds Nullable(Int32)
    ) ENGINE = MergeTree() ORDER BY event_ts
    """
    try:
        # ClickHouse HTTP API
        response = requests.post("http://clickhouse:8123/", data=ddl)
        if response.status_code == 200:
            logger.info("ClickHouse table initialized successfully.")
        else:
            logger.error(f"ClickHouse init failed: {response.text}")
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")

class ClickHouseSink(MapFunction):
    def map(self, value):
        # Only process INSERT or UPDATE_AFTER events
        if value.get_row_kind() not in [RowKind.INSERT, RowKind.UPDATE_AFTER]:
            return value

        try:
            # Handle datetime serialization
            ts = value[4]
            if isinstance(ts, datetime.datetime):
                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
            else:
                ts_str = str(ts)

            row = {
                "id": value[0],
                "content_id": value[1],
                "user_id": value[2],
                "event_type": value[3],
                "event_ts": ts_str,
                "duration_ms": value[5],
                "device": value[6],
                "engagement_seconds": value[7],
                "engagement_pct": value[8],
                "content_type": value[9],
                "length_seconds": value[10]
            }
            
            # Send to ClickHouse via HTTP
            query = "INSERT INTO engagement_enriched FORMAT JSONEachRow"
            data = json.dumps(row)
            requests.post("http://clickhouse:8123/", params={"query": query}, data=data)
        except Exception as e:
            logger.error(f"ClickHouse Sink Error: {e}")
        return value

class RedisSink(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.r = redis.Redis(host='redis', port=6379, db=0)

    def map(self, value):
        # Only process INSERT or UPDATE_AFTER events
        if value.get_row_kind() not in [RowKind.INSERT, RowKind.UPDATE_AFTER]:
            return value

        # Input: (content_id, total_engagement_seconds)
        content_id = value[0]
        score = value[1]
        if content_id and score:
            # Increment score in the "leaderboard" sorted set
            self.r.zincrby("leaderboard", score, content_id)
        return value

class ExternalSink(MapFunction):
    def map(self, value):
        # Only process INSERT or UPDATE_AFTER events
        if value.get_row_kind() not in [RowKind.INSERT, RowKind.UPDATE_AFTER]:
            return value

        try:
            payload = {
                "id": value[0],
                "content_id": value[1],
                "event_type": value[3],
                "engagement_pct": value[8]
            }
            requests.post("http://external-system:5000/event", json=payload)
        except Exception as e:
            logger.error(f"Failed to send to external: {e}")
        return value

def main():
    # Initialize ClickHouse Table
    init_clickhouse()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # 1. Source: Kafka (Upsert)
    t_env.execute_sql("""
    CREATE TABLE kafka_events (
        id BIGINT,
        content_id STRING,
        user_id STRING,
        event_type STRING,
        event_ts TIMESTAMP(3),
        duration_ms INT,
        device STRING,
        raw_payload STRING,
        proc_time AS PROCTIME(),
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'engagement_events',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink-process-group',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """)

    # 2. Lookup: Postgres (Content)
    t_env.execute_sql("""
    CREATE TABLE content_dim (
        id STRING,
        slug STRING,
        title STRING,
        content_type STRING,
        length_seconds INT,
        publish_ts TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/engagement_db',
        'table-name' = 'content_dim_view',
        'username' = 'postgres',
        'password' = 'postgres'
    )
    """)

    # 3. Enrichment Logic (Lookup Join)
    enriched_table = t_env.sql_query("""
        SELECT 
            e.id, 
            e.content_id, 
            e.user_id, 
            e.event_type, 
            e.event_ts, 
            e.duration_ms, 
            e.device,
            CAST(e.duration_ms AS DOUBLE) / 1000.0 AS engagement_seconds,
            CASE 
                WHEN c.length_seconds > 0 THEN ROUND((CAST(e.duration_ms AS DOUBLE) / 1000.0) / c.length_seconds, 2)
                ELSE NULL 
            END AS engagement_pct,
            c.content_type, 
            c.length_seconds,
            e.proc_time
        FROM kafka_events AS e
        LEFT JOIN content_dim FOR SYSTEM_TIME AS OF e.proc_time AS c
        ON e.content_id = c.id
    """)

    # Register view (good practice, though not strictly needed for to_changelog_stream)
    t_env.create_temporary_view("enriched_table", enriched_table)

    # 4. Route to Sinks (DataStream)
    # Use to_changelog_stream to handle updates/deletes from Upsert Kafka source
    ds = t_env.to_changelog_stream(enriched_table)
    
    ds.map(ClickHouseSink())
    ds.map(ExternalSink())
    
    # C. Redis Sink (Aggregation)
    agg_table = t_env.sql_query("""
        SELECT 
            content_id,
            SUM(engagement_seconds)
        FROM enriched_table
        GROUP BY 
            content_id,
            HOP(proc_time, INTERVAL '5' SECOND, INTERVAL '10' MINUTE)
    """)
    
    ds_redis = t_env.to_changelog_stream(agg_table)
    ds_redis.map(RedisSink())
    
    # Execute
    env.execute("Engagement Pipeline")

if __name__ == "__main__":
    main()
