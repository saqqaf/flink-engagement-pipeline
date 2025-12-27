from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 1. Source: Postgres CDC
    t_env.execute_sql("""
    CREATE TABLE source_events (
        id BIGINT,
        content_id STRING,
        user_id STRING,
        event_type STRING,
        event_ts TIMESTAMP(3),
        duration_ms INT,
        device STRING,
        raw_payload STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'postgres-cdc',
        'hostname' = 'postgres',
        'port' = '5432',
        'username' = 'postgres',
        'password' = 'postgres',
        'database-name' = 'engagement_db',
        'schema-name' = 'public',
        'table-name' = 'engagement_events',
        'slot.name' = 'flink_cdc_slot',
        'decoding.plugin.name' = 'pgoutput'
    )
    """)

    # 2. Sink: Kafka
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
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'engagement_events',
        'properties.bootstrap.servers' = 'kafka:29092',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """)

    # 3. Execute Insert
    t_env.execute_sql("INSERT INTO kafka_events SELECT * FROM source_events")

if __name__ == "__main__":
    main()
