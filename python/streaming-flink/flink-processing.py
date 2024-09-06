import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

# Make sure you have the following JARs in your PyFlink environment:
# flink-sql-connector-kafka-1.15.0.jar
# flink-connector-jdbc-1.15.0.jar
# google-cloud-bigquery-1.27.0.jar
# slf4j-api-1.7.30.jar
# slf4j-log4j12-1.7.30.jar

def create_kafka_source_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE clickstream (
            user_id STRING,
            session_id STRING,
            timestamp BIGINT,
            event_type STRING,
            product_id STRING,
            product_category STRING,
            price DOUBLE,
            quantity INT,
            source STRING,
            page_url STRING,
            user_agent STRING,
            ip_address STRING,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000, 'yyyy-MM-dd HH:mm:ss')),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ecommerce_clickstream',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'pyflink-ecommerce-analysis',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

def create_bigquery_sink_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE metrics_sink (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            metric_name STRING,
            metric_value BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=your-project-id;OAuthType=3;',
            'table-name' = 'your-dataset.ecommerce_metrics',
            'driver' = 'com.simba.googlebigquery.jdbc42.Driver',
            'username' = 'dummy',  -- BigQuery JDBC driver doesn't use username
            'password' = '',  -- Use an empty string if using application default credentials
            'additional_properties' = 'oauth_type=application_default'
        )
    """)

def calculate_metrics(t_env):
    # Event counts by type and source
    event_counts = t_env.sql_query("""
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            CONCAT(event_type, '_', source) AS metric_name,
            COUNT(*) AS metric_value
        FROM clickstream
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), event_type, source
    """)

    # Revenue by product category
    revenue = t_env.sql_query("""
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            CONCAT('revenue_', product_category) AS metric_name,
            CAST(SUM(price * quantity) AS BIGINT) AS metric_value
        FROM clickstream
        WHERE event_type = 'purchase'
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), product_category
    """)

    # Unique visitors
    visitors = t_env.sql_query("""
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            'unique_visitors' AS metric_name,
            COUNT(DISTINCT user_id) AS metric_value
        FROM clickstream
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # Union all metrics
    return event_counts.union_all(revenue).union_all(visitors)

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # Adjust based on your cluster resources
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Add required JARs
    dir_path = os.path.dirname(os.path.realpath(__file__))
    jar_path = os.path.join(dir_path, "jars")
    for file in os.listdir(jar_path):
        if file.endswith(".jar"):
            t_env.get_config().get_configuration().set_string(
                "pipeline.jars", f"file://{os.path.join(jar_path, file)}"
            )

    # Create source and sink tables
    create_kafka_source_table(t_env)
    create_bigquery_sink_table(t_env)

    # Calculate metrics and insert into sink
    metrics = calculate_metrics(t_env)
    metrics.execute_insert('metrics_sink')

    # Execute the job
    t_env.execute("E-commerce Clickstream Analysis")

if __name__ == "__main__":
    main()