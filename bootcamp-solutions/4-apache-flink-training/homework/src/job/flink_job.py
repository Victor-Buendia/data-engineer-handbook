from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.expressions import lit
import os

def create_kafka_source(t_env: StreamTableEnvironment):
    """
    Creates and registers a Kafka source table in the Table Environment.
    
    The table schema includes an `event_timestamp` computed column which is crucial
    for event-time processing. A watermark is declared on this column, telling Flink
    how to handle late-arriving events.
    """
    bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS", "broker:29092")
    
    table_name = "web_events"
    # The pattern for the incoming timestamp string
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    
    # DDL for creating the Kafka source table
    source_ddl = f"""
        CREATE TABLE {table_name} (
            `url` VARCHAR,
            `referrer` VARCHAR,
            `user_agent` VARCHAR,
            `host` VARCHAR,
            `ip` VARCHAR,
            `headers` VARCHAR,
            `event_time` VARCHAR,
            -- Convert the string time into a proper SQL timestamp
            `event_timestamp` AS TO_TIMESTAMP(`event_time`, '{pattern}'),
            -- Define a watermark. This is essential for event-time session windows.
            -- It tells Flink that events can be up to 5 seconds late.
            WATERMARK FOR `event_timestamp` AS `event_timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'topic' = 'web_events',
            'properties.group.id' = 'web_events_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        );
    """
    print("--- KAFKA SOURCE DDL ---")
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def postgres_sink(t_env: StreamTableEnvironment):
    """
    Creates a simple print sink table for debugging and viewing results.
    """
    sink_ddl = f"""
        CREATE TABLE print_sink (
            `ip` VARCHAR,
            `host` VARCHAR,
            `session_start` TIMESTAMP(3),
            `session_end` TIMESTAMP(3),
            `event_count` BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = 'print_sink',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    print("\n--- POSTGRES SINK DDL ---")
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return "print_sink"

def sessionize_data(t_env: StreamTableEnvironment, source_table: str, sink_table: str):
    """
    Executes the main sessionization query.
    
    This query reads from the source table, groups events into sessions,
    and inserts the aggregated results into the sink table.
    """
    # TODO: This is the core of the homework.
    # We need to write a SQL query that:
    # 1. Groups events by `ip` and `host`.
    # 2. Uses a SESSION window with a 5-minute gap on the `event_timestamp` column.
    # 3. Calculates the start and end of the session.
    # 4. Counts the number of events in each session.
    
    session_query = f"""
        INSERT INTO {sink_table}
        SELECT
            ip,
            host,
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) as session_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) as session_end,
            COUNT(url) as event_count
        FROM {source_table}
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            ip,
            host
    """
    
    print("\n--- SESSIONIZATION QUERY ---")
    print(session_query)
    
    # Execute the query. This is an INSERT INTO statement, which is a common
    # way to connect a transformation to a sink in Flink SQL.
    t_env.execute_sql(session_query).wait()


def log_processing():
    """
    Main function to set up and run the Flink job.
    """
    print("Starting PyFlink Sessionization Job!")
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    # Checkpointing is good practice, enabling fault tolerance.
    env.enable_checkpointing(10 * 1000) # 10 seconds
    
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # 1. Create and register the Kafka source table
        source_table = create_kafka_source(t_env)
        
        # 2. Create and register the print sink table
        sink_table = postgres_sink(t_env)
        
        # 3. Define and execute the sessionization logic
        sessionize_data(t_env, source_table, sink_table)
        
    except Exception as e:
        print("Job failed:", str(e))


if __name__ == '__main__':
    log_processing()
