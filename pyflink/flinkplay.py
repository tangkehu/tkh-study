
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    setting = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env, environment_settings=setting)
    # t_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    # t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")

    base_dir = f"file://{os.path.abspath(os.path.dirname(__file__))}/jars"
    kafka_jar = f"{base_dir}/flink-sql-connector-kafka_2.11-1.11.1.jar"
    # json_format_jar = f"{base_dir}/flink-json-1.11.1.jar"
    es_jar = f"{base_dir}/flink-sql-connector-elasticsearch7_2.11-1.11.1.jar"
    jar_seq = [kafka_jar, es_jar]
    jars = ";".join(jar_seq)
    t_env.get_config().get_configuration().set_string("pipeline.jars", jars)

    ddl_datagen = """
    CREATE TABLE datagen_tb (
        random_str STRING
    ) WITH (
        'connector'='datagen',
        'row-per-second'='5',
        'fields.random_str.length'='10'
    )
    """
    ddl_print = """
    CREATE TABLE print_tb WITH ('connector' = 'print') LIKE source_tb (EXCLUDING ALL)
    """
    ddl_kafka = """
    CREATE TABLE kafka_tb (
        http ROW(hostname STRING, http_method STRING)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'suricata_http',
        'properties.bootstrap.servers' = '10.0.3.211:9092',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'scan.startup.mode' = 'earliest-offset'
    )
    """
    ddl_file = """
    CREATE TABLE file_tb (
        host_count BIGINT,
        http_method STRING
    ) WITH (
        'connector'='filesystem',
        'path'='./result.csv',
        'format'='csv'
    )
    """
    ddl_es = """
    CREATE TABLE es_tb (
      host_count BIGINT,
      http_method STRING
    ) WITH (
      'connector' = 'elasticsearch-7',
      'hosts' = 'http://10.0.3.68:9200',
      'index' = 'flink_test'
    )
    """

    t_env.execute_sql(ddl_kafka)
    t_env.execute_sql(ddl_es)

    t_r = t_env.execute_sql("""
    INSERT INTO es_tb
    SELECT COUNT(*) AS host_count, http.http_method AS http_method FROM kafka_tb 
    WHERE http.hostname is not NULL 
    GROUP BY http.http_method
    """)
    t_r.get_job_client().get_job_execution_result().result()


if __name__ == '__main__':
    main()
