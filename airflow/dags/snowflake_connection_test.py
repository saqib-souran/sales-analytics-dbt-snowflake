from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(
    dag_id="icommerce_snowflake_connection_test",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Paris")),
    schedule=None,
    catchup=False,
    tags=["icommerce", "snowflake"],
)
def snowflake_connection_test():

    @task
    def check_connection():
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_icommerce"
        )

        result = hook.get_first("""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE()
        """)

        if result is None:
            raise RuntimeError("Snowflake returned no result")

        print(f"Database: {result[0]}")
        print(f"Schema: {result[1]}")
        print(f"Role: {result[2]}")
        print(f"Warehouse: {result[3]}")
        print("Snowflake connection successful!")

    check_connection()


snowflake_connection_test()
