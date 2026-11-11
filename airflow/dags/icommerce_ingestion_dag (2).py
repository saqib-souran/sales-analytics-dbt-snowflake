"""Load CSV batches into Bronze, then run dbt transformations."""
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import dag, get_current_context, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from icommerce_ingestion.datasets import get_dataset
from icommerce_ingestion.generic_batches import (
    digest_file, prepare_batch, validate_batch_id,
)
from icommerce_ingestion.bronze_loader import (
    build_bronze_file, cleanup_prepared, ingest_bronze,
    validate_mode, verify_bronze,
)
from icommerce_ingestion.dbt_orchestration import baselines_ready, run_dbt

CONN_ID = "snowflake_icommerce"
QUEUE_DIR = Path("/opt/airflow/data/queue")
PROCESSED_DIR = Path("/opt/airflow/processed")


@dag(
    dag_id="icommerce_ingestion",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Paris")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["icommerce", "snowflake", "bronze", "dbt"],
)
def icommerce_ingestion():
    @task
    def check_event() -> dict:
        conf = get_current_context()["dag_run"].conf or {}
        dataset, digest, mode = conf.get("dataset"), conf.get("batch_id"), conf.get("mode")
        get_dataset(dataset)
        digest = validate_batch_id(digest)
        validate_mode(dataset, digest, mode)
        source = QUEUE_DIR / f"{dataset.lower()}__{digest}.csv"
        if not source.is_file() or digest_file(source) != digest:
            raise ValueError(f"Queued original missing or modified: {source}")
        print(f"{dataset}: verified {mode} file {digest[:12]}")
        return {"dataset": dataset, "batch_id": digest, "mode": mode, "source": str(source)}

    @task
    def prepare_source(event: dict) -> dict:
        normal = prepare_batch(
            Path(event["source"]), PROCESSED_DIR,
            event["dataset"], event["batch_id"],
        )
        return build_bronze_file(normal, event["mode"])

    @task(retries=1, retry_delay=timedelta(seconds=20))
    def copy_to_original_bronze(info: dict) -> dict:
        return ingest_bronze(SnowflakeHook(snowflake_conn_id=CONN_ID), info)

    @task
    def verify_original_bronze(info: dict) -> dict:
        result = verify_bronze(SnowflakeHook(snowflake_conn_id=CONN_ID), info)
        print(f'{result["dataset"]}: {result["row_count"]} rows verified ({result["status"]})')
        return result

    @task
    def run_dbt_after_ingestion(info: dict) -> dict:
        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        if not baselines_ready(hook):
            if info["mode"] != "initial":
                raise RuntimeError("All seven initial datasets must load before dbt runs")
            print("Waiting for all seven initial CSVs before running dbt")
            return info
        run_dbt(hook.get_connection(CONN_ID))
        return info

    @task
    def cleanup_temp_files(info: dict) -> None:
        cleanup_prepared(info, SnowflakeHook(snowflake_conn_id=CONN_ID))
        print("Deleted disposable prepared files and staged file; raw queue retained")

    cleanup_temp_files(
        run_dbt_after_ingestion(
            verify_original_bronze(
                copy_to_original_bronze(prepare_source(check_event()))
            )
        )
    )


icommerce_ingestion()
