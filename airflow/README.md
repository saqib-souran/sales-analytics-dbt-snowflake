# ICOMMERCE CATEGORY ingestion — Airflow phase 1

This starter preserves the source-to-Bronze responsibilities of the Talend job `jReadAndLoadCategory` and its reusable reader `jReadFileGeneric` while leaving the existing Snowflake and dbt project untouched.

The original data is pipe-separated. One category description is split over two physical lines and `C001` appears twice. This phase reconstructs multiline records and normalizes whitespace without dropping duplicate/missing business values. The project's existing dbt staging models own deduplication and rejection logic.

## 1. Install files into your existing Airflow project

Copy `dags/category_cleaner.py` and `dags/category_preparation_dag.py` into the existing `dags/` folder. Copy `data/CATEGORY.csv` to your `data/` folder. Create an empty folder `processed/` next to `docker-compose-lite.yaml`.

In the `airflow-lite` service in `docker-compose-lite.yaml`, include these bind mounts alongside the named volume you already use:

```yaml
volumes:
  - airflow-lite-data:/opt/airflow
  - ./dags:/opt/airflow/dags
  - ./data:/opt/airflow/data:ro
  - ./processed:/opt/airflow/processed
```

Run in the folder containing your compose YAML:

```powershell
docker compose -f docker-compose.yaml up -d --force-recreate
```

Trigger `icommerce_category_preparation` manually in the Airflow UI. The graph should be:

`check_source → prepare_source → verify_output`

On success, inspect `processed/CATEGORY_clean.csv` on Windows and review the logs. The included original CATEGORY source produces 5 logical records, including the repeated code `C001`, and one multiline description is reconstructed.

**No Snowflake table is created or overwritten in this phase.** The next phase will connect Airflow to Snowflake, test the connection, then load into a separate development table before touching any current `ICOMMERCE.BRONZE` source.

## 2. Run the normalizer without Airflow (optional)

```powershell
python .\dags\category_cleaner.py .\data\CATEGORY.csv .\processed\CATEGORY_clean.csv
python -m unittest discover -s tests -v
```

## Security and limitations

- Do **not** commit `.env`, `profiles.yml`, private keys or Talend connection exports with credentials.
- The included `.gitignore` intentionally excludes raw and prepared files by default. Review licensing before including source data in GitHub.
- This normalizer is designed for the two-column `CATEGORY.csv` format, not a universal CSV parser.
- Airflow `schedule=None` means the DAG must be manually triggered. File-arrival triggering will be added only after ingestion works reliably.
- A prepared output file is overwritten atomically on each run. `max_active_runs=1` limits overlap within this DAG; later we will add batch-specific file names for asynchronous file arrival.
