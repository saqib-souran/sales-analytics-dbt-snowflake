"""Run dbt after the seven original Bronze datasets are available."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile

from icommerce_ingestion.bronze_loader import BASELINE, BASELINE_COUNTS, table_for

PROJECT_DIR = Path("/opt/airflow/dbt_project")
PYTHON = Path("/opt/dbt-venv/bin/python")
DBT = Path("/opt/dbt-venv/bin/dbt")
RUNNER = PROJECT_DIR / "scripts/run_dbt_pipeline.py"


def baselines_ready(hook) -> bool:
    """Check whether all original CSVs have been loaded."""
    checks = [
        f"(SELECT COUNT(*) FROM {table_for(dataset)} "
        f"WHERE SOURCE_SHA256 = '{digest}') = {BASELINE_COUNTS[dataset]}"
        for dataset, digest in BASELINE.items()
    ]
    result = hook.get_first("SELECT " + " AND ".join(checks))
    if not result or result[0] is None:
        raise RuntimeError("Could not verify the seven initial Bronze loads")
    return result[0] in (True, 1)


def _extra(extra: dict, key: str) -> str:
    return str(extra.get(key) or extra.get(f"extra__snowflake__{key}") or "").strip()


def _profile(has_role: bool) -> str:
    role = '      role: "{{ env_var(\'DBT_SNOWFLAKE_ROLE\') }}"\n' if has_role else ""
    return (
        "icommerce:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        "      account: \"{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}\"\n"
        "      user: \"{{ env_var('DBT_SNOWFLAKE_USER') }}\"\n"
        "      password: \"{{ env_var('DBT_ENV_SECRET_SNOWFLAKE_PASSWORD') }}\"\n"
        "      warehouse: \"{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}\"\n"
        f"{role}"
        "      database: ICOMMERCE\n"
        "      schema: BRONZE\n"
        "      threads: 2\n"
    )


def run_dbt(connection, runner=subprocess.run) -> None:
    """Run dbt using Snowflake credentials from the Airflow connection."""
    if not RUNNER.is_file() or not (PROJECT_DIR / "dbt_project.yml").is_file():
        raise FileNotFoundError("The dbt project/scripts are not mounted inside Docker")
    if not PYTHON.is_file() or not DBT.is_file():
        raise FileNotFoundError("dbt is missing in Docker; rebuild the Airflow image")

    extra = connection.extra_dejson or {}
    account = _extra(extra, "account")
    warehouse = _extra(extra, "warehouse")
    user = connection.login or ""
    password = connection.password or ""
    authenticator = _extra(extra, "authenticator").lower()
    if authenticator not in ("", "snowflake"):
        raise ValueError("This local dbt runner supports password-based Snowflake authentication only")
    if not all((account, warehouse, user, password)):
        raise ValueError("Airflow Snowflake connection needs account, warehouse, login and password")

    env = os.environ.copy()
    env.update({
        "DBT_SNOWFLAKE_ACCOUNT": account,
        "DBT_SNOWFLAKE_WAREHOUSE": warehouse,
        "DBT_SNOWFLAKE_USER": user,
        "DBT_ENV_SECRET_SNOWFLAKE_PASSWORD": password,
        "DBT_EXECUTABLE": str(DBT),
    })
    role = _extra(extra, "role")
    if role:
        env["DBT_SNOWFLAKE_ROLE"] = role

    # Keep credentials out of the temporary profiles.yml file.
    with tempfile.TemporaryDirectory(prefix="icommerce-dbt-") as folder:
        (Path(folder) / "profiles.yml").write_text(_profile(bool(role)), encoding="utf-8")
        env["DBT_PROFILES_DIR"] = folder
        runner([str(PYTHON), str(RUNNER)], cwd=PROJECT_DIR, env=env, check=True)
