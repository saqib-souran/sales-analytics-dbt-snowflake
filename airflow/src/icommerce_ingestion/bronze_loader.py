"""Final seven-dataset ICOMMERCE ingest: full baseline once, then append batches.

Uses the existing validated generic_batches parser and Snowflake's table stages.
No temporary business tables, deduplication, or dbt dependencies.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import uuid

from icommerce_ingestion.datasets import DATASETS, get_dataset
from icommerce_ingestion.generic_batches import digest_file, validate_batch_id, verify_batch

DB_SCHEMA = "ICOMMERCE.BRONZE"
STAGE = "ICOMMERCE.BRONZE.ICOMMERCE_CSV_STAGE"

# Hashes of the seven originals validated in this project (unmodified files).
BASELINE = {
    "CATEGORY": "ede458dfe4906b5b1fd33e45e1d0b48a59499f9ff32d7604c2ee5d5940cf7156",
    "SUB_CATEGORY": "8f6ad2425b05ce66eb397422089612fe389556dac855834fdacefa95802aeeef",
    "TYPE_CUSTOMER": "ad9614e225720789688f338655b5d014b2ba55301689f6d69b1ee46dafaea0b0",
    "CUSTOMER": "263382ca2a192352756c6f23268772bb7d735b49a78050493aba6ed723dec0c7",
    "PRODUCT": "7f8f813c07db82af7b5fca6d54907e9d52bfc541a673370e3dc4626e2266052f",
    "SALE": "b32f51ef07c2f8fc346d6089027fd8ca41bca4dd6ebdc3c9d1f03b6288fcb894",
    "DETAIL_SALE": "81b66becf9a051fb2d4398b10020009af95bb58cd54cede65559a1b69572535f",
}

BASELINE_COUNTS = {
    "CATEGORY": 5,
    "SUB_CATEGORY": 18,
    "TYPE_CUSTOMER": 4,
    "CUSTOMER": 17545,
    "PRODUCT": 3799,
    "SALE": 505,
    "DETAIL_SALE": 505,
}


def classify_mode(dataset: str, batch_id: str) -> str:
    get_dataset(dataset)
    digest = validate_batch_id(batch_id)
    return "initial" if BASELINE[dataset] == digest else "incremental"


def validate_mode(dataset: str, batch_id: str, mode: str) -> None:
    if mode not in ("initial", "incremental"):
        raise ValueError("Mode must be 'initial' or 'incremental'")
    actual = classify_mode(dataset, batch_id)
    if mode != actual:
        raise ValueError(f"{dataset}: file hash classifies as {actual}, not {mode}")


def table_for(dataset: str) -> str:
    get_dataset(dataset)  # strict allowlist of identifiers
    return f"{DB_SCHEMA}.{dataset}"


def stage_filename(dataset: str, digest: str) -> str:
    get_dataset(dataset)
    validate_batch_id(digest)
    return f"{dataset}_{digest}_bronze.csv.gz"


def build_bronze_file(info: dict, mode: str) -> dict:
    
    verify_batch(info)
    dataset, digest = info["dataset"], info["batch_id"]
    validate_mode(dataset, digest, mode)
    spec = get_dataset(dataset)
    if mode == "initial" and info["row_count"] != BASELINE_COUNTS[dataset]:
        raise ValueError(f"{dataset}: initial file row count is unexpected")

    load_path = Path(info["load_path"])
    bronze_path = load_path.with_name(f"{dataset}_{digest}_bronze.csv")
    temporary = bronze_path.with_name(f".{uuid.uuid4().hex}.tmp")
    batch_time = (
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        if mode == "incremental" else ""
    )
    key_idx = spec.columns.index(spec.incremental_key) if spec.incremental_key else None
    keys = set()
    count = 0
    try:
        with load_path.open("r", encoding="utf-8", newline="") as src, \
                temporary.open("w", encoding="utf-8", newline="") as dst:
            reader = csv.reader(src, delimiter="|", strict=True)
            writer = csv.writer(dst, delimiter="|", lineterminator="\n")
            header = next(reader, None)
            if header != [*spec.columns, "source_sha256"]:
                raise ValueError("Unexpected prepared CSV header")
            writer.writerow([*spec.columns, "source_sha256", "ingested_at"])
            for row in reader:
                if len(row) != len(header) or row[-1] != digest:
                    raise ValueError("Unexpected prepared CSV record")
                if key_idx is not None and mode == "incremental":
                    key = row[key_idx].strip()
                    if key.upper().startswith("DEMO-"):
                        raise ValueError("Synthetic DEMO business keys must not enter original Bronze")
                    if key:
                        if key in keys:
                            raise ValueError(f"Duplicate {spec.incremental_key} inside incremental batch: {key}")
                        keys.add(key)
                writer.writerow([*row, batch_time])
                count += 1
        if count != int(info["row_count"]):
            raise ValueError("Prepared row count changed")
        temporary.replace(bronze_path)
    finally:
        temporary.unlink(missing_ok=True)

    return {
        **info, "mode": mode, "bronze_path": str(bronze_path),
        "bronze_sha256": digest_file(bronze_path),
    }


def verify_bronze_file(info: dict) -> None:
    validate_mode(info["dataset"], info["batch_id"], info["mode"])
    path = Path(info["bronze_path"])
    if path.name != f'{info["dataset"]}_{info["batch_id"]}_bronze.csv':
        raise ValueError("Unexpected Bronze staging filename")
    if not path.is_file() or digest_file(path) != info["bronze_sha256"]:
        raise ValueError("Bronze CSV was modified")


def build_copy_sql(dataset: str, digest: str) -> str:
    spec = get_dataset(dataset)
    validate_batch_id(digest)
    columns = ", ".join([*(c.upper() for c in spec.columns), "SOURCE_SHA256", "INGESTED_AT"])
    fields = ", ".join(f"s.${i}" for i in range(1, len(spec.columns) + 3))
    return f"""COPY INTO {table_for(dataset)} ({columns})
FROM (SELECT {fields} FROM @{STAGE} s)
FILES = ('{stage_filename(dataset, digest)}')
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
    ESCAPE_UNENCLOSED_FIELD = NONE
    SKIP_HEADER = 1
    ENCODING = 'UTF8'
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
)
ON_ERROR = ABORT_STATEMENT
FORCE = FALSE"""


def _count(hook, sql: str) -> int:
    row = hook.get_first(sql)
    if row is None or row[0] is None:
        raise RuntimeError("Snowflake count query returned no result")
    return int(row[0])


def ingest_bronze(hook, info: dict) -> dict:
    verify_bronze_file(info)
    dataset, digest, mode = info["dataset"], info["batch_id"], info["mode"]
    table = table_for(dataset)
    expected = int(info["row_count"])
    existing = _count(hook, f"SELECT COUNT(*) FROM {table} WHERE SOURCE_SHA256 = '{digest}'")
    if existing == expected:
        return {**info, "status": "already_loaded"}
    if existing:
        raise RuntimeError(f"{dataset}: partial batch already present ({existing} of {expected})")

    total = _count(hook, f"SELECT COUNT(*) FROM {table}")
    if mode == "initial" and total != 0:
        raise RuntimeError(f"{dataset}: initial load requires empty original Bronze; found {total} rows")
    if mode == "incremental" and total == 0:
        raise RuntimeError(f"{dataset}: baseline missing; load original CSV before incremental batches")

    stage = f"@{STAGE}"
    path = Path(info["bronze_path"])
    # Identical checksum => stage filename is stable. Overwrite allows re-stage
    # after a previous failed attempt; the target checksum is checked first.
    hook.run(f"PUT 'file://{path.as_posix()}' {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    records = hook.get_records(build_copy_sql(dataset, digest))
    if len(records) != 1 or len(records[0]) < 4:
        raise RuntimeError(f"{dataset}: unexpected COPY result: {records}")
    loaded = int(records[0][3])
    if loaded != expected:
        raise RuntimeError(f"{dataset}: COPY loaded {loaded} rows, expected {expected}: {records}")
    return {**info, "status": "loaded"}


def verify_bronze(hook, info: dict) -> dict:
    dataset, digest, mode = info["dataset"], info["batch_id"], info["mode"]
    validate_mode(dataset, digest, mode)
    table = table_for(dataset)
    expected = int(info["row_count"])
    got = _count(hook, f"SELECT COUNT(*) FROM {table} WHERE SOURCE_SHA256 = '{digest}'")
    if got != expected:
        raise RuntimeError(f"{dataset}: verification returned {got}, expected {expected}")
    marker = "IS NULL" if mode == "initial" else "IS NOT NULL"
    correct_time = _count(hook, f"SELECT COUNT(*) FROM {table} WHERE SOURCE_SHA256 = '{digest}' AND INGESTED_AT {marker}")
    if correct_time != expected:
        raise RuntimeError(f"{dataset}: ingestion timestamp check failed: {correct_time}/{expected}")
    return info


def cleanup_prepared(info: dict, hook=None) -> None:
    """Clean disposable normalized/staging CSVs after VERIFIED success only.

    The raw queued file is intentionally retained for troubleshooting/replay.
    """
    validate_mode(info["dataset"], info["batch_id"], info["mode"])
    if hook is not None:
        hook.run(f"REMOVE @{STAGE}/{stage_filename(info['dataset'], info['batch_id'])}")
    for field in ("load_path", "bronze_path"):
        path = Path(info[field])
        expected_end = (
            f'{info["dataset"]}_{info["batch_id"]}_load.csv'
            if field == "load_path" else f'{info["dataset"]}_{info["batch_id"]}_bronze.csv'
        )
        if path.name != expected_end:
            raise ValueError("Refusing to remove file with an unexpected name")
        path.unlink(missing_ok=True)
