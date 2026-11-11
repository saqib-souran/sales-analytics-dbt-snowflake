"""Convert allowlisted source CSV files to validated UTF-8 pipe CSV batches.

No business-key deduplication: that belongs in existing dbt staging.
No Airflow dependency: unit-testable on Windows.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import uuid
from pathlib import Path
from typing import Iterator

from icommerce_ingestion.datasets import get_dataset

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def validate_batch_id(value: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError("batch_id must be a lowercase SHA-256 digest")
    return value


def digest_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _decode(source: Path) -> str:
    data = Path(source).read_bytes()
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Some original CSV bytes use a Western European legacy encoding.
        return data.decode("iso-8859-15")


def _squash(value: str) -> str:
    return re.sub(r"[ \t]+", " ", value).strip()


def _repaired_rows(text: str, columns: tuple[str, ...]) -> Iterator[list[str]]:
    """Repair known CATEGORY/SUB_CATEGORY split-line formats.

    CATEGORY example:    C004|Other + next line 'categories'
    SUB_CATEGORY example: SC013|Filing + next line 'Cabinets|C003'
    Deliberately reject quote syntax in this simple repair mode.
    """
    count = len(columns)
    current: list[str] | None = None
    lines = iter(enumerate(text.splitlines(), start=1))
    first = next(lines, None)
    if first is None or tuple(_squash(x).lower() for x in first[1].split("|")) != columns:
        raise ValueError(f"Wrong header: expected {'|'.join(columns)}")

    for line_no, physical in lines:
        if not physical.strip():
            continue
        if '"' in physical:
            raise ValueError(f"Quoted fields are not supported by multiline repair (line {line_no})")
        parts = [_squash(x) for x in physical.split("|")]

        # A partial row continues from the previous physical line.
        if current is not None and len(current) < count:
            current[-1] = _squash(current[-1] + " " + parts[0])
            current.extend(parts[1:])
            if len(current) > count:
                raise ValueError(f"Too many fields in joined record at line {line_no}")
            continue

        # A one-part line after a complete row extends the description.
        if len(parts) == 1 and current is not None:
            current[1] = _squash(current[1] + " " + parts[0])
            continue

        if current is not None:
            if len(current) != count:
                raise ValueError(f"Incomplete record before line {line_no}")
            yield current
        if not 2 <= len(parts) <= count:
            raise ValueError(f"Wrong column count at line {line_no}: {len(parts)}")
        current = parts

    if current is not None:
        if len(current) != count:
            raise ValueError("Incomplete final record")
        yield current


def _ordinary_rows(text: str, columns: tuple[str, ...]) -> Iterator[list[str]]:
    reader = csv.reader(io.StringIO(text, newline=""), delimiter="|", strict=True)
    header = next(reader, None)
    if header is None or tuple(x.strip().lower() for x in header) != columns:
        raise ValueError(f"Wrong header: expected {'|'.join(columns)}")
    for row in reader:
        if len(row) != len(columns):
            raise ValueError(f"Wrong column count: expected {len(columns)}, got {len(row)}")
        yield row


def prepare_batch(source: Path, output_dir: Path, dataset: str, batch_id: str) -> dict:
    spec = get_dataset(dataset)
    batch_id = validate_batch_id(batch_id)
    source = Path(source)
    if not source.is_file() or digest_file(source) != batch_id:
        raise ValueError("Missing or modified queued source file")

    text = _decode(source)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{dataset}_{batch_id}_load.csv"
    temp = output_dir / f".{dataset}_{batch_id}_{uuid.uuid4().hex}.tmp"
    count = 0
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="|", lineterminator="\n")
            writer.writerow([*spec.columns, "source_sha256"])
            reader = (
                _repaired_rows(text, spec.columns)
                if spec.multiline_repair
                else _ordinary_rows(text, spec.columns)
            )
            for row in reader:
                writer.writerow([*row, batch_id])
                count += 1
        if count == 0:
            raise ValueError("Source file has no data records")
        if digest_file(source) != batch_id:
            raise ValueError("Source changed during preparation")
        os.replace(temp, output)
    finally:
        temp.unlink(missing_ok=True)

    return {
        "dataset": dataset,
        "batch_id": batch_id,
        "row_count": count,
        "load_path": str(output),
        "load_sha256": digest_file(output),
    }


def verify_batch(info: dict) -> None:
    spec = get_dataset(info["dataset"])
    batch_id = validate_batch_id(info["batch_id"])
    path = Path(info["load_path"])
    if path.name != f"{info['dataset']}_{batch_id}_load.csv":
        raise ValueError("Unexpected load file name")
    if digest_file(path) != info["load_sha256"]:
        raise ValueError("Prepared file content changed")
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="|", strict=True)
        if reader.fieldnames != [*spec.columns, "source_sha256"]:
            raise ValueError("Prepared file has unexpected columns")
        count = 0
        for row in reader:
            if None in row or row["source_sha256"] != batch_id:
                raise ValueError("Invalid batch checksum or field count")
            count += 1
    if count != int(info["row_count"]):
        raise ValueError("Prepared row count changed")
