# Category cleanup
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import Counter
from pathlib import Path

EXPECTED_HEADER = ["cd_category", "lib_category"]


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def _read_source(path: Path) -> str:
    payload = path.read_bytes()
    # UTF-8 is tried first for portability, then the original encoding.
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        return payload.decode("iso-8859-15")


def parse_category_rows(source: Path) -> list[list[str]]:
    """Join orphan continuation lines with the preceding description.

    An orphan line has no pipe separator. The input is intentionally limited
    to the two-column CATEGORY format; other files need their own validation.
    """
    rows: list[list[str]] = []
    current: list[str] | None = None
    header_seen = False

    for line_no, physical in enumerate(_read_source(source).splitlines(), start=1):
        line = _normalize_whitespace(physical)
        if not line:
            continue

        if not header_seen:
            fields = [_normalize_whitespace(x) for x in line.split("|")]
            if [x.lower() for x in fields] != EXPECTED_HEADER:
                raise ValueError(f"Unexpected CATEGORY header on line {line_no}: {fields!r}")
            header_seen = True
            continue

        if "|" in line:
            fields = [_normalize_whitespace(x) for x in line.split("|")]
            if len(fields) != 2:
                raise ValueError(f"Expected 2 columns on line {line_no}, got {len(fields)}")
            if current is not None:
                rows.append(current)
            current = fields
        else:
            if current is None:
                raise ValueError(f"Orphan line before first record at line {line_no}")
            current[1] = _normalize_whitespace(current[1] + " " + line)

    if not header_seen:
        raise ValueError("CATEGORY source has no header")
    if current is not None:
        rows.append(current)
    if not rows:
        raise ValueError("CATEGORY source has no data rows")
    return rows


def prepare_category(source: Path, destination: Path) -> dict:
    """Write validated pipe-delimited UTF-8 output atomically.

    Preserve missing values and duplicate business codes so dbt can perform
    its established acceptance/rejection rules downstream.
    """
    source = Path(source)
    destination = Path(destination)
    rows = parse_category_rows(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="|", lineterminator="\n")
            writer.writerow(EXPECTED_HEADER)
            writer.writerows(rows)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    counts = Counter(code for code, _ in rows if code)
    return {
        "source_path": str(source),
        "prepared_path": str(destination),
        "row_count": len(rows),
        "repeated_category_codes": sum(n - 1 for n in counts.values() if n > 1),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "prepared_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
    }


def verify_prepared_file(info: dict) -> None:
    destination = Path(info["prepared_path"])
    with destination.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="|")
        if reader.fieldnames != EXPECTED_HEADER:
            raise ValueError(f"Wrong prepared-file header: {reader.fieldnames!r}")
        actual_count = sum(1 for _ in reader)
    if actual_count != info["row_count"]:
        raise ValueError(f"Row count changed: expected {info['row_count']}, got {actual_count}")
    actual_sha = hashlib.sha256(destination.read_bytes()).hexdigest()
    if actual_sha != info["prepared_sha256"]:
        raise ValueError("Prepared file changed after preprocessing")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("source", type=Path)
    cli.add_argument("destination", type=Path)
    options = cli.parse_args()
    result = prepare_category(options.source, options.destination)
    verify_prepared_file(result)
    print(json.dumps(result, indent=2))
