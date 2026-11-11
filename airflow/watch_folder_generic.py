"""Watch Windows-host incoming CSVs and trigger the final one-DAG Bronze pipeline.

After a successful DAG submission the incoming original is removed; its immutable
SHA-addressed copy remains in queue for retry and audit until manually archived.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

AIRFLOW_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(AIRFLOW_DIR / "src"))
from icommerce_ingestion.datasets import dataset_from_filename  # noqa: E402
from icommerce_ingestion.bronze_loader import classify_mode  # noqa: E402

INCOMING = AIRFLOW_DIR / "data" / "incoming"
QUEUE = AIRFLOW_DIR / "data" / "queue"
DAG_ID = "icommerce_ingestion"


def digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def stable_file(path: Path) -> None:
    size_before, stable = None, 0
    for _ in range(30):
        if not path.is_file():
            raise FileNotFoundError(path)
        size = path.stat().st_size
        stable = stable + 1 if size > 0 and size == size_before else 0
        if stable >= 3:
            return
        size_before = size
        time.sleep(0.5)
    raise RuntimeError(f"File not stable: {path.name}")


def submit(path: Path) -> None:
    if not path.is_file() or path.suffix.lower() != ".csv":
        return
    try:
        dataset = dataset_from_filename(path.name)
        stable_file(path)
        batch_id = digest(path)
        mode = classify_mode(dataset, batch_id)
        queued = QUEUE / f"{dataset.lower()}__{batch_id}.csv"
        if queued.exists():
            if digest(queued) != batch_id:
                raise RuntimeError(f"Queued snapshot corrupted: {queued.name}")
        else:
            temporary = QUEUE / f".{uuid.uuid4().hex}.tmp"
            try:
                shutil.copyfile(path, temporary)
                if digest(temporary) != batch_id:
                    raise RuntimeError("Source changed during queue copy")
                temporary.replace(queued)
            finally:
                temporary.unlink(missing_ok=True)

        # Randomized run ID permits explicit retry after an Airflow task failure.
        # Idempotence is enforced in Snowflake using the FULL SHA-256, not run ID.
        run_id = f"{mode}_{dataset.lower()}_{batch_id[:12]}_{uuid.uuid4().hex[:8]}"
        command = [
            "docker", "compose", "-f", "docker-compose.yaml",
            "exec", "-T", "airflow-lite", "airflow", "dags", "trigger",
            "--run-id", run_id,
            "--conf", json.dumps({"dataset": dataset, "batch_id": batch_id, "mode": mode}),
            DAG_ID,
        ]
        result = subprocess.run(command, cwd=AIRFLOW_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout).strip())
        # Safe: queue has an immutable copy and DAG was submitted successfully.
        path.unlink()
        print(f"[triggered] {dataset} {mode} {batch_id[:12]}; incoming cleared", flush=True)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[ERROR] {path.name}: {exc}", flush=True)


def main() -> None:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    INCOMING.mkdir(parents=True, exist_ok=True)
    QUEUE.mkdir(parents=True, exist_ok=True)

    class Handler(FileSystemEventHandler):
        def on_created(self, event):
            if not event.is_directory:
                submit(Path(event.src_path))

        def on_moved(self, event):
            if not event.is_directory:
                submit(Path(event.dest_path))

    observer = Observer()
    observer.schedule(Handler(), str(INCOMING), recursive=False)
    observer.start()
    print(f"Watching {INCOMING}; initial hashes auto-detected", flush=True)
    for path in sorted(INCOMING.glob("*.csv")):
        submit(path)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
