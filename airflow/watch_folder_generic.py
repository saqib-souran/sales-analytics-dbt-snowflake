"""Watch incoming CSV files and submit them to the Airflow ingestion DAG."""

import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

AIRFLOW_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(AIRFLOW_DIR / "src"))

from icommerce_ingestion.bronze_loader import classify_mode
from icommerce_ingestion.datasets import dataset_from_filename
from icommerce_ingestion.generic_batches import digest_file

INCOMING = AIRFLOW_DIR / "data" / "incoming"
QUEUE = AIRFLOW_DIR / "data" / "queue"
DAG_ID = "icommerce_ingestion"


def wait_until_complete(path: Path) -> None:
    """Wait until a copied file stops changing size."""
    previous_size = None
    unchanged = 0
    for _ in range(30):
        if not path.is_file():
            raise FileNotFoundError(path)
        size = path.stat().st_size
        unchanged = unchanged + 1 if size > 0 and size == previous_size else 0
        if unchanged >= 3:
            return
        previous_size = size
        time.sleep(0.5)
    raise RuntimeError(f"File is still being copied: {path.name}")


def submit(path: Path) -> None:
    if not path.is_file() or path.suffix.lower() != ".csv":
        return

    try:
        dataset = dataset_from_filename(path.name)
        wait_until_complete(path)
        batch_id = digest_file(path)
        mode = classify_mode(dataset, batch_id)

        queued = QUEUE / f"{dataset.lower()}__{batch_id}.csv"
        if queued.exists():
            if digest_file(queued) != batch_id:
                raise RuntimeError(f"Queued file has changed: {queued.name}")
        else:
            temporary = QUEUE / f".{uuid.uuid4().hex}.tmp"
            try:
                shutil.copyfile(path, temporary)
                if digest_file(temporary) != batch_id:
                    raise RuntimeError("File changed while being copied")
                temporary.replace(queued)
            finally:
                temporary.unlink(missing_ok=True)

        # Retries get a new Airflow run ID; Snowflake prevents duplicate loads.
        run_id = f"{mode}_{dataset.lower()}_{batch_id[:12]}_{uuid.uuid4().hex[:8]}"
        command = [
            "docker", "compose", "-f", "docker-compose.yaml",
            "exec", "-T", "airflow-lite", "airflow", "dags", "trigger",
            "--run-id", run_id,
            "--conf", json.dumps({"dataset": dataset, "batch_id": batch_id, "mode": mode}),
            DAG_ID,
        ]
        result = subprocess.run(command, cwd=AIRFLOW_DIR, capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError((result.stderr or result.stdout).strip())

        # Keep the queued copy so failed DAG runs can be retried.
        path.unlink()
        print(f"[triggered] {dataset} ({mode}) {batch_id[:12]}", flush=True)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[ERROR] {path.name}: {exc}", flush=True)


def main() -> None:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    INCOMING.mkdir(parents=True, exist_ok=True)
    QUEUE.mkdir(parents=True, exist_ok=True)

    class CsvHandler(FileSystemEventHandler):
        def on_created(self, event):
            if not event.is_directory:
                submit(Path(event.src_path))

        def on_moved(self, event):
            if not event.is_directory:
                submit(Path(event.dest_path))

    observer = Observer()
    observer.schedule(CsvHandler(), str(INCOMING), recursive=False)
    observer.start()
    print(f"Watching {INCOMING}", flush=True)

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
