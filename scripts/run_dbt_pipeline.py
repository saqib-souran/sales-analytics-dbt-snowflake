"""Run staging, snapshots, Silver, Gold, and dbt tests in order."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

PROJECT_DIR = Path(__file__).resolve().parent.parent
COMMANDS = (
    ("deps",),
    ("seed",),
    ("run", "--select", "path:models/staging"),
    ("snapshot",),
    ("build", "--select", "path:models/silver", "path:models/gold"),
    ("test",),
)


def run_pipeline(runner=subprocess.run) -> None:
    dbt = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
    if not dbt:
        raise RuntimeError("dbt executable not found; activate your dbt environment")
    for command in COMMANDS:
        print("Running dbt " + " ".join(command), flush=True)
        args = [dbt, *command]
        if command[0] != "deps":
            args.append("--no-partial-parse")
        runner(args, cwd=PROJECT_DIR, check=True)


if __name__ == "__main__":
    run_pipeline()
