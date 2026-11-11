"""Run the existing ICOMMERCE dbt project in dependency order."""
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
        runner([dbt, *command, *([] if command[0] == "deps" else ["--no-partial-parse"])], cwd=PROJECT_DIR, check=True)


if __name__ == "__main__":
    run_pipeline()
