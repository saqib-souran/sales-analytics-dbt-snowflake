"""Offline checks for the Python dbt runner and Airflow handoff."""
from pathlib import Path
import importlib.util
import sys
import tempfile
from types import SimpleNamespace
from unittest import TestCase, mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from icommerce_ingestion import dbt_orchestration as automation

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/run_dbt_pipeline.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("run_dbt_pipeline", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PythonRunnerTests(TestCase):
    def test_same_six_commands_as_powershell(self):
        module = load_runner()
        run = mock.Mock()
        with mock.patch.dict("os.environ", {"DBT_EXECUTABLE": "dbt"}):
            module.run_pipeline(runner=run)
        self.assertEqual(run.call_count, 6)
        expected = [
            "deps", "seed", "run", "snapshot", "build", "test",
        ]
        self.assertEqual([c.args[0][1] for c in run.call_args_list], expected)
        for c in run.call_args_list:
            self.assertTrue(c.kwargs["check"])
            self.assertEqual(c.kwargs["cwd"], module.PROJECT_DIR)

    def test_baseline_guard(self):
        ready = SimpleNamespace(get_first=lambda sql: (True,))
        not_ready = SimpleNamespace(get_first=lambda sql: (False,))
        self.assertTrue(automation.baselines_ready(ready))
        self.assertFalse(automation.baselines_ready(not_ready))

    def test_handoff_uses_existing_airflow_connection(self):
        conn = SimpleNamespace(
            login="test-user", password="private-password",
            extra_dejson={"account": "test-account", "warehouse": "test-wh", "role": "test-role"}
        )
        with tempfile.TemporaryDirectory() as temp:
            project = Path(temp)
            (project / "dbt_project.yml").write_text("name: 'icommerce'\n")
            (project / "scripts").mkdir()
            (project / "scripts/run_dbt_pipeline.py").write_text("pass\n")
            (project / "python").touch()
            (project / "dbt").touch()
            calls = []
            def fake_run(*args, **kwargs):
                profile_path = Path(kwargs["env"]["DBT_PROFILES_DIR"]) / "profiles.yml"
                profile = profile_path.read_text()
                self.assertNotIn("private-password", profile)
                calls.append((args, kwargs))
            with mock.patch.object(automation, "PROJECT_DIR", project), \
                 mock.patch.object(automation, "PYTHON", project / "python"), \
                 mock.patch.object(automation, "DBT", project / "dbt"), \
                 mock.patch.object(automation, "RUNNER", project / "scripts/run_dbt_pipeline.py"):
                automation.run_dbt(conn, runner=fake_run)
            self.assertEqual(len(calls), 1)
            env = calls[0][1]["env"]
            self.assertEqual(env["DBT_ENV_SECRET_SNOWFLAKE_PASSWORD"], "private-password")
            self.assertEqual(env["DBT_SNOWFLAKE_ROLE"], "test-role")

    def test_missing_password_fails(self):
        conn = SimpleNamespace(login="test-user", password="", extra_dejson={"account": "acct", "warehouse": "wh"})
        with tempfile.TemporaryDirectory() as temp:
            project = Path(temp)
            (project / "dbt_project.yml").touch()
            (project / "scripts").mkdir()
            (project / "scripts/run_dbt_pipeline.py").touch()
            (project / "python").touch()
            (project / "dbt").touch()
            with mock.patch.object(automation, "PROJECT_DIR", project), \
                 mock.patch.object(automation, "PYTHON", project / "python"), \
                 mock.patch.object(automation, "DBT", project / "dbt"), \
                 mock.patch.object(automation, "RUNNER", project / "scripts/run_dbt_pipeline.py"):
                with self.assertRaisesRegex(ValueError, "account, warehouse, login and password"):
                    automation.run_dbt(conn)
