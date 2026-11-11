"""Integration checks for the built Compose image; no external service credentials."""

import importlib.metadata
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class ContainerRuntimeTests(unittest.TestCase):
    def test_scheduler_registered_the_dag_paused_without_a_next_run(self):
        from airflow.models.dag import DagModel
        from airflow.utils.session import create_session

        with create_session() as session:
            stored = session.get(DagModel, "customer360_pipeline")
            self.assertIsNotNone(
                stored, "DAG processor has not registered the mounted DAG"
            )
            self.assertTrue(
                stored.is_paused, "Local validation expects the DAG to remain paused"
            )
            self.assertIsNone(stored.next_dagrun)
            self.assertFalse(stored.has_import_errors)

    def test_airflow_and_snowflake_provider_versions(self):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        self.assertTrue(SnowflakeHook)
        self.assertEqual(importlib.metadata.version("apache-airflow"), "3.3.2")
        self.assertEqual(
            importlib.metadata.version("apache-airflow-providers-snowflake"), "6.17.0"
        )

    def test_dbt_is_available_in_an_isolated_environment(self):
        self.assertEqual(shutil.which("dbt"), "/usr/local/bin/dbt")
        self.assertEqual(Path(shutil.which("dbt")).resolve(), Path("/opt/dbt/bin/dbt"))
        result = subprocess.run(
            [
                "/opt/dbt/bin/python",
                "-c",
                "import importlib.metadata as m, importlib.util, json; "
                "print(json.dumps([m.version('dbt-core'), m.version('dbt-snowflake'), "
                "importlib.util.find_spec('airflow') is None]))",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(json.loads(result.stdout), ["1.12.6", "1.12.5", True])

    def test_sources_and_secret_directories_are_mounted_read_only(self):
        mounts = {}
        for line in Path("/proc/mounts").read_text().splitlines():
            fields = line.split()
            mounts[fields[1]] = fields[3].split(",")
        for directory in (
            "/opt/airflow/dags",
            "/opt/airflow/tests",
            "/opt/customer360/dbt",
            "/opt/airflow/secrets/dbt",
            "/opt/airflow/secrets/keys",
        ):
            with self.subTest(directory=directory):
                self.assertTrue(Path(directory).is_dir())
                self.assertIn("ro", mounts[directory])
        self.assertTrue(Path("/opt/airflow/dags/customer360_pipeline.py").is_file())
        self.assertTrue(Path("/opt/customer360/dbt/dbt_project.yml").is_file())

    def test_non_root_local_defaults_and_runtime_secret_files(self):
        from airflow.configuration import conf

        self.assertNotEqual(os.getuid(), 0)
        self.assertEqual(os.environ["CUSTOMER360_SCHEDULE"], "")
        self.assertTrue(conf.getboolean("core", "dags_are_paused_at_creation"))
        self.assertEqual(conf.get("core", "executor"), "LocalExecutor")
        self.assertEqual(
            os.environ["CUSTOMER360_DBT_PROJECT_DIR"], "/opt/customer360/dbt"
        )
        # Test presence only; never print or read the values into test output.
        for name in (
            "sql_alchemy_conn",
            "fernet_key",
            "jwt_secret",
            "api_secret_key",
            "simple_auth_passwords",
        ):
            with self.subTest(name=name):
                self.assertGreater((Path("/run/secrets") / name).stat().st_size, 0)

    def test_mounted_dbt_project_parses_without_external_credentials(self):
        # Parse only: this profile has no authentication and cannot run a build.
        with tempfile.TemporaryDirectory(prefix="customer360-parse-test-") as directory:
            root = Path(directory)
            (root / "profiles.yml").write_text(
                "customer360:\n  target: dev\n  outputs:\n    dev:\n"
                "      type: snowflake\n      account: unused_for_parse\n"
                "      user: unused_for_parse\n      warehouse: UNUSED_FOR_PARSE\n"
                "      database: CUSTOMER360_TEST\n      schema: SILVER\n"
                "      role: C360_DBT_ROLE\n      threads: 1\n"
            )
            environment = os.environ.copy()
            environment.update(
                DBT_TARGET_PATH=str(root / "target"),
                DBT_LOG_PATH=str(root / "logs"),
                DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            )
            subprocess.run(
                [
                    "dbt",
                    "parse",
                    "--no-partial-parse",
                    "--profiles-dir",
                    directory,
                    "--project-dir",
                    "/opt/customer360/dbt",
                    "--target",
                    "dev",
                ],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )
            manifest = json.loads((root / "target/manifest.json").read_text())
            models = [
                n for n in manifest["nodes"].values() if n["resource_type"] == "model"
            ]
            self.assertGreater(len(models), 0)
            for node in models:
                self.assertEqual(node["database"], "CUSTOMER360_TEST")
                self.assertIn(node["schema"], {"SILVER", "GOLD"})


if __name__ == "__main__":
    unittest.main()
