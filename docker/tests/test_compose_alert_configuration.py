"""Evaluate real Compose precedence using synthetic, isolated dotenv fixtures only."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

COMPOSE_FILE = Path(__file__).resolve().parents[2] / "docker-compose.yaml"


class ComposeAlertConfigurationTests(unittest.TestCase):
    def airflow_services(self, config):
        services = [name for name in config["services"] if name.startswith("airflow-")]
        self.assertTrue(services, "Compose must define Airflow services")
        return services

    def resolve(self, root_value=None, runtime_value=None, shell_value=None):
        with tempfile.TemporaryDirectory(
            prefix="customer360-compose-alerts-"
        ) as directory:
            root = Path(directory)
            dotenv = root / ".env"
            dotenv.write_text(
                ""
                if root_value is None
                else f"CUSTOMER360_ALERT_EMAILS={root_value}\n",
                encoding="utf-8",
            )
            runtime = root / ".local/customer360/runtime.env"
            runtime.parent.mkdir(parents=True)
            runtime.write_text(
                ""
                if runtime_value is None
                else f"CUSTOMER360_ALERT_EMAILS={runtime_value}\n",
                encoding="utf-8",
            )
            # Exclude host configuration; real dotenv files and credentials are never loaded.
            environment = {
                key: value
                for key, value in os.environ.items()
                if not key.upper().startswith(("CUSTOMER360_", "COMPOSE_"))
            }
            if shell_value is not None:
                environment["CUSTOMER360_ALERT_EMAILS"] = shell_value
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "--project-directory",
                    directory,
                    "--env-file",
                    str(dotenv),
                    "-f",
                    str(COMPOSE_FILE),
                    "config",
                    "--format",
                    "json",
                ],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            # Never print expanded Compose output, even when an assertion fails.
            config = json.loads(result.stdout) if result.returncode == 0 else None
            return result.returncode, config, result.stderr

    def test_root_dotenv_wins_over_conflicting_runtime_file(self):
        code, config, _ = self.resolve(
            "root@example.invalid", "runtime@example.invalid"
        )
        self.assertEqual(code, 0, "Synthetic Compose fixture did not resolve")
        for service in self.airflow_services(config):
            self.assertTrue(
                config["services"][service]["environment"]["CUSTOMER360_ALERT_EMAILS"]
                == "root@example.invalid",
                "Root dotenv recipient precedence was not preserved",
            )

    def test_explicit_nonempty_shell_override_is_supported(self):
        code, config, _ = self.resolve(
            "root@example.invalid", "runtime@example.invalid", "shell@example.invalid"
        )
        self.assertEqual(code, 0, "Synthetic Compose fixture did not resolve")
        for service in self.airflow_services(config):
            self.assertTrue(
                config["services"][service]["environment"]["CUSTOMER360_ALERT_EMAILS"]
                == "shell@example.invalid",
                "Explicit shell override was not preserved",
            )

    def test_empty_shell_override_fails_instead_of_erasing_recipients(self):
        code, config, error = self.resolve(
            "root@example.invalid", "runtime@example.invalid", ""
        )
        self.assertNotEqual(code, 0)
        self.assertIsNone(config)
        self.assertIn("CUSTOMER360_ALERT_EMAILS", error)
        self.assertNotIn("root@example.invalid", error)
        self.assertNotIn("runtime@example.invalid", error)

    def test_runtime_file_cannot_supply_missing_authoritative_recipients(self):
        for value in (None, ""):
            code, config, error = self.resolve(
                root_value=value, runtime_value="runtime@example.invalid"
            )
            self.assertNotEqual(code, 0)
            self.assertIsNone(config)
            self.assertIn("CUSTOMER360_ALERT_EMAILS", error)
            self.assertNotIn("runtime@example.invalid", error)

    def test_test_service_policy_defaults_are_explicit_for_all_airflow_services(self):
        code, config, _ = self.resolve("root@example.invalid")
        self.assertEqual(code, 0, "Synthetic Compose fixture did not resolve")
        defaults = {
            "CUSTOMER360_RAW_USER": "SVC_AIRFLOW_TEST",
            "CUSTOMER360_RAW_ROLE": "C360_AIRFLOW_ROLE",
            "CUSTOMER360_RAW_PRIVATE_KEY_PATH": "/opt/airflow/secrets/snowflake/airflow_rsa_key.p8",
            "CUSTOMER360_DBT_USER": "SVC_DBT_TEST",
            "CUSTOMER360_DBT_ROLE": "C360_DBT_ROLE",
            "CUSTOMER360_DBT_PRIVATE_KEY_PATH": "/opt/airflow/secrets/snowflake/dbt_rsa_key.p8",
        }
        for service in self.airflow_services(config):
            for name, expected in defaults.items():
                self.assertTrue(
                    config["services"][service]["environment"].get(name) == expected,
                    "TEST service deployment defaults were not preserved",
                )


if __name__ == "__main__":
    unittest.main()
