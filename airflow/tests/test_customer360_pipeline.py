"""Run in Linux with the pinned, real Airflow runtime; no external credentials needed."""

import copy
import importlib.util
import json
import os
import signal
import subprocess
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "customer360_pipeline.py"
spec = importlib.util.spec_from_file_location("customer360_dag_under_test", DAG_FILE)
pipeline = importlib.util.module_from_spec(spec)
with patch.dict(os.environ, {"CUSTOMER360_SCHEDULE": ""}):
    spec.loader.exec_module(pipeline)


def manifest_fixture():
    nodes = {}
    for name in ("fct_opportunities", "fct_subscriptions", "fct_payments"):
        nodes["model.customer360_dbt." + name] = {
            "name": name,
            "resource_type": "model",
            "database": "CUSTOMER360_TEST",
            "schema": "GOLD",
            "original_file_path": f"models/marts/{name}.sql",
        }
    for name in pipeline.EXPECTED_WARNINGS:
        nodes["test.customer360_dbt." + name] = {
            "name": name,
            "resource_type": "test",
            "config": {"severity": "warn"},
        }
    nodes["test.customer360_dbt.gold_integrity"] = {
        "name": "gold_integrity",
        "resource_type": "test",
        "config": {"severity": "error"},
    }
    return {"metadata": {"invocation_id": "test-run"}, "nodes": nodes, "unit_tests": {}}


def results_fixture(manifest):
    return {
        "metadata": {"invocation_id": "test-run"},
        "results": [
            {
                "unique_id": uid,
                "status": "success"
                if n["resource_type"] == "model"
                else "warn"
                if n["name"] in pipeline.EXPECTED_WARNINGS
                else "pass",
                "failures": 1 if n["name"] in pipeline.EXPECTED_WARNINGS else 0,
            }
            for uid, n in manifest["nodes"].items()
        ],
    }


def connection_fixture():
    return {
        "id": "drive_export",
        "group_id": "test_destination",
        "schema": "raw.mock_workbook",
        "paused": False,
        "succeeded_at": "2026-11-10T12:01:00Z",
        "failed_at": None,
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "warnings": [],
        },
    }


class DagTests(unittest.TestCase):
    def test_dagbag_import_and_serialization_without_network_or_secrets(self):
        with (
            patch(
                "airflow.sdk.Connection.get",
                side_effect=AssertionError("parse attempted secrets lookup"),
            ),
            patch(
                "requests.get", side_effect=AssertionError("parse attempted network")
            ),
            patch(
                "subprocess.Popen",
                side_effect=AssertionError("parse attempted subprocess"),
            ),
            patch.dict(os.environ, {"CUSTOMER360_SCHEDULE": ""}),
        ):
            bag = DagBag(dag_folder=str(DAG_FILE), safe_mode=False)
            self.assertEqual(bag.import_errors, {})
            dag = bag.dags["customer360_pipeline"]
            self.assertTrue(DagSerialization.to_dict(dag))

    def test_exact_task_dependencies(self):
        dag = pipeline.pipeline
        expected = {
            "prepare_sync_monitoring": {"wait_for_fivetran"},
            "wait_for_fivetran": {"validate_raw"},
            "validate_raw": {"run_dbt"},
            "run_dbt": set(),
        }
        self.assertEqual(set(dag.task_ids), set(expected))
        for task_id, downstream in expected.items():
            self.assertEqual(dag.get_task(task_id).downstream_task_ids, downstream)
            self.assertEqual(dag.get_task(task_id).trigger_rule.value, "all_success")

    def test_operational_defaults(self):
        dag = pipeline.pipeline
        self.assertFalse(dag.catchup)
        self.assertIsNone(dag.schedule)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertEqual(dag.default_args["retries"], 2)
        self.assertEqual(dag.default_args["retry_delay"], timedelta(minutes=5))
        self.assertEqual(
            dag.default_args["on_failure_callback"], pipeline.failure_notification
        )
        for task in dag.tasks:
            self.assertIsNotNone(task.execution_timeout)
        self.assertEqual(
            dag.get_task("run_dbt").execution_timeout, timedelta(minutes=55)
        )
        sensor = dag.get_task("wait_for_fivetran")
        self.assertEqual(sensor.partial_kwargs["mode"], "reschedule")
        self.assertEqual(sensor.partial_kwargs["timeout"], 7200)

    def test_runtime_providers_available(self):
        from airflow.providers.smtp.hooks.smtp import SmtpHook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        self.assertTrue(SnowflakeHook)
        self.assertTrue(SmtpHook)

    def test_failure_notification_omits_exception_and_uses_configured_smtp(self):
        context = {
            "task_instance": SimpleNamespace(
                dag_id="customer360_pipeline", task_id="run_dbt"
            ),
            "exception": RuntimeError("synthetic-sensitive-payload"),
        }
        with (
            patch.dict(
                os.environ, {"CUSTOMER360_ALERT_EMAILS": "alerts@example.invalid"}
            ),
            patch("airflow.providers.smtp.hooks.smtp.SmtpHook") as hook,
        ):
            pipeline.failure_notification(context)
        hook.assert_called_once_with(smtp_conn_id="customer360_smtp")
        sent = hook.return_value.__enter__.return_value.send_email_smtp.call_args.kwargs
        self.assertEqual(sent["to"], ["alerts@example.invalid"])
        self.assertIn("run_dbt", sent["html_content"])
        self.assertNotIn("synthetic-sensitive-payload", str(sent))

    def test_notification_delivery_error_is_logged_safely(self):
        with (
            patch.dict(
                os.environ, {"CUSTOMER360_ALERT_EMAILS": "alerts@example.invalid"}
            ),
            patch(
                "airflow.providers.smtp.hooks.smtp.SmtpHook",
                side_effect=RuntimeError("synthetic-sensitive-payload"),
            ),
            self.assertLogs(pipeline.LOG, level="ERROR") as captured,
        ):
            pipeline.failure_notification({})
        self.assertIn("could not be delivered", " ".join(captured.output))
        self.assertNotIn("synthetic-sensitive-payload", " ".join(captured.output))

    def test_missing_recipients_block_preflight_before_fivetran(self):
        prepare = next(
            task.python_callable
            for task in pipeline.pipeline.tasks
            if task.task_id.startswith("prepare_")
        )
        for value in ("", "   ", ", ,"):
            with (
                self.subTest(case="empty recipient list"),
                patch.dict(os.environ, {"CUSTOMER360_ALERT_EMAILS": value}),
                patch.object(pipeline, "_connection") as connection,
                self.assertRaisesRegex(pipeline.AirflowFailException, "ALERT_EMAILS"),
            ):
                prepare()
            connection.assert_not_called()

    def test_configured_recipients_are_normalized(self):
        with patch.dict(
            os.environ, {"CUSTOMER360_ALERT_EMAILS": " alerts@example.invalid, "}
        ):
            self.assertEqual(pipeline._alert_recipients(), ["alerts@example.invalid"])

    def test_missing_recipients_never_attempt_smtp(self):
        with (
            patch.dict(os.environ, {"CUSTOMER360_ALERT_EMAILS": ", ,"}),
            patch("airflow.providers.smtp.hooks.smtp.SmtpHook") as smtp,
            self.assertLogs(pipeline.LOG, level="ERROR") as captured,
        ):
            pipeline.failure_notification({})
        smtp.assert_not_called()
        self.assertIn("Failure email not configured", " ".join(captured.output))


class FivetranTests(unittest.TestCase):
    boundary = "2026-11-10T12:00:00+00:00"

    def test_fresh_success_is_ready(self):
        self.assertTrue(pipeline._sync_finished(connection_fixture(), self.boundary))

    def test_stale_success_or_running_sync_is_not_ready(self):
        for state, success in [
            ("scheduled", "2026-11-10T11:59:00Z"),
            ("syncing", "2026-11-10T12:01:00Z"),
            ("rescheduled", None),
        ]:
            with self.subTest(state=state):
                data = connection_fixture()
                data["status"]["sync_state"] = state
                data["succeeded_at"] = success
                self.assertFalse(pipeline._sync_finished(data, self.boundary))

    def test_latest_failure_blocks(self):
        data = connection_fixture()
        data["failed_at"] = "2026-11-10T12:02:00Z"
        with self.assertRaises(pipeline.AirflowException):
            pipeline._sync_finished(data, self.boundary)

    def test_recovered_failure_accepts_newer_success(self):
        data = connection_fixture()
        data["failed_at"] = "2026-11-10T12:00:30Z"
        self.assertTrue(pipeline._sync_finished(data, self.boundary))

    def test_destination_and_setup_guards(self):
        destination = {
            "id": "test_destination",
            "service": "snowflake",
            "setup_status": "CONNECTED",
            "config": {"database": "CUSTOMER360_TEST"},
        }
        with patch.object(
            pipeline, "_fivetran_get", side_effect=[connection_fixture(), destination]
        ):
            self.assertEqual(
                pipeline._verified_status("drive_export", "test_destination")["id"],
                "drive_export",
            )
        for change in ("production", "wrong_group", "wrong_schema", "paused", "broken"):
            with self.subTest(change=change):
                d, c = copy.deepcopy(destination), connection_fixture()
                if change == "production":
                    d["config"]["database"] = "CUSTOMER360_PROD"
                if change == "wrong_group":
                    c["group_id"] = "other_destination"
                if change == "wrong_schema":
                    c["schema"] = "gold"
                if change == "paused":
                    c["paused"] = True
                if change == "broken":
                    c["status"]["setup_state"] = "broken"
                with (
                    patch.object(pipeline, "_fivetran_get", side_effect=[c, d]),
                    self.assertRaises(pipeline.AirflowFailException),
                ):
                    pipeline._verified_status("drive_export", "test_destination")

    def test_api_errors_do_not_expose_response_or_credentials(self):
        connection = SimpleNamespace(login="synthetic-key", password="synthetic-secret")
        for code in (401, 403, 404, 429, 500, 302):
            with (
                self.subTest(code=code),
                patch.object(
                    pipeline,
                    "_connection",
                    return_value=(connection, ["id"], "destination"),
                ),
                patch(
                    "requests.get",
                    return_value=Mock(status_code=code, text="sensitive-response"),
                ) as get,
            ):
                with self.assertRaises(pipeline.AirflowException) as caught:
                    pipeline._fivetran_get("connections", "id")
                self.assertNotIn("sensitive-response", str(caught.exception))
                self.assertNotIn("synthetic-secret", str(caught.exception))
                self.assertFalse(get.call_args.kwargs["allow_redirects"])
                self.assertEqual(get.call_args.kwargs["timeout"], (10, 30))

    def test_retry_uses_same_run_boundary(self):
        context = {
            "dag_run": SimpleNamespace(start_date="2026-11-10T12:00:00Z"),
            "data_interval_end": "2026-11-10T11:00:00Z",
        }
        fn = pipeline.pipeline.get_task("prepare_sync_monitoring").python_callable
        with (
            patch.dict(
                os.environ, {"CUSTOMER360_ALERT_EMAILS": "alerts@example.invalid"}
            ),
            patch.object(
                pipeline,
                "_connection",
                return_value=(None, ["one", "two"], "destination"),
            ),
            patch.object(pipeline, "_verified_status"),
            patch.object(pipeline, "get_current_context", return_value=context),
        ):
            first, second = fn(), fn()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)
        self.assertEqual(first[0]["not_before"], self.boundary)


class RawTests(unittest.TestCase):
    def cursor(
        self,
        *,
        missing=False,
        empty=False,
        role="C360_AIRFLOW_ROLE",
        user="SVC_AIRFLOW_TEST",
        database="CUSTOMER360_TEST",
        schema="RAW",
    ):
        cursor = Mock()
        cursor.fetchall.return_value = (
            [] if missing else [(t,) for t in pipeline.RAW_TABLES]
        )
        cursor.fetchone.side_effect = [(database, schema, user, role)] + (
            [None] if empty else [(1,)] * 7
        )
        return cursor

    def test_all_seven_tables_checked_without_reading_payloads(self):
        cursor = self.cursor()
        self.assertEqual(pipeline._validate_raw_tables(cursor)["nonempty_tables"], 7)
        queries = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(q.startswith("select 1") for q in queries), 7)
        self.assertTrue(all(q.startswith("select") for q in queries))
        self.assertIn("current_user()", queries[0])
        self.assertIn("current_role()", queries[0])

    def test_missing_empty_admin_and_production_fail(self):
        for kwargs in (
            {"missing": True},
            {"empty": True},
            {"role": "ACCOUNTADMIN"},
            {"role": "OTHER_READ_ROLE"},
            {"user": "PERSONAL_USER"},
            {"database": "CUSTOMER360_PROD"},
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises(pipeline.AirflowException),
            ):
                pipeline._validate_raw_tables(self.cursor(**kwargs))

    def test_wrong_session_identity_blocks_before_table_reads(self):
        for change in ({"user": "PERSONAL_USER"}, {"role": "OTHER_READ_ROLE"}):
            cursor = self.cursor(**change)
            with self.assertRaises(pipeline.AirflowFailException):
                pipeline._validate_raw_tables(cursor)
            self.assertEqual(cursor.execute.call_count, 1)
            cursor.fetchall.assert_not_called()

    def test_wrong_actual_session_schema_blocks_before_table_reads(self):
        cursor = self.cursor(schema="GOLD")
        with self.assertRaises(pipeline.AirflowFailException):
            pipeline._validate_raw_tables(cursor)
        self.assertEqual(cursor.execute.call_count, 1)
        cursor.fetchall.assert_not_called()

    def test_wrong_configured_raw_schema_blocks_before_opening_snowflake(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            config = SimpleNamespace(
                login="SVC_AIRFLOW_TEST",
                schema="GOLD",
                extra_dejson={
                    "database": "CUSTOMER360_TEST",
                    "role": "C360_AIRFLOW_ROLE",
                    "private_key_file": str(key),
                    "authenticator": "snowflake_jwt",
                },
            )
            validate = pipeline.pipeline.get_task("validate_raw").python_callable
            with (
                patch.dict(os.environ, {"CUSTOMER360_RAW_PRIVATE_KEY_PATH": str(key)}),
                patch.object(pipeline.Connection, "get", return_value=config),
                patch(
                    "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
                ) as hook,
                self.assertRaises(pipeline.AirflowFailException),
            ):
                validate([{"status": "success"}])
            hook.assert_not_called()

    def test_connection_requires_approved_identity_and_mounted_key(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            extra = {
                "database": "CUSTOMER360_TEST",
                "role": "C360_AIRFLOW_ROLE",
                "private_key_file": str(key),
                "authenticator": "snowflake_jwt",
            }
            config = SimpleNamespace(
                login="SVC_AIRFLOW_TEST", schema="RAW", extra_dejson=extra
            )
            with patch.dict(os.environ, {"CUSTOMER360_RAW_PRIVATE_KEY_PATH": str(key)}):
                self.assertEqual(
                    pipeline._validate_raw_connection(config)["USER"],
                    "SVC_AIRFLOW_TEST",
                )
                for changes in (
                    {"role": "OTHER_READ_ROLE"},
                    {"database": "CUSTOMER360_PROD"},
                    {"private_key_file": None},
                    {"private_key_file": str(key.parent / "missing.p8")},
                    {"authenticator": "externalbrowser"},
                    {"authenticator": "oauth"},
                    {"token": "synthetic-token"},
                    {"private_key_content": "synthetic-inline-key"},
                ):
                    invalid = SimpleNamespace(
                        login="SVC_AIRFLOW_TEST",
                        schema="RAW",
                        extra_dejson={**extra, **changes},
                    )
                    with self.assertRaises(pipeline.AirflowFailException):
                        pipeline._validate_raw_connection(invalid)
                config.login = "PERSONAL_USER"
                with self.assertRaises(pipeline.AirflowFailException):
                    pipeline._validate_raw_connection(config)

    def test_invalid_raw_connection_is_rejected_without_opening_snowflake(self):
        validate = pipeline.pipeline.get_task("validate_raw").python_callable
        config = SimpleNamespace(login="PERSONAL_USER", schema="RAW", extra_dejson={})
        with (
            patch.object(pipeline.Connection, "get", return_value=config),
            patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook") as hook,
            self.assertRaises(pipeline.AirflowFailException),
        ):
            validate([{"status": "success"}])
        hook.assert_not_called()

    def test_approved_raw_connection_and_session_complete_mocked_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            config = SimpleNamespace(
                login="SVC_AIRFLOW_TEST",
                schema="RAW",
                extra_dejson={
                    "database": "CUSTOMER360_TEST",
                    "role": "C360_AIRFLOW_ROLE",
                    "private_key_file": str(key),
                },
            )
            validate = pipeline.pipeline.get_task("validate_raw").python_callable
            with (
                patch.dict(os.environ, {"CUSTOMER360_RAW_PRIVATE_KEY_PATH": str(key)}),
                patch.object(pipeline.Connection, "get", return_value=config),
                patch(
                    "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
                ) as hook,
            ):
                hook.return_value.get_conn.return_value.cursor.return_value = (
                    self.cursor()
                )
                self.assertEqual(
                    validate([{"status": "success"}])["nonempty_tables"], 7
                )
                self.assertEqual(hook.call_args.kwargs["role"], "C360_AIRFLOW_ROLE")
                with (
                    patch.object(Path, "is_file", return_value=False),
                    self.assertRaises(pipeline.AirflowFailException),
                ):
                    pipeline._validate_raw_connection(config)

    def test_deployment_policy_rejects_admin_role_even_if_overridden(self):
        with (
            patch.dict(os.environ, {"CUSTOMER360_RAW_ROLE": "ACCOUNTADMIN"}),
            self.assertRaises(pipeline.AirflowFailException),
        ):
            pipeline._snowflake_policy("RAW")


class DbtTests(unittest.TestCase):
    def profile_output(self, key):
        return {
            "type": "snowflake",
            "database": "CUSTOMER360_TEST",
            "schema": "SILVER",
            "user": "SVC_DBT_TEST",
            "role": "C360_DBT_ROLE",
            "private_key_path": str(key),
            "authenticator": "snowflake_jwt",
        }

    def write_profile(self, path, output):
        path.write_text(json.dumps({"customer360": {"outputs": {"dev": output}}}))

    def test_three_expected_warnings_are_nonblocking(self):
        manifest = manifest_fixture()
        self.assertEqual(
            pipeline._summarize_dbt(manifest, results_fixture(manifest), 0),
            {"models": 3, "tests": 4, "warnings": 3},
        )

    def test_gold_failure_skips_unexpected_warnings_and_nonzero_exit_block(self):
        for status, code in [
            ("fail", 0),
            ("error", 0),
            ("skipped", 0),
            ("warn", 0),
            ("pass", 1),
        ]:
            with self.subTest(status=status, code=code):
                manifest = manifest_fixture()
                results = results_fixture(manifest)
                results["results"][-1]["status"] = status
                with self.assertRaises(pipeline.AirflowException):
                    pipeline._summarize_dbt(manifest, results, code)

    def test_missing_or_stale_results_block(self):
        manifest = manifest_fixture()
        for change in ("missing", "stale"):
            results = results_fixture(manifest)
            if change == "missing":
                results["results"].pop()
            else:
                results["metadata"]["invocation_id"] = "previous-run"
            with (
                self.subTest(change=change),
                self.assertRaises(pipeline.AirflowException),
            ):
                pipeline._summarize_dbt(manifest, results, 0)

    def test_production_route_and_weakened_gold_test_rejected(self):
        for change in ("production", "gold_warning", "wrong_schema"):
            manifest = manifest_fixture()
            if change == "production":
                manifest["nodes"]["model.customer360_dbt.fct_payments"]["database"] = (
                    "CUSTOMER360_PROD"
                )
            if change == "wrong_schema":
                manifest["nodes"]["model.customer360_dbt.fct_payments"]["schema"] = (
                    "SILVER_GOLD"
                )
            if change == "gold_warning":
                manifest["nodes"]["test.customer360_dbt.gold_integrity"]["config"][
                    "severity"
                ] = "warn"
            with (
                self.subTest(change=change),
                self.assertRaises(pipeline.AirflowFailException),
            ):
                pipeline._validate_manifest(manifest)

    def test_dbt_profile_guards_and_env_rendering(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.yml"
            key = Path(directory) / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            for database, role, valid in [
                ("CUSTOMER360_TEST", "C360_DBT_ROLE", True),
                ("CUSTOMER360_PROD", "C360_DBT_ROLE", False),
                ("CUSTOMER360_TEST", "ACCOUNTADMIN", False),
            ]:
                path.write_text(
                    json.dumps(
                        {
                            "customer360": {
                                "outputs": {
                                    "dev": {
                                        **self.profile_output(key),
                                        "database": "{{ env_var('TEST_DBT_DATABASE') }}",
                                        "schema": "SILVER",
                                        "role": role,
                                    }
                                }
                            }
                        }
                    )
                )
                with (
                    self.subTest(database=database, role=role),
                    patch.dict(
                        os.environ,
                        {
                            "TEST_DBT_DATABASE": database,
                            "CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key),
                        },
                    ),
                ):
                    if valid:
                        pipeline._validate_dbt_profile(path)
                    else:
                        with self.assertRaises(pipeline.AirflowFailException):
                            pipeline._validate_dbt_profile(path)

    def test_dbt_rejects_wrong_identity_and_unapproved_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            path, key = (
                Path(directory) / "profiles.yml",
                Path(directory) / "synthetic.p8",
            )
            key.write_text("public metadata-only test fixture")
            output = self.profile_output(key)
            with patch.dict(os.environ, {"CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key)}):
                for changes in (
                    {"user": "PERSONAL_USER"},
                    {"role": "OTHER_DBT_ROLE"},
                    {"private_key_path": None},
                    {"private_key_path": ""},
                    {"private_key_path": str(key.parent / "missing.p8")},
                    {"password": "synthetic-sensitive-password"},
                    {"authenticator": "externalbrowser"},
                    {"authenticator": "oauth"},
                    {"authenticator": "username_password_mfa"},
                    {"authenticator": "https://identity.example.invalid"},
                    {"token": "synthetic-sensitive-token"},
                    {"private_key": "synthetic-inline-key"},
                    {"type": "other"},
                ):
                    self.write_profile(path, {**output, **changes})
                    with self.assertRaises(pipeline.AirflowFailException) as caught:
                        pipeline._validate_dbt_profile(path)
                    self.assertNotIn("synthetic-sensitive", str(caught.exception))
                self.write_profile(
                    path,
                    {
                        **output,
                        "password": "synthetic-password",
                        "private_key_path": None,
                    },
                )
                with self.assertRaises(pipeline.AirflowFailException):
                    pipeline._validate_dbt_profile(path)

    def test_wrong_dbt_profile_schema_blocks_before_dbt_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            (root / "dbt_project.yml").write_text("name: customer360_dbt")
            self.write_profile(
                root / "profiles.yml", {**self.profile_output(key), "schema": "GOLD"}
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "CUSTOMER360_DBT_PROJECT_DIR": str(root),
                        "CUSTOMER360_DBT_PROFILES_DIR": str(root),
                        "CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key),
                    },
                ),
                patch.object(pipeline, "_run_dbt_command") as command,
                self.assertRaises(pipeline.AirflowFailException),
            ):
                pipeline._build_dbt()
            command.assert_not_called()

    def test_dbt_key_path_rendering_and_metadata_checks_do_not_read_key_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path, key = (
                Path(directory) / "profiles.yml",
                Path(directory) / "synthetic.p8",
            )
            key.write_text("public metadata-only test fixture")
            self.write_profile(
                path,
                {
                    **self.profile_output(key),
                    "private_key_path": "{{ env_var('TEST_RSA_PATH') }}",
                },
            )
            original = Path.read_text

            def safe_read(file, *args, **kwargs):
                if file == key:
                    raise AssertionError("Private key contents must not be read")
                return original(file, *args, **kwargs)

            with (
                patch.dict(
                    os.environ,
                    {
                        "CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key),
                        "TEST_RSA_PATH": str(key),
                    },
                ),
                patch.object(Path, "read_text", safe_read),
                patch.object(
                    Path,
                    "read_bytes",
                    side_effect=AssertionError("Private key bytes must not be read"),
                ),
            ):
                pipeline._validate_dbt_profile(path)
                with (
                    patch.object(Path, "is_file", return_value=False),
                    self.assertRaises(pipeline.AirflowFailException),
                ):
                    pipeline._validate_dbt_profile(path)
                with (
                    patch.object(pipeline.os, "access", return_value=False),
                    self.assertRaises(pipeline.AirflowFailException),
                ):
                    pipeline._validate_dbt_profile(path)
                key.write_text("")
                with self.assertRaises(pipeline.AirflowFailException):
                    pipeline._validate_dbt_profile(path)

    def test_dbt_default_key_auth_and_key_passphrase_configuration_are_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            path, key = (
                Path(directory) / "profiles.yml",
                Path(directory) / "synthetic.p8",
            )
            key.write_text("public metadata-only test fixture")
            with patch.dict(os.environ, {"CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key)}):
                for authenticator in (None, "snowflake", "snowflake_jwt"):
                    self.write_profile(
                        path,
                        {
                            **self.profile_output(key),
                            "authenticator": authenticator,
                            "private_key_passphrase": "{{ env_var('UNRESOLVED_TEST_KEY_PASSPHRASE') }}",
                        },
                    )
                    pipeline._validate_dbt_profile(path)

    def test_approved_deployment_identity_overrides_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            path, key = (
                Path(directory) / "profiles.yml",
                Path(directory) / "synthetic.p8",
            )
            key.write_text("public metadata-only test fixture")
            self.write_profile(
                path,
                {
                    **self.profile_output(key),
                    "user": "SVC_APPROVED_DBT",
                    "role": "C360_APPROVED_DBT_ROLE",
                },
            )
            with patch.dict(
                os.environ,
                {
                    "CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key),
                    "CUSTOMER360_DBT_USER": "SVC_APPROVED_DBT",
                    "CUSTOMER360_DBT_ROLE": "C360_APPROVED_DBT_ROLE",
                },
            ):
                pipeline._validate_dbt_profile(path)

    def test_build_uses_exact_selection_correct_cwd_and_fresh_private_artifacts(self):
        manifest = manifest_fixture()
        calls = []

        def run(command, project, env, **kwargs):
            calls.append((command, project, env))
            target = Path(env["DBT_TARGET_PATH"])
            target.mkdir(exist_ok=True)
            (target / "manifest.json").write_text(json.dumps(manifest))
            if command[1] == "build":
                (target / "run_results.json").write_text(
                    json.dumps(results_fixture(manifest))
                )
            return 0

        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "dbt"
            profile = Path(directory) / "profile"
            project.mkdir()
            profile.mkdir()
            (project / "dbt_project.yml").write_text("name: customer360_dbt")
            key = Path(directory) / "synthetic.p8"
            key.write_text("public metadata-only test fixture")
            self.write_profile(profile / "profiles.yml", self.profile_output(key))
            with (
                patch.dict(
                    os.environ,
                    {
                        "CUSTOMER360_DBT_PROJECT_DIR": str(project),
                        "CUSTOMER360_DBT_PROFILES_DIR": str(profile),
                        "CUSTOMER360_DBT_EXECUTABLE": "/opt/dbt/bin/dbt",
                        "CUSTOMER360_DBT_PRIVATE_KEY_PATH": str(key),
                    },
                ),
                patch.object(pipeline, "_run_dbt_command", side_effect=run),
            ):
                pipeline._build_dbt()
                pipeline._build_dbt()
        self.assertEqual(
            calls[1][0],
            [
                "/opt/dbt/bin/dbt",
                "build",
                "--select",
                "staging",
                "intermediate",
                "marts",
                "--threads",
                "1",
            ],
        )
        self.assertEqual(calls[1][1], project)
        self.assertNotEqual(
            calls[1][2]["DBT_TARGET_PATH"], calls[3][2]["DBT_TARGET_PATH"]
        )
        self.assertFalse(Path(calls[1][2]["DBT_TARGET_PATH"]).exists())

    def test_dbt_timeout_terminates_process_group(self):
        process = Mock(pid=123)
        process.wait.side_effect = [subprocess.TimeoutExpired("dbt", 1), 0]
        process.poll.return_value = None
        with (
            patch.object(pipeline.subprocess, "Popen", return_value=process),
            patch.object(pipeline.os, "killpg", create=True) as kill,
            self.assertRaises(subprocess.TimeoutExpired),
        ):
            pipeline._run_dbt_command(["dbt"], Path("/tmp"), {}, timeout=1)
        kill.assert_called_once_with(123, signal.SIGTERM)


if __name__ == "__main__":
    unittest.main()
