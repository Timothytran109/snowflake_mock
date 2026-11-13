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
from unittest.mock import AsyncMock, Mock, patch

from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "customer360_pipeline.py"
spec = importlib.util.spec_from_file_location("customer360_dag_under_test", DAG_FILE)
pipeline = importlib.util.module_from_spec(spec)
with patch.dict(os.environ, {"CUSTOMER360_SCHEDULE": ""}):
    spec.loader.exec_module(pipeline)

APPROVED_WARNING_TESTS = frozenset(
    {
        "relationships_stg_salesforce__opportunities_account_id__account_id__ref_stg_salesforce__accounts_",
        "relationships_stg_stripe__subscriptions_customer_id__customer_id__ref_stg_stripe__customers_",
        "relationships_stg_stripe__payments_customer_id__customer_id__ref_stg_stripe__customers_",
        "relationships_stg_stripe__payments_subscription_id__subscription_id__ref_stg_stripe__subscriptions_",
        "not_null_stg_adobe__events_event_timestamp",
        "not_null_stg_stripe__customers_created_at",
        "not_null_stg_salesforce__accounts_updated_at",
    }
)


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
    for name in APPROVED_WARNING_TESTS:
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
                if n["name"] in APPROVED_WARNING_TESTS
                else "pass",
                "failures": 1 if n["name"] in APPROVED_WARNING_TESTS else 0,
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
            "prepare_fivetran_sync": {"sync_fivetran"},
            "sync_fivetran": {"validate_raw"},
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
        operator = dag.get_task("sync_fivetran")
        self.assertTrue(operator.partial_kwargs["deferrable"])
        self.assertTrue(operator.partial_kwargs["wait_for_completion"])
        self.assertEqual(operator.execution_timeout, timedelta(hours=2))

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
    def test_connection_reads_runtime_environment_without_extras(self):
        connection = SimpleNamespace(login="synthetic-key", password="synthetic-secret")
        with (
            patch.dict(
                os.environ,
                {
                    "CUSTOMER360_FIVETRAN_CONNECTOR_ID": "test_connector",
                    "CUSTOMER360_FIVETRAN_DESTINATION_ID": "test_destination",
                },
            ),
            patch.object(pipeline.Connection, "get", return_value=connection) as get,
            patch.object(pipeline, "mask_secret") as mask,
        ):
            self.assertEqual(
                pipeline._connection(),
                (connection, ["test_connector"], "test_destination"),
            )
        get.assert_called_once_with("customer360_fivetran")
        self.assertEqual(mask.call_count, 3)

    def test_missing_or_invalid_runtime_ids_fail_closed(self):
        for connector, destination in [
            ("", "test_destination"),
            ("test_connector", ""),
            ("bad/id", "test_destination"),
        ]:
            with (
                self.subTest(connector=connector, destination=destination),
                patch.dict(
                    os.environ,
                    {
                        "CUSTOMER360_FIVETRAN_CONNECTOR_ID": connector,
                        "CUSTOMER360_FIVETRAN_DESTINATION_ID": destination,
                    },
                ),
                patch.object(
                    pipeline.Connection,
                    "get",
                    return_value=SimpleNamespace(login="key", password="secret"),
                ),
                patch.object(pipeline, "mask_secret"),
                self.assertRaises(pipeline.AirflowFailException),
            ):
                pipeline._connection()

    def operator(self):
        operator = pipeline.GuardedFivetranOperator(
            task_id="test_sync",
            connector_id="drive_export",
            fivetran_conn_id=pipeline.FIVETRAN_CONN_ID,
            deferrable=True,
            execution_timeout=timedelta(hours=2),
            retries=2,
        )
        operator.hook = Mock()
        operator.hook.start_fivetran_sync.return_value = "2026-11-10T12:00:00Z"
        operator.hook.is_synced_after_target_time.return_value = False
        return operator

    def execute_mocked(self, operator):
        with (
            patch.object(
                pipeline,
                "_connection",
                return_value=(None, ["drive_export"], "test_destination"),
            ),
            patch.object(
                pipeline, "_verified_status",
                return_value={**connection_fixture(), "schedule_type": "auto"},
            ),
        ):
            return operator.execute({})

    def test_trigger_success_and_async_timeout(self):
        from airflow.sdk.exceptions import TaskDeferred
        from fivetran_provider_async.triggers import FivetranTrigger

        operator = self.operator()
        with self.assertRaises(TaskDeferred) as caught:
            self.execute_mocked(operator)
        operator.hook.start_fivetran_sync.assert_called_once_with(
            connector_id="drive_export"
        )
        operator.hook.prep_connector.assert_called_once_with("drive_export", "auto")
        self.assertEqual(caught.exception.timeout, timedelta(hours=2))
        self.assertIsInstance(caught.exception.trigger, FivetranTrigger)
        self.assertEqual(caught.exception.method_name, "execute_complete")
        self.assertEqual(
            caught.exception.kwargs["completion_after"], "2026-11-10T12:00:00Z"
        )
        # A resumed operator is a new instance; it must use the persisted watermark.
        resumed = self.operator()
        with (
            patch.object(pipeline, "_connection", return_value=(None, ["drive_export"], "test_destination")),
            patch.object(pipeline, "_verified_status", return_value=connection_fixture()),
        ):
            self.assertEqual(
                resumed.resume_execution(
                    "execute_complete",
                    {
                        **caught.exception.kwargs,
                        "event": {"status": "success", "return_value": "2026-11-10T12:01:00Z"},
                    },
                    {},
                ),
                {"connector_id": "drive_export", "status": "success"},
            )

    def test_stale_inflight_failed_or_inconsistent_completion_cannot_release_raw(self):
        for change in ("old_success", "syncing", "rescheduled", "new_failure", "stale_api"):
            details = connection_fixture()
            event = {"status": "success", "return_value": "2026-11-10T12:01:00Z"}
            if change == "old_success":
                details["succeeded_at"] = "2026-11-10T12:00:00Z"
            elif change in {"syncing", "rescheduled"}:
                details["status"]["sync_state"] = change
            elif change == "new_failure":
                details["failed_at"] = "2026-11-10T12:02:00Z"
            else:
                event["return_value"] = "2026-11-10T12:02:00Z"
            with (
                self.subTest(change=change),
                patch.object(pipeline, "_connection", return_value=(None, ["drive_export"], "test_destination")),
                patch.object(pipeline, "_verified_status", return_value=details),
                self.assertRaisesRegex(pipeline.AirflowException, "RAW validation remains blocked"),
            ):
                self.operator().execute_complete({}, event, completion_after="2026-11-10T12:00:00Z")

    def test_completion_requires_a_persisted_watermark_and_valid_timestamp(self):
        for baseline, reported in ((None, "2026-11-10T12:01:00Z"), ("invalid", "2026-11-10T12:01:00Z"), ("2026-11-10T12:00:00Z", None)):
            with self.subTest(baseline=baseline, reported=reported), self.assertRaises(pipeline.AirflowException):
                self.operator().execute_complete({}, {"status": "success", "return_value": reported}, completion_after=baseline)

    def test_immediate_provider_completion_still_requires_new_idle_success(self):
        operator = self.operator()
        operator.hook.is_synced_after_target_time.return_value = True
        details = {**connection_fixture(), "schedule_type": "auto"}
        details["status"]["sync_state"] = "syncing"
        with (
            patch.object(pipeline, "_connection", return_value=(None, ["drive_export"], "test_destination")),
            patch.object(pipeline, "_verified_status", return_value=details),
            self.assertRaises(pipeline.AirflowException),
        ):
            operator.execute({})

    def test_provider_failure_and_invalid_completion_block_raw(self):
        operator = self.operator()
        for event in (None, {}, {"status": "error", "message": "sensitive"}):
            with self.assertRaisesRegex(
                pipeline.AirflowException, "RAW validation remains blocked"
            ):
                operator.execute_complete({}, event)
        operator.hook.start_fivetran_sync.side_effect = pipeline.AirflowException(
            "API unavailable"
        )
        with self.assertRaises(pipeline.AirflowException):
            self.execute_mocked(operator)

    def test_retry_rechecks_guard_and_retriggers_provider(self):
        operator = self.operator()
        operator.hook.start_fivetran_sync.side_effect = [
            pipeline.AirflowException("transient"),
            "2026-11-10T12:00:00Z",
        ]
        operator.hook.is_synced_after_target_time.return_value = True
        with self.assertRaises(pipeline.AirflowException):
            self.execute_mocked(operator)
        self.assertEqual(
            self.execute_mocked(operator),
            {"connector_id": "drive_export", "status": "success"},
        )
        self.assertEqual(operator.hook.start_fivetran_sync.call_count, 2)
        with (
            patch.object(
                pipeline, "_connection", return_value=(None, ["wrong"], "destination")
            ),
            self.assertRaises(pipeline.AirflowFailException),
        ):
            operator.execute({})
        self.assertEqual(operator.hook.start_fivetran_sync.call_count, 2)

    def test_deferral_timeout_raises_instead_of_releasing_raw(self):
        from airflow.sdk.bases.operator import TriggerFailureReason
        from airflow.sdk.exceptions import TaskDeferralTimeout

        operator = self.operator()
        with self.assertRaises(TaskDeferralTimeout):
            operator.resume_execution(
                "__fail__", {"error": TriggerFailureReason.TRIGGER_TIMEOUT}, {}
            )
        operator.hook.start_fivetran_sync.assert_not_called()

    def destination_fixture(self, setup_status="CONNECTED"):
        return {
            "id": "test_destination",
            "service": "snowflake",
            "setup_status": setup_status,
            "config": {"database": "CUSTOMER360_TEST"},
        }

    def test_connected_destination_accepts_normalized_status(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "code": "Success",
            "message": "Destination details retrieved successfully",
            "data": self.destination_fixture(" connected "),
        }
        connection = SimpleNamespace(login="synthetic-key", password="synthetic-secret")
        with (
            patch.object(
                pipeline,
                "_connection",
                return_value=(connection, ["id"], "test_destination"),
            ),
            patch("requests.get", return_value=response),
            self.assertLogs(pipeline.LOG, level="INFO") as captured,
        ):
            destination = pipeline._fivetran_get("destinations", "test_destination")
        logs = " ".join(captured.output)
        self.assertIn("http_status=200", logs)
        self.assertIn("destination_id=test_destination", logs)
        self.assertIn("setup_status=connected", logs.lower())
        self.assertNotIn("synthetic-secret", logs)
        with patch.object(
            pipeline, "_fivetran_get", side_effect=[connection_fixture(), destination]
        ):
            self.assertEqual(
                pipeline._verified_status("drive_export", "test_destination")["id"],
                "drive_export",
            )

    def test_disconnected_destination_fails_closed(self):
        destination = self.destination_fixture("BROKEN")
        with (
            patch.object(
                pipeline,
                "_fivetran_get",
                side_effect=[connection_fixture(), destination],
            ),
            self.assertRaisesRegex(
                pipeline.AirflowFailException, "destination is not connected"
            ),
        ):
            pipeline._verified_status("drive_export", "test_destination")

    def test_missing_destination_logs_safe_api_error(self):
        response = Mock(status_code=404)
        response.json.return_value = {
            "code": "NotFound",
            "message": "Destination not found",
        }
        connection = SimpleNamespace(login="synthetic-key", password="synthetic-secret")
        with (
            patch.object(
                pipeline,
                "_connection",
                return_value=(connection, ["id"], "test_destination"),
            ),
            patch("requests.get", return_value=response),
            self.assertLogs(pipeline.LOG, level="WARNING") as captured,
            self.assertRaisesRegex(pipeline.AirflowFailException, "HTTP 404"),
        ):
            pipeline._fivetran_get("destinations", "test_destination")
        logs = " ".join(captured.output)
        self.assertIn("destination not found", logs.lower())
        self.assertIn("test_destination", logs)
        self.assertNotIn("synthetic-secret", logs)

    def test_unexpected_destination_response_shape_fails_closed(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "code": "Success",
            "message": "synthetic diagnostic",
            "data": [],
            "secret": "must-not-be-logged",
        }
        connection = SimpleNamespace(login="synthetic-key", password="synthetic-secret")
        with (
            patch.object(
                pipeline,
                "_connection",
                return_value=(connection, ["id"], "test_destination"),
            ),
            patch("requests.get", return_value=response),
            self.assertLogs(pipeline.LOG, level="WARNING") as captured,
            self.assertRaisesRegex(
                pipeline.AirflowException, "invalid status response"
            ),
        ):
            pipeline._fivetran_get("destinations", "test_destination")
        logs = " ".join(captured.output)
        self.assertIn("synthetic diagnostic", logs)
        self.assertNotIn("must-not-be-logged", logs)
        self.assertNotIn("synthetic-secret", logs)

    def test_destination_and_setup_guards(self):
        destination = self.destination_fixture()
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


class AsyncFivetranTests(unittest.IsolatedAsyncioTestCase):
    async def test_guard_blocks_provider_success_while_connector_is_still_syncing(self):
        import pendulum
        from fivetran_provider_async.hooks import FivetranHookAsync

        previous = pendulum.parse("2026-11-10T12:00:00Z")
        details = connection_fixture()
        details["status"]["sync_state"] = "syncing"
        hook = FivetranHookAsync(fivetran_conn_id="unused_mocked_connection")
        with patch.object(hook, "get_connector_async", new=AsyncMock(return_value=details)):
            # Provider 2.4.0 passes always_wait_when_syncing=False on its async path.
            self.assertEqual(await hook.get_sync_status_async("drive_export", previous), "success")
        operator = pipeline.GuardedFivetranOperator(task_id="test_sync", connector_id="drive_export")
        with (
            patch.object(pipeline, "_connection", return_value=(None, ["drive_export"], "test_destination")),
            patch.object(pipeline, "_verified_status", return_value=details),
            self.assertRaisesRegex(pipeline.AirflowException, "new idle success"),
        ):
            operator.execute_complete(
                {}, {"status": "success", "return_value": details["succeeded_at"]},
                completion_after=previous.to_iso8601_string(),
            )

    async def test_provider_does_not_complete_on_the_unchanged_previous_sync(self):
        import pendulum
        from fivetran_provider_async.hooks import FivetranHookAsync

        hook = FivetranHookAsync(fivetran_conn_id="unused_mocked_connection")
        previous = pendulum.parse("2026-11-12T20:00:00Z")
        details = {
            **connection_fixture(),
            "succeeded_at": previous.to_iso8601_string(),
            "failed_at": None,
        }
        with patch.object(
            hook, "get_connector_async", new=AsyncMock(return_value=details)
        ):
            self.assertEqual(
                await hook.get_sync_status_async("drive_export", previous), "pending"
            )
            details["succeeded_at"] = "2026-11-12T20:01:00Z"
            self.assertEqual(
                await hook.get_sync_status_async("drive_export", previous), "success"
            )

    async def test_provider_trigger_reports_sync_failure(self):
        import pendulum
        from fivetran_provider_async.triggers import FivetranTrigger

        trigger = FivetranTrigger(
            task_id="sync_fivetran",
            connector_id="drive_export",
            fivetran_conn_id=pipeline.FIVETRAN_CONN_ID,
            previous_completed_at=pendulum.parse("2026-11-10T12:00:00Z"),
        )
        hook = Mock()
        hook.get_sync_status_async = AsyncMock(return_value="error")
        with patch(
            "fivetran_provider_async.triggers.FivetranHookAsync", return_value=hook
        ):
            events = [event async for event in trigger.run()]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], "error")
        self.assertEqual(
            trigger.serialize()[1]["fivetran_conn_id"], pipeline.FIVETRAN_CONN_ID
        )


class RawTests(unittest.TestCase):
    # Independent source-contract fixture: a wrong DAG allowlist must fail tests.
    XLSM_TABLES = (
        "MOCK_SALESFORCE_XLSM_ACCOUNTS",
        "MOCK_SALESFORCE_XLSM_OPPORTUNITIES",
        "MOCK_ADOBE_XLSM_EVENTS",
        "MOCK_ADOBE_XLSM_CAMPAIGNS",
        "MOCK_STRIPE_XLSM_CUSTOMERS",
        "MOCK_STRIPE_XLSM_SUBSCRIPTIONS",
        "MOCK_STRIPE_XLSM_PAYMENTS",
    )

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
            [] if missing else [(t,) for t in self.XLSM_TABLES]
        )
        cursor.fetchone.side_effect = [(database, schema, user, role)] + (
            [None] if empty else [(1,)] * 7
        )
        return cursor

    def test_xlsm_tables_checked_without_legacy_tables_or_reading_payloads(self):
        cursor = self.cursor()
        self.assertCountEqual(pipeline.RAW_TABLES, self.XLSM_TABLES)
        self.assertEqual(pipeline._validate_raw_tables(cursor)["nonempty_tables"], 7)
        queries = [call.args[0] for call in cursor.execute.call_args_list]
        table_queries = [q for q in queries if q.startswith("select 1")]
        self.assertCountEqual(
            table_queries,
            [
                f"select 1 from CUSTOMER360_TEST.RAW.{table} limit 1"
                for table in self.XLSM_TABLES
            ],
        )
        self.assertTrue(all(q.startswith("select") for q in queries))
        self.assertIn("current_user()", queries[0])
        self.assertIn("current_role()", queries[0])

    def test_legacy_tables_alone_do_not_satisfy_raw_validation(self):
        cursor = self.cursor()
        cursor.fetchall.return_value = [
            (table.replace("_XLSM_", "_"),) for table in self.XLSM_TABLES
        ]
        with self.assertRaisesRegex(
            pipeline.AirflowException, "Missing expected RAW tables"
        ):
            pipeline._validate_raw_tables(cursor)
        # Reject missing active tables before any table-data query.
        self.assertEqual(cursor.execute.call_count, 2)

    def test_missing_xlsm_table_rejected_even_when_legacy_tables_exist(self):
        for missing in self.XLSM_TABLES:
            with self.subTest(missing=missing):
                cursor = self.cursor()
                cursor.fetchall.return_value = [
                    (table,) for table in self.XLSM_TABLES if table != missing
                ] + [(table.replace("_XLSM_", "_"),) for table in self.XLSM_TABLES]
                with self.assertRaisesRegex(pipeline.AirflowException, missing):
                    pipeline._validate_raw_tables(cursor)
                self.assertEqual(cursor.execute.call_count, 2)

    def test_each_empty_xlsm_table_blocks_validation(self):
        for index, table in enumerate(self.XLSM_TABLES):
            with self.subTest(empty=table):
                cursor = self.cursor()
                cursor.fetchone.side_effect = (
                    [("CUSTOMER360_TEST", "RAW", "SVC_AIRFLOW_TEST", "C360_AIRFLOW_ROLE")]
                    + [(1,)] * index
                    + [None]
                )
                with self.assertRaisesRegex(
                    pipeline.AirflowException, f"Expected RAW table is empty: {table}"
                ):
                    pipeline._validate_raw_tables(cursor)
                self.assertEqual(
                    cursor.execute.call_args.args[0],
                    f"select 1 from CUSTOMER360_TEST.RAW.{table} limit 1",
                )

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

    def test_only_seven_approved_warnings_are_nonblocking(self):
        self.assertEqual(pipeline.EXPECTED_WARNINGS, APPROVED_WARNING_TESTS)
        manifest = manifest_fixture()
        self.assertEqual(
            pipeline._summarize_dbt(manifest, results_fixture(manifest), 0),
            {"models": 3, "tests": 8, "warnings": 7},
        )

    def test_missing_subscription_warning_allows_completed_exception_snapshot(self):
        manifest = manifest_fixture()
        for name in ("int_stripe__payments", "int_data_quality__orphan_records"):
            manifest["nodes"]["model.customer360_dbt." + name] = {
                "name": name,
                "resource_type": "model",
                "database": "CUSTOMER360_TEST",
                "schema": "SILVER",
                "original_file_path": f"models/intermediate/{name}.sql",
            }
        results = results_fixture(manifest)
        warning = (
            "relationships_stg_stripe__payments_subscription_id__subscription_id__ref_stg_stripe__subscriptions_"
        )
        for result in results["results"]:
            if (
                result["status"] == "warn"
                and manifest["nodes"][result["unique_id"]]["name"] != warning
            ):
                result.update(status="pass", failures=0)
        self.assertEqual(
            pipeline._summarize_dbt(manifest, results, 0),
            {"models": 5, "tests": 8, "warnings": 1},
        )
        # A warning never excuses failure to rebuild the exception table.
        for status in ("skipped", "error"):
            with self.subTest(snapshot_status=status):
                results["results"][-1]["status"] = status
                with self.assertRaises(pipeline.AirflowException):
                    pipeline._summarize_dbt(manifest, results, 0)

    def test_model_refresh_logs_route_count_and_write_time_without_adapter_payload(self):
        manifest = manifest_fixture()
        results = results_fixture(manifest)
        results["results"][0].update(
            adapter_response={
                "rows_affected": 8, "message": "synthetic-sensitive-payload"
            },
            timing=[
                {
                    "name": "execute",
                    "started_at": "2026-11-12T20:32:00Z",
                    "completed_at": "2026-11-12T20:32:02Z",
                }
            ],
        )
        with self.assertLogs(pipeline.LOG, level="INFO") as captured:
            pipeline._summarize_dbt(manifest, results, 0)
        text = " ".join(captured.output)
        self.assertIn("relation=CUSTOMER360_TEST.GOLD.fct_opportunities", text)
        self.assertIn("rows_affected=8", text)
        self.assertIn("execute_completed_at=2026-11-12T20:32:02+00:00", text)
        self.assertNotIn("synthetic-sensitive-payload", text)
        results["results"][0]["adapter_response"]["rows_affected"] = (
            "synthetic-sensitive-payload"
        )
        results["results"][0]["timing"][0]["completed_at"] = (
            "synthetic-sensitive-payload"
        )
        with self.assertLogs(pipeline.LOG, level="INFO") as captured:
            pipeline._summarize_dbt(manifest, results, 0)
        self.assertNotIn("synthetic-sensitive-payload", " ".join(captured.output))

    def test_run_dbt_task_invokes_the_build_after_raw_validation(self):
        raw = {"database": "CUSTOMER360_TEST", "schema": "RAW", "nonempty_tables": 7}
        task = pipeline.pipeline.get_task("run_dbt").python_callable
        with patch.object(pipeline, "_build_dbt", return_value={"models": 14}) as build:
            self.assertEqual(task(raw), {"models": 14})
        build.assert_called_once_with()

    def test_missing_or_skipped_staging_intermediate_or_exceptions_blocks_success(self):
        for name, layer in (
            ("stg_stripe__payments", "staging"),
            ("int_stripe__payments", "intermediate"),
            ("int_data_quality__orphan_records", "intermediate"),
        ):
            for status in ("missing", "skipped"):
                with self.subTest(model=name, status=status):
                    manifest = manifest_fixture()
                    uid = "model.customer360_dbt." + name
                    manifest["nodes"][uid] = {
                        "name": name, "resource_type": "model",
                        "database": "CUSTOMER360_TEST", "schema": "SILVER",
                        "original_file_path": f"models/{layer}/{name}.sql",
                    }
                    results = results_fixture(manifest)
                    if status == "missing":
                        results["results"].pop()
                    else:
                        results["results"][-1]["status"] = "skipped"
                    with self.assertRaises(pipeline.AirflowException):
                        pipeline._summarize_dbt(manifest, results, 0)

    def test_approved_tests_can_pass_when_exceptions_resolve(self):
        manifest = manifest_fixture()
        results = results_fixture(manifest)
        for result in results["results"]:
            if result["status"] == "warn":
                result.update(status="pass", failures=0)
        self.assertEqual(
            pipeline._summarize_dbt(manifest, results, 0)["warnings"], 0
        )

    def test_approved_warning_names_do_not_exempt_errors_failures_or_skips(self):
        for name in APPROVED_WARNING_TESTS:
            for status in ("fail", "error", "skipped"):
                with self.subTest(name=name, status=status):
                    manifest = manifest_fixture()
                    results = results_fixture(manifest)
                    next(
                        result for result in results["results"]
                        if result["unique_id"] == "test.customer360_dbt." + name
                    )["status"] = status
                    with self.assertRaises(pipeline.AirflowException):
                        pipeline._summarize_dbt(manifest, results, 0)

    def test_other_staging_date_warnings_remain_blocking(self):
        for name in (
            "not_null_stg_salesforce__accounts_created_at",
            "not_null_stg_stripe__subscriptions_start_date",
            "not_null_stg_adobe__campaigns_start_date",
        ):
            with self.subTest(name=name):
                manifest = manifest_fixture()
                uid = "test.customer360_dbt." + name
                manifest["nodes"][uid] = {
                    "name": name,
                    "resource_type": "test",
                    "config": {"severity": "error"},
                }
                results = results_fixture(manifest)
                results["results"][-1]["status"] = "warn"
                with self.assertRaises(pipeline.AirflowException):
                    pipeline._summarize_dbt(manifest, results, 0)
                manifest["nodes"][uid]["config"]["severity"] = "warn"
                with self.assertRaises(pipeline.AirflowFailException):
                    pipeline._validate_manifest(manifest)

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
        for name in ("int_stripe__payments", "int_data_quality__orphan_records"):
            manifest["nodes"]["model.customer360_dbt." + name] = {
                "name": name,
                "resource_type": "model",
                "database": "CUSTOMER360_TEST",
                "schema": "SILVER",
                "original_file_path": f"models/intermediate/{name}.sql",
            }
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
                for _ in range(2):
                    self.assertEqual(
                        pipeline._build_dbt(),
                        {"models": 5, "tests": 8, "warnings": 7},
                    )
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

    def test_subprocess_logs_actual_command_without_runtime_credentials(self):
        command = [
            "/opt/dbt/bin/dbt", "build", "--select", "staging", "intermediate",
            "marts", "--threads", "1",
        ]
        process = Mock()
        process.wait.return_value = 0
        process.poll.return_value = 0
        with (
            patch.object(pipeline.subprocess, "Popen", return_value=process) as popen,
            self.assertLogs(pipeline.LOG, level="INFO") as captured,
        ):
            self.assertEqual(
                pipeline._run_dbt_command(
                    command, Path("/project"),
                    {"SYNTHETIC_SECRET": "synthetic-sensitive-payload"},
                ),
                0,
            )
        text = " ".join(captured.output)
        self.assertIn(json.dumps(command), text)
        self.assertIn("project_dir=/project", text)
        self.assertIn("profile=customer360 target=dev", text)
        self.assertNotIn("synthetic-sensitive-payload", text)
        self.assertEqual(popen.call_args.args[0], command)


if __name__ == "__main__":
    unittest.main()
