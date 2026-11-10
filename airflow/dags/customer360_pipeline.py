"""Monitor existing Fivetran ingestion, validate RAW, and build trusted dbt layers.

No network, secrets lookup, or dbt subprocess runs during DAG parsing.
Runtime configuration and the current-state rerun contract are in airflow/README.md.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import tempfile
from contextlib import closing
from datetime import UTC, datetime, timedelta
from html import escape
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.sdk import Connection, PokeReturnValue, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

LOG = logging.getLogger(__name__)
DATABASE = "CUSTOMER360_TEST"
RAW_SCHEMA = "RAW"
FIVETRAN_CONN_ID = "customer360_fivetran"
SNOWFLAKE_CONN_ID = "customer360_snowflake_raw"
RAW_TABLES = (
    "MOCK_SALESFORCE_ACCOUNTS",
    "MOCK_SALESFORCE_OPPORTUNITIES",
    "MOCK_ADOBE_EVENTS",
    "MOCK_ADOBE_CAMPAIGNS",
    "MOCK_STRIPE_CUSTOMERS",
    "MOCK_STRIPE_SUBSCRIPTIONS",
    "MOCK_STRIPE_PAYMENTS",
)
EXPECTED_WARNINGS = frozenset(
    {
        "relationships_stg_salesforce__opportunities_account_id__account_id__ref_stg_salesforce__accounts_",
        "relationships_stg_stripe__subscriptions_customer_id__customer_id__ref_stg_stripe__customers_",
        "relationships_stg_stripe__payments_customer_id__customer_id__ref_stg_stripe__customers_",
    }
)
ADMIN_ROLES = {
    "ACCOUNTADMIN",
    "ORGADMIN",
    "SECURITYADMIN",
    "SYSADMIN",
    "USERADMIN",
    "PUBLIC",
}
SCHEDULE = (
    os.environ.get("CUSTOMER360_SCHEDULE") or None
)  # Explicit manual-only placeholder.


def failure_notification(context):
    """Do not include exception messages, connection details, or subprocess output."""
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "customer360_pipeline")
    task_id = getattr(ti, "task_id", "unknown")
    LOG.error(
        "Customer360 failure dag=%s task=%s; downstream processing blocked",
        dag_id,
        task_id,
    )
    recipients = [
        x.strip()
        for x in os.environ.get("CUSTOMER360_ALERT_EMAILS", "").split(",")
        if x.strip()
    ]
    if not recipients:
        LOG.error(
            "Failure email not configured; set CUSTOMER360_ALERT_EMAILS and customer360_smtp"
        )
        return
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        with SmtpHook(smtp_conn_id="customer360_smtp") as smtp:
            smtp.send_email_smtp(
                to=recipients,
                subject="Customer360 pipeline task failed",
                html_content=f"DAG {escape(dag_id)}, task {escape(task_id)} failed. Check Airflow task logs.",
            )
    except Exception:
        LOG.error(
            "Customer360 failure notification could not be delivered; inspect SMTP configuration"
        )


def _timestamp(value):
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(UTC)
    except (TypeError, ValueError):
        raise AirflowFailException(
            "Invalid timezone-aware Fivetran/run timestamp"
        ) from None


def _connection():
    try:
        connection = Connection.get(FIVETRAN_CONN_ID)
        extra = connection.extra_dejson
        ids = extra["connection_ids"]
        destination_id = extra["destination_id"]
        if not connection.login or not connection.password:
            raise ValueError
        if not isinstance(ids, list) or not ids or len(ids) != len(set(ids)):
            raise ValueError
        if any(
            not isinstance(x, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", x)
            for x in [*ids, destination_id]
        ):
            raise ValueError
        return connection, ids, destination_id
    except Exception:
        raise AirflowFailException(
            "Configure customer360_fivetran credentials, connection_ids and destination_id"
        ) from None


def _fivetran_get(resource, identifier):
    """Fixed HTTPS origin, no redirects, bounded requests, no response-body logging."""
    import requests

    connection, _, _ = _connection()
    try:
        response = requests.get(
            f"https://api.fivetran.com/v1/{resource}/{identifier}",
            auth=(connection.login, connection.password),
            headers={"Accept": "application/json;version=2"},
            timeout=(10, 30),
            allow_redirects=False,
        )
    except requests.RequestException:
        raise AirflowException(
            "Fivetran request failed; Airflow will retry without logging credentials"
        ) from None
    if response.status_code in {401, 403, 404}:
        raise AirflowFailException(
            f"Fivetran configuration/access error: HTTP {response.status_code}"
        )
    if response.status_code != 200:
        raise AirflowException(
            f"Fivetran request unsuccessful: HTTP {response.status_code}"
        )
    try:
        payload = response.json()
        if payload["code"] != "Success" or not isinstance(payload["data"], dict):
            raise ValueError
        return payload["data"]
    except (KeyError, TypeError, ValueError):
        raise AirflowException("Fivetran returned an invalid status response") from None


def _verified_status(connection_id, destination_id):
    details = _fivetran_get("connections", connection_id)
    destination = _fivetran_get("destinations", destination_id)
    # File connections may name their output as schema.table rather than schema.
    schema = str(details.get("schema", "")).split(".", 1)[0].upper()
    if (
        details.get("id") != connection_id
        or details.get("group_id") != destination_id
        or destination.get("id") != destination_id
        or destination.get("service") != "snowflake"
        or destination.get("config", {}).get("database", "").upper() != DATABASE
        or schema != RAW_SCHEMA
    ):
        raise AirflowFailException(
            "Fivetran destination must match the configured CUSTOMER360_TEST.RAW destination"
        )
    if destination.get("setup_status") != "CONNECTED":
        raise AirflowFailException("Fivetran destination is not connected")
    if (
        details.get("paused")
        or details.get("status", {}).get("setup_state") != "connected"
    ):
        raise AirflowFailException(
            "Fivetran connection is paused or requires setup; resolve it upstream"
        )
    return details


def _sync_finished(details, not_before):
    """Never accept an old success, a later failure, or a still-running sync."""
    status = details.get("status", {})
    state = status.get("sync_state")
    if state not in {"scheduled", "syncing", "rescheduled"}:
        raise AirflowFailException(
            "Unexpected Fivetran sync state; resolve connection configuration"
        )
    succeeded = _timestamp(details.get("succeeded_at"))
    failed = _timestamp(details.get("failed_at"))
    boundary = _timestamp(not_before)
    LOG.info(
        "Fivetran connection=%s state=%s succeeded_at=%s failed_at=%s warning_count=%s",
        details["id"],
        state,
        succeeded,
        failed,
        len(status.get("warnings") or []),
    )
    if failed and failed >= boundary and (succeeded is None or failed >= succeeded):
        raise AirflowException(
            "Fivetran reported a new sync failure; RAW validation remains blocked"
        )
    return bool(
        state == "scheduled"
        and succeeded
        and succeeded >= boundary
        and (failed is None or succeeded > failed)
    )


def _validate_raw_tables(cursor):
    cursor.execute("select current_database(), current_schema(), current_role()")
    database, schema, role = cursor.fetchone()
    if (
        database != DATABASE
        or schema != RAW_SCHEMA
        or not role
        or role.upper() in ADMIN_ROLES
    ):
        raise AirflowFailException(
            "RAW validation requires CUSTOMER360_TEST.RAW and a dedicated non-admin role"
        )
    cursor.execute(
        "select table_name from CUSTOMER360_TEST.information_schema.tables where table_schema = %s",
        (RAW_SCHEMA,),
    )
    missing = set(RAW_TABLES) - {row[0] for row in cursor.fetchall()}
    if missing:
        raise AirflowException(
            "Missing expected RAW tables: " + ", ".join(sorted(missing))
        )
    for table in RAW_TABLES:
        # Identifiers come exclusively from the constant allowlist; no row data is logged.
        cursor.execute(f"select 1 from {DATABASE}.{RAW_SCHEMA}.{table} limit 1")
        if cursor.fetchone() is None:
            raise AirflowException(f"Expected RAW table is empty: {table}")
        LOG.info(
            "RAW table verified: %s.%s.%s exists and contains data",
            DATABASE,
            RAW_SCHEMA,
            table,
        )
    return {
        "database": DATABASE,
        "schema": RAW_SCHEMA,
        "nonempty_tables": len(RAW_TABLES),
    }


def _validate_manifest(manifest):
    nodes = manifest.get("nodes", {})
    models = {key: n for key, n in nodes.items() if n.get("resource_type") == "model"}
    if not models or not {
        "fct_opportunities",
        "fct_subscriptions",
        "fct_payments",
    }.issubset({n["name"] for n in models.values()}):
        raise AirflowFailException(
            "dbt manifest is missing the expected trusted Gold models"
        )
    for node in models.values():
        path = node["original_file_path"].replace("\\", "/")
        schema = "GOLD" if path.startswith("models/marts/") else "SILVER"
        if node.get("database") != DATABASE or node.get("schema") != schema:
            raise AirflowFailException(
                "dbt destination guard rejected a model outside CUSTOMER360_TEST SILVER/GOLD"
            )
    for source in manifest.get("sources", {}).values():
        if source.get("database") != DATABASE or source.get("schema") != RAW_SCHEMA:
            raise AirflowFailException(
                "dbt source destination guard rejected a source outside CUSTOMER360_TEST.RAW"
            )
    for node in nodes.values():
        if node.get("resource_type") != "test":
            continue
        if (
            node.get("config", {}).get("severity", "error").lower() != "error"
            and node["name"] not in EXPECTED_WARNINGS
        ):
            raise AirflowFailException(
                "Only the three documented staging relationships may have warning severity"
            )
    return set(models)


def _summarize_dbt(manifest, results, returncode):
    model_ids = _validate_manifest(manifest)
    nodes = manifest["nodes"]
    if results.get("metadata", {}).get("invocation_id") != manifest.get(
        "metadata", {}
    ).get("invocation_id"):
        raise AirflowFailException("dbt artifacts are from different invocations")
    observed = set()
    warnings = []
    counts = {"models": 0, "tests": 0, "warnings": 0}
    blocked = returncode != 0
    for result in results.get("results", []):
        uid, status = result["unique_id"], result["status"]
        node = nodes.get(uid) or manifest.get("unit_tests", {}).get(uid, {})
        observed.add(uid)
        # Never log dbt's message/compiled SQL/adapter response: they can contain sensitive data.
        LOG.info(
            "dbt node=%s status=%s failing_rows=%s", uid, status, result.get("failures")
        )
        if status == "warn":
            if node.get("name") not in EXPECTED_WARNINGS:
                blocked = True
            else:
                warnings.append(node["name"])
                LOG.warning(
                    "Expected orphan warning: %s; facts retained in Silver and excluded from Gold",
                    node["name"],
                )
        elif status not in {"pass", "success"}:
            blocked = True  # Includes fail, error, skipped and partial success.
        if uid in model_ids:
            counts["models"] += 1
        else:
            counts["tests"] += 1
    required = (
        model_ids
        | {key for key, n in nodes.items() if n.get("resource_type") == "test"}
        | set(manifest.get("unit_tests", {}))
    )
    if not required.issubset(observed) or not required:
        blocked = True
    if blocked:
        raise AirflowException(
            "dbt build failed, skipped required nodes, or returned unexpected warnings; inspect node statuses"
        )
    counts["warnings"] = len(warnings)
    LOG.info(
        "dbt complete: models=%s tests=%s expected_warnings=%s",
        counts["models"],
        counts["tests"],
        counts["warnings"],
    )
    return counts


def _run_dbt_command(command, project, environment, timeout=2700):
    # Linux worker: own process group lets timeout/cancellation terminate dbt children too.
    process = subprocess.Popen(
        command,
        cwd=project,
        env=environment,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        return process.wait(timeout=timeout)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def _build_dbt():
    project = Path(
        os.environ.get("CUSTOMER360_DBT_PROJECT_DIR", "/opt/customer360/dbt")
    )
    profiles = Path(
        os.environ.get("CUSTOMER360_DBT_PROFILES_DIR", "/opt/airflow/secrets/dbt")
    )
    executable = os.environ.get("CUSTOMER360_DBT_EXECUTABLE", "/opt/dbt/bin/dbt")
    if (
        not project.is_absolute()
        or not profiles.is_absolute()
        or not (project / "dbt_project.yml").is_file()
        or not (profiles / "profiles.yml").is_file()
    ):
        raise AirflowFailException(
            "Configure absolute dbt project/profile paths and mount the runtime profile"
        )
    _validate_dbt_profile(profiles / "profiles.yml")
    # Each attempt owns its artifacts: no stale run_results and no cross-worker filesystem dependency.
    with tempfile.TemporaryDirectory(prefix="customer360-dbt-") as directory:
        environment = os.environ.copy()
        environment.pop("DBT_WARN_ERROR_OPTIONS", None)
        environment.update(
            {
                "DBT_PROFILES_DIR": str(profiles),
                "DBT_PROFILE": "customer360",
                "DBT_TARGET": "dev",
                "DBT_TARGET_PATH": str(Path(directory) / "target"),
                "DBT_LOG_PATH": str(Path(directory) / "logs"),
                "DBT_PARTIAL_PARSE": "false",
                "DBT_WARN_ERROR": "false",
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "DBT_USE_COLORS": "false",
            }
        )
        manifest_path = Path(environment["DBT_TARGET_PATH"]) / "manifest.json"
        if _run_dbt_command(
            [executable, "parse", "--no-partial-parse"],
            project,
            environment,
            timeout=300,
        ):
            raise AirflowException(
                "dbt preflight parse failed; check the runtime profile and project configuration"
            )
        _validate_manifest(json.loads(manifest_path.read_text()))
        LOG.info(
            "dbt target verified: CUSTOMER360_TEST.SILVER/GOLD; building staging intermediate marts"
        )
        result = _run_dbt_command(
            [
                executable,
                "build",
                "--select",
                "staging",
                "intermediate",
                "marts",
                "--threads",
                "1",
            ],
            project,
            environment,
        )
        result_path = Path(environment["DBT_TARGET_PATH"]) / "run_results.json"
        if not result_path.is_file():
            raise AirflowException(
                f"dbt did not produce run_results.json; process exit code={result}"
            )
        return _summarize_dbt(
            json.loads(manifest_path.read_text()),
            json.loads(result_path.read_text()),
            result,
        )


def _validate_dbt_profile(path):
    """Inspect only routing/role fields; never render or log authentication fields."""
    import yaml
    from jinja2 import Environment, StrictUndefined

    try:
        output = yaml.safe_load(path.read_text())["customer360"]["outputs"]["dev"]
        renderer = Environment(undefined=StrictUndefined)

        def env_var(name, default=None):
            value = os.environ.get(name, default)
            if value is None:
                raise ValueError
            return value

        renderer.globals["env_var"] = env_var
        values = {
            key: renderer.from_string(str(output[key])).render().upper()
            for key in ("database", "schema", "role")
        }
        if (
            values["database"] != DATABASE
            or values["schema"] != "SILVER"
            or not values["role"]
            or values["role"] in ADMIN_ROLES
        ):
            raise ValueError
    except Exception:
        raise AirflowFailException(
            "dbt customer360/dev must use CUSTOMER360_TEST.SILVER and a dedicated non-admin role"
        ) from None


@dag(
    dag_id="customer360_pipeline",
    description="Monitor Fivetran, validate TEST RAW, build and test dbt Silver/intermediate/Gold",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=5),
        "on_failure_callback": failure_notification,
    },
    tags=["customer360", "fivetran", "dbt"],
)
def customer360_pipeline():
    @task(multiple_outputs=False)
    def prepare_sync_monitoring():
        if not os.environ.get("CUSTOMER360_ALERT_EMAILS", "").strip():
            raise AirflowFailException(
                "Configure CUSTOMER360_ALERT_EMAILS and customer360_smtp before running the pipeline"
            )
        _, connection_ids, destination_id = _connection()
        context = get_current_context()
        run_start = _timestamp(context["dag_run"].start_date)
        interval_end = _timestamp(context.get("data_interval_end"))
        # A cleared/retried run keeps its boundary. No historical replay of mutable Drive workbooks.
        not_before = max(
            x for x in (run_start, interval_end) if x is not None
        ).isoformat()
        snapshots = []
        for connection_id in connection_ids:
            _verified_status(connection_id, destination_id)
            snapshots.append(
                {
                    "connection_id": connection_id,
                    "destination_id": destination_id,
                    "not_before": not_before,
                }
            )
        LOG.info(
            "Monitoring %s Fivetran connections; require successful completion at/after %s",
            len(snapshots),
            not_before,
        )
        return snapshots

    @task.sensor(
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
        execution_timeout=timedelta(minutes=2),
    )
    def wait_for_fivetran(snapshot):
        details = _verified_status(
            snapshot["connection_id"], snapshot["destination_id"]
        )
        complete = _sync_finished(details, snapshot["not_before"])
        return PokeReturnValue(
            is_done=complete,
            xcom_value={
                "connection_id": snapshot["connection_id"],
                "succeeded_at": details.get("succeeded_at"),
            }
            if complete
            else None,
        )

    @task(multiple_outputs=False)
    def validate_raw(sync_results):
        completed = list(sync_results)
        if not completed:
            raise AirflowFailException(
                "No completed Fivetran connections; refusing RAW validation"
            )
        try:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            config = Connection.get(SNOWFLAKE_CONN_ID)
            if (
                config.extra_dejson.get("database", "").upper() != DATABASE
                or (config.schema or "").upper() != RAW_SCHEMA
            ):
                raise AirflowFailException(
                    "Configure customer360_snowflake_raw for CUSTOMER360_TEST.RAW"
                )
            hook = SnowflakeHook(
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                database=DATABASE,
                schema=RAW_SCHEMA,
                session_parameters={
                    "QUERY_TAG": "customer360_raw_validation",
                    "STATEMENT_TIMEOUT_IN_SECONDS": 60,
                },
            )
            with (
                closing(hook.get_conn()) as connection,
                closing(connection.cursor()) as cursor,
            ):
                return _validate_raw_tables(cursor)
        except AirflowException:
            raise
        except Exception:
            raise AirflowException(
                "Snowflake RAW validation failed; check connection permissions and availability"
            ) from None

    @task(execution_timeout=timedelta(minutes=55), multiple_outputs=False)
    def run_dbt(raw_validation):
        if raw_validation != {
            "database": DATABASE,
            "schema": RAW_SCHEMA,
            "nonempty_tables": len(RAW_TABLES),
        }:
            raise AirflowFailException("RAW validation prerequisite was not satisfied")
        try:
            return _build_dbt()
        except AirflowException:
            raise
        except Exception:
            raise AirflowException(
                "dbt execution/artifact validation failed; verify executable, mounts and runtime credentials"
            ) from None

    monitored = wait_for_fivetran.expand(snapshot=prepare_sync_monitoring())
    run_dbt(validate_raw(monitored))


pipeline = customer360_pipeline()
