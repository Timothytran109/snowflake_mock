"""Trigger and await existing Fivetran ingestion, validate RAW, and build trusted dbt layers.

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
from airflow.sdk import Connection, dag, task
from airflow.sdk.exceptions import AirflowFailException, TaskDeferred
from airflow.sdk.log import mask_secret
from fivetran_provider_async.operators import FivetranOperator

LOG = logging.getLogger(__name__)
DATABASE = "CUSTOMER360_TEST"
RAW_SCHEMA = "RAW"
FIVETRAN_CONN_ID = "customer360_fivetran"
SNOWFLAKE_CONN_ID = "customer360_snowflake_raw"
RAW_TABLES = (
    "MOCK_SALESFORCE_XLSM_ACCOUNTS",
    "MOCK_SALESFORCE_XLSM_OPPORTUNITIES",
    "MOCK_ADOBE_XLSM_EVENTS",
    "MOCK_ADOBE_XLSM_CAMPAIGNS",
    "MOCK_STRIPE_XLSM_CUSTOMERS",
    "MOCK_STRIPE_XLSM_SUBSCRIPTIONS",
    "MOCK_STRIPE_XLSM_PAYMENTS",
)
EXPECTED_WARNINGS = frozenset(
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
ADMIN_ROLES = {
    "ACCOUNTADMIN",
    "ORGADMIN",
    "SECURITYADMIN",
    "SYSADMIN",
    "USERADMIN",
    "PUBLIC",
}
SNOWFLAKE_DEPLOYMENT_DEFAULTS = {
    "RAW_USER": "SVC_AIRFLOW_TEST",
    "RAW_ROLE": "C360_AIRFLOW_ROLE",
    "RAW_PRIVATE_KEY_PATH": "/opt/airflow/secrets/snowflake/airflow_rsa_key.p8",
    "DBT_USER": "SVC_DBT_TEST",
    "DBT_ROLE": "C360_DBT_ROLE",
    "DBT_PRIVATE_KEY_PATH": "/opt/airflow/secrets/snowflake/dbt_rsa_key.p8",
}
SCHEDULE = (
    os.environ.get("CUSTOMER360_SCHEDULE") or None
)  # Explicit manual-only placeholder.


def _alert_recipients():
    """Use the runtime value passed by Compose; never read host dotenv files here."""
    return [
        value.strip()
        for value in os.environ.get("CUSTOMER360_ALERT_EMAILS", "").split(",")
        if value.strip()
    ]


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
    recipients = _alert_recipients()
    if not recipients:
        LOG.error(
            "Failure email not configured; set CUSTOMER360_ALERT_EMAILS and customer360_smtp"
        )
        return
    for recipient in recipients:
        mask_secret(recipient)
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        with SmtpHook(smtp_conn_id="customer360_smtp") as smtp:
            smtp.send_email_smtp(
                to=recipients,
                subject="Customer360 pipeline task failed",
                html_content=f"DAG {escape(dag_id)}, task {escape(task_id)} failed. Check Airflow task logs.",
            )
    except Exception:  # noqa: BLE001 -- Delivery errors must not replace the original task failure.
        LOG.error(
            "Customer360 failure notification could not be delivered; inspect SMTP configuration"
        )


def _connection():
    try:
        connection = Connection.get(FIVETRAN_CONN_ID)
        connector_id = os.environ.get("CUSTOMER360_FIVETRAN_CONNECTOR_ID", "").strip()
        destination_id = os.environ.get(
            "CUSTOMER360_FIVETRAN_DESTINATION_ID", ""
        ).strip()
        ids = [connector_id]
        if connector_id:
            mask_secret(connector_id)
        if not connection.login or not connection.password:
            raise ValueError
        mask_secret(connection.login)
        mask_secret(connection.password)
        if not isinstance(ids, list) or not ids or len(ids) != len(set(ids)):
            raise ValueError
        if any(
            not isinstance(x, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", x)
            for x in [*ids, destination_id]
        ):
            raise ValueError
        return connection, ids, destination_id
    except Exception:  # noqa: BLE001 -- Secret-backend errors must not expose connection details.
        raise AirflowFailException(
            "Configure customer360_fivetran credentials and CUSTOMER360_FIVETRAN_CONNECTOR_ID / CUSTOMER360_FIVETRAN_DESTINATION_ID"
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
    log_identifier = identifier if resource == "destinations" else "[redacted]"
    LOG.info(
        "Fivetran API response resource=%s identifier=%s http_status=%s",
        resource,
        log_identifier,
        response.status_code,
    )
    try:
        payload = response.json()
    except (TypeError, ValueError):
        payload = None
    api_code = payload.get("code") if isinstance(payload, dict) else None
    api_message = payload.get("message") if isinstance(payload, dict) else None
    safe_code = _safe_api_text(api_code)
    safe_message = _safe_api_text(api_message)
    if response.status_code in {401, 403, 404}:
        LOG.warning(
            "Fivetran API configuration/access error resource=%s identifier=%s "
            "http_status=%s api_code=%s api_message=%s",
            resource,
            log_identifier,
            response.status_code,
            safe_code,
            safe_message,
        )
        raise AirflowFailException(
            f"Fivetran configuration/access error: HTTP {response.status_code}"
        )
    if response.status_code != 200:
        LOG.warning(
            "Fivetran API error resource=%s identifier=%s http_status=%s "
            "api_code=%s api_message=%s",
            resource,
            log_identifier,
            response.status_code,
            safe_code,
            safe_message,
        )
        raise AirflowException(
            f"Fivetran request unsuccessful: HTTP {response.status_code}"
        )
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "Success"
        or not isinstance(payload.get("data"), dict)
    ):
        LOG.warning(
            "Unexpected Fivetran API response resource=%s identifier=%s "
            "http_status=%s api_code=%s api_message=%s",
            resource,
            log_identifier,
            response.status_code,
            safe_code,
            safe_message,
        )
        raise AirflowException("Fivetran returned an invalid status response") from None
    data = payload["data"]
    if resource == "destinations":
        setup_tests = data.get("setup_tests")
        test_statuses = (
            [test.get("status") for test in setup_tests if isinstance(test, dict)]
            if isinstance(setup_tests, list)
            else []
        )
        LOG.info(
            "Fivetran destination status destination_id=%s setup_status=%s "
            "setup_test_statuses=%s",
            identifier,
            _safe_api_text(data.get("setup_status")),
            [_safe_api_text(status) for status in test_statuses],
        )
    return data


def _safe_api_text(value):
    """Bound diagnostic fields and remove control characters; never log payloads."""
    if not isinstance(value, str) or not value:
        return "[unavailable]"
    return re.sub(r"[\x00-\x1f\x7f]", " ", value).strip()[:200]


def _safe_timestamp(value):
    """Log only parsed, timezone-aware timestamps, never arbitrary payload text."""
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return timestamp.astimezone(UTC).isoformat() if timestamp.tzinfo else None
    except (ValueError, OverflowError):
        return None


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
    setup_status = destination.get("setup_status")
    if not isinstance(setup_status, str) or setup_status.strip().upper() != "CONNECTED":
        raise AirflowFailException("Fivetran destination is not connected")
    if (
        details.get("paused")
        or details.get("status", {}).get("setup_state") != "connected"
    ):
        raise AirflowFailException(
            "Fivetran connection is paused or requires setup; resolve it upstream"
        )
    return details


class GuardedFivetranOperator(FivetranOperator):
    """Recheck destination on every trigger attempt; provider owns sync and deferral."""

    def execute(self, context):
        _, ids, destination_id = _connection()
        if self.connector_id not in ids:
            raise AirflowFailException(
                "Fivetran connector is not configured for this project"
            )
        details = _verified_status(self.connector_id, destination_id)
        # Preserve the existing ingestion schedule; do not silently change it to manual.
        self.schedule_type = details.get("schedule_type")
        if self.schedule_type not in {"auto", "manual"}:
            raise AirflowFailException("Fivetran schedule type is invalid")
        LOG.info(
            "Triggering configured Fivetran connector with asynchronous completion"
        )
        try:
            super().execute(context)
        except TaskDeferred as deferred:
            # Persist the provider's pre-sync watermark across operator reconstruction.
            deferred.kwargs = {
                **(deferred.kwargs or {}),
                "completion_after": self._completion_after,
            }
            raise
        # Immediate provider completion returns None; always publish a mapped result.
        return self._verified_completion(self._completion_after)

    def _sync(self, hook):
        self._completion_after = super()._sync(hook)
        LOG.info(
            "Fivetran provider sync watermark completed_after=%s",
            _safe_timestamp(self._completion_after),
        )
        return self._completion_after

    def _verified_completion(self, completion_after, reported_completion=None):
        baseline = _safe_timestamp(completion_after)
        if baseline is None:
            raise AirflowException(
                "Fivetran sync watermark is missing; RAW validation remains blocked"
            )
        _, ids, destination_id = _connection()
        if self.connector_id not in ids:
            raise AirflowFailException(
                "Fivetran connector is not configured for this project"
            )
        details = _verified_status(self.connector_id, destination_id)
        succeeded = _safe_timestamp(details.get("succeeded_at"))
        failed = _safe_timestamp(details.get("failed_at"))
        if (
            details.get("status", {}).get("sync_state") != "scheduled"
            or succeeded is None
            or datetime.fromisoformat(succeeded) <= datetime.fromisoformat(baseline)
            or (
                failed is not None
                and datetime.fromisoformat(failed) >= datetime.fromisoformat(succeeded)
            )
            or (details.get("failed_at") is not None and failed is None)
            or (
                reported_completion is not None
                and datetime.fromisoformat(succeeded)
                < datetime.fromisoformat(reported_completion)
            )
        ):
            raise AirflowException(
                "Fivetran sync completion is not verified as a new idle success; RAW validation remains blocked; Airflow will retry"
            )
        LOG.info(
            "Configured Fivetran connector completed successfully; "
            "completed_after=%s completed_at=%s sync_state=scheduled",
            baseline,
            succeeded,
        )
        return {"connector_id": self.connector_id, "status": "success"}

    def execute_complete(self, context, event=None, completion_after=None):
        # Provider 2.4.0 accepts missing events; fail closed and omit API error bodies.
        if not event or event.get("status") != "success":
            raise AirflowException(
                "Fivetran sync failed; RAW validation remains blocked"
            )
        reported = _safe_timestamp(event.get("return_value"))
        if reported is None:
            raise AirflowException(
                "Fivetran completion timestamp is missing; RAW validation remains blocked"
            )
        return self._verified_completion(completion_after, reported)


def _snowflake_policy(layer):
    """Deployment-owned expectations; defaults are TEST service identities only."""
    values = {
        field: os.environ.get(
            f"CUSTOMER360_{layer}_{field}",
            SNOWFLAKE_DEPLOYMENT_DEFAULTS[f"{layer}_{field}"],
        )
        for field in ("USER", "ROLE", "PRIVATE_KEY_PATH")
    }
    if (
        any(
            not re.fullmatch(r"[A-Z_][A-Z0-9_]*", values[field])
            for field in ("USER", "ROLE")
        )
        or values["ROLE"] in ADMIN_ROLES
        or not values["PRIVATE_KEY_PATH"]
        or not Path(values["PRIVATE_KEY_PATH"]).is_absolute()
    ):
        raise AirflowFailException("Invalid Snowflake service deployment policy")
    return values


def _require_rsa_key_path(value, expected):
    """Validate the approved mounted file using metadata only; never read its bytes."""
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise ValueError
    path = Path(value)
    if (
        path != Path(expected)
        or not path.is_file()
        or path.stat().st_size == 0
        or not os.access(path, os.R_OK)
    ):
        raise ValueError


def _require_key_authentication(values):
    authenticator = values.get("authenticator")
    if authenticator is not None and (
        not isinstance(authenticator, str)
        or authenticator.lower() not in {"snowflake", "snowflake_jwt"}
    ):
        raise ValueError
    for field in (
        "token",
        "oauth_client_id",
        "oauth_client_secret",
        "refresh_token",
        "workload_identity_provider",
        "private_key",
        "private_key_content",
    ):
        if values.get(field) not in (None, ""):
            raise ValueError


def _validate_raw_connection(config):
    """Reject a wrong identity or alternative authentication before opening Snowflake."""
    try:
        policy = _snowflake_policy("RAW")
        extra = dict(config.extra_dejson)
        # Do not allow legacy aliases to override the fields validated below.
        if any(
            f"extra__snowflake__{field}" in extra
            for field in (
                "database",
                "role",
                "private_key_file",
                "private_key_content",
                "authenticator",
                "token",
                "refresh_token",
            )
        ):
            raise ValueError
        if (
            extra.get("database", "").upper() != DATABASE
            or (config.schema or "").upper() != RAW_SCHEMA
            or (config.login or "").upper() != policy["USER"]
            or extra.get("role", "").upper() != policy["ROLE"]
        ):
            raise ValueError
        _require_key_authentication(extra)
        _require_rsa_key_path(extra.get("private_key_file"), policy["PRIVATE_KEY_PATH"])
        # The provider may use Connection.password only as the mounted key's passphrase.
        return policy
    except Exception:  # noqa: BLE001 -- Malformed secret-backed config must become a redacted failure.
        raise AirflowFailException(
            "Snowflake RAW connection must use CUSTOMER360_TEST.RAW, the approved service identity, and its mounted RSA key"
        ) from None


def _validate_raw_tables(cursor):
    policy = _snowflake_policy("RAW")
    cursor.execute(
        "select current_database(), current_schema(), current_user(), current_role()"
    )
    database, schema, user, role = cursor.fetchone()
    if (
        database != DATABASE
        or schema != RAW_SCHEMA
        or user != policy["USER"]
        or role != policy["ROLE"]
    ):
        raise AirflowFailException(
            "RAW validation requires CUSTOMER360_TEST.RAW and the approved service user and role"
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
                "Only the seven approved staging relationship and missing-date tests may have warning severity"
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
                    "Approved data-quality warning: %s; records retained in Silver; Gold applies reason-specific eligibility",
                    node["name"],
                )
        elif status not in {"pass", "success"}:
            blocked = True  # Includes fail, error, skipped and partial success.
        if uid in model_ids:
            counts["models"] += 1
            execution = next(
                (
                    item for item in result.get("timing", [])
                    if item.get("name") == "execute"
                ),
                {},
            )
            affected = result.get("adapter_response", {}).get("rows_affected")
            LOG.info(
                "dbt model refresh node=%s relation=%s.%s.%s rows_affected=%s "
                "execute_started_at=%s execute_completed_at=%s",
                uid,
                node["database"],
                node["schema"],
                node.get("alias", node["name"]),
                affected
                if isinstance(affected, int) and not isinstance(affected, bool)
                else None,
                _safe_timestamp(execution.get("started_at")),
                _safe_timestamp(execution.get("completed_at")),
            )
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
    # The caller supplies CLI arguments only, never authentication parameters.
    # Do not log environment values, profile contents or subprocess output.
    LOG.info(
        "dbt execution command=%s project_dir=%s profile=customer360 target=dev",
        json.dumps(command),
        project,
    )
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
    """Validate routing, service identity and key-auth configuration without reading keys."""
    import yaml
    from jinja2 import Environment, StrictUndefined

    try:
        output = yaml.safe_load(path.read_text())["customer360"]["outputs"]["dev"]
        policy = _snowflake_policy("DBT")
        renderer = Environment(undefined=StrictUndefined)

        def env_var(name, default=None):
            value = os.environ.get(name, default)
            if value is None:
                raise ValueError
            return value

        renderer.globals["env_var"] = env_var
        values = {
            key: renderer.from_string(str(output[key])).render().upper()
            for key in ("database", "schema", "user", "role")
        }
        if (
            values["database"] != DATABASE
            or values["schema"] != "SILVER"
            or values["user"] != policy["USER"]
            or values["role"] != policy["ROLE"]
            or output.get("type") != "snowflake"
            or output.get("password") not in (None, "")
        ):
            raise ValueError
        auth = dict(output)
        if output.get("authenticator") is not None:
            auth["authenticator"] = renderer.from_string(
                str(output["authenticator"])
            ).render()
        _require_key_authentication(auth)
        key_path = renderer.from_string(str(output["private_key_path"])).render()
        _require_rsa_key_path(key_path, policy["PRIVATE_KEY_PATH"])
    except Exception:  # noqa: BLE001 -- YAML/template errors can contain secret-backed profile text.
        raise AirflowFailException(
            "dbt customer360/dev must use CUSTOMER360_TEST.SILVER, the approved service identity, and its mounted RSA key; password and interactive authentication are disallowed"
        ) from None


@dag(
    dag_id="customer360_pipeline",
    description="Trigger Fivetran, validate TEST RAW, build and test dbt Silver/intermediate/Gold",
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
    def prepare_fivetran_sync():
        if not _alert_recipients():
            raise AirflowFailException(
                "Configure CUSTOMER360_ALERT_EMAILS and customer360_smtp before running the pipeline"
            )
        _, connection_ids, _ = _connection()
        return connection_ids

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
            policy = _validate_raw_connection(config)
            hook = SnowflakeHook(
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                database=DATABASE,
                schema=RAW_SCHEMA,
                role=policy["ROLE"],
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
        except Exception:  # noqa: BLE001 -- Driver errors may contain credentials; redact before retry.
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
        except Exception:  # noqa: BLE001 -- Unexpected profile/artifact errors must not expose secrets.
            raise AirflowException(
                "dbt execution/artifact validation failed; verify executable, mounts and runtime credentials"
            ) from None

    monitored = (
        GuardedFivetranOperator.partial(
            task_id="sync_fivetran",
            fivetran_conn_id=FIVETRAN_CONN_ID,
            deferrable=True,
            wait_for_completion=True,
            poll_frequency=60,
            execution_timeout=timedelta(hours=2),
        )
        .expand(connector_id=prepare_fivetran_sync())
        .output
    )
    run_dbt(validate_raw(monitored))


pipeline = customer360_pipeline()
