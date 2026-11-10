# Customer 360 Airflow orchestration

`dags/customer360_pipeline.py` orchestrates the existing pipeline in **CUSTOMER360_TEST**:

```text
Google Drive workbooks -> existing Fivetran connections -> Snowflake RAW
prepare_sync_monitoring -> wait_for_fivetran [mapped per connection]
                       -> validate_raw -> run_dbt
                                           staging -> intermediate -> marts
```

The DAG uses Airflow 3.3.2 TaskFlow and starts paused, with `schedule=None` and
`catchup=False`. Set `CUSTOMER360_SCHEDULE` to a cron expression in the DAG
processor environment when a schedule is agreed. Leave it unset for manual runs.
Airflow must be the only scheduler for this dbt build; disable any duplicate dbt
schedule in another system before enabling this DAG.

## Ingestion and freshness contract

This first DAG **monitors existing Fivetran connections** using read-only REST API
requests. It does not force syncs, change ingestion schedules, or write to RAW.
Fivetran continues to own ingestion from Google Drive. Configure every connection
needed to populate the seven tables below, including separate workbook connections.

Before monitoring, the DAG checks each connection's destination ID, Snowflake
database (`CUSTOMER360_TEST`), RAW schema, setup state and paused state. It accepts
only a successful sync completed at or after the later of the DAG run's start time
and data interval end. A previous success, a still-running sync, or a later failed
sync cannot release RAW validation. Sensors poll every minute in reschedule mode,
freeing worker slots between checks, with a two-hour total timeout.

Fivetran therefore needs an enabled ingestion schedule that completes a fresh sync
within that window, or an operator must initiate a sync in Fivetran after starting
the DAG. A previously completed sync alone will not satisfy a new manual run.
The API identity needs read access to connection and destination details only.
No direct Google Drive credentials are needed by Airflow.

The monitoring boundary is stable across task retries within a run. Reruns rebuild
the current source state: mutable workbooks and RAW do not provide historical
snapshots. Do not use this DAG to claim historical point-in-time backfills. Sync
completion and RAW checks are not a cross-connection transactional snapshot; an
independent later ingestion can still update RAW during a build.

## Runtime and dependencies

Run Airflow on Linux with Python 3.12. Install `airflow/requirements.txt` with the
matching official constraints, in the Airflow environment:

```bash
python -m pip install -r airflow/requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
```

Keep dbt in a separate virtual environment to avoid dependency conflicts. Install
the repository-root `requirements.txt` there during worker image preparation.
Deploy the same dbt project revision and dependencies to every worker. The DAG
uses these environment variables (defaults shown):

- `CUSTOMER360_DBT_PROJECT_DIR=/opt/customer360/dbt`: absolute directory containing `dbt_project.yml`.
- `CUSTOMER360_DBT_EXECUTABLE=/opt/dbt/bin/dbt`: executable in the isolated dbt environment.
- `CUSTOMER360_DBT_PROFILES_DIR=/opt/airflow/secrets/dbt`: absolute directory containing a runtime-mounted `profiles.yml`.
- `CUSTOMER360_ALERT_EMAILS`: required comma-separated operational recipients; configure on workers and callback processors.
- `CUSTOMER360_SCHEDULE`: optional cron expression, read at DAG parsing time; default manual-only.

Mount dbt sources read-only where practical. Temporary target and log directories
are created privately for each attempt and removed afterward. Do not bake `.env`,
profiles, tokens, passwords or private keys into images. Use an approved secrets
backend (for example Azure Key Vault with workload identity) or protected runtime
mounts. Installing/configuring a secrets backend is a deployment responsibility.

## Airflow connections

Provision these through the Airflow connection UI or a secrets backend. Never
place live connection JSON or URIs in this repository or shell history.

### `customer360_fivetran`

Use a generic connection: login is the Fivetran API key and password is its secret.
The API host is fixed to `https://api.fivetran.com`; redirects are disabled.
Configure the following non-secret extra fields, replacing the placeholders with
the IDs of the existing TEST destination and connections:

```json
{
  "destination_id": "existing_test_destination_id",
  "connection_ids": ["existing_workbook_connection_id"]
}
```

The list must be nonempty, unique, and cover all required workbooks. The destination
ID is the Fivetran group ID. File connections may report their output as
`raw.table_name`; that form is supported. Failed setup, paused connections, wrong
destinations and authentication errors fail without retrying configuration errors.
Transient HTTP failures use task retries. Response bodies and credentials are not
logged or placed in XCom.

### `customer360_snowflake_raw`

Use connection type `snowflake`, schema `RAW`, and extra fields `database` set to
`CUSTOMER360_TEST`, plus the appropriate `account`, `warehouse`, and dedicated
read-only `role`. Configure authentication using the provider's supported runtime
credential/key mechanism. Do not use an administrative role.

The role needs warehouse USAGE, database/schema USAGE and SELECT on these tables:

- `CUSTOMER360_TEST.RAW.MOCK_SALESFORCE_ACCOUNTS`
- `CUSTOMER360_TEST.RAW.MOCK_SALESFORCE_OPPORTUNITIES`
- `CUSTOMER360_TEST.RAW.MOCK_ADOBE_EVENTS`
- `CUSTOMER360_TEST.RAW.MOCK_ADOBE_CAMPAIGNS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_CUSTOMERS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_SUBSCRIPTIONS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_PAYMENTS`

After all monitored syncs succeed, validation verifies the actual Snowflake session
database, schema and non-admin role. It checks table visibility through
`information_schema.tables` and uses `select 1 ... limit 1` per table to verify
nonempty data without logging business records. Missing or empty tables block dbt.
No grants or source modifications are performed by the DAG.

### dbt runtime profile

The separately mounted profile must contain `customer360.outputs.dev`, using
database `CUSTOMER360_TEST`, schema `SILVER` and a dedicated transformation role
(the existing project uses `C360_DBT_ROLE`). Credentials remain in runtime secrets.
Routing/role fields can be literal values or use dbt `env_var()` expressions.
The existing project macro routes marts to `CUSTOMER360_TEST.GOLD`.

The task validates the profile, runs `dbt parse --no-partial-parse`, and checks
manifest source/model destinations and test severity before building. It then runs
the exact command below with the dbt project as the working directory:

```bash
dbt build --select staging intermediate marts --threads 1
```

All transformations execute through dbt in Snowflake; Python handles orchestration,
bounded metadata checks and result classification. No existing dbt SQL or test
definitions are changed by this DAG.

### `customer360_smtp`

Configure an SMTP provider connection, including host, port, authentication,
appropriate TLS settings and `from_email` extra. Set `CUSTOMER360_ALERT_EMAILS` to
real operational recipients. The preflight task refuses to run without recipients.
The failure callback emails only the DAG/task identity and a direction to inspect
Airflow logs, excluding exception payloads and business data. A delivery failure is
logged without concealing the original task failure. Test SMTP delivery separately
as part of deployment; import/unit validation does not send email.

## Failures, warnings and reruns

Tasks have two retries, a five-minute initial delay with exponential backoff capped
at fifteen minutes, and execution timeouts. Each sensor poke has a two-minute
execution timeout; RAW/preflight tasks have five minutes; dbt has fifty-five
minutes, including a five-minute parse and a forty-five-minute build subprocess
limit. The DAG run has a four-hour limit. Configuration failures fail immediately.
Subprocess cancellation/timeouts terminate the dbt process group on the Linux worker.

All dependencies require upstream success. `max_active_runs=1` prevents overlapping
runs of this DAG. Coordinate independent manual dbt executions separately. A retry
uses new artifacts, so a previous `run_results.json` cannot report false success.
dbt models rebuild their current-state relations; the entire multi-model build is
not an atomic publication. After a failure, downstream consumers must check the
last successful DAG run and may still see relations from different build attempts.

Only the three existing staging relationship test names in `EXPECTED_WARNINGS`
are accepted as nonblocking data-quality warnings. The currently documented rows are:

- Salesforce opportunity `OP_2002` references missing account `SF_1002`.
- Stripe subscription `SUB_7003` references missing customer `ST_9003`.
- Stripe payment `PAY_8003` references missing customer `ST_9003`.

This is a test-name allowlist, not a hardcoded row-count exemption: future orphans
in those relationships remain visible as warnings and in
`SILVER.int_data_quality__orphan_records`. Operators should investigate changes in
warning counts. Facts stay in Silver with the existing flags; trusted Gold excludes
them. A restored parent can legitimately reduce the number of warnings to zero.
Gold integrity and reconciliation errors, skipped required nodes, nonzero dbt exit
codes, incomplete/stale artifacts, and unexpected test warnings all block the DAG.
The guard also rejects warning severity on any other data test, including Gold.

Logs contain Fivetran IDs/states/timestamps and warning counts, RAW table validation
results, dbt node IDs/statuses/failing-row counts, and a build summary. Raw dbt
stdout/stderr and artifact error messages are deliberately not forwarded because
they can contain credentials or data. Detailed temporary dbt logs are deleted on
exit. If parsing, connection setup or SQL execution needs diagnosis beyond the
safe statuses, use an authorized interactive dbt session with the same project and
runtime profile; inspect its protected logs without publishing sensitive output.

## Validation

The tests use a real Airflow DagBag and serializer, and mock only external I/O.
They cover imports without secret/network access, exact dependencies, operational
defaults, sync freshness/failures, destination checks, RAW checks, dbt warning/error
handling, routing guards, artifact isolation and process cleanup.

From the repository root in PowerShell, with Docker Desktop running:

```powershell
$airflowSource = Join-Path (Get-Location) 'airflow'
$airflowImage = 'apache/airflow:3.3.2-python3.12'
docker run --rm --entrypoint /usr/local/bin/python `
  --mount "type=bind,source=$airflowSource,target=/opt/customer360-airflow,readonly" `
  -e HOME=/home/airflow -e PYTHONDONTWRITEBYTECODE=1 `
  -e AIRFLOW__CORE__DAGS_FOLDER=/opt/customer360-airflow/dags `
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False `
  $airflowImage -m unittest discover -s /opt/customer360-airflow/tests -v

docker run --rm --entrypoint /bin/bash `
  --mount "type=bind,source=$airflowSource,target=/opt/customer360-airflow,readonly" `
  -e HOME=/home/airflow -e PYTHONDONTWRITEBYTECODE=1 `
  -e AIRFLOW__CORE__DAGS_FOLDER=/opt/customer360-airflow/dags `
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False `
  $airflowImage -c '/home/airflow/.local/bin/airflow db migrate && /home/airflow/.local/bin/airflow dags list --local --output json'
```

The tested image already includes the pinned providers in `requirements.txt`.
For custom images, install that requirements file with the matching constraints
and rerun these checks. Validation mounts only `airflow/`, not host credentials or
the dbt profile; it does not start a scheduler or run ingestion. CLI parsing
initializes an ephemeral SQLite metadata database inside the disposable container.
Use an appropriately managed external metadata database for an actual deployment.

Run database validation separately from `dbt/`, with the existing authorized TEST
profile and repository dbt environment:

```powershell
..\.venv\Scripts\dbt.exe parse --no-partial-parse
..\.venv\Scripts\dbt.exe build --select staging intermediate marts --threads 1
```

Before first deployment, provision connections, runtime mounts and alert delivery;
verify the Fivetran connection list covers all workbooks; then perform an observed
manual run before choosing a schedule. Live Fivetran polling and email delivery
are not exercised by the automated import/structure tests.

## Integration references

- [Airflow 3.3.2 TaskFlow](https://airflow.apache.org/docs/apache-airflow/3.3.2/tutorial/taskflow.html)
- [Fivetran connection details and sync status](https://fivetran.com/docs/developer-resources/rest-api/api-reference/connections/connection-details)
- [Fivetran destination details](https://fivetran.com/docs/developer-resources/rest-api/api-reference/destinations/destination-details)
