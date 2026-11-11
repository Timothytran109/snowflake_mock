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

## Local Docker Desktop environment

The repository-root `compose.yaml` uses `docker/airflow.Dockerfile`. There is one
Compose project, `customer360-local`, isolated from other local Airflow projects.
The official `apache/airflow:3.3.2-python3.12` image is extended with the existing
Airflow requirements and matching official constraints. The root pinned dbt
requirements install separately into `/opt/dbt`, including dbt-core 1.12.6 and
dbt-snowflake 1.12.5. `dbt` is available on PATH without replacing Airflow's Python.
The build context allows only the Dockerfile and dependency manifests; no source
credentials, profiles, keys, `.env` files, or repository data enter image layers.

Services:

- `postgres`: private PostgreSQL 16.10 metadata database with persistent storage and a readiness check; no host port.
- `airflow-init`: one-shot, repeatable metadata migration after PostgreSQL becomes healthy.
- `airflow-api-server`: UI/API on **http://localhost:8081**, bound only to `127.0.0.1`, with an HTTP health check.
- `airflow-scheduler`: LocalExecutor scheduler/task runner, started after initialization and API health, with scheduler heartbeat health checks.
- `airflow-dag-processor`: separate Airflow 3 parser process with a DagProcessorJob health check.

Airflow services run as UID 50000, not root. LocalExecutor needs neither Redis nor
a Celery worker. The current reschedule sensors do not require a triggerer.
The SimpleAuthManager is for local development only; this is not a production
deployment. Do not expose its port publicly. The official Airflow documentation
describes [SimpleAuthManager's development scope](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html).

### Startup from Windows PowerShell

Use Docker Desktop in Linux-container mode and current Docker Compose (2.24 or
newer for optional environment files). Allocate sufficient memory for the stack
and any other running containers; 8 GB for Docker is a useful starting point.
Run all commands from the repository root:

```powershell
Set-Location C:\Projects\work_projects\mock_project
.\docker\Initialize-LocalAirflow.ps1
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 300
docker compose ps --all
```

The initializer generates cryptographically random local PostgreSQL, Fernet,
JWT/API and UI passwords under `.local/customer360/secrets/`. It restricts the
local directory's Windows ACL to the current user, SYSTEM and Administrators;
preserves existing nonempty secrets on reruns; and never prints their values.
It also creates empty dbt/key directories and an optional `runtime.env`. It does
not read or copy your existing Snowflake/dbt credentials. Docker Compose mounts
the infrastructure files as secrets; they are not interpolated into Compose YAML.
These local secret files still need host disk protection: Compose file secrets
are mounts, not an encrypted vault. Do not regenerate them while reusing the
metadata volume, especially the PostgreSQL password and Fernet key.
The API server copies the pre-generated UI password into a private writable file
at startup because SimpleAuthManager opens it for append. The source secret stays
read-only, and no generated password needs to appear in startup logs.

Open the UI at http://localhost:8081 and sign in as `localadmin`. Privately open
`.local/customer360/secrets/simple_auth_passwords.json` for the generated password;
do not paste it into logs or chat. No integration credentials are required just
to start, parse or test this stack. The DAG remains paused and **unscheduled**.
Compose explicitly sets `CUSTOMER360_SCHEDULE` to an empty string; a host variable
cannot accidentally schedule it. Existing metadata preserves operator pause state
across restarts, so startup does not forcibly re-pause a previously unpaused DAG.

Optional PowerShell variables before starting Compose:

```powershell
# Only change these when needed. Use absolute Windows paths for custom mounts.
$env:CUSTOMER360_AIRFLOW_PORT = '8081'
# $env:CUSTOMER360_DBT_PROFILES_HOST_DIR = 'C:\private\customer360\dbt'
# $env:CUSTOMER360_KEYS_HOST_DIR = 'C:\private\customer360\keys'
```

There are no required host environment variables for an unconfigured local stack.
`CUSTOMER360_ALERT_EMAILS` and any profile `env_var()` values are required only
before running the pipeline. Put them in the ignored
`.local/customer360/runtime.env` (standard `NAME=value` format). All Airflow
services load that file at container creation; use `docker compose up -d
--force-recreate` after changes. Avoid plain `docker compose config` when runtime
secrets are present because it can display expanded environment values; use
`--quiet` for validation. Do not print `docker inspect` environment payloads.

### Mounted paths and manual credentials

The Airflow services share these read-only bind mounts:

- `airflow/dags` -> `/opt/airflow/dags`
- `airflow/tests` -> `/opt/airflow/tests`
- `dbt` -> `/opt/customer360/dbt`
- `.local/customer360/dbt` (or `CUSTOMER360_DBT_PROFILES_HOST_DIR`) -> `/opt/airflow/secrets/dbt`
- `.local/customer360/keys` (or `CUSTOMER360_KEYS_HOST_DIR`) -> `/opt/airflow/secrets/keys`

Named volumes `customer360-local_postgres-data` and
`customer360-local_airflow-logs` retain metadata and shared task logs. dbt writes
ad-hoc CLI artifacts into `/tmp`; the DAG creates a private directory per attempt.
No Docker socket or whole home directory is mounted. The default profiles and key
directories are deliberately empty, so an accidental pipeline run cannot reach
Snowflake using an existing host profile.

Before an intentional pipeline run, manually configure the three Airflow
Connections documented below: `customer360_fivetran`, `customer360_snowflake_raw`,
and `customer360_smtp`. Use the UI to avoid putting passwords into command history.
Provide the Fivetran API key/secret and existing connection/destination IDs,
Snowflake service-user authentication, warehouse/account and dedicated non-admin
role, plus SMTP authentication and sender details. The UI stores connection
secrets encrypted with the mounted Fernet key.

Place a Linux-compatible `profiles.yml` in the selected dbt profile directory.
Use profile `customer360`, target `dev`, `CUSTOMER360_TEST`, `SILVER`, and
`C360_DBT_ROLE` (or another dedicated transformation role), never `ACCOUNTADMIN`.
If using key authentication, place the key in the selected key directory and
reference `/opt/airflow/secrets/keys/<filename>` in the profile/connection, not a
Windows host path. Use runtime `env_var()` values for sensitive profile fields
where practical. The existing DAG's profile and manifest guards still reject
production destinations and administrative roles. Azure credentials are not needed
by this local setup; a future secret backend must be configured separately.

### Shutdown and restart without removing data

```powershell
docker compose stop
# Resume the same containers and stored metadata:
docker compose start
# Alternatively remove this project's containers/network, retaining named volumes:
docker compose down
# Recreate later:
docker compose up -d --wait --wait-timeout 300
```

Do not add `--volumes`/`-v`, run volume prune, or delete the local secret files.
The setup and validation scripts never remove volumes or modify other projects.

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

From the repository root in PowerShell, with Docker Desktop running, initialize
once and use the reusable validation script. It builds when `-Build` is supplied,
starts/checks services, verifies CLI import results, runs the DAG and container
tests, compares the mounted DAG's SHA256 with the host file, and reads scheduler
logs. It does not trigger the pipeline or test external credentials.

```powershell
.\docker\Initialize-LocalAirflow.ps1
.\docker\Validate-LocalAirflow.ps1 -Build

# Individual checks after startup:
docker compose config --quiet
docker compose ps --all
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler dbt --version
docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v
docker compose logs --no-color --tail 60 airflow-scheduler
```

The container tests require the custom image and Compose mounts; running them
against an unextended base image will fail the dbt availability checks. Provider
availability and versions, read-only mounts, non-root execution, default schedule,
and runtime secret-file presence are checked without displaying secret values.
The Dockerfile also runs `pip check` in both Python environments during the build.

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
- [Airflow Docker environment guidance](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html)
- [Extending the official Airflow image](https://airflow.apache.org/docs/docker-stack/build.html)
- [Fivetran connection details and sync status](https://fivetran.com/docs/developer-resources/rest-api/api-reference/connections/connection-details)
- [Fivetran destination details](https://fivetran.com/docs/developer-resources/rest-api/api-reference/destinations/destination-details)
