# Customer 360 Airflow orchestration

`dags/customer360_pipeline.py` orchestrates the existing pipeline in **CUSTOMER360_TEST**:

```text
Google Drive workbooks -> existing Fivetran connections -> Snowflake RAW
prepare_fivetran_sync -> sync_fivetran [mapped per connection]
                       -> validate_raw -> run_dbt
                                           staging -> intermediate -> marts
```

The DAG uses Airflow 3.3.2 TaskFlow and starts paused, with `schedule=None` and
`catchup=False`. Set `CUSTOMER360_SCHEDULE` to a cron expression in the DAG
processor environment when a schedule is agreed. Leave it unset for manual runs.
Airflow must be the only scheduler for this dbt build; disable any duplicate dbt
schedule in another system before enabling this DAG.

## Ingestion and freshness contract

The DAG uses `airflow-provider-fivetran-async==2.4.0` and its
`FivetranOperator(deferrable=True, wait_for_completion=True)`. A small subclass
rechecks each connector's TEST destination, RAW schema, setup and paused state on
every attempt, then delegates triggering and asynchronous waiting to the provider.
The existing triggerer polls every 60 seconds; the task has a two-hour execution
and deferral timeout. RAW validation requires every mapped sync to succeed.
Missing or error completion events fail closed. No live sync is part of validation.

The guarded operator persists the provider's pre-sync completion watermark in
Airflow's deferred callback kwargs. Before releasing RAW, both immediate and
asynchronous completion recheck the destination, require an idle `scheduled`
connector with a strictly newer successful timestamp, reject a newer failure,
and reconcile any reported completion time with current API metadata. Provider
2.4.0's async path alone can report success while the connector is still syncing;
an unverified result now fails closed and uses the existing retry policy, which
can issue another force request. No additional scheduler or polling task is added.

The API identity needs permission to read connector/destination metadata and force
syncs. Configure every connector needed to populate the seven RAW tables. The
operator preserves each connector's existing manual/automatic schedule. Fivetran
owns ingestion; no direct Google Drive credentials are needed by Airflow.

Airflow retries twice with exponential delays starting at five minutes. Retrying
or clearing the sync task can issue another force request; the provider does not
guarantee exactly-once ingestion. Each attempt rechecks destination safety. A dbt
retry does not rerun successful upstream tasks. Reruns build current source state,
not historical snapshots of mutable workbooks. Independently scheduled ingestion
can still update RAW during a build; completion is not a transactional snapshot.

A full run automatically executes `dbt build --select staging intermediate marts
--threads 1`, including the exception snapshot and Gold. A separate manual dbt
command is not part of the workflow. Save/export and upload workbook changes
before requesting the run; ingestion cannot include changes that arrive after
the connector has read its source. Staging views reflect later RAW changes
immediately, while intermediate/exception/Gold tables reflect their last build.
A payment appearing in a staging view alone does not establish when it arrived
or whether it was available during that build.

Connector and destination IDs are resolved at task execution from
`CUSTOMER360_FIVETRAN_CONNECTOR_ID` and `CUSTOMER360_FIVETRAN_DESTINATION_ID`.
Recipients come from `CUSTOMER360_ALERT_EMAILS` in the ignored repository-root
`.env`, through Compose interpolation. This is the authoritative alert configuration
path; do not configure recipients in `runtime.env`. An explicit nonempty shell
override follows normal Compose precedence; an empty shell override or a missing
root value fails Compose validation rather than clearing recipients. Compose passes
the resolved values into all Airflow services. Connection extras are not required.
Missing configuration blocks ingestion. After changing
runtime environment values, recreate services with `docker compose up -d` when
ready; mocked validation does not recreate services or trigger the DAG.

## Local Docker Desktop environment

The repository-root `compose.yaml` uses `docker/airflow.Dockerfile`. There is one
Compose project, `customer360-local`, isolated from other local Airflow projects.
The official `apache/airflow:3.3.2-python3.12` image is extended with the existing
Airflow requirements and matching official constraints. The pinned
`dbt/requirements.txt` installs separately into `/opt/dbt`, including dbt-core 1.12.6 and
dbt-snowflake 1.12.5. `dbt` is available on PATH without replacing Airflow's Python.
The build context allows only the Dockerfile and dependency manifests; no source
credentials, profiles, keys, `.env` files, or repository data enter image layers.

Services:

- `postgres`: private PostgreSQL 16.10 metadata database with persistent storage and a readiness check; no host port.
- `airflow-init`: one-shot, repeatable metadata migration after PostgreSQL becomes healthy.
- `airflow-api-server`: UI/API on **http://localhost:8081**, bound only to `127.0.0.1`, with an HTTP health check.
- `airflow-scheduler`: LocalExecutor scheduler/task runner, started after initialization and API health, with scheduler heartbeat health checks.
- `airflow-dag-processor`: separate Airflow 3 parser process with a DagProcessorJob health check.
- `airflow-triggerer`: runs `airflow triggerer` for deferrable operators using the shared image, environment, DAG and secret mounts, with a TriggererJob health check.

Airflow services run as UID 50000, not root. LocalExecutor needs neither Redis nor
a Celery worker. The triggerer is an Airflow process, not a separate pip package;
`airflow-provider-fivetran-async==2.4.0` remains installed for async integration.
The SimpleAuthManager is for local development only; this is not a production
deployment. Do not expose its port publicly. The official Airflow documentation
describes [SimpleAuthManager's development scope](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html).

### Startup from Windows PowerShell

Use Docker Desktop in Linux-container mode and current Docker Compose (2.24 or
newer for optional environment files). Allocate sufficient memory for the stack
and any other running containers; 8 GB for Docker is a useful starting point.
Run all commands from the repository root. First configure `CUSTOMER360_ALERT_EMAILS`
privately in the ignored root `.env`; Compose requires a nonempty recipient setting
even when building or starting a local stack. Integration credentials are still
unnecessary for mocked tests and local initialization:

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

No host exports are needed when the root `.env` supplies `CUSTOMER360_ALERT_EMAILS`.
Keep that setting out of `.local/customer360/runtime.env`: values in that file
cannot satisfy or override the authoritative Compose setting. If recipients are
configured in both files, the root `.env` wins; an explicit nonempty shell override
is supported for deployment, while empty overrides fail closed. Missing or empty
authoritative recipients make `docker compose config` fail with a variable-name-only
instruction. Whitespace/comma-only lists are additionally rejected by DAG preflight
before any Fivetran call. The failure callback does not attempt SMTP for such lists.

Other optional runtime variables, including profile `env_var()` values, can use the
ignored `.local/customer360/runtime.env` (standard `NAME=value` format). All Airflow
services load that file at container creation; recreate services only when ready to
apply runtime changes. Avoid plain `docker compose config` when runtime
secrets are present because it can display expanded environment values; use
`--quiet` for validation. Do not print `docker inspect` environment payloads.

### Mounted paths and manual credentials

The Airflow services share these read-only bind mounts:

- `airflow/dags` -> `/opt/airflow/dags`
- `airflow/tests` -> `/opt/airflow/tests`
- `dbt` -> `/opt/customer360/dbt`
- `.local/customer360/dbt` (or `CUSTOMER360_DBT_PROFILES_HOST_DIR`) -> `/opt/airflow/secrets/dbt`
- `.local/customer360/keys` (or `CUSTOMER360_KEYS_HOST_DIR`) -> `/opt/airflow/secrets/keys`
- `keys/snowflake` -> `/opt/airflow/secrets/snowflake`

The Snowflake key directory must exist before starting the containers. All Airflow
services inherit this read-only mount, including the LocalExecutor scheduler.
Reference `/opt/airflow/secrets/snowflake/<filename>` for keys in that directory.
Keys are mounted at runtime and are never copied into the image.

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
Use profile `customer360`, target `dev`, type `snowflake`, `CUSTOMER360_TEST.SILVER`,
user `SVC_DBT_TEST` and role `C360_DBT_ROLE`. Key-pair authentication configuration
is required: the default dbt `private_key_path` is
`/opt/airflow/secrets/snowflake/dbt_rsa_key.p8`, never a Windows host path. A different
service identity, transformation role or mounted path requires deployment-owner
approval and matching changes to the expected `CUSTOMER360_DBT_USER`,
`CUSTOMER360_DBT_ROLE` or `CUSTOMER360_DBT_PRIVATE_KEY_PATH` policy and the profile.
Changing only the profile to another dedicated role is rejected; `ACCOUNTADMIN`
is always rejected. See the approved TEST deployment configuration below.
Use runtime secrets for key passphrases. The DAG checks identities, routing,
authentication configuration and mounted-key metadata; RSA provisioning,
public-key registration, live authentication and effective privileges are separate
deployment checks. Azure credentials are not needed by this local setup; a future
secret backend must be configured separately.

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

Dependency inputs are UTF-8 text with distinct ownership:

- `airflow/requirements.txt`: Airflow 3.3.2 and providers, installed with the official
  Python 3.12 constraints into the image's Airflow Python environment.
- `dbt/requirements.txt`: dbt-core 1.12.6 and dbt-snowflake 1.12.5 only, installed in
  `/opt/dbt` without system site packages. The root `requirements.txt` forwards to
  this file for existing local setup commands.
- `requirements-dev.txt`: pinned pytest and Ruff for development only. The optional
  Docker `development` target installs them into `/opt/customer360/dev`, inheriting
  Airflow's site packages for DAG testing. They are absent from the final runtime
  target and from `/opt/dbt`.

Runtime manifests contain direct dependencies rather than a local `pip freeze`.
pip resolves dbt's transitive dependencies during installation; they are not a
complete lockfile. Airflow retains its matching official constraints. Never install
the combined contents of a developer environment into `/opt/dbt`.

For local dbt commands in PowerShell:

```powershell
py -3.12 -m venv .venv312
.\.venv312\Scripts\python.exe -m pip install -r dbt/requirements.txt
```

Builds may download the base image, Airflow constraints and Python dependencies
from registries and package indexes unless cached. This applies to both the runtime
and optional developer image. Build the developer image first, then run the mocked
DAG tests in a separate container with networking disabled and no credentials or
profiles mounted:

```powershell
docker build -f docker/airflow.Dockerfile --target development -t customer360-airflow-dev:3.3.2-python3.12 .
docker run --rm --network none --entrypoint /opt/customer360/dev/bin/python --mount "type=bind,source=$($PWD.Path)/airflow,target=/opt/airflow/source,readonly" customer360-airflow-dev:3.3.2-python3.12 -m pytest /opt/airflow/source/tests/test_customer360_pipeline.py -q -p no:cacheprovider
```

`docker compose build` builds the final runtime target without the developer
environment. The Dockerfile runs `pip check` in both runtime environments and
fails the build if Airflow is discoverable from `/opt/dbt`. Container tests also
reject pytest and Ruff in that isolated environment.

Deploy the same dbt project revision and dependencies to every worker. The DAG
uses these environment variables (defaults shown):

- `CUSTOMER360_DBT_PROJECT_DIR=/opt/customer360/dbt`: absolute directory containing `dbt_project.yml`.
- `CUSTOMER360_DBT_EXECUTABLE=/opt/dbt/bin/dbt`: executable in the isolated dbt environment.
- `CUSTOMER360_DBT_PROFILES_DIR=/opt/airflow/secrets/dbt`: absolute directory containing a runtime-mounted `profiles.yml`.
- `CUSTOMER360_ALERT_EMAILS`: required comma-separated operational recipients in the ignored root `.env`; Compose supplies workers and callback processors. Missing/empty values fail Compose validation.
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

Use the existing Fivetran connection: login is the API key and password is its secret.
The read-only destination guard uses a fixed HTTPS origin with redirects disabled;
the installed provider owns the trigger and asynchronous polling calls.
Set `CUSTOMER360_FIVETRAN_CONNECTOR_ID` and
`CUSTOMER360_FIVETRAN_DESTINATION_ID` in the ignored root `.env`; do not put IDs or
credentials in Python. The destination ID is the Fivetran group ID. The existing
Airflow connection supplies API authentication only. File connectors may report
`raw.table_name`; that form is supported. Destination, setup and paused-state
guards still apply before each trigger attempt. Task logs omit configured IDs;
runtime values are registered with Airflow secret masking before provider calls.

### `customer360_snowflake_raw`

Use connection type `snowflake`, schema `RAW`, and extra fields `database` set to
`CUSTOMER360_TEST`, plus the appropriate `account` and `warehouse`. Login must be
`SVC_AIRFLOW_TEST`; the `role` extra must be `C360_AIRFLOW_ROLE`. Configure
`private_key_file` as `/opt/airflow/secrets/snowflake/airflow_rsa_key.p8` and use
`authenticator=snowflake_jwt` (or the provider's default Snowflake authenticator
with that key). The file must match the approved deployment path, exist, be
nonempty and readable. Inline keys, token/OAuth and browser/interactive mechanisms
are rejected. The connection password field, if needed, is only the encrypted
key's passphrase; password-only authentication cannot pass preflight.

Connection preflight validates identity, routing and key configuration before
opening Snowflake. After connection, `CURRENT_USER()` and `CURRENT_ROLE()` must
match the approved service identity before any RAW table scan. No administrative
role or other read-only role can substitute in the primary-role identity check.
`CURRENT_ROLE()` identifies the primary role; it does not establish effective
privileges. The DAG does not disable or inspect secondary roles. Before deployment,
the owner must verify the service users' secondary-role defaults/session behavior,
role inheritance, PUBLIC access and object grants, then approve a least-privilege
policy for both RAW and dbt. This verification remains outstanding. If the policy
requires only the named role to contribute privileges, the deployment must enforce
that secondary-role policy and verify it in an authorized live session.

The role needs warehouse USAGE, database/schema USAGE and SELECT on these tables:

- `CUSTOMER360_TEST.RAW.MOCK_SALESFORCE_XLSM_ACCOUNTS`
- `CUSTOMER360_TEST.RAW.MOCK_SALESFORCE_XLSM_OPPORTUNITIES`
- `CUSTOMER360_TEST.RAW.MOCK_ADOBE_XLSM_EVENTS`
- `CUSTOMER360_TEST.RAW.MOCK_ADOBE_XLSM_CAMPAIGNS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_XLSM_CUSTOMERS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_XLSM_SUBSCRIPTIONS`
- `CUSTOMER360_TEST.RAW.MOCK_STRIPE_XLSM_PAYMENTS`

These are the active XLSM workbook tables, matching the explicit physical
identifiers in `dbt/models/staging/_sources.yml`. Legacy non-XLSM tables are not
required, queried or used as fallbacks. The dbt logical source names stay unchanged.

After all provider sync tasks succeed, validation verifies the actual Snowflake session
database, schema, service user and exact role. It checks table visibility through
`information_schema.tables` and uses `select 1 ... limit 1` per table to verify
nonempty data without logging business records. Missing or empty tables block dbt.
This preflight does not measure freshness or reconcile row counts; see the
[staging source documentation](../dbt/models/staging/README.md) for those checks.
No grants or source modifications are performed by the DAG.

### dbt runtime profile

The separately mounted profile must contain `customer360.outputs.dev`, using
type `snowflake`, database `CUSTOMER360_TEST`, schema `SILVER`, user `SVC_DBT_TEST`
and role `C360_DBT_ROLE`. Its `private_key_path` must match the approved deployment
path `/opt/airflow/secrets/snowflake/dbt_rsa_key.p8`, and the mounted file must exist,
be nonempty and readable. Identity, routing and key-path fields can use dbt
`env_var()` expressions. The allowed authenticator values are `snowflake_jwt` or
`snowflake`; omission uses the connector's key-based default. Password fields,
inline private keys, tokens/OAuth and browser/interactive authenticators are
rejected, even if a key is also present. Encrypted keys may use the dbt-specific
`private_key_passphrase` field, delivered through an approved runtime secret.
The guard never renders a passphrase or reads key contents.
The existing project macro routes marts to `CUSTOMER360_TEST.GOLD`.

### Approved TEST service deployment configuration

The shared Compose environment defines these non-secret expectations with TEST
defaults. The DAG resolves the same defaults at task execution outside Compose:

- `CUSTOMER360_RAW_USER=SVC_AIRFLOW_TEST`
- `CUSTOMER360_RAW_ROLE=C360_AIRFLOW_ROLE`
- `CUSTOMER360_RAW_PRIVATE_KEY_PATH=/opt/airflow/secrets/snowflake/airflow_rsa_key.p8`
- `CUSTOMER360_DBT_USER=SVC_DBT_TEST`
- `CUSTOMER360_DBT_ROLE=C360_DBT_ROLE`
- `CUSTOMER360_DBT_PRIVATE_KEY_PATH=/opt/airflow/secrets/snowflake/dbt_rsa_key.p8`

Only deployment owners should change these expected identities or approved paths,
after review, through the ignored root `.env`/deployment environment. An override
must still be a valid service identifier and non-admin role; it does not relax
the fixed `CUSTOMER360_TEST.RAW` and `CUSTOMER360_TEST.SILVER` guards. Matching the
expected identity in both the policy and the Connection/profile is mandatory.
Changing a Connection alone does not change its approved identity.

The DAG enforces approved service identities, routing, authentication configuration
and mounted-key metadata (approved path, existence, nonempty file and readability).
It never reads private-key contents. Deployment owners must separately verify:

- RSA key provisioning and algorithm, protected key storage, and matching public-key
  registration for the approved service users.
- Live key-pair authentication and actual identity through an explicitly authorized
  deployment test.
- Effective privileges, including role inheritance, PUBLIC grants and secondary-role
  state for both service users; a matching primary role alone does not prove least
  privilege.

Mount private keys read-only; do not copy them into images or tracked profiles.
No key rotation, Connection edit, profile edit, live authentication or effective-
privilege check is performed by offline validation. These deployment checks remain
outstanding and are not certified by the mocked tests or profile metadata guard.

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
appropriate TLS settings and `from_email` extra. Set `CUSTOMER360_ALERT_EMAILS`
privately in the ignored root `.env` to operational recipients. Do not put this
setting in `runtime.env`. Compose rejects missing/empty values, and DAG preflight
rejects empty parsed recipient lists before ingestion.
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

Only seven exact test names in `EXPECTED_WARNINGS` are accepted as nonblocking
data-quality warnings: the three historical missing-parent staging relationships,
the payment-to-subscription relationship
`relationships_stg_stripe__payments_subscription_id__subscription_id__ref_stg_stripe__subscriptions_`, and these
three staging date checks:

- `not_null_stg_adobe__events_event_timestamp`
- `not_null_stg_stripe__customers_created_at`
- `not_null_stg_salesforce__accounts_updated_at`

Historical missing-parent observations were:

- Salesforce opportunity `OP_2002` references missing account `SF_1002`.
- Stripe subscription `SUB_7003` references missing customer `ST_9003`.
- Stripe payment `PAY_8003` references missing customer `ST_9003`.

This is a test-name allowlist, not a hardcoded row-count exemption: affected rows
in those relationships or date checks remain visible as warnings and in
`SILVER.int_data_quality__orphan_records`. Operators should investigate changes in
warning counts. All records stay in Silver. Gold excludes relationship orphans;
missing dates alone do not invalidate customers/accounts as parents or exclude
facts from date-independent metrics. The exception table retains its historical
name, but now records every applicable relationship or missing-date reason,
with deterministic IDs including the reason. Exception row counts can exceed
affected-record counts. A corrected date or restored parent can legitimately
reduce warnings to zero after a complete rebuild. See the
[exception policy](../dbt/models/intermediate/README.md).
Gold integrity and reconciliation errors, skipped required nodes, nonzero dbt exit
codes, incomplete/stale artifacts, and unexpected test warnings all block the DAG.
The guard also rejects warning severity on any other data test, including Gold.
Even an approved test name does not exempt an error, failure or skipped result.

The missing-subscription warning lets intermediate models and the exception
snapshot rebuild in the same `staging intermediate marts` build. The payment
stays in Silver, receives a `missing_subscription` exception, and stays out of
Gold until a valid subscription for the same customer is restored and rebuilt.
Gold relationship and reconciliation tests retain error severity.

Logs contain Fivetran HTTP status, destination ID, bounded API error code/message,
destination setup status and setup-test statuses. They never include the complete
API response. Logs also contain connector trigger/completion status, RAW validation
results, the exact dbt argument list/project directory and fixed profile/target,
dbt node IDs/statuses/failing-row counts, and a build summary. For each model,
safe logs include the target relation, numeric adapter `rows_affected` and
execution timestamps; asynchronous sync completion also logs the provider's
completion timestamp when valid. Adapter counts are not payment-level evidence:
view DDL can report one affected row regardless of source cardinality. Raw dbt
stdout/stderr and artifact error messages are deliberately not forwarded because
they can contain credentials or data. Detailed temporary dbt logs are deleted on
exit. If parsing, connection setup or SQL execution needs diagnosis beyond the
safe statuses, use an authorized interactive dbt session with the same project and
runtime profile; inspect its protected logs without publishing sensitive output.

For a refresh discrepancy, compare one specific run's sync-completion timestamp,
`run_dbt` execution timestamps and model statuses with the source change/arrival
time. Confirm that intermediate, exceptions and Gold have successful results,
and check the payment's relationship eligibility separately. A green DAG label
alone is insufficient if retained task records/logs disagree. Compare the actual
account, database, schema, role and user of local and container builds; matching
profile names do not imply the same authenticated identity or row visibility.
Do not change credentials or remove guards to reconcile a difference. Protected
query history/source-arrival evidence may be needed to establish the historical
cause; current mounted code cannot establish past source availability.

## Validation

The tests use a real Airflow DagBag and serializer, and mock only external I/O.
They cover imports without secret/network access, exact dependencies, operational
defaults, provider triggering/deferral/timeouts/retries, destination response shapes,
RAW checks, dbt warning/error handling, routing guards, artifact isolation and
process cleanup.

Run Ruff with this repository's `pyproject.toml` explicitly mounted. Otherwise the
Airflow development image can discover its own parent configuration, producing
different diagnostics. Windows bind mounts can expose these modules as mode
`0777`, although Git stores them as non-executable `100644` Python modules; that
does not make them standalone executable scripts requiring shebangs.

```powershell
docker run --rm --network none --entrypoint /opt/customer360/dev/bin/ruff --mount "type=bind,source=$($PWD.Path)/airflow,target=/opt/airflow/source,readonly" --mount "type=bind,source=$($PWD.Path)/pyproject.toml,target=/opt/customer360/pyproject.toml,readonly" customer360-airflow-dev:3.3.2-python3.12 check --no-cache --config /opt/customer360/pyproject.toml /opt/airflow/source/dags/customer360_pipeline.py /opt/airflow/source/tests/test_customer360_pipeline.py
```

The six outer exception boundaries deliberately redact arbitrary SDK, driver,
profile/template and subprocess/artifact errors, or preserve the original task
failure if notification fails. Each has a reasoned, line-scoped `BLE001` annotation;
no rule is globally disabled. Adding `--extend-select BLE001` to the command above
also checks those documented exceptions explicitly. Existing `AirflowException`
handling still preserves intentional task failure and retry behavior.

Alert configuration precedence tests evaluate the real Compose file against
temporary dotenv files containing synthetic addresses only. They do not load the
repository's private `.env`/`runtime.env`, start services, or call integrations:

```powershell
python -m unittest discover -s docker/tests -p test_compose_alert_configuration.py -v
```

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

### Dependency validation without restarting the stack

The following checks use newly built images in temporary containers. Build commands
may use networking; test containers use `--network none`. They do not start Airflow
processes, recreate services, or contact Snowflake, Fivetran or SMTP. Run from the
repository root in PowerShell, with the existing scheduler container present:

```powershell
docker compose config --quiet
docker compose build airflow-scheduler
docker build -f docker/airflow.Dockerfile --target development -t customer360-airflow-dev:3.3.2-python3.12 .

# DAG imports, provider behavior and serialization with all external calls mocked.
docker run --rm --network none --entrypoint /opt/customer360/dev/bin/python --mount "type=bind,source=$($PWD.Path)/airflow,target=/opt/airflow/source,readonly" customer360-airflow-dev:3.3.2-python3.12 -m pytest /opt/airflow/source/tests/test_customer360_pipeline.py -q -p no:cacheprovider

# Both runtime environments must have consistent installed dependencies.
docker run --rm --network none --entrypoint python customer360-airflow:3.3.2-python3.12 -m pip check
docker run --rm --network none --entrypoint /opt/dbt/bin/python customer360-airflow:3.3.2-python3.12 -m pip check

# Six container checks: pins, isolation, pip check, mounts, defaults and dbt parse.
# Reuse only read-only mounts, not the scheduler's environment or running processes.
$customer360Scheduler = docker compose ps -q airflow-scheduler
if (-not $customer360Scheduler) { throw 'An existing scheduler container is required for mount validation.' }
docker run --rm --network none --volumes-from "${customer360Scheduler}:ro" --entrypoint python -e PYTHONPATH=/opt/airflow/tests -e CUSTOMER360_SCHEDULE= -e CUSTOMER360_DBT_PROJECT_DIR=/opt/customer360/dbt -e AIRFLOW__CORE__EXECUTOR=LocalExecutor -e AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true customer360-airflow:3.3.2-python3.12 -m unittest discover -s /opt/airflow/tests -p test_container_runtime.py -k version -k isolated -k consistent_dependencies -k read_only -k non_root -k parses -v
```

The container checks verify `/usr/local/bin/dbt` resolves to `/opt/dbt/bin/dbt`,
dbt Core/adapter versions match their pins, and Airflow, pytest and Ruff are absent
from `/opt/dbt`. Parsing uses a temporary dummy profile and cannot authenticate to
Snowflake. The live metadata pause-state test is outside these dependency checks;
it remains part of the full stack suite above. No operator pause state is changed.
Running services retain their current image until explicitly recreated, even when
the same image tag has been rebuilt.

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
