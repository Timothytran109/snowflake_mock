# Focused follow-up review: F-04 and F-05

## Metadata

- Date: 2026-11-11 (America/Los_Angeles)
- Audit run: run-006
- Branch: `feat/airflow-orchestration`
- HEAD: `041ee2750e0043174a24c27b17ed796752a63567`
- Working tree at review start: six modified tracked files, prior untracked
  reports run-001 through run-005, and an untracked validation record. This
  report is the only persistent file created by this review.
- Prior report reviewed: [run-005](../run-005/repository-audit.md)
- Sensitive-data handling: `.env` and private-key contents were not read or
  printed. Validation used synthetic test fixtures and read-only mounts.

## Determinations

### F-04 — RESOLVED

The configured RAW connection checks database, schema, user, role, allowed
authentication settings, and the approved mounted key path before opening the
connection ([DAG lines 353–386](../../../../airflow/dags/customer360_pipeline.py#L353)).
The actual session check compares database, schema, `CURRENT_USER()` and
`CURRENT_ROLE()` before reading RAW tables ([DAG lines 389–400](../../../../airflow/dags/customer360_pipeline.py#L389)).
Compose defaults agree with the documented RAW identity and key path
([Compose lines 41–46](../../../../docker-compose.yaml#L41)).

The follow-up adds direct negative tests for all three schema cases:

- Wrong configured RAW schema: the mocked hook is asserted not to be called
  ([test lines 592–616](../../../../airflow/tests/test_customer360_pipeline.py#L592)).
- Wrong actual session schema: the test verifies failure after the identity
  query and before table reads
  ([test lines 585–590](../../../../airflow/tests/test_customer360_pipeline.py#L585)).
- Wrong dbt profile schema: `_build_dbt()` fails before the mocked dbt command
  ([test lines 863–885](../../../../airflow/tests/test_customer360_pipeline.py#L863)).

Existing wrong-user/role, wrong-database, authentication, and key-path checks
remain in the suite. No identity guard was weakened in the scoped follow-up.

### F-05 — RESOLVED (configuration-level key-pair enforcement)

The dbt profile guard requires the expected configured user and role,
`CUSTOMER360_TEST.SILVER`, approved key path and file metadata, and key-based
authentication configuration. It rejects password fields, inline keys, tokens,
OAuth and interactive authentication
([DAG lines 619–664](../../../../airflow/dags/customer360_pipeline.py#L619)).
The profile schema rejection is directly tested before subprocess execution
([test lines 863–885](../../../../airflow/tests/test_customer360_pipeline.py#L863)).
Compose's expected DBT identity/path match the README and code defaults
([Compose lines 44–46](../../../../docker-compose.yaml#L44);
[README lines 333–355](../../../../airflow/README.md#L333)).

This closes the configuration finding: a matching key path and permitted
key-auth configuration are required, and the documented password/alternate
authentication bypasses are rejected. The offline guard deliberately does not
read the key or prove its cryptographic algorithm. RSA provisioning and
algorithm, public-key registration, live key-pair authentication, secondary
roles, and effective privileges are now explicitly identified as separate
deployment checks in the README
([README lines 364–376](../../../../airflow/README.md#L364)). Those checks were
not attempted and are not blockers to closing this configuration finding.

## BLE001 annotations and exception behavior

All six `BLE001` suppressions are line-scoped; no Ruff rule was globally
disabled. Their handlers redact details while still making failures observable:

- The SMTP failure callback logs a generic delivery error and does not replace
  the original task failure ([DAG line 105](../../../../airflow/dags/customer360_pipeline.py#L105)).
- `_connection` raises a sanitized `AirflowFailException` for secret-backend or
  connection errors ([DAG line 133](../../../../airflow/dags/customer360_pipeline.py#L133)).
- RAW connection validation raises a sanitized `AirflowFailException`
  ([DAG line 383](../../../../airflow/dags/customer360_pipeline.py#L383)).
- dbt profile validation raises a sanitized `AirflowFailException`
  ([DAG line 657](../../../../airflow/dags/customer360_pipeline.py#L657)).
- `validate_raw` re-raises Airflow exceptions and converts other driver errors
  into a generic retryable `AirflowException`
  ([DAG line 723](../../../../airflow/dags/customer360_pipeline.py#L723)).
- `run_dbt` re-raises Airflow exceptions and converts other failures into a
  generic `AirflowException` ([DAG line 740](../../../../airflow/dags/customer360_pipeline.py#L740)).

The callback's notification error is intentionally suppressed so it cannot
conceal the task failure, but it is logged explicitly. The other handlers raise
explicit sanitized failures rather than silently returning success.

## Ruff and EXE002 assessment

The repository's [pyproject.toml](../../../../pyproject.toml#L15) selects
`E`, `F`, `I`, `B`, `UP`, and `SIM`; `BLE001` and `EXE002` are not globally
selected. The six justified `BLE001` sites pass when that rule is explicitly
enabled. The earlier eight-diagnostic run used no repository configuration;
the validation record's corrected project-config invocation is the relevant
baseline.

Git records both Python modules as mode `100644`, not executable. In this
Windows/Docker Desktop bind mount, both appear inside Linux containers as
`0o777`; this mount translation can trigger an unconfigured `EXE002` check.
These files are imported modules, not executable entry points. The project
configuration does not enable `EXE` rules, and the Git modes reveal no
repository executable-bit defect. No shebang or permission workaround is
warranted.

## Independent checks

Commands were run against existing images/configuration only. No image was
rebuilt, service recreated, DAG triggered, or external integration contacted.

- Network-disabled mocked DAG suite, read-only Airflow source mount:
  **43 passed**.
- `python -m unittest discover -s docker/tests -p test_compose_alert_configuration.py -v`:
  **5 passed**.
- `docker compose config --quiet`: passed without printing expanded
  configuration.
- Ruff using the read-only mounted repository `pyproject.toml`, covering the
  DAG and both Airflow/Compose test directories: **passed**.
- The same configured Ruff check with `--extend-select BLE001` on the DAG and
  Airflow tests: **passed**.
- `git diff --check`: passed; Git emitted only LF/CRLF working-copy notices.
- `git ls-files --stage -- airflow/dags/customer360_pipeline.py airflow/tests/test_customer360_pipeline.py`:
  both mode `100644`.
- A network-disabled container `stat` check of the Windows bind mounts showed
  both files as `0o777`.

The full commands and additional Codex-reported results are recorded in
[snowflake-guards-validation.txt](../../../validation/2026-11-11/snowflake-guards-validation.txt).
Its Compose, Ruff, schema-test, and 43-test claims were independently
reproduced here. The initially incorrect renamed-config attempt in that record
is not a source/configuration defect; the corrected mount retains the
`pyproject.toml` basename and passes.

## Scope, limitations, and readiness

The follow-up adds the three schema rejection tests, six local annotations,
and documentation clarifying configuration versus deployment verification.
The validation record states that prior Fivetran work was preserved rather than
changed by this follow-up. The overall uncommitted diff still contains those
broader Fivetran changes and its provider dependency; they were not audited
here and should not be included automatically with the F-04/F-05 changes.
The follow-up adds tests rather than weakening them; the suite increased from
40 to 43 passing DAG tests.

No concrete blocker remains for the scoped F-04/F-05 configuration changes.
**Scoped changes are ready to commit**, provided only the relevant hunks are
staged and the unrelated Fivetran work is separated. Live RSA provisioning,
authentication, secondary-role state, and effective privileges remain
deployment checks, not results of this offline review.
