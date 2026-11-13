# Focused review: F-03 alert-recipient configuration

## Metadata

- Date: 2026-11-11 (America/Los_Angeles)
- Audit run: run-004
- Branch: `feat/airflow-orchestration`
- HEAD: `17556262dce28b8f068ba0b8f27e04ec836b6757`
- Governing instructions: [AGENTS.md](../../../../AGENTS.md)
- Existing audits reviewed: [run-001](../run-001/repository-audit.md),
  [run-002](../run-002/repository-audit.md), and
  [run-003](../run-003/repository-audit.md)
- F-03 determination: **RESOLVED**

## Working-tree status and scope

At review time, `git status --short --untracked-files=all` showed:

- Modified: `airflow/README.md`, `airflow/dags/customer360_pipeline.py`,
  `airflow/requirements.txt`, `airflow/tests/test_customer360_pipeline.py`,
  and `docker-compose.yaml`.
- Untracked: `docker/tests/test_compose_alert_configuration.py` and the
  previously existing audit reports for run-001, run-002, and run-003.
- The root `.env` exists and is ignored by Git. Its contents and the
  `.local/customer360/runtime.env` contents were not read or printed.

Reviewed the F-03 working-tree diff and
[docker-compose.yaml](../../../../docker-compose.yaml),
[airflow/dags/customer360_pipeline.py](../../../../airflow/dags/customer360_pipeline.py),
[airflow/tests/test_customer360_pipeline.py](../../../../airflow/tests/test_customer360_pipeline.py),
[docker/tests/test_compose_alert_configuration.py](../../../../docker/tests/test_compose_alert_configuration.py),
[airflow/README.md](../../../../airflow/README.md),
[docker/Initialize-LocalAirflow.ps1](../../../../docker/Initialize-LocalAirflow.ps1),
the root ignore rules, and the validation-record inventory. No F-03-specific
execution result appears in
[dependency-validation.txt](../../../validation/2026-11-11/dependency-validation.txt);
that record covers the earlier dependency work.

## F-03 — RESOLVED

The Compose environment now requires a nonempty interpolation value for
`CUSTOMER360_ALERT_EMAILS`, with a variable-name-only error if the value is
missing or empty. The repository-root `.env` is documented as the authoritative
default source; the former `runtime.env` path is explicitly excluded for
recipients. An explicit nonempty shell override is documented and tested as an
intentional Compose override, while an empty shell override fails closed.

The required value is part of the shared Airflow environment anchor. The
Compose test checks that the resolved value reaches all five Airflow services:
`airflow-init`, `airflow-api-server`, `airflow-scheduler`,
`airflow-dag-processor`, and `airflow-triggerer`.

The DAG parses the comma-separated runtime value, trims entries, and rejects an
empty parsed list in `prepare_fivetran_sync` before it calls `_connection`.
`AirflowFailException` identifies the missing configuration. The failure
callback also logs an explicit configuration error and returns without
calling SMTP when no recipient remains.

## Evidence and test safety

### Checks independently executed

- `docker compose config --quiet` — passed against the working-tree Compose
  file. This quiet mode did not print expanded configuration.
- `python -m unittest discover -s docker/tests -p test_compose_alert_configuration.py -v`
  — **4 passed**. These tests invoke the real Compose CLI with temporary,
  isolated dotenv fixtures containing synthetic `.invalid` values. They cover
  root dotenv precedence over `runtime.env`, an intentional nonempty shell
  override, rejection of an empty shell override, and rejection when only the
  runtime file supplies a value. Config output is captured and asserted in
  memory, not printed.
- A separate temporary-project check copied the Compose file, supplied a
  synthetic project `.env` and conflicting synthetic `runtime.env`, and ran
  `docker compose config --format json` with host `CUSTOMER360_*` and
  `COMPOSE_*` variables removed. It confirmed the default project `.env`
  mechanism supplies the value to all five Airflow services. The JSON was
  inspected in memory only; no expanded configuration or address was printed.
- The relevant Airflow alert tests in the existing development image ran with
  `--network none`, a read-only mount of the Airflow source, and no credentials
  or runtime dotenv files:
  `docker run --rm --network none --entrypoint /opt/customer360/dev/bin/python --mount "type=bind,source=<repo>\airflow,target=/opt/airflow/source,readonly" customer360-airflow-dev:3.3.2-python3.12 -m pytest /opt/airflow/source/tests/test_customer360_pipeline.py -q -p no:cacheprovider -k 'failure_notification or notification_delivery_error or missing_recipients or configured_recipients'`
  — **5 passed**.
- The full mocked Airflow DAG suite ran with the same network-disabled,
  read-only source mount — **31 passed**.
- `git check-ignore -q -- .env` confirmed the existing root `.env` is ignored;
  only its presence and ignore status were checked.
- `git diff --check` — passed; Git emitted only an LF/CRLF working-copy notice
  for `airflow/requirements.txt`.

The Compose tests use only synthetic recipients and never start services or
send email. The Airflow tests mock `SmtpHook`; the missing-recipient callback
test asserts it is not called, and preflight tests mock `_connection` to verify
that a missing recipient blocks before Fivetran setup. The network-disabled
test run could not contact SMTP, Snowflake, or Fivetran.

### Codex-reported or unavailable evidence

- No F-03-specific Codex validation log or CI artifact was found in the
  inspected validation record. The test files alone are not evidence of prior
  execution; the results above are from this review.
- Live service environment values were not inspected, and services were not
  recreated. Runtime behavior in the already-running stack is therefore **NOT
  VERIFIED**. The synthetic Compose tests exercise the real Compose file and
  verify the relevant service environment without exposing actual values.
- No SMTP delivery, DAG trigger, or external integration test was run.

## Documentation, regression, and scope review

- The README consistently identifies the ignored repository-root `.env` as the
  alert source, says not to put recipients in `runtime.env`, documents the
  explicit shell override and fail-closed empty behavior, and describes the
  DAG preflight. The initializer creates an optional `runtime.env` without
  copying or displaying recipient values.
- No actual recipient address, credential, or full environment-file content
  was included in this report. The synthetic tests use reserved `.invalid`
  addresses only.
- The current working-tree diff is broader than F-03: it also adds the
  Fivetran async provider and contains substantial Fivetran DAG, test, and
  README changes. These are outside the alert-precedence fix; the diff alone
  does not establish whether they were introduced with this fix or are
  pre-existing branch work. Do not stage the entire dirty worktree as an F-03
  change.
- The README still refers to a root `compose.yaml` in one section while the
  repository file is `docker-compose.yaml`, a previously recorded
  documentation mismatch unrelated to F-03.
- No secret files or generated test artifacts appeared in the working-tree
  status after validation. No image was rebuilt and no Compose service was
  started or recreated; only disposable test containers were run. No
  implementation file was modified during this review.

## Commit readiness and recommended next action

The **F-03-specific fix is ready to commit** based on the independently passing
Compose-precedence and mocked Airflow tests. Stage only the Compose
recipient-interpolation change, the recipient guard/tests, the synthetic
Compose test, and corresponding README hunks. Review and separate the broader
Fivetran/provider changes rather than committing the whole worktree together.
No live-stack check is needed to establish the Compose precedence fix; any
future SMTP delivery validation should use an explicitly authorized test
connection and synthetic/non-production recipients.
