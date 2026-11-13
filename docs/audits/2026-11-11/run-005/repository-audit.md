# Focused review: F-04 and F-05 Snowflake guards

## Metadata

- Date: 2026-11-11 (America/Los_Angeles)
- Audit run: run-005
- Branch: `feat/airflow-orchestration`
- HEAD: `041ee2750e0043174a24c27b17ed796752a63567`
- Working tree at review start: six modified tracked files and four existing
  untracked audit reports for runs 001–004. This report is the only additional
  persistent file created by the review; no implementation files were changed.
- Prior reports reviewed: [run-001](../run-001/repository-audit.md),
  [run-002](../run-002/repository-audit.md),
  [run-003](../run-003/repository-audit.md), and
  [run-004](../run-004/repository-audit.md).
- Sensitive-data handling: no `.env` or private-key contents were printed. The
  existing profile and key were mounted read-only for a guard check; output was
  limited to a generic pass result. No actual recipient addresses or credentials
  are included here.

## Scope reviewed

Read `AGENTS.md` and the audit history, then reviewed the working-tree diff
relative to the F-03 commit and the relevant code, tests, Compose defaults, and
documentation:

- [customer360_pipeline.py](../../../../airflow/dags/customer360_pipeline.py)
- [test_customer360_pipeline.py](../../../../airflow/tests/test_customer360_pipeline.py)
- [docker-compose.yaml](../../../../docker-compose.yaml)
- [airflow/README.md](../../../../airflow/README.md)
- [airflow/requirements.txt](../../../../airflow/requirements.txt)
- [test_container_runtime.py](../../../../airflow/tests/test_container_runtime.py)
- [test_compose_alert_configuration.py](../../../../docker/tests/test_compose_alert_configuration.py)

The diff is broader than F-04/F-05: it also contains substantial Fivetran DAG,
test, and documentation changes. The only dependency-manifest delta is the
addition of `airflow-provider-fivetran-async==2.4.0`; this is Fivetran-related
work, not an F-04/F-05 dependency. The existing Airflow and Snowflake provider
pins are unchanged. The broader Fivetran changes were not audited here and
should not be staged as part of these identity fixes without separate review.

## Findings

### F-04 — RESOLVED

The original identity gap is addressed at both configuration and session levels.
`_snowflake_policy("RAW")` establishes the TEST defaults and rejects malformed
identifiers, administrative roles, and non-absolute key paths
([DAG lines 297–317](../../../../airflow/dags/customer360_pipeline.py#L297)).
`_validate_raw_connection` checks the configured `CUSTOMER360_TEST.RAW`
database/schema, expected login and role, authentication fields, and exact
approved mounted key path before opening Snowflake
([DAG lines 353–386](../../../../airflow/dags/customer360_pipeline.py#L353)).
After connecting, the guard queries database, schema, user, and role and checks
all four before scanning RAW tables
([DAG lines 389–400](../../../../airflow/dags/customer360_pipeline.py#L389)).
The hook is also configured with the policy role
([DAG lines 706–714](../../../../airflow/dags/customer360_pipeline.py#L706)).
Compose defaults and the primary setup instructions agree on
`SVC_AIRFLOW_TEST`, `C360_AIRFLOW_ROLE`, and the mounted key path
([Compose lines 41–46](../../../../docker-compose.yaml#L41);
[README lines 285–299](../../../../airflow/README.md#L285)).

Tests exercise wrong configured and actual users/roles, an incorrect database,
missing key paths, and rejected authentication fields
([tests lines 552–666](../../../../airflow/tests/test_customer360_pipeline.py#L552)).
The mocked actual-session identity test asserts rejection before table reads.
The code checks the schema, but tests do not directly exercise a wrong RAW
connection schema or a wrong schema returned by the session.

**Secondary-role limitation:** the hook sets the primary role and checks
`CURRENT_ROLE()`, but does not issue `USE SECONDARY ROLES NONE` or inspect
secondary-role state. If the service user has secondary roles with additional
grants, those grants could affect effective access while the primary-role
identity check still passes. No live grants or secondary-role configuration
were checked. Thus F-04's service-identity validation is resolved; this is not
proof of least privilege or an assurance that only the named role contributes
privileges.

### F-05 — PARTIALLY RESOLVED

The dbt guard now requires the configured deployment identity to match the
profile's user and role, fixes routing to `CUSTOMER360_TEST.SILVER`, rejects a
password field and disallowed auth fields/methods, and requires the exact
deployment-approved mounted key path to exist, be nonempty, and readable
([DAG lines 319–350](../../../../airflow/dags/customer360_pipeline.py#L319);
[DAG lines 619–664](../../../../airflow/dags/customer360_pipeline.py#L619)).
The defaults are `SVC_DBT_TEST`, `C360_DBT_ROLE`, and the dbt key path
([DAG lines 54–64](../../../../airflow/dags/customer360_pipeline.py#L54);
[Compose lines 41–46](../../../../docker-compose.yaml#L41)).

Tests cover wrong users/roles, production database, missing/nonexistent,
unreadable and empty key files, password-only configuration, inline key/token,
OAuth and interactive authenticators. They also verify generic errors do not
include synthetic sensitive values and that key bytes are not read
([tests lines 748–923](../../../../airflow/tests/test_customer360_pipeline.py#L748)).
The dbt manifest test rejects a wrong model schema
([tests lines 727–745](../../../../airflow/tests/test_customer360_pipeline.py#L727)),
but the profile-guard test does not directly vary the profile schema.

The guard validates key-path metadata and accepted connector configuration; it
does **not** inspect the mounted file's cryptographic algorithm or verify the
registered Snowflake public key. The README explicitly documents this boundary
and requires RSA provisioning/registration through a separately authorized
process ([README lines 319–360](../../../../airflow/README.md#L319)). Therefore
the configuration-level password/OAuth bypass is addressed, but actual RSA key
type and live key-pair authentication are not established by this guard or this
review. The README's earlier setup guidance also says “or another dedicated
transformation role” without saying there that the deployment policy must be
updated too ([README lines 177–180](../../../../airflow/README.md#L177)); the
later deployment section clarifies that profile and policy identities must
match, but reconciling the two passages would reduce ambiguity.

## Validation evidence

### Independently executed in this review

- Full mocked Airflow DAG suite, in a disposable container with `--network none`
  and read-only source: **40 passed**.
- `python -m unittest discover -s docker/tests -p test_compose_alert_configuration.py -v`:
  **5 passed**.
- `docker compose config --quiet`: passed; expanded Compose configuration was
  not printed.
- Six selected container-runtime tests, in a disposable `--network none`
  container borrowing only the existing scheduler's mounts read-only:
  **6 passed**. No service was started or recreated.
- Existing dbt profile guard, in a disposable `--network none` container with
  profile, key directory, and source mounted read-only: **passed**. The result
  was generic; key contents were not read or printed.
- `git diff --check`: passed, with only working-copy LF/CRLF notices.
- Targeted Ruff check on the changed DAG and Airflow test modules: **failed**,
  reporting eight diagnostics (two `EXE002` and six `BLE001`). This does not
  establish a Snowflake runtime failure, but it does not support a claim that
  the targeted Ruff check passes.
- Read-only Compose service-state query showed existing services; no external
  service, DAG, Snowflake, Fivetran, or SMTP operation was performed.

### Codex-reported or not verified

Codex reports 40 mocked DAG tests, five Compose tests, six container checks,
Compose validation, Ruff, and diff checks passed. This review independently
reproduced the stated test counts, Compose validation, and diff check. The
independent Ruff result above did not pass. No F-04/F-05-specific execution log
was found; [dependency-validation.txt](../../../validation/2026-11-11/dependency-validation.txt)
covers earlier F-01/F-02 dependency work and is not evidence for these guards.

**NOT VERIFIED:** live Snowflake authentication, RSA algorithm/public-key
registration, grants and secondary-role state, or actual service-user
privileges. These require a separately authorized deployment check and were
intentionally not attempted. The RAW wrong-schema test case is also absent,
although the implementation contains the schema comparisons.

## Regressions, limitations, and readiness

- The F-04 identity guard is correctly placed before RAW table reads and checks
  the actual session identity; no direct regression was found in the reviewed
  path.
- The F-05 guard prevents the tested profile-level password, inline-key, token,
  OAuth, and interactive-authentication bypasses. It deliberately avoids key
  contents, so it cannot certify that the mounted file is RSA or that Snowflake
  accepts its public-key registration.
- Test coverage should add direct wrong-schema cases for RAW connection/session
  and the dbt profile guard. Secondary-role policy and least-privilege grants
  remain separate deployment concerns.
- The worktree includes extensive Fivetran changes outside this review, and
  Ruff did not pass on the targeted files. No credentials, generated test
  artifacts, or implementation changes were introduced by this review.

**Ready to commit:** **No, not as an undivided change.** F-04's identity fix is
resolved. F-05 is only partially resolved because the guard does not establish
actual RSA key type/authentication, and the targeted Ruff check failed. The
broader Fivetran work also needs to be separated or reviewed independently.

**Recommended next action:** clarify whether F-05 requires cryptographic RSA
file validation or only configuration-level key-pair enforcement; document
that boundary consistently, add the missing schema tests, review/fix the
targeted Ruff findings, and split the unrelated Fivetran changes before
committing. Verify secondary-role grants and live key-pair authentication only
through an explicitly authorized deployment procedure.
