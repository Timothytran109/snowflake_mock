# Customer 360 repository production-readiness audit

## Metadata

- Audit date: 2026-11-11
- Run number: run-001
- Git branch: `feat/airflow-orchestration`
- HEAD commit: `cc14de290bf52c86aa172f44a316ad3b074010cd`
- Result: **FAIL — not ready for production/customer handoff**
- Repository scope: `AGENTS.md`; all tracked project files under `airflow/`, `dbt/`, and `docker/`; root Docker, Compose, dependency, ignore, and README files; and local runtime configuration by filename, tracking status, selected non-secret profile metadata, and ACL metadata.
- Sensitive-data handling: `.env`, private-key contents, credentials, and full runtime profile contents were not printed or included. Two ignored dbt log files were scanned for credential-like patterns without returning matching text. Vendored `.venv/` packages and generated dbt artifacts were not reviewed as project source.
- Existing worktree changes: preserved and assessed as found: `airflow/README.md`, `airflow/dags/customer360_pipeline.py`, `airflow/requirements.txt`, `airflow/tests/test_customer360_pipeline.py`, `docker-compose.yaml`, and `requirements.txt`. These predate this audit.
- Prior audit directories: `docs/audits/` did not exist, so this is `run-001`.

## FAIL

### F-01 — Docker cannot reliably parse the dbt requirements manifest

- File: [requirements.txt:1](../../../../requirements.txt#L1); consumer: [docker/airflow.Dockerfile:20](../../../../docker/airflow.Dockerfile#L20)
- Exact issue: `requirements.txt` begins with a UTF-16LE BOM (`FF FE`), rather than UTF-8. The Dockerfile passes it directly to `pip install -r`; standard pip requirement parsing expects UTF-8 input.
- Why it matters: the image build is expected to fail while decoding or parsing the manifest. `docker compose config --quiet` does not build the image and does not validate this.
- Recommended remediation: re-encode the manifest as plain UTF-8, then build the image and run the container checks.
- Blocks customer handoff: **Yes.**

### F-02 — The supposedly isolated dbt environment installs Airflow and unrelated packages

- Files: [requirements.txt:13-20](../../../../requirements.txt#L13); [docker/airflow.Dockerfile:1](../../../../docker/airflow.Dockerfile#L1), [docker/airflow.Dockerfile:19-20](../../../../docker/airflow.Dockerfile#L19); [airflow/tests/test_container_runtime.py:47](../../../../airflow/tests/test_container_runtime.py#L47)
- Exact issue: the root manifest includes `apache-airflow==3.4.0`, Airflow core/providers and other unrelated packages (including Ruff), and the Dockerfile installs that full manifest into `/opt/dbt`. The Airflow image itself is based on Airflow 3.3.2. The container test explicitly expects that importing `airflow` from the dbt environment is impossible.
- Why it matters: the dbt environment is not isolated as documented, contains an Airflow version different from the service environment, expands the image unnecessarily, and makes the isolation assertion fail once the invalid encoding in F-01 is corrected.
- Recommended remediation: make the dbt dependency input a UTF-8 dbt-only pinned manifest; keep Airflow and test tooling in their own manifests/environments. Rebuild and verify `pip check` and the container isolation test.
- Blocks customer handoff: **Yes.**

### F-03 — Documented alert-recipient configuration can be overridden to empty

- Files: [docker-compose.yaml:10-11](../../../../docker-compose.yaml#L10), [docker-compose.yaml:35](../../../../docker-compose.yaml#L35); [airflow/README.md:123-127](../../../../airflow/README.md#L123); [airflow/dags/customer360_pipeline.py:567-570](../../../../airflow/dags/customer360_pipeline.py#L567)
- Exact issue: the README directs operators to put `CUSTOMER360_ALERT_EMAILS` in `.local/customer360/runtime.env`, which Compose loads as `env_file`. But the `environment` entry interpolates the same variable from the shell/project `.env` and defaults it to an empty string. Compose `environment` values override `env_file` values, so following the runtime.env instruction alone leaves the DAG's required recipient empty.
- Why it matters: `prepare_fivetran_sync` fails before ingestion, despite the recipient appearing configured in the documented file. This is a startup/handoff failure and creates ambiguity about which file is authoritative.
- Recommended remediation: use one documented source of truth and ensure Compose does not overwrite a populated `env_file` value with an empty interpolation; add a configuration test covering this precedence.
- Blocks customer handoff: **Yes, for operators following the runtime.env instructions.**

### F-04 — Airflow Snowflake validation does not enforce the required service identity

- Files: [airflow/dags/customer360_pipeline.py:284-292](../../../../airflow/dags/customer360_pipeline.py#L284), [airflow/dags/customer360_pipeline.py:583-599](../../../../airflow/dags/customer360_pipeline.py#L583); [airflow/tests/test_customer360_pipeline.py:507-530](../../../../airflow/tests/test_customer360_pipeline.py#L507)
- Exact issue: RAW validation checks the current database/schema and rejects a blocklist of administrative roles, but it does not select or validate `CURRENT_USER()` and accepts any other nonempty role. Connection preflight checks the configured database/schema, not the expected user or role. The test fixture explicitly uses a different generic read role. The requested `SVC_AIRFLOW_TEST` and `C360_AIRFLOW_ROLE` contract is not enforced in code or connection setup documentation.
- Why it matters: a different non-admin user or a non-approved role can pass the guard, including one with broader-than-intended privileges. This audit could not inspect live Airflow Connections or Snowflake because external connections were prohibited.
- Recommended remediation: define and validate the approved service user and exact role (or document an explicitly approved role policy), test rejection of wrong users/roles, and verify the live Connection through an authorized deployment procedure.
- Blocks customer handoff: **Yes, until approved identity/privileges are verified.**

### F-05 — dbt preflight does not enforce the service user or RSA-only authentication

- File: [airflow/dags/customer360_pipeline.py:511-535](../../../../airflow/dags/customer360_pipeline.py#L511)
- Exact issue: `_validate_dbt_profile` validates only database, schema, and a non-admin role. It does not validate `user`, require `SVC_DBT_TEST`, inspect the authentication method, or reject password authentication. Therefore the guard permits personal users and password-based profiles, even though the handoff contract calls for `SVC_DBT_TEST`, `C360_DBT_ROLE`, and RSA key-pair authentication.
- Why it matters: a future or production runtime profile can bypass the intended service identity and passwordless-key policy while still passing the safety guard.
- Recommended remediation: validate the required service user and key-auth configuration without rendering/logging credential values; add tests for wrong users and password-only profiles.
- Blocks customer handoff: **Yes, for enforcing the stated runtime policy.**
- Verified local configuration: the ignored local profile's safe metadata is `target=dev`, `CUSTOMER360_TEST.SILVER`, `C360_DBT_ROLE`, `SVC_DBT_TEST`, and a configured `private_key_path`; no password value is configured. Its referenced key basename matches a file in the read-only `/opt/airflow/secrets/snowflake` mount. This confirms only the local profile, not other runtime profiles.

### F-06 — Stripe payment deduplication is not deterministic on ties

- File: [dbt/models/staging/stg_stripe__payments.sql:79-83](../../../../dbt/models/staging/stg_stripe__payments.sql#L79)
- Exact issue: the `row_number()` ordering uses `_fivetran_synced` and `payment_at` only. Unlike the other staging models, it does not use `_line` as a final tie-breaker and does not retain `_line`.
- Why it matters: duplicate rows for a payment with tied sync and payment timestamps can select different source values between executions, violating deterministic staging/deduplication requirements and making results difficult to reconcile.
- Recommended remediation: include the available Fivetran `_line` (or another documented unique source-order field) as the final ordering key and add a unit test with tied timestamps and distinct row values.
- Blocks customer handoff: **Yes, if duplicate source IDs with tied timestamps are possible; verify or resolve before production.**

### F-07 — Ignored private keys and `.env` are modifiable by broad local principals

- Paths: `.env`; `keys/dbt/dbt_rsa_key.p8`; `keys/fivetran/fivetran_rsa_key.p8`; `keys/snowflake/airflow_rsa_key.p8`; `keys/snowflake/dbt_rsa_key.p8`. These are local-only paths, not tracked source files.
- Exact issue: read-only ACL inspection found inherited access for broad Windows principals on `.env` and each listed private-key file; the applicable broad allow rules include `Modify`. The bootstrap script restricts `.local/customer360` only, not the root `.env` or `keys/` tree. Git ignore and read-only container mounts do not protect the host copies.
- Why it matters: other authenticated local principals can read or replace active private keys and alter runtime configuration. This is a credential exposure/integrity risk even though Git tracking and Docker build-context exclusions are correct.
- Recommended remediation: immediately restrict host ACLs on `.env` and `keys/` to the intended owner and approved administrators; verify no unauthorized access occurred. Treat active Snowflake RSA keys in `keys/snowflake/` as potentially exposed and rotate/revoke their public-key registrations. Rotate other private-key credentials under `keys/` if active or accessible to unauthorized principals. If `.env` contains any credentials, rotate them too. Do not copy secret values into the report or repository.
- Blocks customer handoff: **Yes.**

### F-08 — The provided deployment is explicitly local-development-only

- Files: [airflow/README.md:71-74](../../../../airflow/README.md#L71), [docker-compose.yaml:18-23](../../../../docker-compose.yaml#L18), [docker/Initialize-LocalAirflow.ps1:5-21](../../../../docker/Initialize-LocalAirflow.ps1#L5)
- Exact issue: the Compose deployment uses the SimpleAuthManager and locally generated file secrets. The README itself states this is not a production deployment. No production authentication, approved secrets backend/managed identity, or production deployment configuration is included.
- Why it matters: the local Compose stack does not meet the repository's production secret-management and deployment expectations and should not be promoted as a production-ready platform.
- Recommended remediation: keep this Compose setup explicitly local-only; design and separately review a production deployment with approved authentication, managed secret delivery, least-privilege service identities, operational monitoring, backup/recovery, and deployment controls.
- Blocks customer handoff: **Yes, for production handoff.** It may be used for local evaluation after the other blockers are resolved.

## WARNING

### W-01 — Only one Fivetran connector can be configured

- Files: [airflow/dags/customer360_pipeline.py:98-105](../../../../airflow/dags/customer360_pipeline.py#L98), [airflow/dags/customer360_pipeline.py:567-575](../../../../airflow/dags/customer360_pipeline.py#L567), [airflow/dags/customer360_pipeline.py:634-640](../../../../airflow/dags/customer360_pipeline.py#L634)
- Exact issue: `CUSTOMER360_FIVETRAN_CONNECTOR_ID` is converted to a one-element list, even though the DAG maps the sync task and the README asks operators to configure every connector needed for the seven RAW tables.
- Why it matters: if the seven tables are spread over multiple connectors, only one is synced and RAW validation will fail or validate stale data.
- Recommended remediation: confirm one configured connector populates all seven tables, or support a validated list of connector IDs and test mapping and failure behavior for each.
- Blocks customer handoff: **Conditionally — yes if more than one connector is needed.**

### W-02 — Runtime environment values are shared with every Airflow service

- File: [docker-compose.yaml:1-16](../../../../docker-compose.yaml#L1)
- Exact issue: the common Airflow service anchor applies the same optional `runtime.env` file to API server, scheduler, DAG processor, triggerer, and initialization service. The README permits sensitive profile fields via `env_var()`.
- Why it matters: values needed only by execution tasks are available in unrelated service containers and process environments, expanding exposure and making them visible in container configuration.
- Recommended remediation: keep credentials in Airflow Connections/a secrets backend or scope runtime secrets to only the components that need them. Avoid storing secrets in a broadly shared environment file.
- Blocks customer handoff: **Yes for production secret delivery; local-only risk should be understood.**

### W-03 — Fivetran diagnostic messages are bounded but not allowlisted

- File: [airflow/dags/customer360_pipeline.py:144-183](../../../../airflow/dags/customer360_pipeline.py#L144)
- Exact issue: API `message` and status strings are stripped of control characters and truncated, then logged. They are still arbitrary response text rather than a fixed allowlist. No credential-like pattern was found in the two local dbt logs, but that does not prove future API messages are secret-free.
- Why it matters: an upstream diagnostic could include sensitive context; truncation is not redaction.
- Recommended remediation: log allowlisted API codes/statuses and a generic message, or apply explicit secret/PII redaction before logging response-derived text.
- Blocks customer handoff: **Not by itself, but resolve before production logging is enabled.**

### W-04 — Documentation has inconsistent file and mount references

- Files: [airflow/README.md:50](../../../../airflow/README.md#L50), [airflow/README.md:139-166](../../../../airflow/README.md#L139); [docker-compose.yaml:64-78](../../../../docker-compose.yaml#L64)
- Exact issue: the README refers to `compose.yaml`, while the repository file is `docker-compose.yaml`. It describes both `/opt/airflow/secrets/snowflake` and `/opt/airflow/secrets/keys` for key use; the checked local profile points to the former. Connection instructions also do not state the requested Airflow/dbt service-user and role names. The root `README.md` contains only a title, so setup is discoverable only in the Airflow README.
- Why it matters: operators may mount or reference the wrong key path and cannot use the root README as a project handoff guide.
- Recommended remediation: designate one supported key path, reconcile the README with Compose and the profile, document exact service identities and authentication requirements, and provide a concise root README entry point.
- Blocks customer handoff: **Yes until the runtime instructions identify the supported profile/key and identity setup unambiguously.**

### W-05 — Ignore rules contain redundant and overly broad patterns

- File: [.gitignore:28-36](../../../../.gitignore#L28)
- Exact issue: `keys/` and `*.pem` are repeated; `*.json` at line 53 ignores every JSON file, not just generated or secret JSON.
- Why it matters: redundant rules obscure intent and the broad JSON rule can silently hide legitimate source/configuration files from Git.
- Recommended remediation: deduplicate rules and scope JSON ignores to specific local secret/runtime paths or known generated files.
- Blocks customer handoff: **No; no currently tracked JSON files or ignored-secret tracking violations were found.**

### W-06 — Runtime and data validation suites were not executed

- Files: [airflow/tests/test_customer360_pipeline.py](../../../../airflow/tests/test_customer360_pipeline.py), [airflow/tests/test_container_runtime.py](../../../../airflow/tests/test_container_runtime.py), [dbt/](../../../../dbt)
- Exact issue: Python syntax and Compose configuration were checked, but the Airflow unit/container tests, Docker image build, dbt parse/build, and Snowflake validation were not run. The container test requires the intended built Linux image; dbt build would connect to Snowflake and modify development relations.
- Why it matters: static validation cannot establish provider runtime compatibility, container test success, dbt compilation, or actual Snowflake grants/data correctness.
- Recommended remediation: after fixing blockers, run local image/container tests. Run dbt parse and TEST-only build only under an explicitly authorized `CUSTOMER360_TEST` profile; do not use production credentials or targets.
- Blocks customer handoff: **Yes until the applicable local tests pass; live TEST validation requires separate authorization.**

## PASS

- Current Git tracking contains no `.env`, runtime `profiles.yml`, private-key/certificate files, credential files, database files, or logs. `git check-ignore` confirmed the present `.env`, `.local/` profile/runtime files, `keys/**/*.p8`, Airflow logs, and dbt target/log paths are ignored.
- The `.dockerignore` denies the build context by default and explicitly excludes `.env`, `.local`, profiles, keys, credentials, logs, and generated artifacts. The Dockerfile copies dependency manifests only; it does not copy source credentials or private keys into image layers. The Compose private-key/profile bind mounts are read-only.
- Compose paths for `/opt/customer360/dbt`, `/opt/airflow/secrets/dbt`, `/opt/airflow/secrets/keys`, `/opt/airflow/secrets/snowflake`, and `/opt/dbt/bin/dbt` align between `docker-compose.yaml` and the DAG's expected paths.
- `docker compose config --quiet` passed. The triggerer, LocalExecutor scheduler, API server, DAG processor, health checks, non-root Airflow UID, TEST database destination guard, retries, timeouts, paused/manual schedule defaults, and expected task ordering are present.
- Fivetran credentials are obtained from the `customer360_fivetran` Airflow Connection; the async provider is configured with deferral and completion waiting. The DAG checks the destination database/schema and RAW data before invoking dbt. No real sync or external call was run.
- The local ignored dbt profile metadata matches `customer360`, `dev`, `CUSTOMER360_TEST`, `SILVER`, `C360_DBT_ROLE`, and `SVC_DBT_TEST`; it has a private-key path and no configured password. Credential and key contents were not read or printed.
- dbt sources use `source()` and models use `ref()`. Staging and marts have explicit output columns and tests for key constraints, accepted values, relationships, integrity, reconciliation, and orphan handling. Gold marts are materialized in `CUSTOMER360_TEST.GOLD` by the schema macro.
- DAG tests mock provider/network, Snowflake, dbt process, and SMTP behavior; test code does not intentionally trigger a real Fivetran sync, send mail, or modify Snowflake. Expected warning tests are bounded by test names and other dbt failures remain blocking.

## Commands executed and validation results

- `git status --short`, `git branch --show-current`, `git rev-parse HEAD`, `git ls-files` — passed; captured branch/SHA and existing user modifications.
- `git check-ignore -v` on the local `.env`, runtime profile, private-key paths, Airflow logs, and dbt artifacts — passed; all inspected paths were ignored.
- `git diff --check` — no whitespace error; Git emitted a line-ending notice for the working copy of `airflow/requirements.txt`.
- `docker compose config --quiet` — passed with no configuration error and no expanded environment output.
- In-memory Python `compile()` of the DAG and two Python test modules, with bytecode writing disabled — passed for three Python files.
- Read-only metadata inspection of `.local/customer360/dbt/profiles.yml` — confirmed only target/database/schema/role/user and presence/absence of authentication fields; no secret values were printed.
- Read-only Windows ACL summaries for `.env`, private-key files under `keys/`, and `.local/customer360` — broad inherited `Modify` access was detected on `.env` and each inspected `keys/**/*.p8` file; `.local/customer360` did not have broad allow rules.
- Credential-pattern scan of `logs/dbt.log` and `dbt/logs/dbt.log` — no matching patterns detected; scan output did not include log contents.
- Byte-signature check — `requirements.txt` has a UTF-16LE BOM.
- Not run: Docker build/start, Airflow unit/container tests, dbt parse/build, Fivetran sync, SMTP test, or Snowflake connection/query. No external service was contacted and no DAG was triggered.

## 1. Critical blockers before handoff

1. Re-encode and split the dependency manifests; then prove the intended dbt environment excludes Airflow.
2. Resolve the alert-recipient Compose precedence so the documented runtime configuration works.
3. Enforce or verify the exact RAW and dbt service identities and key-only authentication.
4. Make Stripe payment deduplication deterministic.
5. Restrict local host ACLs on `.env` and `keys/`; handle potentially exposed active keys through controlled rotation.
6. Do not present this SimpleAuthManager/local-file-secret Compose stack as production-ready.
7. Run the required container and dbt validations after the fixes; authorize any Snowflake TEST build separately.

## 2. Security issues requiring immediate attention

- Treat active private keys under `keys/` as potentially exposed because the inspected files inherit broad `Modify` access. Rotate active Snowflake DBT/Airflow keys and revoke old public-key registrations after an authorized key replacement. Determine whether the Fivetran key file is active before deciding on its rotation.
- Restrict `.env` ACLs. Its contents were deliberately not read; rotate any credentials it may contain if broad local access was not authorized.
- Runtime environment files are shared with all Airflow services; production secrets should move to a scoped secret backend or appropriately scoped runtime mounts.
- No tracked secret candidate was found, and no secret-like value was printed in this report. No API credentials were tested, so this audit cannot certify that external credentials are valid or uncompromised.

## 3. Reliability and operational risks

- One configured Fivetran connector may not populate all seven tables.
- dbt model/table publication is not atomic; independently scheduled ingestion can change RAW during a build. The README documents the current-state rerun and partial-publication behavior.
- `dbt build` and Snowflake role/grant checks remain unverified. Local static checks do not prove actual external data completeness or permissions.
- API response-derived messages remain in logs after truncation/control-character stripping.

## 4. Recommended improvements

- Add focused tests for required Snowflake users/roles, key-only profile authentication, Compose `env_file` precedence, and payment deduplication ties.
- Consolidate key mounts/documentation and state whether one or multiple Fivetran connectors are supported.
- Remove duplicate `.gitignore` rules and narrow the blanket JSON ignore.
- Add a root README that links to setup, local validation, deployment boundaries, and recovery guidance.
- Keep the local Docker stack separate from a production deployment and secret-management design.

## 5. Items already correct

- No sensitive file candidates were tracked; current local secret and generated paths are ignored.
- `.dockerignore` is deny-by-default and the Dockerfile does not copy local keys, profiles, or `.env`.
- Profile and Snowflake key mounts are read-only at container runtime.
- The local dbt profile's non-secret routing/user fields match the TEST contract and it uses a private-key path without a configured password.
- DAG import-time behavior is designed to avoid network/secret access; external operations are behind tasks and mocked in unit tests.
- Raw validation, production database checks, role blocklisting, output table checks, and dbt result/artifact checks fail closed.
- The docs explain paused/manual scheduling, retries, reruns, orphan warnings, and the local-only nature of SimpleAuthManager.

## 6. Exact validation commands to run next

After resolving F-01 through F-07:

```powershell
git diff --check
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 300
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v
docker compose exec -T airflow-scheduler dbt --version
```

The image build may download dependencies; run it only in an approved environment. The local Compose startup does not trigger the DAG. For dbt, first run `dbt parse --no-partial-parse` with the verified TEST profile. Run `dbt build --select <model> --threads 1` only after confirming `CUSTOMER360_TEST` and obtaining authorization to write development relations. Do not run these commands against production.

## 7. Files safe to commit

- This redacted audit report is safe to commit after owner review.
- No credentials were identified in tracked source files, but the six pre-existing modified files listed in Metadata are not release-ready because of the blockers above. Do not commit or push as part of this audit; no remediation changes were made.

## 8. Files that must remain ignored or local-only

- `.env`, `.env.*`, `.local/`, runtime `profiles.yml`, Airflow local secrets/Connections, and private-key material under `keys/` (including `*.p8`, `*.pem`, `*.key`, and certificates).
- Airflow logs, metadata databases, PID/runtime files; dbt `target/`, `logs/`, and `dbt_packages/`; Python virtual environments/caches; local datasets and Docker runtime/volume files.
- Keep private keys out of Git and image layers even after fixing host ACLs; mount them only through an approved protected runtime mechanism.
