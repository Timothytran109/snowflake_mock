# Focused review: F-01 and F-02 dependency fixes

## Metadata

- Date: 2026-11-11 (America/Los_Angeles)
- Audit run: run-003
- Branch: `feat/airflow-orchestration`
- HEAD: `cc14de290bf52c86aa172f44a316ad3b074010cd`
- Instructions: [AGENTS.md](../../../../AGENTS.md)
- Prior audits: [run-001](../run-001/repository-audit.md), [run-002](../run-002/repository-audit.md)
- Finding decisions: **F-01 RESOLVED; F-02 RESOLVED.**

## Working-tree status and scope

The worktree was already dirty before this report. At review time, `git status
--short --untracked-files=all` showed:

- Modified: `.dockerignore`, `airflow/README.md`,
  `airflow/dags/customer360_pipeline.py`, `airflow/requirements.txt`,
  `airflow/tests/test_container_runtime.py`,
  `airflow/tests/test_customer360_pipeline.py`, `docker-compose.yaml`,
  `docker/airflow.Dockerfile`, and `requirements.txt`.
- Untracked: `dbt/requirements.txt`, `requirements-dev.txt`,
  `docs/audits/2026-11-11/run-001/repository-audit.md`,
  `docs/audits/2026-11-11/run-002/repository-audit.md`, and
  `docs/validation/2026-11-11/dependency-validation.txt`.
- No generated dbt targets, logs, bytecode, images, or test output were added
  under source directories by the checks in this review. The only persistent
  file created by this review is this run-003 report.

The run-001 audit recorded the DAG, Compose, README, Airflow requirements, and
DAG-test changes as pre-existing. The dependency-related changes reviewed here
are the root/dbt/development manifests, Dockerfile and ignore changes, the
container-runtime test additions, and related README updates. The validation
record is untracked and was reviewed as evidence; it was not modified.

Reviewed [requirements.txt](../../../../requirements.txt),
[dbt/requirements.txt](../../../../dbt/requirements.txt),
[airflow/requirements.txt](../../../../airflow/requirements.txt),
[requirements-dev.txt](../../../../requirements-dev.txt),
[docker/airflow.Dockerfile](../../../../docker/airflow.Dockerfile),
[.dockerignore](../../../../.dockerignore),
[docker-compose.yaml](../../../../docker-compose.yaml),
[airflow/tests/test_container_runtime.py](../../../../airflow/tests/test_container_runtime.py),
[airflow/tests/test_customer360_pipeline.py](../../../../airflow/tests/test_customer360_pipeline.py),
[airflow/dags/customer360_pipeline.py](../../../../airflow/dags/customer360_pipeline.py),
[airflow/README.md](../../../../airflow/README.md), both prior audits, and
[dependency-validation.txt](../../../validation/2026-11-11/dependency-validation.txt).

## Finding determinations

### F-01 — RESOLVED

The former UTF-16LE root requirements input is no longer installed by the
Dockerfile. The root manifest is now a UTF-8 include of the dbt-only
`dbt/requirements.txt`, and the Dockerfile copies and installs that dbt manifest
directly. Independent UTF-8 decoding and pip requirements parsing passed for all
four manifests; the root include expands to the same two dbt pins.

The validation record reports that `docker compose build airflow-scheduler`
completed with exit code 0. The tagged runtime image is present locally, and its
image ID matches the ID recorded in that validation record. This corroborates
the reported successful build. I did not rebuild the image.

### F-02 — RESOLVED

The runtime Dockerfile creates `/opt/dbt` without system site packages and
installs only `dbt/requirements.txt`. The separate development stage installs
pytest and Ruff into `/opt/customer360/dev`; it inherits the Airflow runtime
site packages for DAG tests but does not add those tools to `/opt/dbt` or the
final runtime stage. Compose specifies no build target, and the Dockerfile's
last stage is `final`, based on `runtime`; therefore the default Compose build
selects the runtime stage, not `development`.

Independent checks against the existing runtime image confirmed:

- Airflow is 3.3.2; dbt-core is 1.12.6 and dbt-snowflake is 1.12.5.
- `pip check` passes in both the Airflow and `/opt/dbt` Python environments.
- `/usr/local/bin/dbt` resolves to `/opt/dbt/bin/dbt`.
- Airflow, pytest, and Ruff are not importable from `/opt/dbt`.
- pytest and Ruff are present in the development environment and absent from
  the final runtime environment.

The container-runtime test change adds the pytest/Ruff isolation assertions and
checks `pip check` in both runtime environments; inspection of its diff found
additions, not removed or weakened checks.

## Validation evidence and provenance

### Checks independently executed in this review

- `git diff --check` — passed; Git emitted only LF/CRLF working-copy notices.
- Read-only UTF-8 decode/BOM checks — all four dependency manifests decode as
  UTF-8 without a BOM.
- Pip requirements parser — all four manifests parse: root include (2 entries),
  dbt (2), Airflow (6), and development (2). No package installation or
  dependency resolution was run.
- `docker compose config --quiet` — passed.
- Mocked DAG suite in the existing development image, with `--network none` and
  only a read-only mount of `airflow/`:
  `docker run --rm --network none --entrypoint /opt/customer360/dev/bin/python --mount "type=bind,source=<repo>\airflow,target=/opt/airflow/source,readonly" customer360-airflow-dev:3.3.2-python3.12 -m pytest /opt/airflow/source/tests/test_customer360_pipeline.py -q -p no:cacheprovider`
  — **28 passed**. The suite includes DAG import/serialization checks and
  mocked Fivetran, Snowflake/RAW, dbt, and SMTP behavior.
- `docker run --rm --network none --entrypoint python customer360-airflow:3.3.2-python3.12 -m pip check`
  — passed, no broken requirements.
- `docker run --rm --network none --entrypoint /opt/dbt/bin/python customer360-airflow:3.3.2-python3.12 -m pip check`
  — passed, no broken requirements.
- Explicit Snowflake, SMTP, Fivetran operator, and Fivetran sensor imports in a
  network-disabled runtime-image container — passed.
- Runtime-image metadata/import checks — confirmed the dbt versions, executable
  resolution, and absence of Airflow/pytest/Ruff in `/opt/dbt`; confirmed pytest
  and Ruff are absent from runtime and available in development.
- Selected `test_container_runtime.py` checks in a disposable, network-disabled
  container — **6 passed**, covering provider versions, both `pip check` calls,
  dbt executable/version/isolation, credential-free dbt parse, non-root and
  local defaults, secret-file presence by metadata only, and read-only mounts.
  The container inherited the existing scheduler's mounts read-only, not its
  environment or processes. The tests did not read or print secret values.
- Read-only Docker inspection found the runtime and development image tags
  present. The runtime image ID is
  `sha256:b25edef90d7acc7558a7da88e4e2de992d268bb5091043d26e7e432d848517d7`,
  matching the validation record. The existing Compose services still use a
  different, older image ID; no service was recreated or updated.

### Results supported by the available Codex validation record

The untracked [dependency-validation.txt](../../../validation/2026-11-11/dependency-validation.txt)
records successful exit codes for:

- `docker compose build airflow-scheduler` (runtime image).
- `docker build -f docker/airflow.Dockerfile --target development -t customer360-airflow-dev:3.3.2-python3.12 .`
- Both runtime `pip check` commands, provider imports, dbt path/isolation
  assertions, and the same 28-test mocked suite.
- Six selected container-runtime tests.

The local runtime image ID and both image tags corroborate that images were
produced. The tests, imports, and runtime checks listed above were independently
rerun successfully in this review. No raw BuildKit logs or CI artifacts were
available, so the exact build invocations and their exit codes remain supported
by the validation record plus local image artifacts, rather than by a fresh
build in this review.

### Claims without independent execution evidence / not verified

- I did not rerun either image build: the user explicitly prohibited rebuilding.
  The successful-build result is supported as described above, not independently
  reproduced here.
- The unselected `test_scheduler_registered_the_dag_paused_without_a_next_run`
  test queries the live Airflow metadata database. It was intentionally not
  run. If that state check is needed, run it in an authorized local validation
  stack with:
  `docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests -p test_container_runtime.py -k scheduler_registered -v`
- Full live-stack, Snowflake, Fivetran, SMTP, and dbt build/integration
  validation remains unverified and was outside this dependency review.

No evidence was found that a DAG was triggered or Snowflake, Fivetran, or SMTP
was contacted by the network-disabled tests. The earlier image builds in the
validation record can require registry/package-index access when their layers
are not cached; the README now distinguishes that from network-disabled test
runs.

## Dependency, documentation, and regression review

- Intended direct versions are preserved: Airflow 3.3.2, dbt-core 1.12.6,
  dbt-snowflake 1.12.5. The Airflow, dbt, and development manifests parse and
  retain separate ownership. No unintended direct dependency version change
  was found in the reviewed manifests.
- The build context remains deny-by-default and explicitly includes the
  dependency inputs required by the Dockerfile; sensitive paths and generated
  artifacts remain excluded. No credential, profile, or key file appears in
  the working-tree status or dependency manifests.
- The README now correctly says runtime and development image builds may need
  registry, constraints, and package-index access, while the subsequent mocked
  test container uses `--network none`.
- **Separate follow-up:** dbt direct dependencies are pinned, but their complete
  transitive graph is not locked. The README documents this. It affects
  reproducibility and should be tracked separately; it is not evidence that
  F-01 or F-02 remains open.
- Existing documentation still refers to root `compose.yaml` in places although
  the repository file is `docker-compose.yaml`; this pre-existing path mismatch
  is unrelated to the dependency fixes.
- No generated source-tree artifacts or new unrelated implementation changes
  were observed. Run-001 recorded several unrelated DAG, Compose, and test
  edits as pre-existing. The full dirty worktree should not be staged as one
  change.

## Commit readiness and recommended next action

**The scoped F-01/F-02 dependency change is ready to commit** based on the
manifest, image, isolation, import, and mocked-test evidence above. Stage and
review only the intended dependency, Docker, container-test, and corresponding
documentation files; do not stage the entire pre-existing dirty worktree.
Track complete dbt transitive locking as a separate follow-up. This finding
review does not certify the whole repository or the running Compose stack as
production-ready; the running services still use the older image.
