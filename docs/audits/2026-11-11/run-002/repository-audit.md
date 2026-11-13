# Review of dependency fixes for F-01 and F-02

## Metadata

- Review date: 2026-11-11
- Run number: run-002
- Git branch: `feat/airflow-orchestration`
- HEAD commit: `cc14de290bf52c86aa172f44a316ad3b074010cd`
- Governing specification: [AGENTS.md](../../../../AGENTS.md)
- Original audit: [run-001 repository audit](../run-001/repository-audit.md)
- Result: F-01 and F-02 are resolved in the current source/configuration diff; Docker image and container-level validation remain unverified.
- Scope: read-only review of the dependency-related working-tree diff, including the root and dbt/Airflow requirement manifests, `.dockerignore`, Dockerfile, Compose build configuration, related container tests, and dependency documentation. Existing modifications and the original audit were preserved.

## Finding determinations

### F-01 — RESOLVED in source/configuration; Docker build not verified

- Original issue: [run-001 F-01](../run-001/repository-audit.md#f-01--docker-cannot-reliably-parse-the-dbt-requirements-manifest)
- Evidence:
  - The root [requirements.txt](../../../../requirements.txt#L1) is now a UTF-8 compatibility include for `dbt/requirements.txt`; the Dockerfile no longer copies or installs the root file.
  - The new [dbt/requirements.txt](../../../../dbt/requirements.txt#L1) is UTF-8 and contains the dbt runtime requirements.
  - [docker/airflow.Dockerfile:8-9](../../../../docker/airflow.Dockerfile#L8) copies the Airflow and dbt manifests separately; [docker/airflow.Dockerfile:18-20](../../../../docker/airflow.Dockerfile#L18) installs the dbt manifest into `/opt/dbt`.
  - `.dockerignore` now allows `dbt/requirements.txt` and `requirements-dev.txt` into the build context.
- Determination: the originally identified UTF-16LE input is no longer used by the image build, and the replacement file is valid UTF-8. This resolves F-01 at the source/configuration level.
- Remaining validation: `docker compose build` was not run, so actual pip installation inside the image has not been demonstrated.

### F-02 — RESOLVED in source/configuration; runtime tests not verified

- Original issue: [run-001 F-02](../run-001/repository-audit.md#f-02--the-supposedly-isolated-dbt-environment-installs-airflow-and-unrelated-packages)
- Evidence:
  - [dbt/requirements.txt:2-3](../../../../dbt/requirements.txt#L2) pins only `dbt-core==1.12.6` and `dbt-snowflake==1.12.5`; the old Airflow 3.4.0/full-environment manifest is no longer installed into `/opt/dbt`.
  - [docker/airflow.Dockerfile:18-23](../../../../docker/airflow.Dockerfile#L18) creates `/opt/dbt` without `--system-site-packages`, runs `pip check`, and fails the image build if the `airflow` module is discoverable there.
  - Development-only pytest/Ruff dependencies are now in [requirements-dev.txt:3-4](../../../../requirements-dev.txt#L3) and are installed only in the optional `development` stage at [docker/airflow.Dockerfile:28-35](../../../../docker/airflow.Dockerfile#L28). The final stage is `runtime` at [docker/airflow.Dockerfile:37](../../../../docker/airflow.Dockerfile#L37); Compose has no explicit development build target.
  - [airflow/tests/test_container_runtime.py:43-65](../../../../airflow/tests/test_container_runtime.py#L43) now checks dbt versions, excludes Airflow/pytest/Ruff from `/opt/dbt`, and runs `pip check` in the two runtime Python environments.
  - `.dockerignore` includes the new manifest paths required by the Dockerfile.
- Determination: the dependency ownership and isolation defect described by F-02 is corrected in the inspected configuration. The new Airflow provider pin, `airflow-provider-fivetran-async==2.4.0`, in [airflow/requirements.txt:7](../../../../airflow/requirements.txt#L7) matches the DAG's provider use and is not an unexplained dbt dependency.
- Remaining validation: neither the image build nor the container tests were run. The added build assertions and tests are useful safeguards, but their presence is not evidence that they pass.

## Dependency and regression review

- The Airflow base remains `apache/airflow:3.3.2-python3.12`; the dbt pins remain dbt-core 1.12.6 and dbt-snowflake 1.12.5. No unintended direct-version change was found in the reviewed manifests.
- The previous root environment snapshot is replaced by a compatibility include. Airflow dependencies remain in `airflow/requirements.txt`; pytest and Ruff move to `requirements-dev.txt`; dbt runtime dependencies move to `dbt/requirements.txt`. The README now documents these ownership boundaries.
- The Docker Compose build has no `target` override, and the Dockerfile ends with `FROM runtime AS final`, so the default Compose build selects the runtime stage rather than the optional development stage.
- The new `.dockerignore` exceptions cover both files copied by runtime and development stages. No missing dependency manifest in the inspected build context was found.
- **Residual reproducibility warning:** `dbt/requirements.txt` pins the two direct dbt packages but not the complete transitive dependency graph. The README explicitly states that pip resolves transitive versions at installation time and that this is not a lockfile. `pip check` detects incompatible installed packages; it does not make dependency resolution reproducible. This is separate from the Airflow leakage in F-02, but full reproducible production pinning remains an `AGENTS.md` concern. Consider a generated constraints/lock input before treating this as a reproducible production build.
- **Documentation warning:** [airflow/README.md:222-225](../../../../airflow/README.md#L222) introduces the optional developer image as usable “without ... networking,” but its preceding `docker build` may need to download the base image, constraints, and packages unless they are already cached. Only the subsequent test container is explicitly run with `--network none`.
- The existing root `requirements.txt` entry point now installs dbt dependencies only. Developer tooling must be installed from `requirements-dev.txt`; the updated README makes that separation explicit.

## Validation provenance

### Checks executed by this reviewer

- Reviewed `git diff` for `requirements.txt`, `airflow/requirements.txt`, `docker/airflow.Dockerfile`, `.dockerignore`, `docker-compose.yaml`, `airflow/tests/test_container_runtime.py`, and `airflow/README.md`; separately inspected the new untracked `dbt/requirements.txt` and `requirements-dev.txt`.
- `git diff --check` — passed without whitespace errors; Git emitted only working-copy LF/CRLF notices.
- `docker compose config --quiet` — passed after the current changes; this validates Compose configuration, not image construction.
- Read-only byte/encoding inspection and pip's requirement parser — passed for all four manifests, including the root file's local include. No installation or network request was performed.
- In-memory Python `compile()` of the modified container test — passed with bytecode writing disabled.
- `git check-ignore` on the four dependency manifests — none is ignored.
- Inspected current `git status`, branch, and HEAD; the dependency changes are working-tree modifications/untracked files and are not represented by the HEAD commit.

### Results reported or described, not independently verified

- No Docker build log, Airflow/container test output, CI result, or separate Codex validation record for these changes was present in the inspected workspace.
- The Dockerfile's `pip check` and import assertion are build-time commands in source, and the README's build/test commands are instructions; neither is evidence of execution.
- The `docker compose config --quiet` and Python syntax results recorded in run-001 predate this dependency diff. The Compose check and modified-test syntax check were rerun by this reviewer as listed above.

### Not run

- `docker compose build`, `docker build --target development`, image startup, `pip check` inside either image, Airflow unit/container tests, and dbt parse/build.
- Image builds can fetch dependencies from external registries/package sources; they were not run in this read-only review. No external service, Snowflake, Fivetran, SMTP, or DAG was contacted or triggered.

## Conclusion

F-01 and F-02 are fixed by the current dependency separation and Dockerfile changes, but should be considered **source-level resolved, execution-unverified** until the runtime image and container tests pass. The main remaining dependency concern is the lack of a complete dbt transitive lock; the developer-image “without networking” wording should also be corrected. No implementation files were changed in this review.
