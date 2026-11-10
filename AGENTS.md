# Customer 360 Data Engineering Project

## Purpose

This repository contains a production-oriented Customer 360 platform integrating Salesforce, Adobe, Stripe, Fivetran, Snowflake, dbt, Airflow, Docker, Azure Key Vault, and Snowflake Cortex.

Prioritize reliability, security, idempotency, maintainability, least privilege, and clear ownership.

## General Rules

- Inspect the repository before editing.
- Preserve unrelated user changes.
- Prefer small, reviewable changes.
- Diagnose root causes before changing code.
- Do not introduce credentials, secrets, or private keys into the repository.
- Do not make production changes from local development without explicit authorization.
- Use explicit names and add tests for new behavior.
- Keep documentation synchronized with implementation.

## Environments

- Development database: `CUSTOMER360_TEST`
- Raw schema: `RAW`
- Transformed schema: `SILVER`
- Business schema: `GOLD`
- Production database: `CUSTOMER360_PROD`

Local dbt development must target `CUSTOMER360_TEST`. Do not write development data to `CUSTOMER360_PROD`.

## Fivetran and Snowflake

Fivetran is responsible for ingestion only. Snowflake is responsible for storage, compute, roles, privileges, database isolation, masking, governance, and query execution.

- Load Fivetran data into `RAW`.
- Keep RAW source-aligned and minimally transformed.
- Do not implement business logic in Fivetran.
- Do not add manual `COPY INTO` commands for native Fivetran table ingestion.
- Preserve Fivetran metadata columns when available.
- Configure schema evolution intentionally.
- Verify the target database before starting a sync.
- Do not use `ACCOUNTADMIN` for application workloads.
- Keep ingestion, transformation, BI, and administration roles separate.
- Use least-privilege grants and dedicated service users.

## dbt Structure and Naming

The dbt project is located in `dbt/`.

- `models/staging/`: source cleaning; `stg_<source>__<entity>.sql`
- `models/intermediate/`: reusable business logic; `int_<domain>__<logic>.sql`
- `models/marts/`: business-facing models; `dim_<entity>.sql`, `fct_<entity>.sql`, `customer_360.sql`
- `models/**/schema.yml`: documentation and tests
- `macros/`: reusable dbt macros
- `tests/`: singular data tests

Staging models must use `source()` and perform basic cleaning, casting, normalization, and deterministic deduplication. Intermediate models must use `ref()`, document identity-matching assumptions, and prevent join fan-out. Mart models must define a stable grain, explicit business keys, clear business names, and reconciled metrics.

SQL rules:

- Use lowercase SQL keywords.
- Use CTEs for multi-step transformations.
- Select explicit final columns.
- Do not use `SELECT *` in final business-facing models.
- Use explicit `PARTITION BY` and `ORDER BY` in window functions.
- Use `unique_key` for incremental models only.
- Make transformations idempotent.
- Avoid unnecessary scans and cross joins.

## dbt Testing and Lineage

Staging models should include appropriate `not_null`, `unique`, `accepted_values`, and relationship tests. Intermediate and mart models should include key, relationship, duplicate, reconciliation, join fan-out, and business-rule tests where applicable.

Create lineage with `{{ source('raw', 'table_name') }}` and `{{ ref('model_name') }}`. Do not hardcode dbt model dependencies.

Validate with:

```powershell
dbt parse
dbt build --select <model> --threads 1
```

Generate documentation with `dbt docs generate` after completing a layer.

## Airflow

Use Airflow for scheduling, dependencies, retries, alerts, backfills, and monitoring.

- Use the TaskFlow API where appropriate.
- Set `catchup=False` unless backfilling is required.
- Configure retries, retry delays, task timeouts, and failure notifications.
- Make tasks idempotent and safe to retry.
- Do not perform heavy transformations inside Airflow workers.
- Delegate transformations to Snowflake and dbt.
- Do not schedule the same dbt pipeline in two independent schedulers.
- Use logical execution dates and design backfills explicitly.

## Docker

- Use Python 3.12 unless a dependency requires another version.
- Pin production dependencies.
- Keep images minimal and use `.dockerignore`.
- Do not copy credentials, `.env`, profiles, or private keys into images.
- Do not hardcode secrets in Dockerfiles.
- Run containers as non-root where practical.
- Add health checks to long-running services.
- Separate development and production configuration.
- Never print secrets in container logs.

## Azure Key Vault

Store production secrets in Azure Key Vault or an approved secret manager. Use managed identity or workload identity where supported. Inject secrets at runtime and make them rotatable without code changes.

Never commit `.env`, `profiles.yml`, private keys, passwords, API tokens, Snowflake credentials, or Airflow connection secrets. Exclude secrets from logs, errors, and stack traces.

## Git and Pull Requests

Use feature branches such as `feat/dbt-staging-models`, `feat/dbt-intermediate-customer360`, `feat/dbt-gold-marts`, `feat/airflow-orchestration`, `feat/docker-deployment`, and `feat/azure-secrets`.

Before committing, run `git status` and `git diff`, review the staged diff, and confirm no secrets are included.

Do not commit `.venv/`, `.venv312/`, `.env`, `profiles.yml`, `keys/`, `*.p8`, `*.pem`, `target/`, `logs/`, `dbt_packages/`, or `__pycache__/`.

Do not push branches or create pull requests unless explicitly requested. When requested, create a feature branch, implement the scoped change, run tests, review the diff, commit, push, and open a PR targeting `main`. Do not merge without explicit authorization.

## Completion Checklist

Before completing a change:

- Run relevant dbt parse, build, and test commands.
- Verify the expected Snowflake database and schema.
- Review generated or compiled SQL when needed.
- Check that no credentials were exposed.
- Report files changed, commands executed, and remaining warnings or limitations.
