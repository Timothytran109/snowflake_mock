# Data-quality exceptions and trusted facts

RAW ingestion and the seven staging transformations remain unchanged. Identity
is an exact match on the cleaned source ID, never on email, amount or name.

Silver preserves every staged fact in `int_salesforce__opportunities`,
`int_stripe__subscriptions`, and `int_stripe__payments`, adding `is_orphan`.
Parent key sets are distinct; the intermediate subscription primary-key tests
enforce the payment join grain. Error-level row comparisons preserve all
original fact values, and primary-key tests detect join fan-out.

Opportunities require a staged Salesforce account. Subscriptions and payments
require a staged Stripe customer. A null payment subscription ID is allowed.
A non-null subscription ID must exist, have a valid customer, and belong to
the same customer as the payment. Payments expose separate flags for each
condition as well as the combined `is_orphan` flag.

`CUSTOMER360_TEST.SILVER.INT_DATA_QUALITY__ORPHAN_RECORDS` retains its historical
name and now covers missing dates as well as missing-parent relationships. Its
grain is `(source_system, record_type, record_id, exception_reason)`;
`exception_reason` is the machine-readable exception type. `exception_id` is a
stable SHA-256 of that four-element tuple serialized as a JSON array. Adding the
reason changes existing IDs on the first rebuild; consumers must refresh IDs
and must not assume one exception row per fact. The output column names remain
unchanged. `parent_key` and `detected_at` retain their existing meanings.

All existing missing-parent reasons remain. Payments now emit a separate row
for every applicable relationship flag instead of suppressing lower-priority
reasons: `missing_customer`, `missing_subscription`, `orphaned_subscription`,
and `subscription_customer_mismatch`. The first uses the customer ID as
`parent_key`; the others use the subscription ID. Opportunities still report
`missing_account`, and subscriptions report `missing_customer`. Missing required
foreign keys may have a null `parent_key`; required-key tests remain errors.

Three additional reason/entity combinations report null **parsed** dates:

- Adobe `event`: `missing_event_timestamp` when `event_timestamp` is null.
- Stripe `customer`: `missing_created_at` when `created_at` is null.
- Salesforce `account`: `missing_updated_at` when `updated_at` is null.

Date exceptions use a null `parent_key`. Null, blank or unsupported source dates
that parse to null are included. Staging retains those records and null dates;
no date is fabricated or replaced by `loaded_at` or an ingestion timestamp.

`detected_at` is the UTC start of the dbt invocation that rebuilt the exception
table. This is a current-state evaluation snapshot, not first detection or a
historical audit log. Rebuilding does not append duplicate exceptions. If an
upstream source correction arrives, the next complete build recalculates the
flags, removes resolved exceptions, and includes eligible facts in Gold.
No parent is synthesized and no RAW record is edited by this workflow.

Four staging relationship tests and only the three corresponding staging
date `not_null` tests use warning severity so these exceptions do not prevent
the exception workflow from running. The tests still
execute over every row. Keeping them as errors would stop dependent dbt
models before exceptions and trusted outputs could be refreshed. No global
warning setting or key-specific exclusion is used. Other staging tests,
all exception checks, and all Gold integrity checks remain errors.

The payment-to-subscription staging relationship is one of those warnings.
A payment with a non-null, missing subscription stays in Silver with
`is_subscription_missing = true` and `is_orphan = true`, produces a
`missing_subscription` exception whose `parent_key` is the subscription ID,
and is excluded from Gold. Once the subscription is restored for the same valid
customer, a complete rebuild clears the flags and exception and makes the
payment eligible for Gold. A null optional subscription ID remains allowed.

Gold contains `FCT_OPPORTUNITIES`, `FCT_SUBSCRIPTIONS`, and `FCT_PAYMENTS`, each
at one row per source business key. All exclude `is_orphan = true`; Gold
payments additionally require any subscription reference to resolve in Gold
to the same customer. Customer/account relationships point to the validated
Silver parent models; no cross-system identity resolution is implied.
Missing customer `created_at` does not invalidate that customer as a parent;
missing account `updated_at` does not invalidate that account. Do not anti-join
all exception records out of Gold or mark date-only exceptions as orphans.
The current three Gold facts use none of these three parent/event dates, so
their eligibility filters and strict row reconciliations remain unchanged.
Future event-time metrics must require `event_timestamp is not null`; customer
creation cohorts must require `created_at is not null`; account update-age
metrics must require `updated_at is not null`. Apply those exclusions only to
the metric needing the date, keeping the records eligible for other metrics.
Amounts retain their source currency and must not be summed across currencies
without an explicit conversion. Statuses are preserved, including failed
payments; these models do not define a revenue recognition rule.

Gold relationship tests explicitly use error severity. Bidirectional row
comparisons reconcile Gold to the full valid Silver subset, including metrics.
An independent staging-based test reconciles all reasons, parent keys, derived
IDs and per-reason row multiplicities in both directions. Duplicate, missing,
unexpected or stale exception rows fail. Exception counts are not excluded-fact
counts: one fact may have several reasons, while a date-only exception need not
exclude a fact at all. Count affected records at their source/entity/key grain
separately from exception rows.
Unit tests cover valid/orphan facts, null keys, parent duplicates, optional
subscriptions, transitive orphans, customer mismatches, and Gold exclusions.
Additional fixtures cover null/blank/unparseable dates, retained parents with
missing dates, multiple reasons, and resolved current-state exceptions.

Run mocked relational checks first, using the existing image without networking
or mounted credentials, from the repository root:

```powershell
docker run --rm --network none --entrypoint /opt/dbt/bin/python --mount "type=bind,source=$($PWD.Path)/dbt,target=/opt/customer360/dbt,readonly" customer360-airflow:3.3.2-python3.12 -m unittest discover -s /opt/customer360/dbt/local_tests -v
```

These execute the checked-in intermediate/Gold/completeness SQL with synthetic
SQLite inputs and narrow adaptations for Snowflake hash/JSON/null-equality
functions and the evaluation timestamp cast. They check relational behavior,
not Snowflake execution semantics or dbt fixture compilation. Credential-free
`dbt parse --no-partial-parse` validates definitions only. Live dbt unit/data
tests, source compatibility, grants and date parsing on Snowflake remain
deployment validation; no live results are implied by either offline check.

Run from `dbt/` with the verified `dev` target:

```powershell
dbt parse --no-partial-parse
dbt build --select staging intermediate --threads 1
dbt build --select marts --threads 1
dbt docs generate
```

For a complete refresh in one invocation:

```powershell
dbt build --select staging intermediate marts --threads 1
```

`indirect_selection: buildable` limits layer builds to tests whose dependencies
are selected models or their ancestors; it avoids testing unbuilt downstream
Gold tables during a Silver-only build. Always build/test Gold before using
its refreshed outputs. Tables reflect their last successful build, not live
RAW changes, and dbt does not publish all models as one atomic transaction.

The schema macro maps `+schema: GOLD` to `CUSTOMER360_TEST.GOLD` only for the
verified `dev` / `CUSTOMER360_TEST` / `SILVER` profile. Other target/schema
combinations retain dbt's default schema-prefix isolation. No production
profile or Snowflake grant is changed.

Source owners should review the exception table and dbt warnings. A successful
build with approved warnings means relationship-invalid facts remain excluded
from Gold and date-only exceptions remain eligible for date-independent facts;
it does not repair source data. Historical orphans included `OP_2002 -> SF_1002`,
`SUB_7003 -> ST_9003`, and `PAY_8003 -> ST_9003`. Current XLSM exception counts
must be measured in an authorized build; no fixed count is assumed.
