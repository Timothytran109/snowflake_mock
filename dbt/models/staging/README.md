# Customer 360 staging

These seven views read Fivetran mock exports from `CUSTOMER360_TEST.RAW`
through `source('raw', ...)` and build in `CUSTOMER360_TEST.SILVER` with the
local `dev` profile. Confirm the profile database, schema and role before
running; the profile and credentials remain outside the repository.

The six new models retain one row per trimmed source ID. Salesforce accounts
are keyed by `account_id`, never email; opportunities are keyed by
`opportunity_id`, never account or amount. Blank identifiers become null and
fail the required-key tests rather than being silently filtered out.

Duplicate IDs select the latest `_fivetran_synced`, then the latest source
date, then the highest `_line`. Accounts use `updated_at` before `created_at`.
Null timestamps sort last. The inspected exports have unique, non-null
`_line` values. The six models expose that line and preserve
`_fivetran_synced` as `loaded_at`. No inspected table has `_fivetran_deleted`.

Payments preserve their existing priority: `_fivetran_synced desc`, then
`payment_at desc nulls last`. The sync timestamp retains its existing implicit
null ordering. Ties then prefer the highest `_line` (nulls last), following the
other staging models' line-number convention. The checked-in source contract
guarantees `_line` and `_fivetran_synced`, but no file/sheet identifier or unique
ingestion row ID; a line number alone is not unique across files or sheets.
If lines also tie, compare the complete normalized tuple `customer_id`, `email`,
`subscription_id`, `amount`, `currency`, `status`, each descending with nulls last.
String comparisons explicitly use UTF-8 collation so case/accent-insensitive
deployment defaults cannot equate distinct output values (see
[Snowflake collation](https://docs.snowflake.com/en/sql-reference/collation)).
This content ordering is a reproducible fallback, not evidence of recency or
business correctness. Normalized-identical rows yield the same output regardless
of the selected physical row. `_line` is internal to payments deduplication;
the nine output columns, cleaning and casts remain unchanged.

Emails are trimmed and lowercased; display names retain their case. Stages,
event types and campaign channels use lowercase underscore-separated values.
Plans, statuses and device types are trimmed and lowercased. Accepted-value
tests describe the current mock source contract; new legitimate categories
require an explicit contract update.

`staging_timestamp` parses the observed ISO, US month/day/year and
year/month/day formats, including timestamps. It returns `TIMESTAMP_NTZ`
without inferring a timezone. Subscription and campaign boundaries are cast
to `DATE`. Missing or blank optional dates stay null. Required-date tests
and the optional-date parsing test detect unsuccessful conversions.
Opportunity amounts become `DECIMAL(18,2)` after removing dollar signs,
commas and surrounding spaces; invalid amounts fail the non-null test.

Validation from `dbt/`, using the installed dbt 1.12 environment:

```powershell
dbt parse --no-partial-parse
dbt build --select staging --threads 1
dbt docs generate
```

The build includes six existing unit tests with synthetic fixtures covering date
formats, null dates, normalization, duplicate winners, shared emails and
equal opportunity amounts. Data tests check keys, required fields, accepted
values, relationships, source-key coverage across all seven models, and
optional-date parsing on the selected source rows. Generated docs, logs and
compiled SQL stay in ignored local directories.

Four payment unit tests additionally cover timestamp precedence, tied timestamps
with conflicting rows and different lines, repeated/null lines with conflicts in
each remaining output field, and reversed input order for the same conflicts.
They also retain an orphan payment and collapse normalized-identical rows. Run
these tests against an explicitly authorized TEST Snowflake environment with:

```powershell
dbt test --select stg_stripe__payments,test_type:unit --threads 1
```

`dbt parse --no-partial-parse` validates project and fixture definitions without
executing these tests. Snowflake execution is required to verify the model's
casting, ordering and unit-test results on the actual adapter; an offline parse
does not establish that the unit tests pass.

Three intentional missing-parent relationships remain visible as warnings:
opportunity `OP_2002` references absent account `SF_1002`; subscription
`SUB_7003` and payment `PAY_8003` reference absent customer `ST_9003`.
Only these three relationship test definitions use warning severity; they
still report every missing parent, without an ID allowlist. Other staging
tests, including the payment-to-subscription relationship, remain errors.

RAW data and the orphan-handling policy are unchanged. Silver intermediate models
retain all staged facts and attach explicit orphan flags. The exception snapshot
records affected facts, and trusted Gold fact models exclude them with
error-level integrity and reconciliation tests. See
[orphan handling](../intermediate/README.md) for the policy and build commands.

Expected views under `CUSTOMER360_TEST.SILVER`:

- `STG_STRIPE__CUSTOMERS`
- `STG_STRIPE__SUBSCRIPTIONS`
- `STG_STRIPE__PAYMENTS`
- `STG_SALESFORCE__ACCOUNTS`
- `STG_SALESFORCE__OPPORTUNITIES`
- `STG_ADOBE__EVENTS`
- `STG_ADOBE__CAMPAIGNS`
