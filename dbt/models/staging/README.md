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
The existing payments SQL and its deduplication behavior are unchanged.

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

The build includes six unit tests with synthetic fixtures covering date
formats, null dates, normalization, duplicate winners, shared emails and
equal opportunity amounts. Data tests check keys, required fields, accepted
values, relationships, source-key coverage across all seven models, and
optional-date parsing on the selected source rows. Generated docs, logs and
compiled SQL stay in ignored local directories.

The initial live validation built all seven views and passed all six unit
tests and 53 of 56 data tests. Three relationship tests correctly failed on
missing RAW parent records: opportunity `OP_2002` references absent account
`SF_1002`; subscription `SUB_7003` and payment `PAY_8003` reference absent
customer `ST_9003`. The same gaps exist in RAW even when comparing IDs without
case sensitivity. Correct these records in the source/ingestion workflow,
then rerun the staging build. No rows are dropped, parents invented, or test
severities reduced to conceal these gaps.

Expected views under `CUSTOMER360_TEST.SILVER`:

- `STG_STRIPE__CUSTOMERS`
- `STG_STRIPE__SUBSCRIPTIONS`
- `STG_STRIPE__PAYMENTS`
- `STG_SALESFORCE__ACCOUNTS`
- `STG_SALESFORCE__OPPORTUNITIES`
- `STG_ADOBE__EVENTS`
- `STG_ADOBE__CAMPAIGNS`
