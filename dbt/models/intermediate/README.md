# Orphan handling and trusted facts

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

`CUSTOMER360_TEST.SILVER.INT_DATA_QUALITY__ORPHAN_RECORDS` is a table with one
row per `(source_system, record_type, record_id)`. `exception_id` is a stable
SHA-256 of that tuple serialized as a JSON array. It includes `parent_key`,
`exception_reason` and `detected_at`. A missing required foreign key can have
a null `parent_key`; staging required-key tests still fail such input.

For a payment with several invalid relationships, one primary reason is
chosen: missing customer, missing subscription, orphaned subscription, then
subscription/customer mismatch. The associated parent key identifies the
customer for the first reason and the subscription for the other reasons.
The detailed Silver flags retain every applicable condition.

`detected_at` is the UTC start of the dbt invocation that rebuilt the exception
table. This is a current-state evaluation snapshot, not first detection or a
historical audit log. Rebuilding does not append duplicate exceptions. If an
upstream source correction arrives, the next complete build recalculates the
flags, removes resolved exceptions, and includes eligible facts in Gold.
No parent is synthesized and no RAW record is edited by this workflow.

Three staging relationship tests use warning severity so intentional missing
parents do not prevent the exception workflow from running. The tests still
execute over every row. Keeping them as errors would stop dependent dbt
models before exceptions and trusted outputs could be refreshed. No global
warning setting or key-specific exclusion is used. Other staging tests,
all exception checks, and all Gold integrity checks remain errors.

Gold contains `FCT_OPPORTUNITIES`, `FCT_SUBSCRIPTIONS`, and `FCT_PAYMENTS`, each
at one row per source business key. All exclude `is_orphan = true`; Gold
payments additionally require any subscription reference to resolve in Gold
to the same customer. Customer/account relationships point to the validated
Silver parent models; no cross-system identity resolution is implied.
Amounts retain their source currency and must not be summed across currencies
without an explicit conversion. Statuses are preserved, including failed
payments; these models do not define a revenue recognition rule.

Gold relationship tests explicitly use error severity. Bidirectional row
comparisons reconcile Gold to the full valid Silver subset, including metrics.
An independent staging-based test checks exception completeness and reasons.
Unit tests cover valid/orphan facts, null keys, parent duplicates, optional
subscriptions, transitive orphans, customer mismatches, and Gold exclusions.

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
build with warnings means trusted outputs exclude incomplete facts; it does
not mean source referential integrity was repaired. Initially the snapshot
contains `OP_2002 -> SF_1002`, `SUB_7003 -> ST_9003`, and `PAY_8003 -> ST_9003`.
