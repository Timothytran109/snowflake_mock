# Focused review: F-06 Stripe payment deduplication

## Metadata

- Date: 2026-11-12 (America/Los_Angeles)
- Audit run: run-001
- Branch: `feat/airflow-orchestration`
- HEAD: `907ae81254c1fa06d080e5322f4016fceb9e407f`
- Working tree at review start: ten modified tracked files, six existing
  untracked audit reports from 2026-11-11, and an untracked validation record.
  No staged changes were present. This report is the only persistent file
  created by this review.
- Instructions and original finding: [AGENTS.md](../../../../AGENTS.md) and
  [F-06 in run-001](../../2026-11-11/run-001/repository-audit.md).
- Sensitive-data handling: `.env` and credentials were not read or printed.
  No external services were contacted.

## Scope and worktree attribution

Reviewed the payment model diff and related source contract, unit-test fixtures,
schema tests, staging documentation, and downstream payment/orphan models:

- [stg_stripe__payments.sql](../../../../dbt/models/staging/stg_stripe__payments.sql)
- [_sources.yml](../../../../dbt/models/staging/_sources.yml)
- [unit_tests.yml](../../../../dbt/models/staging/unit_tests.yml)
- [schema.yml](../../../../dbt/models/staging/schema.yml)
- [staging README](../../../../dbt/models/staging/README.md)
- [int_stripe__payments.sql](../../../../dbt/models/intermediate/int_stripe__payments.sql)
- [fct_payments.sql](../../../../dbt/models/marts/fct_payments.sql)

The F-06 diff is confined to five files under `dbt/models/staging/`: the
payment model, staging source/model descriptions, staging README, and unit
tests. The working tree also contains unrelated modified Airflow and Compose
files; they are outside this review and are not attributed to F-06. No
intermediate or mart implementation file is changed.

## F-06 — RESOLVED

The payment window preserves the existing leading order, `_fivetran_synced
desc` followed by `payment_at desc nulls last`, then adds `_line desc nulls
last` ([model lines 81–86](../../../../dbt/models/staging/stg_stripe__payments.sql#L81)).
Comparison against `HEAD` confirms the two pre-existing ordering expressions
were not removed or reordered.

The source contract guarantees `_line` and `_fivetran_synced`, but explicitly
does not claim that `_line` is globally unique or provide a file/sheet
identifier or unique ingestion-row ID
([source lines 12–18](../../../../dbt/models/staging/_sources.yml#L12)).
The implementation therefore uses `_line` only as an ordering preference.
When lines tie, it orders by every remaining normalized output value:
`customer_id`, `email`, `subscription_id`, `amount`, `currency`, and `status`.
Strings use explicit UTF-8 collation; each of these fields, including `_line`,
uses descending order with nulls last
([model lines 86–94](../../../../dbt/models/staging/stg_stripe__payments.sql#L86)).
Because the partition key `payment_id`, leading timestamp fields, and all
remaining output values are then equal, any residual tie yields the same
projected output, regardless of which physical duplicate is selected.

The existing implicit null ordering of `_fivetran_synced desc` is preserved
rather than changed. The README calls out that inherited behavior and clearly
describes the later line/content ordering as a deterministic fallback, not
proof of recency or business correctness
([README lines 19–33](../../../../dbt/models/staging/README.md#L19)).
`payment_at`, `_line`, and content tie-breakers explicitly sort nulls last.
Fixtures cover null `payment_at`, null `_line`, and a null content value.
There is no new direct fixture with null `_fivetran_synced`; its implicit
Snowflake null ordering remains a limitation if environment-independent
ordering for missing sync timestamps is required.

### Tests, schema, and orphan behavior

Four payment unit tests are added, with no deletions in the unit-test file
(`git diff --numstat` showed 148 added lines and zero removed):

- The first asserts sync timestamp precedence before payment timestamp
  precedence ([tests lines 4–31](../../../../dbt/models/staging/unit_tests.yml#L4)).
- The second asserts the higher line wins conflicting tied-timestamp rows,
  null-line handling, normalized-identical duplicates, and retention of an
  orphan payment ([tests lines 32–55](../../../../dbt/models/staging/unit_tests.yml#L32)).
- The third covers repeated lines with a distinct expected winner for each of
  the six remaining output fields and null content/line values
  ([tests lines 56–103](../../../../dbt/models/staging/unit_tests.yml#L56)).
- The fourth reverses those conflicting rows and expects the same winners
  ([tests lines 104–151](../../../../dbt/models/staging/unit_tests.yml#L104)).

The final projection remains the same nine columns; `_line` is carried only
internally for ranking and is not exposed
([model lines 68–79](../../../../dbt/models/staging/stg_stripe__payments.sql#L68)).
The schema edit changes the payment model description only. `payment_id`
`not_null`/`unique` tests and customer/subscription relationship tests remain
unchanged ([schema lines 4–35](../../../../dbt/models/staging/schema.yml#L4)).
The model has no orphan filter; the existing intermediate model still
classifies orphans, and Gold's existing payment model still excludes them.
No tests or downstream orphan handling were weakened in the scoped F-06 diff.

The documentation accurately states that the source line is not globally
unique, lists the ordering and null behavior, and distinguishes deterministic
content selection from business recency. It also explicitly says `dbt parse`
does not execute the payment unit tests and directs warehouse execution to an
authorized TEST environment ([README lines 19–33 and 64–77](../../../../dbt/models/staging/README.md#L19)).

## Validation evidence

### Independently executed

- Credential-free dbt parse using the existing runtime image, `--network none`,
  a read-only dbt project mount, and a temporary dummy profile:
  `test_mounted_dbt_project_parses_without_external_credentials` — **1 passed**.
- A second credential-free `dbt parse --no-partial-parse`, followed by
  `dbt ls --resource-type unit_test --output name`, listed all four
  `stripe_payments_*` unit-test nodes. This verifies project/fixture discovery,
  not SQL execution.
- `git diff --check -- dbt/models/staging` — passed.
- Compared the model diff and baseline projection: the nine output columns and
  original ordering expressions remain unchanged. The schema diff only updates
  the payment model description; the unit-test diff adds four cases without
  removing any.
- Confirmed `git diff --name-only HEAD -- dbt` contains only the five staging
  files listed above. No `dbt build` or `dbt test` was run.

One exploratory manifest inspection initially assumed the payment unit-test
names would appear in a particular `manifest.nodes` shape and found none. The
project parse itself had succeeded; I corrected the inspection by querying the
supported `dbt ls --resource-type unit_test --output name`, which listed all
four expected tests.

### Codex-reported, not independently reproduced

The untracked [stripe-payment-deduplication-validation.txt](../../../validation/2026-11-11/stripe-payment-deduplication-validation.txt)
reports SQLGlot parsing, 400 SQLite ranking/synthetic-fixture checks, source
and schema comparisons, and four unit tests parsing. Those additional checks
were not rerun here. SQLite simulation and SQLGlot parsing are not Snowflake
execution evidence.

**Warehouse test execution: NOT VERIFIED.** The payment dbt unit tests, data
tests, and model query were not executed against Snowflake. No warehouse test
is reported as passed by this review. The README and the validation record both
state that offline parsing does not establish Snowflake unit-test results.

## Limitations and readiness

- `_fivetran_synced desc` retains its existing implicit null ordering. The
  source contract does not state whether this field is nullable; no fixture
  explicitly tests a null sync timestamp. If null sync values are possible and
  results must be independent of Snowflake session defaults, define and test
  that null policy in a separate, deliberate change.
- A deterministic content order is not a semantic rule for which conflicting
  payment row is correct. The documentation states this limitation.
- Snowflake execution semantics, adapter compilation, casts, and all warehouse
  unit/data-test results remain unverified. No Fivetran, Snowflake, SMTP, or
  other external service was contacted.
- The broader working-tree Airflow/Compose edits are outside scope and should
  not be bundled automatically with F-06.

**Ready to commit:** **Yes, for the scoped F-06 changes**, provided only the
five reviewed staging files are staged separately from the unrelated worktree
changes. This does not constitute a Snowflake execution or production
validation sign-off.

**Recommended next action:** stage and review only the five F-06 staging files.
Before relying on the model in TEST, run the documented
`dbt test --select stg_stripe__payments,test_type:unit --threads 1` against an
explicitly authorized TEST profile; report that result separately from this
offline parse.
