"""Offline relational checks, not a replacement for dbt tests on Snowflake.

Execute the actual intermediate/mart and completeness SQL on synthetic SQLite
tables. Only the evaluation timestamp cast and Snowflake JSON/hash/null-equality
functions are adapted. Snowflake casts, dbt fixture compilation, grants and live
source data are outside this test boundary. Run with the isolated dbt Python.
"""

import hashlib
import json
import re
import sqlite3
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import yaml
from jinja2 import Environment, StrictUndefined

PROJECT = Path(__file__).resolve().parents[1]
SNAPSHOT = "int_data_quality__orphan_records"


class ExceptionSqlTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function(
            "array_construct",
            -1,
            lambda *values: json.dumps(values, separators=(",", ":")),
        )
        self.db.create_function("to_json", 1, lambda value: value)
        self.db.create_function(
            "sha2",
            2,
            lambda value, bits: (
                hashlib.sha256(value.encode()).hexdigest() if bits == 256 else None
            ),
        )
        self.db.create_function("equal_null", 2, lambda left, right: left == right)
        self.schemas = {}
        for layer in ("staging", "intermediate", "marts"):
            schema = yaml.safe_load(
                (PROJECT / "models" / layer / "schema.yml").read_text()
            )
            for model in schema["models"]:
                columns = [column["name"] for column in model["columns"]]
                self.schemas[model["name"]] = columns
                definitions = [
                    f'"{column}" '
                    + (
                        "real"
                        if column == "amount"
                        else "integer"
                        if column.startswith("is_") or column == "_line"
                        else "text"
                    )
                    for column in columns
                ]
                self.db.execute(
                    f'create table "{model["name"]}" ({", ".join(definitions)})'
                )

    def render(self, path, detected_at="2026-11-12T00:00:00+00:00"):
        environment = Environment(undefined=StrictUndefined)
        environment.globals.update(
            ref=lambda name: f'"{name}"',
            config=lambda **kwargs: "",
            run_started_at=SimpleNamespace(isoformat=lambda: detected_at),
        )
        sql = environment.from_string(path.read_text()).render()
        return re.sub(r"::timestamp_tz\b", "", sql)

    def model(self, name, detected_at="2026-11-12T00:00:00+00:00"):
        path = next((PROJECT / "models").rglob(name + ".sql"))
        return [dict(row) for row in self.db.execute(self.render(path, detected_at))]

    def replace(self, name, rows):
        self.db.execute(f'delete from "{name}"')
        for row in rows:
            columns = list(row)
            self.assertTrue(set(columns).issubset(self.schemas[name]))
            values = [
                value.isoformat() if hasattr(value, "isoformat") else value
                for value in row.values()
            ]
            quoted = ", ".join(f'"{column}"' for column in columns)
            self.db.execute(
                f'insert into "{name}" ({quoted}) values ({", ".join("?" for _ in columns)})',
                values,
            )

    def fixtures(self, layer):
        return yaml.safe_load(
            (PROJECT / "models" / layer / "unit_tests.yml").read_text()
        )["unit_tests"]

    def load_fixture(self, fixture):
        for name in self.schemas:
            self.replace(name, [])
        for source in fixture["given"]:
            match = re.fullmatch(r"ref\('([^']+)'\)", source["input"])
            self.assertIsNotNone(match)
            self.replace(match[1], source["rows"])

    def completeness(self):
        return self.db.execute(
            self.render(PROJECT / "tests/orphan_records_complete.sql")
        ).fetchall()

    def load_coherent_pipeline(self):
        # C1 and A1 are valid parents despite their date exceptions.
        self.replace(
            "stg_stripe__customers", [{"customer_id": "C1", "created_at": None}]
        )
        self.replace(
            "stg_salesforce__accounts",
            [{"account_id": "A1", "updated_at": None, "created_at": "2026-01-01"}],
        )
        self.replace(
            "stg_adobe__events",
            [{"event_id": "E1", "campaign_id": "CAM1", "event_timestamp": None}],
        )
        self.replace(
            "stg_salesforce__opportunities",
            [
                {"opportunity_id": "O1", "account_id": "A1", "amount": 10},
                {"opportunity_id": "OX", "account_id": "AX", "amount": 20},
            ],
        )
        self.replace(
            "stg_stripe__subscriptions",
            [
                {"subscription_id": "S1", "customer_id": "C1"},
                {"subscription_id": "SX", "customer_id": "CX"},
            ],
        )
        self.replace(
            "stg_stripe__payments",
            [
                {
                    "payment_id": "P1",
                    "customer_id": "C1",
                    "subscription_id": "S1",
                    "amount": 99,
                },
                {
                    "payment_id": "standalone",
                    "customer_id": "C1",
                    "subscription_id": None,
                    "amount": 99,
                },
                {
                    "payment_id": "PX",
                    "customer_id": "CX",
                    "subscription_id": "SX",
                    "amount": 99,
                },
                {
                    "payment_id": "missing_both",
                    "customer_id": "CX",
                    "subscription_id": "unknown",
                    "amount": 99,
                },
                {
                    "payment_id": "mismatch",
                    "customer_id": "C1",
                    "subscription_id": "SX",
                    "amount": 99,
                },
            ],
        )
        for model in (
            "int_salesforce__opportunities",
            "int_stripe__subscriptions",
            "int_stripe__payments",
        ):
            self.replace(model, self.model(model))
        self.replace(SNAPSHOT, self.model(SNAPSHOT))

    def test_intermediate_and_gold_dbt_unit_fixtures_offline(self):
        for layer in ("intermediate", "marts"):
            for fixture in self.fixtures(layer):
                with self.subTest(fixture=fixture["name"]):
                    self.load_fixture(fixture)
                    actual = self.model(fixture["model"])
                    expected = fixture["expect"]["rows"]
                    columns = sorted({key for row in expected for key in row})
                    self.assertEqual(len(actual), len(expected))
                    self.assertEqual(
                        Counter(
                            tuple(row.get(key) for key in columns) for row in actual
                        ),
                        Counter(
                            tuple(row.get(key) for key in columns) for row in expected
                        ),
                    )

    def test_ids_include_reason_and_survive_input_reordering_and_rebuild(self):
        fixture = next(
            test
            for test in self.fixtures("intermediate")
            if test["name"]
            == "exception_snapshot_keeps_all_parent_reasons_and_healthy_dates"
        )
        self.load_fixture(fixture)
        before = self.model(SNAPSHOT)
        for source in fixture["given"]:
            name = re.fullmatch(r"ref\('([^']+)'\)", source["input"])[1]
            self.replace(name, list(reversed(source["rows"])))
        after = self.model(SNAPSHOT, "2026-11-13T00:00:00+00:00")
        self.assertEqual(
            {row["exception_id"] for row in before},
            {row["exception_id"] for row in after},
        )
        self.assertEqual(len({row["exception_id"] for row in before}), len(before))
        payment = [
            row
            for row in before
            if row["record_type"] == "payment"
            and row["record_id"] == "customer_missing"
        ]
        self.assertEqual(
            {row["exception_reason"] for row in payment},
            {"missing_customer", "orphaned_subscription"},
        )
        for row in before:
            encoded = json.dumps(
                [
                    row[key]
                    for key in (
                        "source_system",
                        "record_type",
                        "record_id",
                        "exception_reason",
                    )
                ],
                separators=(",", ":"),
            )
            self.assertEqual(
                row["exception_id"], hashlib.sha256(encoded.encode()).hexdigest()
            )

    def test_resolved_dates_disappear_without_changing_silver_records(self):
        fixture = next(
            test
            for test in self.fixtures("intermediate")
            if test["name"]
            == "exception_snapshot_records_missing_dates_without_ingestion_fallback"
        )
        self.load_fixture(fixture)
        before = self.model(SNAPSHOT)
        self.assertEqual(len(before), 3)
        self.assertEqual(len({row["exception_id"] for row in before}), 3)
        self.replace(SNAPSHOT, before)
        for table, column in (
            ("stg_adobe__events", "event_timestamp"),
            ("stg_stripe__customers", "created_at"),
            ("stg_salesforce__accounts", "updated_at"),
        ):
            self.db.execute(
                f'update "{table}" set "{column}" = ? where "{column}" is null',
                ("2026-01-01",),
            )
            self.assertEqual(
                self.db.execute(f'select count(*) from "{table}"').fetchone()[0], 2
            )
        self.replace(SNAPSHOT, self.model(SNAPSHOT))
        self.assertEqual(
            self.db.execute(f'select count(*) from "{SNAPSHOT}"').fetchone()[0], 0
        )

    def test_resolving_one_reason_preserves_other_reason_and_its_id(self):
        self.load_coherent_pipeline()
        original = [row for row in self.model(SNAPSHOT) if row["record_id"] == "PX"]
        self.assertEqual(len(original), 2)
        self.db.execute(
            "update int_stripe__payments set is_subscription_orphan = 0 where payment_id = 'PX'"
        )
        remaining = [row for row in self.model(SNAPSHOT) if row["record_id"] == "PX"]
        self.assertEqual(
            remaining,
            [row for row in original if row["exception_reason"] == "missing_customer"],
        )

    def test_missing_parent_dates_do_not_invalidate_date_independent_gold_facts(self):
        self.load_coherent_pipeline()
        for model in ("fct_subscriptions", "fct_payments", "fct_opportunities"):
            self.replace(model, self.model(model))
        self.assertEqual(
            {
                row[0]
                for row in self.db.execute(
                    "select subscription_id from fct_subscriptions"
                )
            },
            {"S1"},
        )
        self.assertEqual(
            {row[0] for row in self.db.execute("select payment_id from fct_payments")},
            {"P1", "standalone"},
        )
        self.assertEqual(
            {
                row[0]
                for row in self.db.execute(
                    "select opportunity_id from fct_opportunities"
                )
            },
            {"O1"},
        )
        self.assertIsNone(
            self.db.execute(
                "select created_at from stg_stripe__customers where customer_id = 'C1'"
            ).fetchone()[0]
        )
        self.assertIsNone(
            self.db.execute(
                "select updated_at from stg_salesforce__accounts where account_id = 'A1'"
            ).fetchone()[0]
        )

    def payment_view_fixture(self):
        self.load_coherent_pipeline()
        columns = ", ".join(f'"{name}"' for name in self.schemas["stg_stripe__payments"])
        self.db.execute(
            f"create table raw_payment_updates as select {columns} from stg_stripe__payments"
        )
        self.db.execute("drop table stg_stripe__payments")
        self.db.execute(
            f"create view stg_stripe__payments as select {columns} from raw_payment_updates"
        )
        self.refresh_downstream_tables()

    def refresh_downstream_tables(self):
        for name in (
            "int_salesforce__opportunities", "int_stripe__subscriptions",
            "int_stripe__payments", SNAPSHOT, "fct_opportunities",
            "fct_subscriptions", "fct_payments",
        ):
            self.replace(name, self.model(name))

    def add_new_raw_payment(self):
        self.db.execute(
            "insert into raw_payment_updates "
            "(payment_id, customer_id, email, subscription_id, amount, currency, status, payment_at, loaded_at) "
            "values ('PAY_1000', 'C1', 'a@example.invalid', 'S1', 100, 'USD', 'succeeded', "
            "'2026-11-12 20:30:00', '2026-11-12 20:31:00 +00:00')"
        )

    def assert_new_payment_present(self, name, expected):
        self.assertEqual(
            self.db.execute(
                f"select count(*) from {name} where payment_id = 'PAY_1000'"
            ).fetchone()[0],
            expected,
        )

    def test_payment_available_before_build_reaches_intermediate_and_gold(self):
        self.payment_view_fixture()
        self.add_new_raw_payment()
        self.refresh_downstream_tables()
        for name in ("stg_stripe__payments", "int_stripe__payments", "fct_payments"):
            self.assert_new_payment_present(name, 1)
        self.assertFalse(self.completeness())

    def test_missing_subscription_is_retained_excepted_and_resolves_after_restoration(self):
        self.load_coherent_pipeline()
        self.db.execute(
            "insert into stg_stripe__payments "
            "(payment_id, customer_id, subscription_id, amount) "
            "values ('subscription_missing', 'C1', 'restored_subscription', 125)"
        )
        for restored in (False, True):
            with self.subTest(subscription_restored=restored):
                if restored:
                    # Restore a synthetic parent, never real RAW data.
                    self.db.execute(
                        "insert into stg_stripe__subscriptions (subscription_id, customer_id) "
                        "values ('restored_subscription', 'C1')"
                    )
                self.refresh_downstream_tables()
                silver = dict(
                    self.db.execute(
                        "select * from int_stripe__payments where payment_id = 'subscription_missing'"
                    ).fetchone()
                )
                self.assertEqual(silver["amount"], 125)
                self.assertEqual(silver["customer_id"], "C1")
                self.assertEqual(silver["subscription_id"], "restored_subscription")
                self.assertEqual(bool(silver["is_subscription_missing"]), not restored)
                self.assertEqual(bool(silver["is_orphan"]), not restored)
                for flag in (
                    "is_customer_orphan", "is_subscription_orphan",
                    "is_subscription_customer_mismatch",
                ):
                    self.assertFalse(silver[flag])
                exceptions = [
                    dict(row)
                    for row in self.db.execute(
                        f"select * from {SNAPSHOT} where record_id = 'subscription_missing'"
                    )
                ]
                if restored:
                    self.assertEqual(exceptions, [])
                else:
                    self.assertEqual(len(exceptions), 1)
                    self.assertEqual(
                        {
                            key: exceptions[0][key]
                            for key in (
                                "source_system", "record_type", "parent_key", "exception_reason"
                            )
                        },
                        {
                            "source_system": "stripe", "record_type": "payment",
                            "parent_key": "restored_subscription",
                            "exception_reason": "missing_subscription",
                        },
                    )
                gold = self.db.execute(
                    "select amount from fct_payments where payment_id = 'subscription_missing'"
                ).fetchall()
                self.assertEqual([row[0] for row in gold], [125] if restored else [])
                self.assertFalse(self.completeness())
                # Exercise the unchanged strict Gold consistency SQL and full valid subset.
                self.assertFalse(
                    self.db.execute(self.render(
                        PROJECT / "tests/gold_payment_subscription_customer.sql"
                    )).fetchall()
                )
                columns = self.schemas["fct_payments"]
                self.assertEqual(
                    Counter(
                        tuple(row[column] for column in columns)
                        for row in self.db.execute("select * from fct_payments")
                    ),
                    Counter(
                        tuple(row[column] for column in columns)
                        for row in self.db.execute(
                            "select * from int_stripe__payments where not is_orphan"
                        )
                    ),
                )

    def test_payment_arriving_after_build_updates_view_but_waits_for_next_table_build(self):
        self.payment_view_fixture()
        self.add_new_raw_payment()
        self.assert_new_payment_present("stg_stripe__payments", 1)
        for name in ("int_stripe__payments", "fct_payments"):
            self.assert_new_payment_present(name, 0)
        self.refresh_downstream_tables()
        for name in ("int_stripe__payments", "fct_payments"):
            self.assert_new_payment_present(name, 1)

    def test_completeness_accepts_multiple_types_and_rejects_missing_extra_duplicate_or_wrong_ids(
        self,
    ):
        self.load_coherent_pipeline()
        rows = self.model(SNAPSHOT)
        self.assertFalse(self.completeness())
        for change in (
            "missing_reason",
            "missing_date",
            "extra_reason",
            "duplicate",
            "wrong_id",
            "wrong_parent",
        ):
            changed = [row.copy() for row in rows]
            target = next(
                row
                for row in changed
                if row["record_id"] == "PX"
                and row["exception_reason"] == "missing_customer"
            )
            if change == "missing_reason":
                changed.remove(target)
            elif change == "missing_date":
                changed.remove(
                    next(
                        row
                        for row in changed
                        if row["exception_reason"] == "missing_created_at"
                    )
                )
            elif change == "extra_reason":
                extra = target.copy()
                extra.update(record_id="not_in_source", exception_id="unexpected")
                changed.append(extra)
            elif change == "duplicate":
                changed.append(target.copy())
            elif change == "wrong_id":
                target["exception_id"] = "wrong"
            else:
                target["parent_key"] = "wrong"
            with self.subTest(change=change):
                self.replace(SNAPSHOT, changed)
                self.assertTrue(self.completeness())


if __name__ == "__main__":
    unittest.main()
