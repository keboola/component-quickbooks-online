"""
Tests for the opt-in, experimental report flattener.

Two layers, per the design doc
(docs/superpowers/specs/2026-08-26-report-flattener-design.md):

* ``TestFlattenReport`` - pure ``flatten_report`` assertions over representative
  QuickBooks Reports API fixtures (tests/fixtures/*.json). No I/O.
* ``TestReportMappingParseReportsIntegration`` - ``ReportMapping`` output-path tests:
  the ``parse_reports=False`` regression (byte-for-byte unchanged single-cell output),
  the new ``output_rows`` path, and the defensive fallback for an unflattenable
  response. ``report_mapping.DEFAULT_FILE_DESTINATION`` is monkeypatched to a temp dir
  so nothing touches the real data dir.
"""

import csv
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

# The component uses flat imports (``from report_flattener import ...``); put ``src``
# on the path, same convention as tests/test_request_retry.py.
SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import report_mapping as report_mapping_module  # noqa: E402
from report_mapping import ReportMapping  # noqa: E402
from report_flattener import flatten_report, ReportNotFlattenable  # noqa: E402

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

BASE_ROW = {"ReportName": "SomeReport", "StartPeriod": "2026-01-01", "EndPeriod": "2026-01-31"}


def load_fixture(name):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)


class TestFlattenReport(unittest.TestCase):
    """Pure transformation tests - no I/O."""

    def test_general_ledger_flattens_with_account_ancestry_and_summary_rows(self):
        data = load_fixture("general_ledger.json")

        rows, columns = flatten_report(data, BASE_ROW)

        self.assertEqual(
            [
                "ReportName",
                "StartPeriod",
                "EndPeriod",
                "group_0",
                "tx_date",
                "txn_type",
                "doc_num",
                "subt_nat_amount",
                "row_type",
                "row_number",
            ],
            columns,
        )

        # 2 data rows + 1 summary for "Checking", 1 data row + 1 summary for
        # "Accounts Payable".
        self.assertEqual(5, len(rows))

        row_types = [r["row_type"] for r in rows]
        self.assertEqual(["data", "data", "summary", "data", "summary"], row_types)

        # row_number is monotonic across the whole walk.
        self.assertEqual([1, 2, 3, 4, 5], [r["row_number"] for r in rows])

        # Account ancestry is carried down onto every row under that section, data and
        # summary alike.
        self.assertEqual(
            ["Checking", "Checking", "Checking", "Accounts Payable", "Accounts Payable"],
            [r["group_0"] for r in rows],
        )

        first_data_row = rows[0]
        self.assertEqual("2026-01-05", first_data_row["tx_date"])
        self.assertEqual("Check", first_data_row["txn_type"])
        self.assertEqual("1001", first_data_row["doc_num"])
        self.assertEqual("-150.00", first_data_row["subt_nat_amount"])
        self.assertEqual(BASE_ROW["ReportName"], first_data_row["ReportName"])

        checking_summary = rows[2]
        self.assertEqual("summary", checking_summary["row_type"])
        # Summary.ColData is mapped positionally onto the same report columns as data
        # rows, so the label lands in the first report column (tx_date).
        self.assertEqual("Total for Checking", checking_summary["tx_date"])
        self.assertEqual("350.00", checking_summary["subt_nat_amount"])

        ap_summary = rows[4]
        self.assertEqual("Total for Accounts Payable", ap_summary["tx_date"])
        self.assertEqual("-450.00", ap_summary["subt_nat_amount"])

    def test_transaction_list_flattens_to_one_row_per_transaction(self):
        data = load_fixture("transaction_list.json")

        rows, columns = flatten_report(data, BASE_ROW)

        # Flat report, no sections -> no group_* columns at all.
        self.assertNotIn("group_0", columns)
        self.assertEqual(
            ["ReportName", "StartPeriod", "EndPeriod", "tx_date", "txn_type", "doc_num", "name",
             "subt_nat_amount", "row_type", "row_number"],
            columns,
        )

        self.assertEqual(3, len(rows))
        self.assertTrue(all(r["row_type"] == "data" for r in rows))
        self.assertEqual([1, 2, 3], [r["row_number"] for r in rows])

        self.assertEqual(
            [("Invoice", "Acme Corp", "1200.00"), ("Payment", "Acme Corp", "-1200.00"),
             ("Bill", "Vendor Co", "450.00")],
            [(r["txn_type"], r["name"], r["subt_nat_amount"]) for r in rows],
        )

    def test_trial_balance_flattens_to_per_account_rows_plus_grand_total_summary(self):
        """
        Summary-shaped validation: TrialBalance ends with a `group`+`Summary`-only
        row (no nested `Rows`) for the grand total. Because it is tagged
        `type: "Section"`, it is still representable via the Section rule.
        """
        data = load_fixture("trial_balance.json")

        rows, columns = flatten_report(data, BASE_ROW)

        self.assertIn("group_0", columns)
        self.assertEqual(3, len(rows))

        checking, accounts_payable, grand_total = rows
        self.assertEqual("data", checking["row_type"])
        self.assertEqual("Checking", checking["account_name"])
        self.assertEqual("1000.00", checking["debit_amt"])
        self.assertEqual("", checking.get("group_0", ""))

        self.assertEqual("data", accounts_payable["row_type"])
        self.assertEqual("Accounts Payable", accounts_payable["account_name"])
        self.assertEqual("1000.00", accounts_payable["credit_amt"])

        self.assertEqual("summary", grand_total["row_type"])
        self.assertEqual("GrandTotal", grand_total["group_0"])
        self.assertEqual("TOTAL", grand_total["account_name"])
        self.assertEqual("1000.00", grand_total["debit_amt"])
        self.assertEqual("1000.00", grand_total["credit_amt"])

        self.assertEqual([1, 2, 3], [r["row_number"] for r in rows])

    def test_raises_when_columns_are_missing_or_empty(self):
        data = load_fixture("degenerate_report.json")

        with self.assertRaises(ReportNotFlattenable):
            flatten_report(data, BASE_ROW)

    def test_raises_when_columns_key_absent_entirely(self):
        data = {"Header": {}, "Rows": {"Row": [{"type": "Data", "ColData": [{"value": "x"}]}]}}

        with self.assertRaises(ReportNotFlattenable):
            flatten_report(data, BASE_ROW)

    def test_raises_when_rows_key_absent_entirely(self):
        data = {"Header": {}, "Columns": {"Column": [{"ColTitle": "A"}]}}

        with self.assertRaises(ReportNotFlattenable):
            flatten_report(data, BASE_ROW)

    def test_column_name_resolution_falls_back_and_dedupes(self):
        """
        Column naming order: MetaData ColKey, else slugified ColTitle, else
        positional col_<i>; collisions get a positional suffix so the mapping stays
        1:1 with response column order.
        """
        data = {
            "Header": {},
            "Columns": {
                "Column": [
                    {"ColTitle": "Amount", "MetaData": [{"Name": "ColKey", "Value": "amount"}]},
                    {"ColTitle": "Debit / Credit"},
                    {"ColTitle": ""},
                    {"ColTitle": "Amount"},
                ]
            },
            "Rows": {
                "Row": [
                    {"type": "Data", "ColData": [{"value": "1"}, {"value": "2"}, {"value": "3"}, {"value": "4"}]},
                ]
            },
        }

        rows, columns = flatten_report(data, BASE_ROW)

        self.assertEqual(["amount", "debit_credit", "col_2", "amount_3"], columns[-6:-2])
        self.assertEqual("1", rows[0]["amount"])
        self.assertEqual("2", rows[0]["debit_credit"])
        self.assertEqual("3", rows[0]["col_2"])
        self.assertEqual("4", rows[0]["amount_3"])


class TestReportMappingParseReportsIntegration(unittest.TestCase):
    """
    Output-path tests. ``report_mapping.DEFAULT_FILE_DESTINATION`` is monkeypatched to
    a fresh temp dir per test.
    """

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp() + os.sep
        self._patcher = mock.patch.object(report_mapping_module, "DEFAULT_FILE_DESTINATION", self.tmp_dir)
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _csv_path(self, filename):
        return os.path.join(self.tmp_dir, filename)

    def _manifest_path(self, filename):
        return os.path.join(self.tmp_dir, filename + ".manifest")

    def _read_manifest(self, filename):
        with open(self._manifest_path(filename)) as f:
            return json.load(f)

    def test_parse_reports_false_still_produces_exactly_one_json_cell(self):
        """
        Regression: with parse_reports unset/false, a report_cant_parse report keeps
        today's single-JSON-cell output untouched - same columns, same PK.
        """
        data = load_fixture("general_ledger.json")

        ReportMapping(endpoint="GeneralLedger", data=data)  # parse_reports defaults to False

        with open(self._csv_path("GeneralLedger.csv")) as f:
            rows = list(csv.reader(f))

        self.assertEqual(2, len(rows))
        header, values = rows
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod", "value"], header)
        self.assertEqual("GeneralLedger", values[0])
        self.assertEqual("2026-01-01", values[1])
        self.assertEqual("2026-01-31", values[2])
        self.assertEqual(data, json.loads(values[3]))

        manifest = self._read_manifest("GeneralLedger.csv")
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod"], manifest["primary_key"])
        self.assertTrue(manifest["incremental"])

    def test_parse_reports_explicit_false_is_identical_to_default(self):
        data = load_fixture("general_ledger.json")

        ReportMapping(endpoint="GeneralLedger", data=data, parse_reports=False)

        with open(self._csv_path("GeneralLedger.csv")) as f:
            rows = list(csv.reader(f))
        self.assertEqual(2, len(rows))
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod", "value"], rows[0])

    def test_parse_reports_true_flattens_general_ledger_into_rows(self):
        data = load_fixture("general_ledger.json")

        ReportMapping(endpoint="GeneralLedger", data=data, parse_reports=True)

        with open(self._csv_path("GeneralLedger.csv")) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames

        self.assertEqual(
            ["ReportName", "StartPeriod", "EndPeriod", "group_0", "tx_date", "txn_type", "doc_num",
             "subt_nat_amount", "row_type", "row_number"],
            fieldnames,
        )
        self.assertEqual(5, len(rows))
        self.assertEqual(["1", "2", "3", "4", "5"], [r["row_number"] for r in rows])
        self.assertEqual("GeneralLedger", rows[0]["ReportName"])
        self.assertEqual("Checking", rows[0]["group_0"])

        manifest = self._read_manifest("GeneralLedger.csv")
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod", "row_number"], manifest["primary_key"])
        self.assertTrue(manifest["incremental"])

    def test_parse_reports_true_flattens_transaction_list_one_row_per_transaction(self):
        data = load_fixture("transaction_list.json")

        ReportMapping(endpoint="TransactionList", data=data, parse_reports=True)

        with open(self._csv_path("TransactionList.csv")) as f:
            rows = list(csv.DictReader(f))

        self.assertEqual(3, len(rows))
        self.assertEqual(["data", "data", "data"], [r["row_type"] for r in rows])
        self.assertEqual(["Acme Corp", "Acme Corp", "Vendor Co"], [r["name"] for r in rows])

    def test_parse_reports_true_flattens_trial_balance_into_account_and_summary_rows(self):
        data = load_fixture("trial_balance.json")

        ReportMapping(endpoint="TrialBalance", data=data, parse_reports=True)

        with open(self._csv_path("TrialBalance.csv")) as f:
            rows = list(csv.DictReader(f))

        self.assertEqual(3, len(rows))
        self.assertEqual(["data", "data", "summary"], [r["row_type"] for r in rows])
        self.assertEqual("GrandTotal", rows[2]["group_0"])

    def test_parse_reports_true_respects_accounting_type_filenames(self):
        """The existing accrual/cash filename logic is reused unchanged for flattened output."""
        data = load_fixture("general_ledger.json")

        ReportMapping(endpoint="GeneralLedger", data=data, accounting_type="accrual", parse_reports=True)

        self.assertTrue(os.path.isfile(self._csv_path("GeneralLedger_accrual.csv")))
        self.assertFalse(os.path.isfile(self._csv_path("GeneralLedger.csv")))

    def test_parse_reports_true_falls_back_to_1cell_on_unflattenable_response_and_warns(self):
        """
        A degenerate response (no Columns/Rows) must not raise: it logs a warning
        naming the report and falls back to the original single-cell JSON output.
        """
        data = load_fixture("degenerate_report.json")

        with self.assertLogs(level="WARNING") as logs:
            ReportMapping(endpoint="CashFlow", data=data, parse_reports=True)

        self.assertTrue(any("CashFlow" in message for message in logs.output))

        with open(self._csv_path("CashFlow.csv")) as f:
            rows = list(csv.reader(f))

        self.assertEqual(2, len(rows))
        header, values = rows
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod", "value"], header)
        self.assertEqual(data, json.loads(values[3]))

        manifest = self._read_manifest("CashFlow.csv")
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod"], manifest["primary_key"])

    def test_parse_reports_true_falls_back_to_1cell_on_unexpected_flattening_error(self):
        """
        An UNEXPECTED error inside flatten_report (not ReportNotFlattenable) must not
        fail the job: enabling this experimental flag can never turn a previously
        working report into an exit-2 failure. It degrades to the single-cell JSON and
        surfaces the error in the logs.
        """
        data = load_fixture("general_ledger.json")

        with mock.patch.object(report_mapping_module, "flatten_report", side_effect=RuntimeError("boom")):
            with self.assertLogs(level="WARNING") as logs:
                ReportMapping(endpoint="GeneralLedger", data=data, parse_reports=True)

        self.assertTrue(any("GeneralLedger" in message for message in logs.output))

        with open(self._csv_path("GeneralLedger.csv")) as f:
            rows = list(csv.reader(f))

        self.assertEqual(2, len(rows))
        self.assertEqual(["ReportName", "StartPeriod", "EndPeriod", "value"], rows[0])
        self.assertEqual(data, json.loads(rows[1][3]))


if __name__ == "__main__":
    unittest.main()
