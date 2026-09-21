"""
Tests for the GeneralLedger ``columns`` selection in ``QuickbooksClient``.

A multicurrency QuickBooks company answers ``debt_amt`` / ``credit_amt`` with the foreign
(transaction currency) amounts and leaves ``net_amount``, ``tax_amount``,
``subt_nat_amount`` and ``rbal_nat_amount`` empty, because the home currency amounts live
in the ``*_home_*`` columns. The component never asked for those, so multicurrency
companies got empty amount columns (SUPPORT-17563).

The client now reads ``Preferences.CurrencyPrefs.MultiCurrencyEnabled`` and, for a
multicurrency company only, ADDS the home currency columns to the request.

The two ``EXPECTED_LEGACY_*`` strings below are copied from the code as it was before the
fix. They are the regression guard: a single currency company must keep sending exactly
that request, character for character.

Everything is mocked at the ``requests.Session`` boundary - no network, no datadir.
"""

import json
import os
import sys
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlparse

# The component uses flat imports (``from client import ...``); put ``src`` on the path.
SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import client as client_module  # noqa: E402
from client import GENERAL_LEDGER_MULTI_CURRENCY_COLUMNS, QuickbooksClient  # noqa: E402

# Verbatim copies of the pre-fix column strings.
EXPECTED_LEGACY_COLUMNS = (
    "klass_name,account_name,account_num,chk_print_state,create_by,create_date,"
    "cust_name,doc_num,emp_name,inv_date,is_adj,is_ap_paid,is_ar_paid,is_cleared,item_name,"
    "last_mod_by,last_mod_date,memo,name,quantity,rate,split_acc,tx_date,txn_type,vend_name,"
    "net_amount,tax_amount,tax_code,dept_name,subt_nat_amount,rbal_nat_amount,debt_amt,"
    "credit_amt "
)
EXPECTED_LEGACY_COLUMNS_DATED = (
    "dklass_name,account_name,account_num,chk_print_state,"
    "create_by,create_date,cust_name,doc_num,emp_name,inv_date,is_adj,"
    "is_ap_paid,is_ar_paid,"
    "is_cleared,item_name,last_mod_by,last_mod_date,memo,name,quantity,rate,"
    "split_acc,tx_date,"
    "txn_type,vend_name,net_amount,tax_amount,tax_code,dept_name,"
    "subt_nat_amount,rbal_nat_amount,debt_amt,credit_amt"
)

REPORT_PAYLOAD = {"Header": {"ReportName": "GeneralLedger"}, "Rows": {}}


def preferences_payload(multicurrency):
    return {"QueryResponse": {"Preferences": [{"CurrencyPrefs": {"MultiCurrencyEnabled": multicurrency}}]}}


def make_client():
    return QuickbooksClient(
        company_id="COMPANY",
        access_token="ACCESS",
        refresh_token="REFRESH",
        oauth=mock.Mock(appKey="KEY", appSecret="SECRET"),
        sandbox=False,
    )


def ok_response(payload):
    return mock.Mock(text=json.dumps(payload), headers={})


def requested_columns_string(url):
    """The raw ``columns`` value, exactly as it was put on the wire."""
    return parse_qs(urlparse(url).query, keep_blank_values=True)["columns"][0]


class TestGeneralLedgerColumns(unittest.TestCase):
    def _fetch_general_ledger(self, multicurrency, start_date="2024-01-01", end_date="2024-01-31"):
        """Runs one GeneralLedger fetch and returns the ``columns`` string of every report call."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [
                ok_response(preferences_payload(multicurrency)),
                ok_response(REPORT_PAYLOAD),
                ok_response(REPORT_PAYLOAD),
            ]
            qb = make_client()
            qb.fetch(endpoint="GeneralLedger", report_api_bool=True, start_date=start_date, end_date=end_date)

            self.assertIn("Preferences", session.get.call_args_list[0].args[0])
            return [requested_columns_string(call.args[0]) for call in session.get.call_args_list[1:]]

    def _report_request_undated(self, multicurrency):
        """Same, for the undated branch of ``report_request``."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [
                ok_response(preferences_payload(multicurrency)),
                ok_response(REPORT_PAYLOAD),
                ok_response(REPORT_PAYLOAD),
            ]
            make_client().report_request(endpoint="GeneralLedger", start_date="", end_date="")

            self.assertIn("Preferences", session.get.call_args_list[0].args[0])
            return [requested_columns_string(call.args[0]) for call in session.get.call_args_list[1:]]

    def test_single_currency_dated_request_is_unchanged(self):
        for columns in self._fetch_general_ledger(multicurrency=False):
            self.assertEqual(EXPECTED_LEGACY_COLUMNS_DATED, columns)

    def test_single_currency_undated_request_is_unchanged(self):
        # ``fetch`` refuses a report without dates, so the undated branch of
        # ``report_request`` is exercised directly.
        for columns in self._report_request_undated(multicurrency=False):
            self.assertEqual(EXPECTED_LEGACY_COLUMNS, columns)

    def test_multicurrency_request_only_adds_home_currency_columns(self):
        for columns in self._fetch_general_ledger(multicurrency=True):
            requested = columns.split(",")
            # nothing the report returns today may disappear
            for legacy_column in EXPECTED_LEGACY_COLUMNS_DATED.split(","):
                self.assertIn(legacy_column, requested)
            # and the missing home currency amounts are now asked for
            for home_column in GENERAL_LEDGER_MULTI_CURRENCY_COLUMNS:
                self.assertIn(home_column, requested)
            self.assertEqual(
                EXPECTED_LEGACY_COLUMNS_DATED.split(",") + GENERAL_LEDGER_MULTI_CURRENCY_COLUMNS,
                requested,
            )

    def test_multicurrency_undated_request_only_adds_home_currency_columns(self):
        for columns in self._report_request_undated(multicurrency=True):
            requested = columns.split(",")
            self.assertEqual(
                EXPECTED_LEGACY_COLUMNS.strip().split(",") + GENERAL_LEDGER_MULTI_CURRENCY_COLUMNS,
                requested,
            )

    def test_preferences_are_queried_once_per_client(self):
        with mock.patch.object(client_module, "requesting") as session:
            session.get.return_value = ok_response(preferences_payload(True))
            qb = make_client()

            self.assertTrue(qb.is_multicurrency_enabled())
            self.assertTrue(qb.is_multicurrency_enabled())

        self.assertEqual(1, session.get.call_count)

    def test_unreadable_preferences_fall_back_to_single_currency(self):
        with mock.patch.object(client_module, "requesting") as session:
            session.get.return_value = ok_response({"QueryResponse": {}})

            self.assertFalse(make_client().is_multicurrency_enabled())

    def test_failing_preferences_request_does_not_fail_the_run(self):
        """The preference lookup is new; it must never turn a working run into a failed one."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [
                client_module.requests.exceptions.ConnectionError("boom"),
                ok_response(REPORT_PAYLOAD),
                ok_response(REPORT_PAYLOAD),
            ]
            qb = make_client()
            qb.fetch(endpoint="GeneralLedger", report_api_bool=True, start_date="2024-01-01", end_date="2024-01-31")

            reports = [requested_columns_string(call.args[0]) for call in session.get.call_args_list[1:]]

        self.assertEqual(2, len(reports))
        for columns in reports:
            self.assertEqual(EXPECTED_LEGACY_COLUMNS_DATED, columns)

    def test_other_reports_do_not_query_preferences(self):
        with mock.patch.object(client_module, "requesting") as session:
            session.get.return_value = ok_response(REPORT_PAYLOAD)
            qb = make_client()
            qb.fetch(endpoint="ProfitAndLoss", report_api_bool=True, start_date="2024-01-01", end_date="2024-01-31")

        self.assertEqual(2, session.get.call_count)
        for call in session.get.call_args_list:
            self.assertNotIn("Preferences", call.args[0])


if __name__ == "__main__":
    unittest.main()
