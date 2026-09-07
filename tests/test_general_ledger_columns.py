"""
Tests for the GeneralLedger ``columns`` selection in ``QuickbooksClient``.

The QuickBooks API accepts a different set of amount columns depending on the company's
multicurrency setting (``debt_amt``/``credit_amt``/... for single currency vs.
``debt_home_amt``/``credit_home_amt``/... when multicurrency is enabled). Requesting the
single-currency set against a multicurrency company yields empty amount columns, so the
client reads ``Preferences.CurrencyPrefs.MultiCurrencyEnabled`` and picks the matching set.

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
from client import QuickbooksClient  # noqa: E402

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


def requested_columns(url):
    return parse_qs(urlparse(url).query)["columns"][0].split(",")


class TestGeneralLedgerColumns(unittest.TestCase):
    def _fetch_general_ledger(self, multicurrency):
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [
                ok_response(preferences_payload(multicurrency)),
                ok_response(REPORT_PAYLOAD),
                ok_response(REPORT_PAYLOAD),
            ]
            qb = make_client()
            qb.fetch(endpoint="GeneralLedger", report_api_bool=True, start_date="2024-01-01", end_date="2024-01-31")
        return session

    def test_single_currency_company_requests_single_currency_amount_columns(self):
        session = self._fetch_general_ledger(multicurrency=False)

        self.assertEqual(3, session.get.call_count)
        self.assertIn("Preferences", session.get.call_args_list[0].args[0])
        for call in session.get.call_args_list[1:]:
            columns = requested_columns(call.args[0])
            self.assertIn("debt_amt", columns)
            self.assertIn("credit_amt", columns)
            self.assertIn("klass_name", columns)
            self.assertNotIn("dklass_name", columns)
            self.assertNotIn("debt_home_amt", columns)

    def test_multicurrency_company_requests_home_currency_amount_columns(self):
        session = self._fetch_general_ledger(multicurrency=True)

        self.assertEqual(3, session.get.call_count)
        for call in session.get.call_args_list[1:]:
            columns = requested_columns(call.args[0])
            self.assertIn("debt_home_amt", columns)
            self.assertIn("credit_home_amt", columns)
            self.assertIn("currency", columns)
            self.assertIn("exch_rate", columns)
            self.assertIn("nat_foreign_amount", columns)
            self.assertNotIn("debt_amt", columns)
            self.assertNotIn("credit_amt", columns)

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
