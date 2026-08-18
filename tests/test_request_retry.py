"""
Regression tests for the transport-level retry in ``QuickbooksClient``.

The production failure: the QuickBooks API answered a ``select count(*)`` query with a
response advertising ``Content-Encoding: gzip`` whose body did not decode - "Error -3
while decompressing data: incorrect header check". ``requests`` raises
``ContentDecodingError`` while it is still building the response object, so nothing in
the client ever saw it; the exception escaped all the way out and the job died with an
opaque internal error (exit 2) over what is a one-off corrupt payload on the wire.

These tests lock the fix, and - just as importantly - lock what it must NOT change:

* an intact response is still fetched with exactly ONE call and parsed exactly as
  before, so the happy path is untouched;
* a corrupt body is retried and the run carries on once a good response arrives;
* a persistently corrupt body still raises after the bounded number of attempts, so a
  genuine upstream problem keeps failing the job loudly - it is never swallowed and
  never turns into an empty success;
* errors outside the transport family are not retried, so the retry cannot mask
  anything else.

Everything is mocked at the ``requests.Session`` boundary - no network, no datadir.
"""

import json
import os
import sys
import unittest
from unittest import mock

import requests

# The component uses flat imports (``from client import ...``); put ``src`` on the path.
SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import client as client_module  # noqa: E402
from client import QuickbooksClient  # noqa: E402

# What the endpoint that failed in production ("select count(*) from JournalEntry")
# returns when the response arrives intact.
COUNT_PAYLOAD = {"QueryResponse": {"totalCount": 7}}

URL = "https://quickbooks.example.invalid/v3/company/COMPANY/query"


def make_client():
    """A client wired to a stub OAuth object; only appKey/appSecret are read in __init__."""
    return QuickbooksClient(
        company_id="COMPANY",
        access_token="ACCESS",
        refresh_token="REFRESH",
        oauth=mock.Mock(appKey="KEY", appSecret="SECRET"),
        sandbox=False,
    )


def ok_response(payload):
    """Stand-in for the requests.Response the session hands back on success."""
    return mock.Mock(text=json.dumps(payload), headers={})


def gzip_decode_error():
    """The exact exception requests raised in production."""
    return requests.exceptions.ContentDecodingError(
        "Received response with content-encoding: gzip, but failed to decode it. "
        "Error -3 while decompressing data: incorrect header check"
    )


# backoff sleeps between attempts; tests must not actually wait.
@mock.patch("time.sleep", mock.Mock())
class TestTransportRetry(unittest.TestCase):
    def test_intact_response_is_fetched_once_and_parsed_unchanged(self):
        """The happy path must be exactly what it was: one GET, same parsed payload."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.return_value = ok_response(COUNT_PAYLOAD)

            result = make_client()._request(URL)

        self.assertEqual(COUNT_PAYLOAD, result)
        self.assertEqual(1, session.get.call_count)

    def test_corrupt_body_is_retried_and_the_run_continues(self):
        """Two corrupt bodies then a good one: the caller sees the good payload."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [
                gzip_decode_error(),
                gzip_decode_error(),
                ok_response(COUNT_PAYLOAD),
            ]

            result = make_client()._request(URL)

        self.assertEqual(COUNT_PAYLOAD, result)
        self.assertEqual(3, session.get.call_count)

    def test_persistently_corrupt_body_still_fails_the_job(self):
        """The retry is bounded and re-raises - a real upstream problem is not hidden."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = gzip_decode_error()

            with self.assertRaises(requests.exceptions.ContentDecodingError):
                make_client()._request(URL)

        self.assertEqual(client_module.TRANSPORT_MAX_TRIES, session.get.call_count)

    def test_errors_outside_the_transport_family_are_not_retried(self):
        """Only body-decode failures are retried; anything else propagates immediately."""
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = requests.exceptions.ConnectionError("name resolution failed")

            with self.assertRaises(requests.exceptions.ConnectionError):
                make_client()._request(URL)

        self.assertEqual(1, session.get.call_count)

    def test_a_fault_response_is_still_handled_exactly_as_before(self):
        """
        A ``Fault`` payload is not a transport error, so the retry must stay out of its
        way: the client still refreshes the token once and then raises
        QuickBooksClientException (which the component maps to a UserException).
        """
        fault = {"Fault": {"Error": [{"Message": "message OAuth token rejected"}]}}
        with mock.patch.object(client_module, "requesting") as session:
            session.get.return_value = ok_response(fault)
            qb = make_client()
            with mock.patch.object(qb, "refresh_access_token") as refresh:
                # Mirror what a real refresh does, so the second pass takes the raise branch.
                refresh.side_effect = lambda: setattr(qb, "access_token_refreshed", True)

                with self.assertRaises(client_module.QuickBooksClientException):
                    qb._request(URL)

        self.assertEqual(1, refresh.call_count)
        self.assertEqual(2, session.get.call_count)


@mock.patch("time.sleep", mock.Mock())
class TestProductionScenario(unittest.TestCase):
    def test_count_query_survives_the_corrupt_gzip_response(self):
        """
        End-to-end reproduction of the alert: fetch() on a data endpoint resolves the
        record count, and the count request is the one that came back with an
        undecodable gzip body.
        """
        with mock.patch.object(client_module, "requesting") as session:
            session.get.side_effect = [gzip_decode_error(), ok_response(COUNT_PAYLOAD)]

            qb = make_client()
            qb.fetch(
                endpoint="JournalEntry",
                report_api_bool=False,
                start_date=None,
                end_date=None,
            )

        self.assertEqual(7, qb.count)
        self.assertEqual(2, session.get.call_count)


if __name__ == "__main__":
    unittest.main()
