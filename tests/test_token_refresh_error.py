"""
Regression tests for how a failed OAuth token refresh is reported.

The production failure: Intuit's token endpoint answered the initial refresh with
``400 Bad Request`` (``invalid_grant`` - the stored authorization was expired or
revoked). The ``HTTPError`` escaped ``process_oauth_tokens`` and the job died with an
opaque internal error (exit 2), although only the user can fix it by re-authorizing.

These tests lock the fix and what it must NOT change:

* a successful refresh still returns and saves the new tokens exactly as before;
* a 400 / 401 from the token endpoint now fails the job with a ``UserException``;
* any other refresh failure (e.g. a 5xx) is re-raised unchanged.

Everything is mocked at the ``requests.post`` boundary - no network, no datadir.
"""

import os
import sys
import types
import unittest
from unittest import mock

import requests
from keboola.component.exceptions import UserException

SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import client as client_module  # noqa: E402
from client import QuickbooksClient  # noqa: E402
from component import Component  # noqa: E402


def make_client():
    return QuickbooksClient(
        company_id="COMPANY",
        access_token="ACCESS",
        refresh_token="REFRESH",
        oauth=mock.Mock(appKey="KEY", appSecret="SECRET"),
        sandbox=False,
    )


def make_component_stub():
    """Just the attributes ``process_oauth_tokens`` touches."""
    return types.SimpleNamespace(refresh_token="REFRESH", access_token="ACCESS", save_new_oauth_tokens=mock.Mock())


def response(status, payload):
    r = requests.models.Response()
    r.status_code = status
    r._content = requests.compat.json.dumps(payload).encode()
    r.url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    return r


@mock.patch("backoff._sync.time.sleep", lambda *_: None)
class TestTokenRefreshError(unittest.TestCase):
    def run_process(self, resp):
        comp = make_component_stub()
        with mock.patch.object(client_module.requests, "post", return_value=resp) as post:
            Component.process_oauth_tokens(comp, make_client())
        return comp, post

    def test_successful_refresh_unchanged(self):
        comp, post = self.run_process(response(200, {"access_token": "NEW_A", "refresh_token": "NEW_R"}))
        self.assertEqual(post.call_count, 1)
        comp.save_new_oauth_tokens.assert_called_once_with("NEW_R", "NEW_A")
        self.assertEqual((comp.refresh_token, comp.access_token), ("NEW_R", "NEW_A"))

    def test_invalid_grant_400_raises_user_exception(self):
        comp = make_component_stub()
        with mock.patch.object(
            client_module.requests, "post", return_value=response(400, {"error": "invalid_grant"})
        ) as post:
            with self.assertRaises(UserException) as ctx:
                Component.process_oauth_tokens(comp, make_client())
        # The existing bounded retry still runs before the failure is reported.
        self.assertEqual(post.call_count, 3)
        self.assertIn("re-authorize", str(ctx.exception))
        self.assertIn("HTTP 400, invalid_grant", str(ctx.exception))
        comp.save_new_oauth_tokens.assert_not_called()

    def test_401_raises_user_exception(self):
        with self.assertRaises(UserException):
            self.run_process(response(401, {"error": "invalid_client"}))

    def test_server_error_is_reraised_unchanged(self):
        with self.assertRaises(client_module.QuickBooksClientException) as ctx:
            self.run_process(response(503, {}))
        self.assertNotIsInstance(ctx.exception, UserException)
        self.assertIsInstance(ctx.exception.__cause__, requests.exceptions.HTTPError)


if __name__ == "__main__":
    unittest.main()
