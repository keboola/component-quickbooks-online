"""
Unit tests for the OAuth token / state-file handling (SUPPORT-15835).

These lock the behaviour that the streaming tests don't touch:

* the state-file SCHEMA is unchanged, so an OLD-format state file written by the
  previous component version is read by the new code with no migration;
* tokens are persisted AFTER the refresh (the fix), so the rotated refresh token -
  not the stale pre-refresh one - is what survives to the next run;
* the ``original_refresh_token`` guard in ``process_oauth_tokens`` still detects a
  change even though the on-refresh callback mutates ``self.refresh_token`` first;
* the encryption API URL no longer doubles the ``.com`` suffix.

The token methods only touch a few attributes and the ``get_state_file`` /
``write_state_file`` / ``save_new_oauth_tokens`` boundaries, so we build a bare
Component (bypassing the heavy ComponentBase __init__) and mock those boundaries -
no datadir, no real HTTP.
"""

import os
import sys
import unittest
from unittest import mock

SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import component as component_module  # noqa: E402
from component import Component  # noqa: E402


def make_component(refresh_token="R0", access_token="A0"):
    """A Component instance without running ComponentBase.__init__ (which needs a datadir)."""
    comp = Component.__new__(Component)
    comp.refresh_token = refresh_token
    comp.access_token = access_token
    return comp


def oauth(created, refresh_token="OAUTH_R", access_token="OAUTH_A"):
    return {"data": {"refresh_token": refresh_token, "access_token": access_token}, "created": created}


# A state file written by the OLD component version. Same schema the new code writes -
# the point being there is nothing to migrate. ``Z`` suffix + offset are both tz-aware.
def old_state(ts, refresh_token="STATE_R", access_token="STATE_A"):
    return {"tokens": {"ts": ts, "#refresh_token": refresh_token, "#access_token": access_token}}


class GetTokensTest(unittest.TestCase):
    def test_uses_state_tokens_when_state_is_newer_than_oauth(self):
        comp = make_component()
        comp.get_state_file = mock.Mock(return_value=old_state("2026-06-10T00:00:00.000000Z"))
        refresh_token, access_token = comp.get_tokens(oauth("2026-01-01T00:00:00+00:00"))
        self.assertEqual((refresh_token, access_token), ("STATE_R", "STATE_A"))

    def test_uses_oauth_tokens_when_oauth_is_newer(self):
        # e.g. the user just re-authorized -> oauth.created beats the stale state ts.
        comp = make_component()
        comp.get_state_file = mock.Mock(return_value=old_state("2026-01-01T00:00:00.000000Z"))
        refresh_token, access_token = comp.get_tokens(oauth("2026-06-10T00:00:00+00:00"))
        self.assertEqual((refresh_token, access_token), ("OAUTH_R", "OAUTH_A"))

    def test_falls_back_to_oauth_when_state_has_no_timestamp(self):
        comp = make_component()
        comp.get_state_file = mock.Mock(return_value={})
        refresh_token, access_token = comp.get_tokens(oauth("2026-06-10T00:00:00+00:00"))
        self.assertEqual((refresh_token, access_token), ("OAUTH_R", "OAUTH_A"))


class SaveTokensTest(unittest.TestCase):
    def test_save_writes_current_tokens_with_timestamp(self):
        comp = make_component(refresh_token="NEW_R", access_token="NEW_A")
        comp.write_state_file = mock.Mock()
        comp._save_tokens_to_state_file()
        comp.write_state_file.assert_called_once()
        written = comp.write_state_file.call_args.args[0]
        self.assertEqual(written["tokens"]["#refresh_token"], "NEW_R")
        self.assertEqual(written["tokens"]["#access_token"], "NEW_A")
        self.assertIn("ts", written["tokens"])

    def test_on_token_refresh_updates_and_persists_immediately(self):
        comp = make_component(refresh_token="OLD_R", access_token="OLD_A")
        comp.write_state_file = mock.Mock()
        comp._on_token_refresh("ROT_R", "ROT_A")
        self.assertEqual((comp.refresh_token, comp.access_token), ("ROT_R", "ROT_A"))
        written = comp.write_state_file.call_args.args[0]
        self.assertEqual(written["tokens"]["#refresh_token"], "ROT_R")
        self.assertEqual(written["tokens"]["#access_token"], "ROT_A")


class ProcessOauthTokensTest(unittest.TestCase):
    def test_rotated_token_triggers_encrypted_save_despite_callback_mutation(self):
        comp = make_component(refresh_token="OLD_R", access_token="OLD_A")
        comp.write_state_file = mock.Mock()
        comp.save_new_oauth_tokens = mock.Mock()

        # Mimic QuickbooksClient: refreshing fires on_token_refresh (which mutates
        # comp.refresh_token) BEFORE returning the rotated tokens.
        class FakeClient:
            def get_new_refresh_token(_self):
                comp._on_token_refresh("ROT_R", "ROT_A")
                return "ROT_R", "ROT_A"

        comp.process_oauth_tokens(FakeClient())

        # The guard captured the original token before the callback mutated it, so the
        # change is still detected and the encrypted API save fires with the new token.
        comp.save_new_oauth_tokens.assert_called_once_with("ROT_R", "ROT_A")
        self.assertEqual(comp.refresh_token, "ROT_R")

    def test_unchanged_token_does_not_trigger_save(self):
        comp = make_component(refresh_token="SAME_R", access_token="SAME_A")
        comp.write_state_file = mock.Mock()
        comp.save_new_oauth_tokens = mock.Mock()

        class FakeClient:
            def get_new_refresh_token(_self):
                return "SAME_R", "SAME_A"  # no rotation, no callback

        comp.process_oauth_tokens(FakeClient())
        comp.save_new_oauth_tokens.assert_not_called()


class OldToNewStateTransitionTest(unittest.TestCase):
    """The headline backward-compat case: a config upgraded from the old version, whose
    persisted state holds the stale pre-refresh token, ends a run with the FRESH token."""

    def test_stale_state_is_refreshed_and_fresh_token_persisted(self):
        comp = make_component()
        comp.get_state_file = mock.Mock(return_value=old_state("2026-06-10T00:00:00.000000Z",
                                                               refresh_token="STALE_R", access_token="STALE_A"))
        persisted = {}
        comp.write_state_file = mock.Mock(side_effect=lambda state: persisted.update(state))
        comp.save_new_oauth_tokens = mock.Mock()

        creds = oauth("2026-01-01T00:00:00+00:00")  # old created ts -> state wins

        # 1) New code reads the OLD-format state file (schema unchanged) and picks its token.
        refresh_token, access_token = comp.get_tokens(creds)
        comp.refresh_token, comp.access_token = refresh_token, access_token
        self.assertEqual(refresh_token, "STALE_R")

        # 2) The refresh rotates the token (client fires the callback during the call).
        class FakeClient:
            def get_new_refresh_token(_self):
                comp._on_token_refresh("FRESH_R", "FRESH_A")
                return "FRESH_R", "FRESH_A"

        comp.process_oauth_tokens(FakeClient())

        # 3) The run-level save then persists the FRESH (post-refresh) token to state -
        #    the old code would have left the stale pre-refresh token here instead.
        comp._save_tokens_to_state_file()
        self.assertEqual(persisted["tokens"]["#refresh_token"], "FRESH_R")
        self.assertEqual(persisted["tokens"]["#access_token"], "FRESH_A")


class EncryptUrlTest(unittest.TestCase):
    def test_encrypt_url_has_no_double_com_suffix(self):
        comp = make_component()
        comp.environment_variables = mock.Mock(
            component_id="keboola.ex-quickbooks-online", project_id="123", config_id="456"
        )
        captured = {}

        class Resp:
            text = "ENC"

            def raise_for_status(_self):
                return None

        def fake_post(url, data=None, params=None, headers=None):
            captured["url"] = url
            return Resp()

        with mock.patch.object(component_module, "URL_SUFFIX", "keboola.com"), mock.patch.object(
            component_module.requests, "post", side_effect=fake_post
        ):
            result = comp.encrypt("secret")

        self.assertEqual(result, "ENC")
        self.assertEqual(captured["url"], "https://encryption.keboola.com/encrypt")
        self.assertNotIn(".com.com", captured["url"])


if __name__ == "__main__":
    unittest.main()
