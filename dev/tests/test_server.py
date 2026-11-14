"""Native server acceptance through actual OAuth, TCP WebSockets and persistence.

Run only after the parent marks the all-features binary ready. Missing artifacts
fail: core acceptance is never skipped and this module never builds or installs.
"""
from __future__ import annotations

from collections import defaultdict
import json
import time
import unittest

import jwt
from websockets.exceptions import ConnectionClosed

from harness import Browser, NativeSocket, Server, operation_id, upgrade_status


def submit_params(phase, identifier=None):
    return {"operation_id": identifier or operation_id(),
        "input": {"parts": [{"type": "text", "text": phase}], "variables": {"set": {}, "remove": []}}}


def texts(snapshot):
    return [message.get("text", "") for message in snapshot["messages"]]


class Lifecycle:
    backend: str

    def completed(self, channel, identifier, phase):
        receipt = channel.terminal(identifier)
        self.assertEqual(receipt["status"], "completed", "real turn must reach successful durable terminal receipt")
        canonical = channel.event(lambda event: event["event"] == "snapshot" and
            event["data"].get("session", {}).get("status") == "idle" and
            "canonical:" + phase in texts(event["data"]))["data"]
        self.assertIn("canonical:" + phase, texts(canonical))
        return canonical

    def assert_observations(self, channel, identifier, minimum_calls=1):
        calls = defaultdict(list)
        for event in channel.observed:
            if event["event"] == "model_observation" and event["data"]["operation_id"] == identifier:
                calls[event["data"]["call_id"]].append(event["data"]["phase"])
        self.assertGreaterEqual(len(calls), minimum_calls, "must observe actual provider calls")
        for phases in calls.values():
            self.assertEqual(phases[0], "started")
            self.assertEqual(phases[-1], "validated")
            self.assertNotIn("invalidated", phases)
        self.assertTrue(any("text_delta" in phases for phases in calls.values()), "real SSE preview deltas required")

    def test_oauth_turn_disconnect_policy_history_and_restart(self):
        with Server(self.backend) as server:
            alice = Browser(server)
            alice_identity = alice.login("alice")
            alice_cookie = alice.cookie
            application_cookie = next(cookie for cookie in alice.cookies if cookie.name == "microcrab_session")
            self.assertIn("HttpOnly", application_cookie._rest)
            self.assertEqual(application_cookie._rest.get("SameSite"), "Lax")
            bob = Browser(server)
            bob_identity = bob.login("bob")
            self.assertNotEqual(alice_identity["owner"], bob_identity["owner"])
            with alice.websocket() as channel:
                capabilities = channel.command("server.capabilities")
                self.assertEqual(capabilities["protocol"], 1)
                for method in ["turn.submit", "session.subscribe", "history.travel", "permission.set", "session.set_mode"]:
                    self.assertIn(method, capabilities["methods"])
                self.assertTrue(capabilities["features"]["model_observations"])
                self.assertEqual(channel.command("identity.current")["id"], alice_identity["owner"])
                self.assertEqual(channel.command("project.list")["items"][0]["id"], "synthetic")
                self.assertEqual(channel.command("model.list")["items"][0]["id"], "synthetic-integration-model")
                catalog = channel.command("agent.catalog", params={"project_id": "synthetic"})["items"]
                coding = next(agent for agent in catalog if agent["name"] == "coding")
                self.assertTrue({"plan", "build"}.issubset({mode["name"] for mode in coding["modes"]}))
                create = {"operation_id": operation_id(), "project_id": "synthetic", "agent": "coding",
                    "mode": "plan", "title": "Synthetic acceptance session"}
                session = channel.command("session.create", params=create)
                identifier = session["id"]
                self.assertEqual(channel.command("session.create", params=create)["id"], identifier)
                self.assertIn(identifier, [item["id"] for item in channel.command("session.list")["items"]])
                subscription = channel.command("session.subscribe", identifier)
                generation = subscription["generation"]
                channel.last_sequence[(subscription["subscription_id"], generation)] = subscription["sequence"]
                self.assertEqual(subscription["snapshot"]["session"]["id"], identifier)
                self.assertEqual(subscription["snapshot"]["messages"], [])
                self.assertEqual(subscription["snapshot"]["permission"]["config"]["mode"], "plan")

                # A second authenticated subject cannot see Alice's backend-owned
                # Project or sessions. Every command/subscription checks ownership.
                with bob.websocket() as other:
                    self.assertEqual(other.command("project.list")["items"], [])
                    self.assertNotIn(identifier, [item["id"] for item in other.command("session.list")["items"]])
                    for method, params in [("session.get", {}), ("session.subscribe", {}),
                        ("session.rename", {"title": "forbidden", "expected_revision": session["revision"]}),
                        ("permission.set", {"expected_revision": 0, "overrides": [], "review": False}),
                        ("session.set_mode", {"mode": "build"}), ("session.interrupt", {})]:
                        self.assertEqual(other.rejected(method, identifier, params)["code"], "not_found")
                self.assertEqual(channel.command("session.get", identifier)["session"]["title"], session["title"])

                first = submit_params("lifecycle-one")
                accepted = channel.command("turn.submit", identifier, first)
                self.assertEqual(accepted["id"], first["operation_id"])
                self.assertEqual(accepted["status"], "accepted")
                canonical = self.completed(channel, first["operation_id"], "lifecycle-one")
                self.assert_observations(channel, first["operation_id"], minimum_calls=2)
                tool_messages = [message for message in canonical["messages"] if message["role"] == "tool"]
                self.assertTrue(any(message.get("tool_name") == "read" and "synthetic-seed-content" in message["text"]
                    for message in tool_messages), "real guest/Host read must reach canonical tool message")
                requests = server.provider.snapshot()
                self.assertEqual(len(requests), 2)
                self.assertTrue(any(message["role"] == "tool" and "synthetic-seed-content" in str(message["content"])
                    for message in requests[1]["messages"]))
                frozen_tools = requests[0]["tools"]
                frozen_system = [message for message in requests[0]["messages"] if message["role"] == "system"]
                self.assertTrue(frozen_tools)
                self.assertTrue(frozen_system)
                self.assertEqual(channel.command("turn.submit", identifier, first)["status"], "completed")
                self.assertEqual(len(server.provider.snapshot()), 2, "operation retry must not call provider again")
                conflict = submit_params("lifecycle-two", first["operation_id"])
                self.assertEqual(channel.rejected("turn.submit", identifier, conflict)["code"], "operation_conflict")

                changed = channel.command("session.set_mode", identifier, {"mode": "build"})
                self.assertEqual(changed["session"]["mode"], "build")
                policy = channel.command("permission.get", identifier)
                overrides = [{"capability": "file.write", "action": "deny"}]
                edited = channel.command("permission.set", identifier, {"expected_revision": policy["revision"],
                    "overrides": overrides, "review": False})
                self.assertGreater(edited["revision"], policy["revision"])
                self.assertEqual(edited["config"]["overrides"][0]["action"], "deny")
                self.assertEqual(channel.rejected("permission.set", identifier, {"expected_revision": policy["revision"],
                    "overrides": [], "review": False})["code"], "revision_conflict")
                self.assertEqual(channel.command("permission.get", identifier), edited)
                second = submit_params("lifecycle-two")
                self.assertEqual(channel.command("turn.submit", identifier, second)["status"], "accepted")
                second_canonical = self.completed(channel, second["operation_id"], "lifecycle-two")
                self.assertTrue(any(message["role"] == "tool" and message.get("tool_name") == "write"
                    and "den" in message["text"].lower() for message in second_canonical["messages"]),
                    "live deny policy must produce a real canonical write rejection")
                self.assertFalse((server.project / "blocked.txt").exists())
                self.assert_observations(channel, second["operation_id"], minimum_calls=2)
                for request in server.provider.snapshot():
                    self.assertEqual(request["tools"], frozen_tools, "mode/policy edit must preserve frozen tool catalog")
                    self.assertEqual([message for message in request["messages"] if message["role"] == "system"], frozen_system,
                        "mode/policy edit must preserve frozen system content")

                held = submit_params("disconnect-held")
                self.assertEqual(channel.command("turn.submit", identifier, held)["status"], "accepted")
                channel.event(lambda event: event["event"] == "model_observation" and
                    event["data"].get("operation_id") == held["operation_id"] and event["data"]["phase"] == "text_delta")
                self.assertTrue(server.provider.entered.wait(timeout=1))
                # Closing a browser must not own or cancel the manager driver.
            with alice.websocket() as reconnect:
                subscription = reconnect.command("session.subscribe", identifier)
                self.assertEqual(subscription["generation"], generation)
                self.assertNotIn("canonical:disconnect-held", texts(subscription["snapshot"]), "uncommitted previews are not canonical")
                self.assertEqual(subscription["snapshot"]["session"]["status"], "running")
                # An operation receipt survives socket replacement; never resend it.
                current = reconnect.command("turn.get", identifier, {"operation_id": held["operation_id"]})
                self.assertIn(current["status"], ("accepted", "running"))
                server.provider.release.set()
                canonical = self.completed(reconnect, held["operation_id"], "disconnect-held")
                self.assertEqual(server.provider.counts["disconnect-held"], 1)
                self.assertEqual(reconnect.command("turn.get", identifier, {"operation_id": held["operation_id"]})["status"], "completed")
                before_travel = list(texts(canonical))
                self.assertIsNotNone(reconnect.command("history.get", identifier)["cursor"])
                undone = reconnect.command("history.travel", identifier, {"direction": "undo"})
                self.assertNotIn("canonical:disconnect-held", texts(undone))
                self.assertIn("canonical:lifecycle-two", texts(undone))
                redone = reconnect.command("history.travel", identifier, {"direction": "redo"})
                self.assertEqual(texts(redone), before_travel)
                executions = reconnect.command("executions.list", identifier)["items"]
                self.assertTrue(executions, "real tool calls must remain visible in execution history")
                self.assertTrue(all(item["status"] in {"succeeded", "failed", "cancelled", "interrupted"}
                    for item in executions),
                    "completed turns must leave no live execution")
                files = reconnect.command("workspace.files", identifier, {"pattern": "**/*.txt", "limit": 20})["items"]
                self.assertTrue(any(str(path).endswith("seed.txt") for path in files))
                self.assertEqual(reconnect.command("session.close", identifier), {"closed": True})

            self.assertEqual(server.config_path, server.project / ".microcrab" / "config.json")
            self.assertTrue((server.runtime / "agents" / "coding.wasm").is_file())
            identity_files = list((server.runtime / "execution").glob("backend-*.json"))
            self.assertEqual(len(identity_files), 1)
            binding_scope = dict(server.native_host.scope)
            self.assertEqual(json.loads(identity_files[0].read_text(encoding="utf-8")), binding_scope["backend_id"])
            if self.backend == "sqlite":
                self.assertTrue((server.runtime / "storage.sqlite").is_file())
            else:
                self.assertTrue((server.runtime / "directory").is_dir())
                self.assertTrue((server.runtime / "sessions").is_dir())
            server.restart()
            self.assertEqual(upgrade_status(server.origin, alice_cookie), 401, "restart must invalidate prior process cookies")
            restored_browser = Browser(server)
            self.assertEqual(restored_browser.login("alice")["owner"], alice_identity["owner"])
            self.assertEqual(server.native_host.scope, binding_scope)
            with restored_browser.websocket() as restored:
                self.assertIn(identifier, [item["id"] for item in restored.command("session.list")["items"]])
                reopened = restored.command("session.open", identifier)
                self.assertEqual(texts(reopened), before_travel)
                self.assertEqual(reopened["session"]["mode"], "build")
                self.assertEqual(reopened["permission"]["config"]["overrides"][0]["action"], "deny")
                self.assertEqual(restored.command("turn.get", identifier, {"operation_id": held["operation_id"]})["status"], "completed")
                restored.command("session.subscribe", identifier)
                final = submit_params("after-restart")
                restored.command("turn.submit", identifier, final)
                self.completed(restored, final["operation_id"], "after-restart")
                restored.command("session.close", identifier)
            self.assertEqual(server.provider.errors, [])
            self.assertEqual((server.project / "seed.txt").read_text(encoding="utf-8"), "synthetic-seed-content\n")


class FileLifecycle(Lifecycle, unittest.TestCase):
    backend = "file"


class SqliteLifecycle(Lifecycle, unittest.TestCase):
    backend = "sqlite"


class AutomaticNaming(unittest.TestCase):
    def test_clean_title_generation_runs_once_and_survives_restart(self):
        with Server(auto_rename=True) as server:
            browser = Browser(server)
            browser.login()
            with browser.websocket() as channel:
                created = channel.command("session.create", params={"project_id": "synthetic", "agent": "coding",
                    "mode": "plan", "operation_id": operation_id()})
                self.assertEqual(created["title"], "New session")
                identifier = created["id"]
                channel.command("session.subscribe", identifier)
                first = submit_params("lifecycle-one")
                channel.command("turn.submit", identifier, first)
                self.assertEqual(channel.terminal(first["operation_id"])["status"], "completed")
                named = channel.event(lambda event: event["event"] == "snapshot" and
                    event["data"].get("session", {}).get("title") == "Native fixture title" and
                    "canonical:lifecycle-one" in texts(event["data"]))["data"]
                self.assertEqual(named["session"]["title"], "Native fixture title")
                functional = [request for request in server.provider.snapshot() if not request.get("tools")]
                self.assertEqual(len(functional), 1)
                self.assertEqual([message["role"] for message in functional[0]["messages"]], ["system", "user"])
                self.assertNotIn("synthetic-seed-content", json.dumps(functional[0]))
                self.assertEqual(functional[0]["messages"][1]["content"], "lifecycle-one")
                followup = submit_params("naming-followup")
                channel.command("turn.submit", identifier, followup)
                self.assertEqual(channel.terminal(followup["operation_id"])["status"], "completed")
                channel.event(lambda event: event["event"] == "snapshot" and
                    "canonical:naming-followup" in texts(event["data"]))
                self.assertEqual(server.provider.counts["functional-rename"], 1)
                custom = channel.command("session.create", params={"project_id": "synthetic", "agent": "coding",
                    "mode": "plan", "title": "Keep custom title", "operation_id": operation_id()})
                custom_identifier = custom["id"]
                self.assertEqual(custom["title"], "Keep custom title")
                channel.command("session.subscribe", custom_identifier)
                custom_turn = submit_params("naming-followup")
                channel.command("turn.submit", custom_identifier, custom_turn)
                self.assertEqual(channel.terminal(custom_turn["operation_id"])["status"], "completed")
                custom_snapshot = channel.event(lambda event: event["event"] == "snapshot" and
                    event["data"].get("session", {}).get("id") == custom_identifier and
                    "canonical:naming-followup" in texts(event["data"]))["data"]
                self.assertEqual(custom_snapshot["session"]["title"], "Keep custom title")
                self.assertEqual(server.provider.counts["functional-rename"], 1,
                    "an explicit custom title must not trigger functional naming")
                channel.command("session.close", custom_identifier)
                self.assertEqual(channel.command("session.close", identifier), {"closed": True})
            server.restart()
            browser = Browser(server)
            browser.login()
            with browser.websocket() as channel:
                reopened = channel.command("session.open", identifier)
                self.assertEqual(reopened["session"]["title"], "Native fixture title")
                channel.command("session.subscribe", identifier)
                after_restart = submit_params("after-restart")
                channel.command("turn.submit", identifier, after_restart)
                self.assertEqual(channel.terminal(after_restart["operation_id"])["status"], "completed")
                channel.event(lambda event: event["event"] == "snapshot" and
                    "canonical:after-restart" in texts(event["data"]))
                self.assertEqual(server.provider.counts["functional-rename"], 1,
                    "persisted naming claim must prevent another functional call")
                self.assertEqual(channel.command("session.get", identifier)["session"]["title"], "Native fixture title")
                restored_custom = channel.command("session.open", custom_identifier)
                self.assertEqual(restored_custom["session"]["title"], "Keep custom title")
                self.assertIn("canonical:naming-followup", texts(restored_custom))
                channel.command("session.close", custom_identifier)
                self.assertEqual(server.provider.counts["functional-rename"], 1)
                channel.command("session.close", identifier)
            self.assertEqual(server.provider.errors, [])


class AuthenticationAndProtocol(unittest.TestCase):
    def test_cookie_jwt_origin_logout_and_retired_http_boundaries(self):
        with Server() as server:
            browser = Browser(server)
            browser.login()
            cookie = browser.cookie
            self.assertEqual(upgrade_status(server.origin, None), 401)
            self.assertEqual(upgrade_status(server.origin, cookie, ws_origin=None), 403)
            self.assertEqual(upgrade_status(server.origin, cookie, ws_origin="http://untrusted.invalid"), 403)
            self.assertEqual(upgrade_status(server.origin, cookie + "; " + cookie), 401)
            raw = cookie.split("=", 1)[1]
            claims = jwt.decode(raw, options={"verify_signature": False})
            for field, value in [("iss", "wrong-app"), ("aud", "wrong-audience"),
                    ("exp", int(time.time()) - 1), ("nbf", int(time.time()) + 300)]:
                invalid = jwt.encode({**claims, field: value}, server.jwt_secret, algorithm="HS256")
                self.assertEqual(upgrade_status(server.origin, "microcrab_session=" + invalid), 401)
            wrong_signature = jwt.encode(claims, "distinct-test-only-signing-secret-32-bytes", algorithm="HS256")
            self.assertEqual(upgrade_status(server.origin, "microcrab_session=" + wrong_signature), 401)
            for path in ["/api/session", "/api/sessions", "/api/event", "/api/provider", "/api/project"]:
                status, body, _ = browser.request(path)
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body)["error"]["code"], "not_found")
            with browser.websocket() as active:
                self.assertEqual(active.rejected("session.list", version=99)["code"], "unsupported_version")
                self.assertEqual(active.rejected("retired.compatibility.method")["code"], "unsupported")
                self.assertEqual(active.command("session.list")["items"], [])
                self.assertEqual(browser.request("/api/auth/logout", method="POST")[0], 403)
                self.assertEqual(browser.request("/api/auth/logout", method="POST", origin="http://untrusted.invalid")[0], 403)
                self.assertEqual(active.command("identity.current")["subject"], "alice")
                browser.logout()
                active.expect_closed()
            self.assertEqual(upgrade_status(server.origin, cookie), 401, "logged-out cookie must not reconnect")

    def test_fixed_http_origin_and_websocket_transport_boundaries(self):
        with Server() as server:
            anonymous = Browser(server)
            self.assertEqual(anonymous.request("/api/v1/sessions")[0], 401)
            alice = Browser(server)
            principal = alice.login("alice")
            bob = Browser(server)
            other = bob.login("bob")
            self.assertNotEqual(principal["owner"], other["owner"])
            for browser, identity in ((alice, principal), (bob, other)):
                status, body, _ = browser.request("/api/v1/identity")
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(body)["id"], identity["owner"])
            # Origin/MIME checks precede session lookup; no Host relay is needed.
            path = "/api/v1/sessions/" + operation_id()
            for origin in (None, "http://untrusted.invalid", server.origin + "/"):
                status, body, _ = alice.request(path, method="PATCH", origin=origin, data={"title": "forbidden", "expected_revision": 0})
                self.assertEqual(status, 403)
                self.assertTrue(json.loads(body)["error"]["code"])
            self.assertEqual(alice.request(path, method="PATCH", origin=server.origin)[0], 415)
            status, body, _ = alice.request(path, method="PATCH", origin=server.origin, data={"title": "missing", "expected_revision": 0})
            self.assertEqual(status, 404)
            self.assertEqual(json.loads(body)["error"]["code"], "not_found")
            with alice.websocket() as channel:
                request_id = operation_id()
                channel.pending_ids.add(request_id)
                # Bypass semantic routing only to verify the actual WS rejection.
                channel.socket.send(json.dumps({"v": 1, "type": "command", "request_id": request_id,
                    "method": "session.list", "params": {}}))
                rejected = channel.await_result(request_id)
                self.assertFalse(rejected["ok"])
                self.assertEqual(rejected["error"]["code"], "http_required")
                self.assertEqual(channel.command("session.list")["items"], [])

    def test_expiry_closes_existing_socket_and_rejects_reconnect(self):
        with Server(ttl=3) as server:
            browser = Browser(server)
            browser.login()
            cookie = browser.cookie
            with browser.websocket() as active:
                self.assertEqual(active.command("identity.current")["subject"], "alice")
                active.expect_closed(timeout=6)
            self.assertEqual(upgrade_status(server.origin, cookie), 401)

    def test_malformed_envelope_is_rejected_without_business_mutation(self):
        with Server() as server:
            browser = Browser(server)
            browser.login()
            with browser.websocket() as invalid:
                invalid.socket.send('{"v":1,"type":"legacy","request_id":"invalid","method":"session.create","params":{}}')
                try:
                    response = invalid.receive(timeout=5)
                    self.assertFalse(response.get("ok", True))
                    invalid.expect_closed()
                except ConnectionClosed:
                    pass  # Protocol permits closing before the queued safe error flushes.
            with browser.websocket() as valid:
                self.assertEqual(valid.command("session.list")["items"], [])
            self.assertEqual(server.provider.snapshot(), [])


# unittest's module loader includes imported TestCases; keep focused regressions
# separate while preserving the parent's established test_server.py invocation.
from regressions import PrivateHostBoundaries, SlowCommandBoundaries


if __name__ == "__main__":
    unittest.main()
