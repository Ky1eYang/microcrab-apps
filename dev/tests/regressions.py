"""Focused concurrency and Host-boundary regressions, loaded by test_server.

These use actual server processes and the same existing fixtures. No build/run is
performed at import, and raw synthetic signing material never appears in asserts.
"""
import hashlib
import json
import time
import unittest

from harness import Browser, Server, eventually, operation_id, upgrade_status


def _turn(phase):
    return {"operation_id": operation_id(), "input": {"parts": [{"type": "text", "text": phase}],
        "variables": {"set": {}, "remove": []}}}


def _texts(snapshot):
    return [message.get("text", "") for message in snapshot["messages"]]


class SlowCommandBoundaries(unittest.TestCase):
    def create_session(self, channel):
        identifier = channel.command("session.create", params={"project_id": "synthetic", "agent": "coding",
            "mode": "plan", "title": "Held compaction regression", "operation_id": operation_id()})["id"]
        channel.command("session.subscribe", identifier)
        return identifier

    def hold(self, channel, server, identifier):
        operation = operation_id()
        admitted = channel.command("session.compact", identifier,
            {"operation_id": operation, "instruction": "compact-held"})
        self.assertEqual(admitted["session"]["id"], identifier)
        self.assertEqual(admitted["session"]["status"], "running")
        self.assertTrue(server.provider.compact_entered.wait(timeout=5), "actual compaction provider call must begin")
        self.assertFalse(server.provider.compact_release.is_set())
        receipt = channel.command("turn.get", identifier, {"operation_id": operation})
        self.assertEqual(receipt["method"], "session.compact")
        self.assertIn(receipt["status"], ("accepted", "running"))
        return operation

    def assert_cancelled_after_auth_loss(self, browser, identifier, operation):
        # Authentication loss closes both browser and the fixture's owner-bound
        # Host relay. The resulting Host suspension interrupts admitted work;
        # inspect its durable receipt only after explicit owner reauthentication.
        browser.login("alice")
        with browser.websocket() as recovered:
            def terminal():
                receipt = recovered.command("turn.get", identifier, {"operation_id": operation})
                return receipt if receipt["status"] not in ("accepted", "running") else None
            receipt = eventually(terminal, 3, "held compaction cancellation receipt deadline exceeded")
            self.assertEqual(receipt["status"], "cancelled")
            self.assertEqual(receipt["method"], "session.compact")

    def test_same_socket_interrupt_is_prompt_during_held_compaction(self):
        with Server() as server:
            browser = Browser(server)
            browser.login()
            with browser.websocket() as channel:
                identifier = self.create_session(channel)
                compact = self.hold(channel, server, identifier)
                started = time.monotonic()
                interrupt = channel.send_command("session.interrupt", identifier)
                self.assertNotIn(interrupt, channel.http_pending, "interrupt must remain on the realtime socket")
                acknowledgement = channel.await_result(interrupt, timeout=2)
                self.assertLess(time.monotonic() - started, 2, "interrupt must not queue behind compaction")
                self.assertTrue(acknowledgement["ok"])
                self.assertTrue(acknowledgement["data"]["requested"])
                cancelled = channel.event(lambda event: event["event"] == "turn" and
                    event["data"].get("id") == compact and
                    event["data"].get("status") not in ("accepted", "running"), timeout=2)["data"]
                self.assertEqual(cancelled["status"], "cancelled")
                self.assertEqual(cancelled["method"], "session.compact")
                self.assertEqual(channel.command("turn.get", identifier, {"operation_id": compact})["status"], "cancelled")
                self.assertFalse(server.provider.compact_release.is_set(), "cancellation must not depend on provider completion")
                self.assertEqual(channel.command("identity.current")["subject"], "alice")
                channel.command("session.close", identifier)

    def test_logout_closes_same_socket_during_held_compaction(self):
        with Server() as server:
            browser = Browser(server)
            browser.login()
            with browser.websocket() as channel:
                identifier = self.create_session(channel)
                compact = self.hold(channel, server, identifier)
                expired_cookie = browser.cookie
                browser.logout()
                channel.expect_closed(timeout=3)
                self.assertFalse(server.provider.compact_release.is_set(), "logout close must not wait for provider release")
            self.assertEqual(upgrade_status(server.origin, expired_cookie), 401)
            self.assert_cancelled_after_auth_loss(browser, identifier, compact)
            self.assertFalse(server.provider.compact_release.is_set(), "logout cancellation must not depend on provider release")

    def test_expiry_closes_same_socket_during_held_compaction(self):
        with Server(ttl=8) as server:
            browser = Browser(server)
            browser.login()
            with browser.websocket() as setup:
                identifier = self.create_session(setup)
            # Refresh after guest construction so that only the active socket's
            # auth window is measured by this bounded slow-command regression.
            principal = browser.login()
            with browser.websocket() as channel:
                compact = self.hold(channel, server, identifier)
                expired_cookie = browser.cookie
                allowance = max(0.1, principal["expires_at"] - time.time()) + 2.5
                channel.expect_closed(timeout=allowance)
                self.assertLess(time.time(), principal["expires_at"] + 2.5)
                self.assertFalse(server.provider.compact_release.is_set(), "expiry close must not wait for provider release")
            self.assertEqual(upgrade_status(server.origin, expired_cookie), 401)
            self.assert_cancelled_after_auth_loss(browser, identifier, compact)
            self.assertFalse(server.provider.compact_release.is_set(), "expiry cancellation must not depend on provider release")


class PrivateHostBoundaries(unittest.TestCase):
    def canonical(self, channel, identifier, phase):
        params = _turn(phase)
        self.assertEqual(channel.command("turn.submit", identifier, params)["status"], "accepted")
        self.assertEqual(channel.terminal(params["operation_id"])["status"], "completed")
        return channel.event(lambda event: event["event"] == "snapshot" and
            "canonical:" + phase in _texts(event["data"]))["data"]

    def test_bound_host_cannot_expose_private_state_escape_root_or_bypass_process_policy(self):
        for backend in ("file", "sqlite"):
            with self.subTest(storage=backend), Server(backend, project_at_startup=True) as server:
                # Standalone execution is bridge-owned. Backend configuration and
                # frontend assets stay outside that workspace; bridge persistence
                # remains protected even inside it. Process denial belongs to Runtime.
                self.assertEqual(server.startup, server.root)
                self.assertNotEqual(server.project, server.startup)
                self.assertNotIn("data_dir", server.configuration, "must exercise actual default .microcrab")
                private_dir = server.project / ".microcrab"
                private_dir.mkdir(exist_ok=True)
                private = private_dir / "fixture-private.txt"
                marker = "synthetic-private-data-marker-" + operation_id()
                private.write_text(marker, encoding="utf-8")
                original_config = server.config_path.read_bytes()
                original_private = private.read_bytes()
                frontend_asset = server.frontend / "index.html"
                original_frontend = frontend_asset.read_bytes()
                self.assertEqual(original_frontend, b"integration-static", "must target the isolated existing frontend fixture")
                server.provider.private_read_paths = [str(server.config_path), ".microcrab/fixture-private.txt"]
                server.provider.private_versions = {
                    str(server.config_path): hashlib.sha256(original_config).hexdigest(),
                    ".microcrab/fixture-private.txt": hashlib.sha256(original_private).hexdigest(),
                    str(frontend_asset): hashlib.sha256(original_frontend).hexdigest(),
                }
                browser = Browser(server)
                browser.login()
                with browser.websocket() as channel:
                    self.assertTrue(server.native_host.descriptor["workspace_id"])
                    self.assertTrue(server.project.samefile(server.native_host.descriptor["workspace_root"]))
                    created = channel.command("session.create", params={"project_id": "synthetic", "agent": "coding",
                        "mode": "build", "title": "Private Host boundary regression", "operation_id": operation_id()})
                    identifier = created["id"]
                    channel.command("session.subscribe", identifier)
                    policy = channel.command("permission.get", identifier)
                    channel.command("permission.set", identifier, {"expected_revision": policy["revision"], "review": False,
                        "overrides": [{"capability": capability, "action": "allow"}
                            for capability in ("file.write", "tool.invoke")]
                            + [{"capability": "process.execute", "action": "deny"}]})
                    read = self.canonical(channel, identifier, "private-read")
                    real_tools = [message.get("tool_name") for message in read["messages"] if message["role"] == "tool"]
                    self.assertEqual(real_tools.count("read"), 2)
                    self.assertIn("glob", real_tools)
                    self.assertIn("grep", real_tools)
                    encoded = json.dumps({"snapshot": read, "requests": server.provider.snapshot(), "events": channel.observed})
                    self.assertFalse(server.jwt_secret in encoded, "synthetic signing material escaped the private Host boundary")
                    self.assertFalse(marker in encoded, "private data-directory content escaped the Host boundary")
                    listed = channel.command("workspace.files", identifier, {"pattern": "**/*", "limit": 100})["items"]
                    self.assertFalse(any("private-config.json" in str(path) or ".microcrab" in str(path) for path in listed),
                        "workspace listing must exclude protected configuration and persistence")
                    written = self.canonical(channel, identifier, "private-write")
                    self.assertEqual(sum(message["role"] == "tool" and message.get("tool_name") == "write"
                        for message in written["messages"]), 3)
                    self.assertTrue(server.config_path.read_bytes() == original_config, "protected configuration changed despite Host protection")
                    self.assertTrue(private.read_bytes() == original_private, "protected persistence changed despite Host protection")
                    self.assertTrue(frontend_asset.read_bytes() == original_frontend,
                        "live write permission modified served frontend asset")
                    status, served, _ = browser.request("/")
                    self.assertEqual(status, 200)
                    self.assertTrue(served == original_frontend, "served frontend response changed after guest write attempt")
                    probe = _turn("private-process")
                    channel.command("turn.submit", identifier, probe)
                    terminal = channel.terminal(probe["operation_id"])
                    self.assertEqual(terminal["status"], "completed")
                    process = channel.event(lambda event: event["event"] == "snapshot" and
                        "canonical:private-process" in _texts(event["data"]))["data"]
                    self.assertTrue(any(message["role"] == "tool" and
                        message.get("tool_name") in ("pwsh", "bash") and "den" in message["text"].lower()
                        for message in process["messages"]), "process policy must reject the real shell invocation")
                    self.assertFalse((server.project / "process-probe-marker.txt").exists(), "denied process probe executed")
                    latest = channel.command("session.get", identifier)
                    encoded = json.dumps({"snapshot": latest, "requests": server.provider.snapshot(), "events": channel.observed})
                    self.assertFalse(server.jwt_secret in encoded, "synthetic signing material leaked after denied operations")
                    self.assertFalse(marker in encoded, "private persistence content leaked after denied operations")
                    channel.command("session.close", identifier)
