"""Real standalone v0.1.11 acceptance; parent builds binaries before invocation.

No build/install occurs here. Real WASM/Host traffic uses the maintained harness.
Assertions and diagnostics must not expose credentials or private fixture logs.
"""
from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
import unittest

from harness import Browser, ModelProvider, Server, operation_id


class HeldReadProvider(ModelProvider):
    """Hold next request before response, then ask builtin job to drain once."""
    def __init__(self):
        super().__init__()
        self.next_entered = threading.Event()
        self.next_release = threading.Event()
        self.read_receipt = None
        self.next_request_entered_ms = None

    def phase(self, request):
        phase = super().phase(request)
        tools = [message for message in request.get("messages", []) if message.get("role") == "tool"]
        if phase == "lifecycle-one" and len(tools) == 1 and not self.next_entered.is_set():
            self.read_receipt = json.loads(tools[0]["content"])
            self.next_request_entered_ms = int(time.time() * 1000)
            self.next_entered.set()
            if not self.next_release.wait(timeout=60):
                raise TimeoutError("native v0111 inspection barrier not released")
            # The maintained first lifecycle-two response is another scripted call.
            # Here it invokes job drain; subsequent request returns normal SSE.
            return "lifecycle-two"
        return phase

    def scripted_calls(self, phase):
        if phase == "lifecycle-one":
            return [("read", {"path": "seed.txt", "offset_line": 1, "max_lines": 2000, "max_bytes": 65536})]
        if phase == "lifecycle-two":
            if self.read_receipt is None:
                raise AssertionError("buffered read must precede drain")
            return [("job", {"id": self.read_receipt["id"], "action": "drain"})]
        return super().scripted_calls(phase)


class V0111Server(Server):
    def __init__(self, storage):
        super().__init__(storage)
        self.provider.server.server_close()
        self.provider = HeldReadProvider()
        lines, size = [], 0
        while size < 32 * 1024:
            line = f'line-{len(lines) + 1:04}\t"quoted"\\path 界🙂\x01\n'
            lines.append(line)
            size += len(line.encode("utf-8"))
        self.numbered_text = "".join(f"{index}\t{line}" for index, line in enumerate(lines, 1))
        (self.project / "seed.txt").write_text("".join(lines), encoding="utf-8", newline="")

    def __exit__(self, *args):
        self.provider.next_release.set()
        return super().__exit__(*args)


def http_json(browser, path):
    status, body, _ = browser.request(path)
    if status != 200:
        raise AssertionError(f"v0111 HTTP status mismatch: {status}")
    return json.loads(body)


def turn_params(text, images=()):
    return {"operation_id": operation_id(), "input": {
        "parts": [{"type": "text", "text": text}, *images],
        "variables": {"set": {}, "remove": []}}}


class NativeV0111:
    backend: str

    def assert_statistics(self, snapshot):
        statistics = snapshot["statistics"]
        context = statistics["context"]
        self.assertEqual(context["scope"], "current_selected_context")
        self.assertEqual(context["provenance"], "context_budget_json_v1")
        self.assertGreater(context["bytes"], 0)
        self.assertEqual(sum(context["categories"].values()), context["bytes"])
        self.assertEqual(context["remaining_bytes"], max(0, context["limit_bytes"] - context["bytes"]))
        self.assertEqual(len(context["fingerprint"]), 64)
        self.assertEqual(statistics["usage"]["scope"], "append_only_consumption")
        self.assertIsNone(statistics["usage"]["main_cache_hit_rate"])
        self.assertIsNone(statistics["usage"]["aggregate_cache_hit_rate"])
        # This provider declares neither a model window nor cache token usage.
        self.assertIsNone(statistics["model"]["context_window_tokens"])
        self.assertIsNone(statistics["model"]["measured_headroom_tokens"])
        self.assertEqual(statistics["model"]["headroom_reason"], "model_window_unknown")
        timing = statistics["provider"]["timing"]
        if timing is not None:
            if timing["timing_source"] is None:
                self.assertIsNone(timing["generation_ms"])
                self.assertIsNone(timing["first_token_ms"])
                self.assertIsNone(timing["rate_basis"])
                self.assertIsNone(timing["tokens_per_second"])
            else:
                self.assertEqual(timing["timing_source"], "transport_stream")
                self.assertGreaterEqual(timing["first_token_ms"], 0)
                if timing["generation_ms"] is None:
                    self.assertIsNone(timing["rate_basis"])
                    self.assertIsNone(timing["tokens_per_second"])
                else:
                    self.assertGreaterEqual(timing["generation_ms"], 0)
                    self.assertEqual(timing["rate_basis"], "generation_interval")
                    if timing["generation_ms"] == 0 or timing["output_tokens"] is None:
                        self.assertIsNone(timing["tokens_per_second"])
                    else:
                        self.assertAlmostEqual(timing["tokens_per_second"], timing["output_tokens"] * 1000 / timing["generation_ms"])
            if timing["elapsed_ms"] in (None, 0) or timing["output_tokens"] is None:
                self.assertIsNone(timing["request_tokens_per_second"])
            else:
                self.assertAlmostEqual(timing["request_tokens_per_second"], timing["output_tokens"] * 1000 / timing["elapsed_ms"])
        return statistics

    def test_live_tool_peek_image_compaction_and_statistics_survive_native_recovery(self):
        with V0111Server(self.backend) as server:
            alice = Browser(server)
            principal = alice.login("alice")
            with alice.websocket() as channel:
                session = channel.command("session.create", params={"project_id": "synthetic", "agent": "coding", "mode": "plan", "operation_id": operation_id()})
                identifier = session["id"]
                subscribed = channel.command("session.subscribe", identifier)
                self.assertIsNone(self.assert_statistics(subscribed["snapshot"])["provider"]["latest_prompt"])
                first = turn_params("lifecycle-one")
                channel.command("turn.submit", identifier, first)
                self.assertTrue(server.provider.next_entered.wait(timeout=20), "real next provider request must be held")
                self.assertFalse(server.provider.next_release.is_set())
                tool = channel.event(lambda event: event["event"] == "tool" and
                    event["data"].get("operation_id") == first["operation_id"] and
                    event["data"].get("name") == "read" and event["data"].get("phase") == "completed")["data"]
                self.assertIsInstance(tool["execution_id"], int)
                self.assertIsInstance(tool["started_at_unix_ms"], int)
                self.assertIsInstance(tool["elapsed_ms"], int)
                self.assertGreaterEqual(tool["elapsed_ms"], 0)
                self.assertLessEqual(tool["started_at_unix_ms"], server.provider.next_request_entered_ms)
                self.assertLessEqual(tool["started_at_unix_ms"] + tool["elapsed_ms"], server.provider.next_request_entered_ms + 100)
                self.assertTrue(tool["preview_omitted"])
                self.assertNotIn("arguments", tool)
                self.assertNotIn("result", tool)
                self.assertIn(channel.command("turn.get", identifier, {"operation_id": first["operation_id"]})["status"], ("accepted", "running"))
                self.assertEqual(len(server.provider.snapshot()), 2, "tool inspection precedes next response")
                execution_id = tool["execution_id"]
                prefix = f"/api/v1/sessions/{identifier}/executions/{execution_id}"
                def retained_content(field):
                    cursor, source_hash, end_cursor, fragments = 0, None, None, []
                    for _ in range(128):
                        page = http_json(alice, prefix + f"/content?field={field}&cursor={cursor}&max_bytes=65536")
                        self.assertTrue(page["available"])
                        self.assertEqual(page["session_id"], identifier)
                        self.assertEqual(page["execution_id"], execution_id)
                        self.assertEqual(page["call_id"], tool["call_id"])
                        self.assertEqual(page["field"], field)
                        self.assertEqual(page["cursor"], cursor)
                        if source_hash is None:
                            source_hash, end_cursor = page["source_hash"], page["end_cursor"]
                            self.assertIsInstance(source_hash, str)
                        self.assertEqual(page["source_hash"], source_hash)
                        self.assertEqual(page["end_cursor"], end_cursor)
                        self.assertLessEqual(page["next_cursor"], end_cursor)
                        self.assertEqual(len(page["content"].encode("utf-8")), page["next_cursor"] - cursor)
                        fragments.append(page["content"])
                        if page["next_cursor"] == end_cursor:
                            content = "".join(fragments)
                            self.assertEqual(len(content.encode("utf-8")), end_cursor)
                            self.assertEqual(source_hash, hashlib.sha256(content.encode("utf-8")).hexdigest())
                            return content
                        self.assertGreater(page["next_cursor"], cursor)
                        cursor = page["next_cursor"]
                    self.fail("retained content page bound exhausted")

                arguments = retained_content("arguments")
                result = retained_content("result")
                self.assertEqual(json.loads(arguments)["path"], "seed.txt")
                receipt = json.loads(result)
                self.assertEqual(receipt["execution_id"], execution_id)
                self.assertEqual(receipt["session_id"], identifier)
                self.assertEqual(receipt["status"], "succeeded")
                self.assertTrue(receipt["output"]["closed"])
                cursor, output = 0, []
                for _ in range(128):
                    path = prefix + f"/output?cursor={cursor}&max_bytes=2048"
                    page = http_json(alice, path)
                    self.assertEqual(page, http_json(alice, path), "UI peek must not consume output")
                    self.assertEqual(page["execution_id"], execution_id)
                    self.assertTrue(page["closed"])
                    for chunk in page["chunks"]:
                        self.assertEqual(chunk["channel"], "result")
                        output.append(chunk["content"])
                    if page["next_cursor"] == page["end_cursor"]:
                        break
                    self.assertGreater(page["next_cursor"], cursor)
                    cursor = page["next_cursor"]
                else:
                    self.fail("output page bound exhausted")
                full_body = "".join(output)
                self.assertEqual(json.loads(full_body)["content"], server.numbered_text)
                self.assertFalse(server.provider.next_release.is_set())
                server.provider.next_release.set()
                self.assertEqual(channel.terminal(first["operation_id"])["status"], "completed")
                canonical = channel.event(lambda event: event["event"] == "snapshot" and
                    event["data"].get("session", {}).get("status") == "idle" and
                    any(message.get("text") == "canonical:lifecycle-one" for message in event["data"].get("messages", [])))["data"]
                drain_message = next(message for message in canonical["messages"] if message.get("tool_name") == "job")
                delivered = [message for request in server.provider.snapshot()
                    for message in request.get("messages", [])
                    if message.get("role") == "tool" and message.get("tool_call_id") == drain_message["call_id"]]
                self.assertTrue(delivered, "real provider must receive the actual job drain result")
                drain = json.loads(delivered[-1]["content"])
                self.assertEqual(drain["execution_id"], execution_id)
                self.assertEqual(drain["cursor"], 0, "UI peek must not advance model drain cursor")
                self.assertEqual(drain["expired_before"], 0)
                drained_body = "".join(chunk["content"] for chunk in drain["chunks"])
                self.assertTrue(full_body.startswith(drained_body))
                self.assertGreater(len(drained_body), 0)
                canonical_stats = self.assert_statistics(canonical)
                self.assertIsNotNone(canonical_stats["provider"]["timing"])
                self.assertEqual(canonical_stats["provider"]["timing"]["timing_source"], "transport_stream")

                png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9VQAAAAASUVORK5CYII=")
                image_turn = turn_params("after-restart", [{"type": "image", "source": {"type": "data", "media_type": "image/png", "data": list(png)}}])
                channel.command("turn.submit", identifier, image_turn)
                self.assertEqual(channel.terminal(image_turn["operation_id"])["status"], "completed")
                image_snapshot = channel.event(lambda event: event["event"] == "snapshot" and
                    event["data"].get("session", {}).get("status") == "idle" and
                    any(message.get("text") == "canonical:after-restart" for message in event["data"].get("messages", [])))["data"]
                self.assert_statistics(image_snapshot)
                message = next(message for message in image_snapshot["messages"] if message.get("role") == "user" and message.get("text") == "after-restart")
                source = message["parts"][1]["source"]
                image_url = source["url"]
                self.assertEqual(source["type"], "url")
                self.assertEqual(image_url, f"/api/v1/sessions/{identifier}/images/{message['id']}/1")
                for browser, expected in ((alice, 200), (Browser(server), 401)):
                    status, data, headers = browser.request(image_url)
                    self.assertEqual(status, expected)
                    if expected == 200:
                        self.assertEqual(data, png)
                        self.assertEqual(headers.get("Content-Type"), "image/png")
                        self.assertEqual(headers.get("Cache-Control"), "no-store")
                        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
                bob = Browser(server)
                bob.login("bob")
                self.assertEqual(bob.request(image_url)[0], 404)
                self.assertEqual(channel.command("session.close", identifier), {"closed": True})
                self.assert_statistics(channel.command("session.get", identifier))
                self.assertEqual(alice.request(image_url)[1], png)
            calls_before_restart = len(server.provider.snapshot())
            server.restart()
            alice = Browser(server)
            self.assertEqual(alice.login("alice")["owner"], principal["owner"])
            self.assertEqual(alice.request(image_url)[1], png)
            self.assertEqual(len(server.provider.snapshot()), calls_before_restart, "image recovery must not call provider")
            with alice.websocket() as channel:
                self.assert_statistics(channel.command("session.get", identifier))
                channel.command("session.open", identifier)
                channel.command("session.subscribe", identifier)
                compact_id = operation_id()
                params = {"operation_id": compact_id, "instruction": "compact-held"}
                server.provider.compact_release.set()
                channel.command("session.compact", identifier, params)
                self.assertEqual(channel.terminal(compact_id)["status"], "completed")
                compacted = channel.event(lambda event: event["event"] == "snapshot" and
                    event["data"].get("session", {}).get("status") == "idle" and bool(event["data"].get("compactions")))["data"]
                marker = compacted["compactions"][0]
                self.assertTrue(marker["id"])
                self.assertIn("## Objective", marker["summary"])
                receipt = channel.command("turn.get", identifier, {"operation_id": compact_id})
                self.assertEqual(receipt["result"]["compaction_id"], marker["id"])
                stats = self.assert_statistics(compacted)
                self.assertTrue(stats["context"]["pending_epoch"])
                if stats["provider"]["latest_prompt"] is not None:
                    self.assertEqual(stats["provider"]["latest_prompt"]["freshness"], "stale")
                    self.assertEqual(stats["provider"]["latest_prompt"]["freshness_reason"], "pending_epoch")
                calls_after_compact = len(server.provider.snapshot())
                self.assertEqual(calls_after_compact, calls_before_restart + 1)
                replay = channel.command("session.compact", identifier, params)
                self.assertEqual(replay["compactions"][0]["id"], marker["id"])
                self.assertEqual(channel.rejected("session.compact", identifier, {**params, "instruction": "different"})["code"], "operation_conflict")
                channel.command("session.close", identifier)
                self.assertEqual(channel.command("session.get", identifier)["compactions"][0], marker)
                self.assertEqual(len(server.provider.snapshot()), calls_after_compact, "compaction must not auto-submit a turn")
            server.restart()
            alice = Browser(server)
            alice.login("alice")
            with alice.websocket() as channel:
                self.assertEqual(channel.command("session.get", identifier)["compactions"][0], marker)
                self.assert_statistics(channel.command("session.get", identifier))
                self.assertEqual(channel.command("turn.get", identifier, {"operation_id": compact_id})["status"], "completed")
                self.assertEqual(len(server.provider.snapshot()), calls_after_compact)
            self.assertEqual(server.provider.errors, [])


class FileNativeV0111(NativeV0111, unittest.TestCase):
    backend = "file"


class SqliteNativeV0111(NativeV0111, unittest.TestCase):
    backend = "sqlite"


if __name__ == "__main__":
    unittest.main()
