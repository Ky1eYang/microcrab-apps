"""Real-process fixtures for native server acceptance; never log credentials/URLs.

No build/install is performed. Children inherit files for stdout/stderr, never
pipes. All workspace input is synthetic and all networking stays on loopback.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import AbstractContextManager
import http.cookiejar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener
import uuid

from websockets.sync.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

REPOSITORY = Path(__file__).resolve().parents[2]
TERMINAL = {"completed", "failed", "cancelled", "interrupted", "unknown"}
MAX_EVENTS = 1024


def operation_id() -> str:
    return str(uuid.uuid4())


def eventually(predicate, timeout: float, failure: str):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(failure)
        time.sleep(min(0.05, remaining))


def vacant_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


class Child:
    """Process lifetime with bounded cleanup; raw logs stay in private temp files."""
    def __init__(self, args: list[str], cwd: Path, log: Path):
        self.output = log.open("wb")
        options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
        try:
            self.process = subprocess.Popen(args, cwd=cwd, stdin=subprocess.DEVNULL,
                stdout=self.output, stderr=self.output, **options)
        except BaseException:
            self.output.close()
            raise

    def alive(self):
        if self.process.poll() is not None:
            raise AssertionError("managed test process exited before readiness")

    def stop(self):
        if self.process.poll() is None:
            try:
                self.process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
                self.process.wait(timeout=2)
            except (OSError, subprocess.TimeoutExpired):
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=2)
        self.output.close()


class ModelProvider(AbstractContextManager):
    """Bounded real JSON/SSE provider with a release barrier for disconnect tests."""
    def __init__(self):
        self.requests: list[dict] = []
        self.errors: list[str] = []
        self.lock = threading.Lock()
        self.counts: dict[str, int] = {}
        self.entered = threading.Event()
        self.release = threading.Event()
        self.compact_entered = threading.Event()
        self.compact_release = threading.Event()
        self.private_versions = {}
        self.private_read_paths = [".microcrab/config.json", ".microcrab/fixture-private.txt"]
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def log_message(self, *_args):
                pass

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if self.path != "/v1/chat/completions" or not 0 < length <= 256 * 1024:
                        raise ValueError("invalid synthetic provider request")
                    request = json.loads(self.rfile.read(length))
                    with owner.lock:
                        if len(owner.requests) >= 32:
                            raise ValueError("provider request budget exceeded")
                        owner.requests.append(request)
                    compact = any(message.get("role") == "user" and "compact-held" in str(message.get("content", ""))
                        for message in request.get("messages", []))
                    phase = "compact-held" if compact else owner.phase(request) if request.get("tools") else "functional-rename"
                    with owner.lock:
                        count = owner.counts.get(phase, 0)
                        owner.counts[phase] = count + 1
                    if phase == "compact-held":
                        owner.compact_entered.set()
                        if not owner.compact_release.wait(timeout=15):
                            raise TimeoutError("compaction acceptance barrier not released")
                        text = "## Objective\n- Synthetic\n## Important Details\n- None\n## Work State\n### Completed\n- Synthetic\n### Active\n- None\n### Blocked\n- None\n## Next Move\n1. Continue\n## Relevant Files\n- None"
                        body = json.dumps({"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}]}).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    if phase == "functional-rename":
                        body = json.dumps({"choices": [{"message": {"role": "assistant", "content": "Native fixture title"},
                            "finish_reason": "stop"}], "usage": {"prompt_tokens": 3, "completion_tokens": 2}}).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    if (phase in ("lifecycle-one", "lifecycle-two", "private-read", "private-write", "private-process", "browser-shell", "browser-fork-read") and count == 0) or (phase == "browser-shell" and count == 1):
                        calls = [{"id": f"synthetic-{phase}-{count}-{index}" if phase == "browser-shell" else f"synthetic-{phase}-{index}", "type": "function",
                            "function": {"name": name, "arguments": json.dumps(arguments)}}
                            for index, (name, arguments) in enumerate(owner.scripted_calls(phase, count))]
                        body = json.dumps({"choices": [{"message": {"role": "assistant", "content": "", "tool_calls": calls},
                            "finish_reason": "tool_calls"}], "usage": {"prompt_tokens": 13, "completion_tokens": 4}}).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    text = "canonical:" + phase
                    half = max(1, len(text) // 2)
                    chunks = [
                        {"choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
                        {"choices": [{"index": 0, "delta": {"content": text[:half]}, "finish_reason": None}]},
                        {"choices": [{"index": 0, "delta": {"content": text[half:]}, "finish_reason": None}]},
                        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                        {"choices": [], "usage": {"prompt_tokens": 13, "completion_tokens": 4, "total_tokens": 17}},
                    ]
                    pieces = [("data: " + json.dumps(chunk) + "\n\n").encode() for chunk in chunks]
                    pieces.append(b"data: [DONE]\n\n")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Content-Length", str(sum(map(len, pieces))))
                    self.end_headers()
                    for index, piece in enumerate(pieces):
                        self.wfile.write(piece)
                        self.wfile.flush()
                        if phase == "disconnect-held" and index == 1:
                            owner.entered.set()
                            if not owner.release.wait(timeout=15):
                                raise TimeoutError("disconnect acceptance barrier not released")
                except Exception as error:
                    with owner.lock:
                        owner.errors.append(type(error).__name__)
                    self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)

    def scripted_calls(self, phase, count=0):
        if phase == "browser-fork-read":
            return [("read", {"path": "browser-created.txt"})]
        if phase == "browser-shell":
            if count == 0:
                return [("wasmsh", {"command": "echo browser-shell-ok; python3 -c \"print(6 * 7)\"; echo browser-durable > browser-created.txt", "yield_ms": 1000})]
            return [("read", {"path": "browser-created.txt"})]
        if phase == "lifecycle-one":
            return [("read", {"path": "seed.txt"})]
        if phase == "lifecycle-two":
            return [("write", {"path": "blocked.txt", "content": "must-not-be-written"})]
        if phase == "private-read":
            return [("read", {"path": path}) for path in self.private_read_paths] + [
                ("glob", {"root": ".", "pattern": "**/*", "include_hidden": True, "include_ignored": True, "max_results": 100}),
                ("grep", {"root": ".", "pattern": "jwt_secret|synthetic-private-data-marker", "include_glob": "**/*", "max_matches": 100})]
        if phase == "private-write":
            return [("write", {"path": path, "content": "synthetic-forbidden-replacement", "expected_version": version})
                for path, version in self.private_versions.items()]
        if phase == "private-process":
            command = "Set-Content -LiteralPath 'process-probe-marker.txt' -Value 'synthetic-marker'" if os.name == "nt" else "printf synthetic-marker > process-probe-marker.txt"
            return [("pwsh" if os.name == "nt" else "bash", {"command": command, "yield_ms": 1000})]
        raise AssertionError("unsupported scripted fixture call")

    @staticmethod
    def phase(request):
        for message in reversed(request.get("messages", [])):
            if message.get("role") != "user":
                continue
            content = message.get("content", "")
            if isinstance(content, list):
                content = "\n".join(part.get("text", "") for part in content if part.get("type") == "text")
            if content in {"lifecycle-one", "lifecycle-two", "disconnect-held", "after-restart", "naming-followup", "private-read", "private-write", "private-process", "browser-shell", "browser-fork-read"}:
                return content
        raise AssertionError("model request lost the synthetic user input")

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_port}/v1/chat/completions"

    def snapshot(self):
        with self.lock:
            return list(self.requests)

    def __enter__(self):
        self.worker.start()
        return self

    def __exit__(self, *_args):
        self.release.set()
        self.compact_release.set()
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(timeout=2)


# Keep aligned with crates/server/src/route/session.rs and project.rs.
HTTP_COMMANDS = {
    "server.capabilities": ("GET", "/capabilities"),
    "identity.current": ("GET", "/identity"),
    "project.create": ("POST", "/projects"),
    "project.list": ("GET", "/projects"),
    "model.list": ("GET", "/models"),
    "agent.catalog": ("GET", "/projects/{project_id}/agents"),
    "session.list": ("GET", "/sessions"),
    "session.create": ("POST", "/sessions"),
    "session.get": ("GET", "/sessions/{session_id}"),
    "session.rename": ("PATCH", "/sessions/{session_id}"),
    "session.archive": ("DELETE", "/sessions/{session_id}"),
    "session.open": ("PUT", "/sessions/{session_id}/runtime"),
    "session.close": ("DELETE", "/sessions/{session_id}/runtime"),
    "session.messages": ("GET", "/sessions/{session_id}/messages"),
    "history.get": ("GET", "/sessions/{session_id}/history"),
    "history.travel": ("POST", "/sessions/{session_id}/history/travel"),
    "checkpoint.create": ("POST", "/sessions/{session_id}/checkpoints"),
    "session.compact": ("POST", "/sessions/{session_id}/compact"),
    "permission.get": ("GET", "/sessions/{session_id}/permission"),
    "permission.set": ("PUT", "/sessions/{session_id}/permission"),
    "session.set_mode": ("PUT", "/sessions/{session_id}/mode"),
    "executions.list": ("GET", "/sessions/{session_id}/executions"),
    "executions.read": ("GET", "/sessions/{session_id}/executions/{execution_id}/output"),
    "executions.cancel": ("POST", "/sessions/{session_id}/executions/{execution_id}/cancel"),
    "workspace.files": ("GET", "/sessions/{session_id}/workspace/files"),
    "turn.get": ("GET", "/sessions/{session_id}/operations/{operation_id}"),
}


class NativeSocket(AbstractContextManager):
    """One realtime socket plus independent, never-retried HTTP command waiters."""
    def __init__(self, origin: str, cookie: str | None, *, ws_origin=...):
        headers = {"Cookie": cookie} if cookie else {}
        self.socket = connect(origin.replace("http://", "ws://", 1) + "/api/v1/ws",
            origin=origin if ws_origin is ... else ws_origin, additional_headers=headers,
            open_timeout=5, close_timeout=2, max_size=1024 * 1024, proxy=None)
        self.origin = origin
        self.cookie = cookie
        self.http_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="native-http")
        self.http_pending = {}
        self.queue = []
        self.observed = []
        self.last_sequence: dict[tuple[str, str], int] = {}
        self.pending_ids = set()
        self.results = {}

    def receive(self, timeout=10):
        value = json.loads(self.socket.recv(timeout=timeout))
        if value.get("v") != 1 or value.get("type") not in ("result", "event"):
            raise AssertionError("server sent an invalid native frame")
        if value["type"] == "event":
            key = (value["subscription_id"], value["generation"])
            previous = self.last_sequence.get(key, 0)
            if value["sequence"] <= previous:
                raise AssertionError("subscription event sequence did not advance")
            self.last_sequence[key] = value["sequence"]
            if len(self.observed) >= MAX_EVENTS:
                raise AssertionError("server event budget exceeded")
            self.observed.append(value)
        return value

    def _http_result(self, request):
        method = request["method"]
        verb, path = HTTP_COMMANDS[method]
        params = dict(request["params"])
        if "{session_id}" in path:
            if request.get("session_id") is None:
                return {"v": 1, "type": "result", "request_id": request["request_id"],
                    "ok": False, "error": {"code": "invalid_request", "message": "Session is required"}}
            path = path.replace("{session_id}", quote(str(request["session_id"]), safe=""))
        for field in ("project_id", "operation_id", "execution_id"):
            if "{" + field + "}" in path:
                if field not in params:
                    raise AssertionError(f"native command {method} missing path parameter {field}")
                path = path.replace("{" + field + "}", quote(str(params.pop(field)), safe=""))
        headers = {"Cookie": self.cookie} if self.cookie else {}
        data = None
        if verb == "GET":
            if params:
                path += "?" + urlencode(params)
        else:
            headers.update({"Origin": self.origin, "Content-Type": "application/json"})
            data = json.dumps(params).encode("utf-8")
        http_request = Request(self.origin + "/api/v1" + path, method=verb, headers=headers, data=data)
        try:
            # One attempt only: a timeout/disconnect cannot authorize mutation replay.
            with build_opener().open(http_request, timeout=60) as response:
                body = response.read(1024 * 1024)
                status = response.status
        except HTTPError as error:
            with error:
                body = error.read(1024 * 1024)
                status = error.code
        except (URLError, TimeoutError, OSError):
            raise AssertionError(f"native command {method} HTTP transport failed; reconcile before retrying") from None
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            raise AssertionError(f"native command {method} invalid HTTP response: {status}") from None
        result = {"v": 1, "type": "result", "request_id": request["request_id"], "ok": status < 400}
        if result["ok"]:
            result["data"] = payload
        else:
            wire_error = payload.get("error", {}) if isinstance(payload, dict) else {}
            code = wire_error.get("code", "invalid_http_error") if isinstance(wire_error, dict) else "invalid_http_error"
            if not isinstance(code, str) or not code or len(code) > 80 or any(
                    character not in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in code):
                code = "invalid_http_error"
            result["error"] = {"code": code, "message": f"HTTP command {method} rejected: {code}"}
        return result

    def send_command(self, method: str, session: str | None = None, params: dict | None = None, *, version=1):
        request = {"v": version, "type": "command", "request_id": operation_id(),
            "method": method, "params": dict(params or {})}
        if session is not None:
            request["session_id"] = session
        if len(self.pending_ids) >= 16:
            raise AssertionError("test command waiter budget exceeded")
        self.pending_ids.add(request["request_id"])
        if version == 1 and method in HTTP_COMMANDS:
            self.http_pending[request["request_id"]] = self.http_executor.submit(self._http_result, request)
        else:
            self.socket.send(json.dumps(request))
        return request["request_id"]

    def await_result(self, request_id, timeout=15):
        if request_id in self.http_pending:
            try:
                result = self.http_pending[request_id].result(timeout=timeout)
            except FutureTimeoutError:
                raise AssertionError("native HTTP command result deadline exceeded; reconcile before retrying") from None
            self.http_pending.pop(request_id)
            self.pending_ids.remove(request_id)
            return result
        deadline = time.monotonic() + timeout
        while True:
            if request_id in self.results:
                self.pending_ids.remove(request_id)
                return self.results.pop(request_id)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("native command result deadline exceeded")
            response = self.receive(remaining)
            if response["type"] == "event":
                self.queue.append(response)
                continue
            correlated = response.get("request_id")
            if correlated not in self.pending_ids or correlated in self.results:
                raise AssertionError("native command result correlation mismatch")
            self.results[correlated] = response

    def result(self, method: str, session: str | None = None, params: dict | None = None, *, version=1):
        request_id = self.send_command(method, session, params, version=version)
        return self.await_result(request_id)

    def command(self, method, session=None, params=None):
        response = self.result(method, session, params)
        if not response.get("ok"):
            code = response.get("error", {}).get("code", "missing_error_code")
            raise AssertionError(f"native command {method} rejected: {code}")
        return response["data"]

    def rejected(self, method, session=None, params=None, **options):
        result = self.result(method, session, params, **options)
        if result.get("ok") is not False or not result.get("error", {}).get("code"):
            raise AssertionError(f"native command {method} unexpectedly succeeded")
        return result["error"]

    def event(self, predicate, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            for index, value in enumerate(self.queue):
                if predicate(value):
                    return self.queue.pop(index)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("expected native event deadline exceeded")
            value = self.receive(remaining)
            if value["type"] != "event":
                raise AssertionError("unsolicited result while awaiting event")
            self.queue.append(value)

    def terminal(self, identifier):
        return self.event(lambda event: event["event"] == "turn" and
            event["data"].get("id") == identifier and event["data"].get("status") in TERMINAL)["data"]

    def expect_closed(self, timeout=5):
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                self.socket.recv(timeout=max(0.001, deadline - time.monotonic()))
        except ConnectionClosed:
            return
        except TimeoutError:
            pass
        raise AssertionError("authentication invalidation did not close existing WebSocket")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        try:
            self.socket.close()
        finally:
            # Running HTTP calls retain their sole waiter until completion; never replay.
            self.http_executor.shutdown(wait=False, cancel_futures=True)


class Browser:
    def __init__(self, server: "Server"):
        self.server = server
        self.cookies = http.cookiejar.CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookies))

    def request(self, path: str, *, method="GET", origin=None, cookie=None, data=None):
        headers = {}
        if origin is not None:
            headers["Origin"] = origin
        if cookie is not None:
            headers["Cookie"] = cookie
        body = None
        if data is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(data).encode("utf-8")
        request = Request(self.server.origin + path, method=method, headers=headers, data=body)
        try:
            with self.opener.open(request, timeout=60) as response:
                return response.status, response.read(256 * 1024), response.headers
        except HTTPError as error:
            with error:
                return error.code, error.read(256 * 1024), error.headers
        except (URLError, TimeoutError):
            raise AssertionError("loopback browser HTTP request failed") from None

    def login(self, account="alice"):
        status, _, _ = self.request("/api/auth/login?" + urlencode({"login_hint": account}))
        if status != 200:
            raise AssertionError(f"browser OAuth redirect flow failed with status {status}")
        status, body, _ = self.request("/api/auth/session")
        if status != 200:
            raise AssertionError(f"browser auth session probe failed with status {status}")
        principal = json.loads(body)["principal"]
        if self.server.manage_host:
            self.server.ensure_project(self, principal)
        return principal

    def logout(self):
        status, body, headers = self.request("/api/auth/logout", method="POST", origin=self.server.origin)
        if status != 200:
            raise AssertionError(f"configured OIDC logout returned status {status}")
        redirect = json.loads(body).get("redirect")
        if not isinstance(redirect, str):
            raise AssertionError("OIDC logout omitted its redirect")
        target = urlsplit(redirect)
        config = self.server.configuration["auth"]
        endpoint = urlsplit(config["end_session_endpoint"])
        if (target.scheme, target.netloc, target.path) != (endpoint.scheme, endpoint.netloc, endpoint.path) or target.fragment:
            raise AssertionError("OIDC logout redirect does not match the configured IdP endpoint")
        query = parse_qs(target.query, keep_blank_values=True)
        if query.get("client_id") != [config["client_id"]]:
            raise AssertionError("OIDC logout client identity does not match configuration")
        hints = query.get("id_token_hint", [])
        if len(hints) != 1 or not hints[0]:
            raise AssertionError("OIDC logout omitted its authenticated ID token hint")
        expected_return = config.get("post_logout_redirect_uri")
        if query.get("post_logout_redirect_uri") != ([expected_return] if expected_return else None):
            raise AssertionError("OIDC logout return URI does not match configuration")
        cleared = headers.get_all("Set-Cookie", [])
        for name in ("microcrab_session", "microcrab_login"):
            if not any(value.startswith(name + "=;") and "Max-Age=0" in value for value in cleared):
                raise AssertionError("OIDC logout did not expire an application cookie")
            if any(cookie.name == name for cookie in self.cookies):
                raise AssertionError("OIDC logout retained an application cookie")

    @property
    def cookie(self):
        tokens = [cookie for cookie in self.cookies if cookie.name == "microcrab_session"]
        if len(tokens) != 1:
            raise AssertionError("browser did not receive exactly one application cookie")
        return "microcrab_session=" + tokens[0].value

    def websocket(self):
        return NativeSocket(self.server.origin, self.cookie)


class DirectHost:
    """HTTP attachment leaves native transport and resume credentials with Server."""
    def __init__(self, server, browser, ready, paired):
        params = {"url": ready["url"]}
        if not paired:
            params["token"] = ready["pairing_token"]
        status, body, _ = browser.request("/api/v1/projects/synthetic/host/connect",
            method="POST", origin=server.origin, data=params)
        attached = json.loads(body)
        if status != 200 or not attached.get("ready"):
            raise AssertionError("synthetic direct native Host did not become ready")
        self.scope = attached["scope"]
        self.descriptor = attached["descriptor"]


class Server(AbstractContextManager):
    def __init__(self, backend="file", *, ttl=900, auto_rename=False, frontend_dir=None, project_at_startup=False):
        self.backend = backend
        self.manage_host = True
        self.ttl = ttl
        self.auto_rename = auto_rename
        self.project_at_startup = project_at_startup
        executable = "microcrab-server.exe" if os.name == "nt" else "microcrab-server"
        self.binary = Path(os.environ.get("MICROCRAB_SERVER_BIN", str(REPOSITORY / "target" / "debug" / executable))).resolve()
        self.guest = Path(os.environ.get("MICROCRAB_CODING_WASM", str(REPOSITORY / ".microcrab" / "agents-v0110-release" / "coding.wasm"))).resolve()
        if not self.binary.is_file():
            raise AssertionError("native server binary missing; parent must build the all-features server gate first")
        if not self.guest.is_file():
            raise AssertionError("coding component missing; set MICROCRAB_CODING_WASM to the established component artifact")
        self.temporary = tempfile.TemporaryDirectory(prefix="microcrab-native-acceptance-")
        self.root = Path(self.temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.startup = self.root if project_at_startup else self.project
        (self.project / "seed.txt").write_text("synthetic-seed-content\n", encoding="utf-8")
        self.frontend = Path(frontend_dir).resolve() if frontend_dir is not None else self.root / "frontend"
        if frontend_dir is None:
            self.frontend.mkdir()
            (self.frontend / "index.html").write_text("integration-static", encoding="utf-8")
        elif not (self.frontend / "index.html").is_file():
            self.temporary.cleanup()
            raise AssertionError("built frontend index missing; parent must finish frontend dist first")
        self.origin = f"http://127.0.0.1:{vacant_port()}"
        self.provider = ModelProvider()
        self.native = None
        self.runtime = self.startup / ".microcrab"
        self.runtime.mkdir()
        self.services = self.root / ".fixture-services"
        self.services.mkdir()
        self.config_path = self.runtime / "config.json"
        agents = self.runtime / "agents"
        agents.mkdir()
        import shutil
        shutil.copyfile(self.guest, agents / "coding.wasm")
        self.jwt_secret = secrets.token_urlsafe(48)
        self.starts = 0
        self.bridge_child = None
        self.native_host = None
        self.bridge_ready = None
        self.host_paired = False
        self.project_owner = None

    def ensure_project(self, browser, principal):
        # The first login owns this synthetic Project; other subjects never inherit it.
        if self.project_owner is not None and self.project_owner != principal["owner"]:
            return
        if self.native_host is not None:
            return
        with browser.websocket() as channel:
            projects = channel.command("project.list")["items"]
            if not any(project["id"] == "synthetic" for project in projects):
                channel.command("project.create", params={"id": "synthetic",
                    "name": "Synthetic integration project", "root": str(self.project)})
        if self.bridge_child is None:
            executable = "microcrab-local-bridge.exe" if os.name == "nt" else "microcrab-local-bridge"
            binary = Path(os.environ.get("MICROCRAB_LOCAL_BRIDGE_BIN",
                str(REPOSITORY / "target" / "debug" / executable))).resolve()
            if not binary.is_file():
                raise AssertionError("native Host bridge binary missing; parent must finish its native gate")
            private = self.project / ".microcrab"
            private.mkdir(exist_ok=True)
            ready = private / ("bridge-ready-" + operation_id() + ".json")
            self.bridge_child = Child([str(binary), "--root", str(self.project), "--origin", self.origin,
                "--require-token", "--listen", "127.0.0.1:0", "--ready-file", str(ready)],
                self.project, self.services / "bridge.log")
            def metadata():
                self.bridge_child.alive()
                try:
                    return json.loads(ready.read_text(encoding="utf-8"))
                except (FileNotFoundError, json.JSONDecodeError):
                    return None
            self.bridge_ready = eventually(metadata, 15, "synthetic Host bridge readiness deadline exceeded")
        self.native_host = DirectHost(self, browser, self.bridge_ready, self.host_paired)
        self.host_paired = True
        self.project_owner = principal["owner"]

    def start_native(self):
        self.starts += 1
        self.native = Child([str(self.binary)], self.startup,
            self.services / f"server-{self.starts}.log")
        def healthy():
            self.native.alive()
            try:
                status, body, _ = Browser(self).request("/api/v1/health")
                return status == 200 and json.loads(body).get("protocol") == 1
            except AssertionError:
                return False
        eventually(healthy, 60, "native server readiness deadline exceeded")

    def restart(self):
        self.native_host = None
        self.native.stop()
        self.native = None
        self.start_native()

    def __enter__(self):
        try:
            self.provider.__enter__()
            issuer = self.origin + "/api/dev/oidc"
            endpoints = {"dev_oidc": True, "discovery": True, "issuer": issuer,
                "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token",
                "jwks_uri": issuer + "/jwks", "end_session_endpoint": issuer + "/logout",
                "post_logout_redirect_uri": self.origin + "/login", "client_id": "microcrab-dev"}
            self.configuration = {
                "listen": self.origin.removeprefix("http://"),
                "model_metadata": {"enabled": False},
                "storage": self.backend, "frontend_dir": str(self.frontend), "auto_rename": self.auto_rename,
                "auth": {**endpoints, "redirect_uri": self.origin + "/api/auth/callback",
                    "post_login_redirect": "/", "allowed_origins": [self.origin],
                    "jwt_secret": self.jwt_secret, "jwt_issuer": "native-integration",
                    "jwt_audience": "native-integration-browser", "cookie_secure": False,
                    "allow_http_loopback": True, "session_ttl_seconds": self.ttl},
                "provider": {"endpoint": self.provider.endpoint, "model": "synthetic-integration-model",
                    "timeout_ms": 30000, "max_response_bytes": 256 * 1024},
                "agent": {"name": "coding", "mode": "plan"},
            }
            self.config_path.write_text(json.dumps(self.configuration), encoding="utf-8")
            self.start_native()
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *_args):
        self.provider.release.set()
        self.provider.compact_release.set()
        if self.native is not None:
            self.native.stop()
        if self.bridge_child is not None:
            self.bridge_child.stop()
        self.provider.__exit__()
        self.temporary.cleanup()


def upgrade_status(origin: str, cookie: str | None, *, ws_origin=...):
    try:
        with NativeSocket(origin, cookie, ws_origin=ws_origin):
            return 101
    except InvalidStatus as error:
        return error.response.status_code
