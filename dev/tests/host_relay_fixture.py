"""Managed native backend + filesystem bridge for browser Host-relay acceptance.

No builds occur here. Workspace data is synthetic. The default provider is a local
script; --real-provider-config explicitly selects an existing private compatible
catalog for external model acceptance. Credentials and child logs stay inside
private fixture directories; public readiness never contains credentials.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
import uuid

from harness import Browser, Child, ModelProvider, REPOSITORY, Server, eventually


class RelayProvider(ModelProvider):
    def scripted_calls(self, phase):
        if phase == "lifecycle-two":
            return [("write", {"path": "relay-written.txt", "content": "relay-write-content\n"})]
        return super().scripted_calls(phase)


class RelayServer(Server):
    def __init__(self, storage, provider_config=None, provider_name=None, *, auto_rename=False):
        self.provider_config = provider_config
        self.provider_name = provider_name
        super().__init__(storage, frontend_dir=Path(os.environ.get("MICROCRAB_FRONTEND_DIR", str(REPOSITORY / "public"))), auto_rename=auto_rename)
        self.manage_host = False
        self.provider.server.server_close()
        self.provider = RelayProvider()
        self.remote = self.root / "remote-workspace"
        self.remote.mkdir()
        (self.remote / "seed.txt").write_text("synthetic-seed-content\n", encoding="utf-8")
        # Unrelated data must never cause automatic workspace capture.
        (self.remote / "unrelated.txt").write_text("unrelated-workspace-content\n", encoding="utf-8")
        (self.project / "seed.txt").write_text("backend-decoy-must-never-be-read\n", encoding="utf-8")

    def start_native(self):
        if self.provider_config is not None:
            self.configuration["provider"] = {"config_file": str(self.provider_config)}
            if self.provider_name:
                self.configuration["provider"]["name"] = self.provider_name
        self.config_path.write_text(json.dumps(self.configuration), encoding="utf-8")
        super().start_native()


def inspect_cache(data_dir, storage, session_id):
    """Inspect only this session's journal; return booleans, never persisted bodies."""
    if len(session_id) != 32 or any(char not in "0123456789abcdef" for char in session_id):
        raise ValueError("invalid session identity")
    if storage == "file":
        nodes = (data_dir / "sessions" / session_id / "journal" / "nodes").glob("*.json")
        values = [json.loads(path.read_text(encoding="utf-8")) for path in nodes]
    else:
        with closing(sqlite3.connect((data_dir / "storage.sqlite").as_uri() + "?mode=ro", uri=True)) as database:
            values = [json.loads(row[0]) for row in database.execute(
                "SELECT body FROM journal_nodes WHERE session_id = ?", (session_id,))]
    reports = [value["checkpoint"]["recover"].get("cache_report") for value in values]
    reports = [report for report in reports if report is not None]
    points = [point for report in reports for point in report["points"]]
    expected = b"relay-write-content\n"
    write_report = False
    for report in reports:
        selected = [point for point in report["points"] if point["path"] == "relay-written.txt"]
        for index, before in enumerate(selected):
            if (before.get("data") is None and before.get("version") is None
                    and before.get("tool") == "write" and before.get("sequence") == 0
                    and isinstance(before.get("call_id"), str) and before["call_id"]):
                write_report |= any(after.get("data") == list(expected)
                    and after.get("version") == hashlib.sha256(expected).hexdigest()
                    and after.get("tool") == "write" and after.get("sequence") == 1
                    and after.get("call_id") == before["call_id"]
                    for after in selected[index + 1:])
    return {"has_checkpoints": bool(values), "point_count": len(points),
        "write_before_after": write_report,
        "scope_ids_valid": all(all(isinstance(report.get(key), int) and report[key] >= 0
            for key in ("epoch_id", "turn_id", "round_id")) for report in reports),
        "only_selected_path": all(point["path"] == "relay-written.txt" for point in points)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--storage", choices=("file", "sqlite"), default="file")
    parser.add_argument("--real-provider-config", type=Path)
    parser.add_argument("--provider-name")
    parser.add_argument("--auto-rename", action="store_true")
    parser.add_argument("--ui-project-root", action="store_true",
        help="Create the initial Project with the bridge workspace for normal UI connection")
    args = parser.parse_args()
    provider_config = args.real_provider_config.resolve(strict=True) if args.real_provider_config else None
    stopped = threading.Event()
    def stop(_signal, _frame):
        stopped.set()
    for item in (signal.SIGINT, signal.SIGTERM):
        signal.signal(item, stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, stop)
    executable = "microcrab-local-bridge.exe" if os.name == "nt" else "microcrab-local-bridge"
    binary = Path(os.environ.get("MICROCRAB_LOCAL_BRIDGE_BIN", str(REPOSITORY / "target" / "debug" / executable))).resolve()
    if not binary.is_file():
        raise AssertionError("Host bridge binary missing; parent must complete its native gate")
    with RelayServer(args.storage, provider_config, args.provider_name, auto_rename=args.auto_rename) as server:
        browser = Browser(server)
        browser.login()
        with browser.websocket() as channel:
            # The reference relay default deliberately ignores a bad backend-local hint.
            # Normal UI connection passes the declared fixed directory, so its fixture
            # must declare the actual remote workspace instead of that decoy hint.
            project_root = server.remote if args.ui_project_root else server.root / "nonexistent-backend-workspace"
            channel.command("project.create", params={"id": "synthetic", "name": "Synthetic integration project",
                "root": str(project_root)})
        private = server.remote / ".microcrab"
        private.mkdir()
        bridge = None
        bridge_starts = 0

        def start_bridge():
            nonlocal bridge_starts
            bridge_starts += 1
            bridge_ready = private / ("bridge-ready-" + str(bridge_starts) + "-" + uuid.uuid4().hex + ".json")
            child = Child([str(binary), "--root", str(server.remote), "--origin", server.origin,
                "--require-token", "--listen", "127.0.0.1:0", "--ready-file", str(bridge_ready)], server.remote,
                server.services / ("host-bridge-" + str(bridge_starts) + ".log"))
            try:
                def ready_bridge():
                    child.alive()
                    try:
                        value = json.loads(bridge_ready.read_text(encoding="utf-8"))
                        return value if isinstance(value.get("url"), str) and isinstance(value.get("pairing_token"), str) else None
                    except (FileNotFoundError, json.JSONDecodeError):
                        return None
                eventually(ready_bridge, 15, "Host bridge readiness deadline exceeded")
                return child, bridge_ready
            except BaseException:
                child.stop()
                raise

        try:
            bridge, bridge_ready = start_bridge()
            ready = args.ready_file.resolve()
            ready.parent.mkdir(parents=True, exist_ok=True)
            marker_prefix = ready.name + "." + uuid.uuid4().hex
            stop_file = ready.with_name(marker_prefix + ".stop")
            controls = {name: ready.with_name(marker_prefix + "." + name) for name in (
                "backend_restart_request", "backend_restart_ack", "bridge_restart_request", "bridge_restart_ack",
                "cache_report_request", "cache_report_ack", "provider_release_request", "provider_release_ack")}
            provider_held_file = ready.with_name(marker_prefix + ".provider-held")
            public = {"url": server.origin, "login_url": server.origin + "/api/auth/login",
                "login_entry_url": server.configuration["auth"]["issuer"] + "/",
                "real_provider": provider_config is not None,
                "project_id": "synthetic", "root": str(server.remote.resolve()), "backend_root": str(server.project.resolve()),
                "data_dir": str(server.runtime.resolve()), "require_token": True,
                "provider_held_file": str(provider_held_file),
                "bridge_ready_file": str(bridge_ready), "stop_file": str(stop_file), "storage": args.storage,
                **{name: str(path) for name, path in controls.items()}}

            def publish():
                temporary = ready.with_name(marker_prefix + ".update")
                temporary.write_text(json.dumps(public, indent=2) + "\n", encoding="utf-8")
                temporary.replace(ready)

            def request(name):
                path = controls[name + "_request"]
                try:
                    value = path.read_text(encoding="utf-8").strip()
                except FileNotFoundError:
                    return None
                # UUID nonce bounds fixture control input and prevents repeat execution.
                if len(value) != 32 or any(c not in "0123456789abcdef" for c in value):
                    return None
                ack = controls[name + "_ack"]
                if ack.exists() and ack.read_text(encoding="utf-8").strip() == value:
                    return None
                return value

            publish()
            print(json.dumps({"status": "ready", "storage": args.storage}), flush=True)
            while not stopped.wait(0.25):
                if stop_file.exists():
                    break
                if server.provider.entered.is_set() and not provider_held_file.exists():
                    provider_held_file.write_text("entered\n", encoding="utf-8")
                nonce = request("provider_release")
                if nonce:
                    server.provider.release.set()
                    controls["provider_release_ack"].write_text(nonce + "\n", encoding="utf-8")
                nonce = request("backend_restart")
                if nonce:
                    server.restart()
                    controls["backend_restart_ack"].write_text(nonce + "\n", encoding="utf-8")
                nonce = request("bridge_restart")
                if nonce:
                    bridge.stop()
                    bridge = None
                    bridge, bridge_ready = start_bridge()
                    public["bridge_ready_file"] = str(bridge_ready)
                    publish()
                    controls["bridge_restart_ack"].write_text(nonce + "\n", encoding="utf-8")
                try:
                    cache_request = json.loads(controls["cache_report_request"].read_text(encoding="utf-8"))
                except (FileNotFoundError, json.JSONDecodeError):
                    cache_request = None
                if cache_request is not None:
                    nonce = cache_request.get("nonce", "")
                    if len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce):
                        raise ValueError("invalid cache inspection nonce")
                    ack_path = controls["cache_report_ack"]
                    previous = json.loads(ack_path.read_text(encoding="utf-8")) if ack_path.exists() else {}
                    if previous.get("nonce") != nonce:
                        evidence = inspect_cache(server.runtime.resolve(), args.storage, cache_request["session_id"])
                        ack_path.write_text(json.dumps({"nonce": nonce, **evidence}) + "\n", encoding="utf-8")
                server.native.alive()
                bridge.alive()
            errors = list(server.provider.errors)
            if server.provider.entered.is_set() and server.provider.release.is_set():
                errors = [error for error in errors if error not in ("BrokenPipeError", "ConnectionResetError", "ConnectionAbortedError")]
            if errors:
                raise AssertionError("synthetic provider reported errors")
        finally:
            if bridge is not None:
                bridge.stop()


if __name__ == "__main__":
    main()
