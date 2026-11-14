"""Keep a real native backend with embedded issuer/mock provider alive for acceptance.

Parent must build server and frontend first. No build/install occurs here. Ready
metadata is public only; child stdout/stderr remain private temporary log files.
"""
from __future__ import annotations

import os

import argparse
import json
from pathlib import Path
import signal
import threading
import uuid

from harness import Browser, REPOSITORY, Server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--storage", choices=("file", "sqlite"), default="file")
    args = parser.parse_args()
    stopped = threading.Event()

    def stop(_signal, _frame):
        stopped.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, stop)
    with Server(args.storage, frontend_dir=Path(os.environ.get("MICROCRAB_FRONTEND_DIR", str(REPOSITORY / "public"))), auto_rename=False) as server:
        Browser(server).login()
        ready = args.ready_file.resolve()
        ready.parent.mkdir(parents=True, exist_ok=True)
        stop_file = ready.with_name(ready.name + "." + uuid.uuid4().hex + ".stop")
        public = {"url": server.origin, "login_url": server.origin + "/api/auth/login",
            "project_id": "synthetic", "root": str(server.project), "stop_file": str(stop_file)}
        ready.write_text(json.dumps(public, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(public), flush=True)
        # Browser steps use lifecycle-one for actual guest read then SSE, and
        # lifecycle-two for a guest write attempt. The initial coding mode is plan.
        # disconnect-held deliberately needs release; use only in unittest gate.
        while not stopped.wait(timeout=0.25):
            if stop_file.exists():
                break
            server.native.alive()


if __name__ == "__main__":
    main()
