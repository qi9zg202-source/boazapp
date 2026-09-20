#!/usr/bin/env python3
"""Run synthetic loopback HTTP contracts without weakening production startup.

The production binary must refuse an unactivated generation. Positive HTTP
routes are exercised by the Rust integration test binary, never a TEST_BYPASS
or a locally forged active-set pointer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "Server/target/debug/boaz-health-receiver"


def run_binary(environment: dict[str, str], command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BINARY), command],
        cwd=ROOT / "Server",
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def production_startup_negative() -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="boaz-health-http-") as temporary:
        root = Path(temporary).resolve(strict=True)
        data = root / "data"
        control = root / "control"
        backups = root / "backups"
        coord = control / "coord"
        control.mkdir(mode=0o700)
        data.mkdir(mode=0o700)
        backups.mkdir(mode=0o700)
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("BOAZ_HEALTH_")
        }
        environment.update(
            BOAZ_HEALTH_DATA_ROOT=str(data),
            BOAZ_HEALTH_DB=str(data / "health.db"),
            BOAZ_HEALTH_CONTROL_DB=str(control / "control.db"),
            BOAZ_HEALTH_CONTROL_MIRROR_DIR=str(control / "mirror"),
            BOAZ_HEALTH_BACKUP_DIR=str(backups),
            BOAZ_HEALTH_COORD_DIR=str(coord),
            BOAZ_HEALTH_UPLOAD_ENABLED="0",
            BOAZ_HEALTH_BOOTSTRAP="1",
            BOAZ_HEALTH_VM_STORAGE=str(root / "vm-unavailable"),
            BOAZ_HEALTH_VM_BINARY=str(root / "vm-unavailable"),
        )
        first = run_binary(environment, "bootstrap-coordinator")
        if first.returncode != 0:
            raise AssertionError("bootstrap-coordinator failed: " + first.stderr)
        second = run_binary(environment, "init-storage")
        if second.returncode != 0:
            raise AssertionError("init-storage failed: " + second.stderr)
        health = data / "health.db"
        ledger = control / "control.db"
        environment["BOAZ_HEALTH_BOOTSTRAP"] = "0"
        verify = run_binary(environment, "verify-storage")
        if verify.returncode != 0 or "storage_verified=true" not in verify.stdout:
            raise AssertionError("read-only storage verification failed: " + verify.stderr)
        before = (digest(health), digest(ledger))
        environment["BOAZ_HEALTH_BOOTSTRAP"] = "1"
        denied_adoption = subprocess.run(
            [str(BINARY), "adopt-active-set", str(backups / "boaz-health-genesis.db")],
            cwd=ROOT / "Server",
            env=environment,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if denied_adoption.returncode == 0 or (coord / "adopted-unactivated.json").exists():
            raise AssertionError("adoption accepted an unverified recovery volume")
        if (digest(health), digest(ledger)) != before:
            raise AssertionError("denied adoption modified a database")
        environment["BOAZ_HEALTH_BOOTSTRAP"] = "0"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        environment["BOAZ_HEALTH_PORT"] = str(port)
        denied = run_binary(environment, "serve")
        if denied.returncode == 0:
            raise AssertionError("production serve accepted an unactivated generation")
        if (digest(health), digest(ledger)) != before:
            raise AssertionError("denied production startup modified a database")
        if (coord / "active-set.json").exists():
            raise AssertionError("negative test unexpectedly published an active pointer")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                raise AssertionError("denied production startup left a listener")
        except ConnectionRefusedError:
            pass
        if "incomplete" not in denied.stderr and "refusing" not in denied.stderr:
            raise AssertionError("production rejection did not explain its gate")
        return {
            "check": "production_unactivated_startup",
            "status": "PASS",
            "storage_unchanged": True,
            "upload_enabled": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true", help="Build the production receiver offline")
    args = parser.parse_args()
    if args.build or not BINARY.is_file():
        subprocess.run(
            ["cargo", "build", "--offline", "--locked"],
            cwd=ROOT / "Server",
            timeout=180,
            check=True,
        )
    negative = production_startup_negative()
    positive = subprocess.run(
        ["cargo", "test", "--offline", "--locked", "--test", "api", "--", "--exact", "synthetic_loopback_http_contracts", "--nocapture"],
        cwd=ROOT / "Server",
        timeout=180,
        check=False,
        capture_output=True,
        text=True,
    )
    if positive.returncode != 0:
        raise AssertionError("synthetic loopback route test failed:\n" + positive.stdout + positive.stderr)
    print(json.dumps(
        {
            "scope": "synthetic_routes_and_production_fail_closed",
            "results": [
                negative,
                {"check": "adoption_rejects_unverified_volume", "status": "PASS"},
                {"check": "synthetic_loopback_routes", "status": "PASS"},
            ],
            "rust_test_output": positive.stdout[-1000:],
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, OSError, subprocess.SubprocessError, ValueError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1) from error
