import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/prepare-ci-zig-cache.sh"


class DependencyPrefetchTests(unittest.TestCase):
    def setUp(self):
        temporary_root = Path(os.environ.get("TMPDIR", str(Path.home() / "tmp"))) / "pi"
        temporary_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="raftz-prefetch-test-", dir=temporary_root)
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.calls = self.base / "calls"
        self.payload = b"pinned dependency fixture\n"
        self.responses = [200]
        self.requests = 0
        case = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status = case.responses[min(case.requests, len(case.responses) - 1)]
                case.requests += 1
                self.send_response(status)
                self.send_header("Content-Length", str(len(case.payload)))
                self.end_headers()
                self.wfile.write(case.payload)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.01})
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.url = f"http://127.0.0.1:{self.server.server_port}/fixture.tar.gz"
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "TMPDIR": str(self.base / "tmp"),
            "NO_PROXY": "127.0.0.1",
            "no_proxy": "127.0.0.1",
            "CALL_LOG": str(self.calls),
            "COUNT_FILE": str(self.base / "build-count"),
            "ACTUAL_HASH": "fixture-package-hash",
            "BUILD_FAILURES": "0",
        }
        self.stub("sleep", "exit 0")
        self.stub("zig", '''
printf '%s\n' "$*" >> "$CALL_LOG"
if [[ $1 == fetch ]]; then
    [[ -f $2 ]]
    printf '%s\n' "$ACTUAL_HASH"
    exit 0
fi
[[ $1 == build ]]
count=0
if [[ -f $COUNT_FILE ]]; then count=$(cat "$COUNT_FILE"); fi
count=$((count + 1))
printf '%s' "$count" > "$COUNT_FILE"
if (( count <= BUILD_FAILURES )); then exit 37; fi
''')

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())

    def stub(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o755)

    def run_prefetch(self, sha256=None):
        source = SCRIPT.read_text()
        prefix = source[:source.index("\nfetch_package \\\n")]
        suffix = source[source.index("\nfor attempt in "):]
        checksum = sha256 if sha256 is not None else hashlib.sha256(self.payload).hexdigest()
        fixture = shlex.join(["fetch_package", "fixture", self.url,
                              checksum, "fixture-package-hash"])
        script = self.base / "prefetch.sh"
        script.write_text(prefix + "\n" + fixture + "\n" + suffix)
        result = subprocess.run(["bash", str(script)], env=self.env, text=True,
                                capture_output=True, timeout=45)
        self.assertEqual(list((self.base / "tmp").iterdir()), [])
        return result

    def build_calls(self):
        if not self.calls.exists():
            return []
        return [line for line in self.calls.read_text().splitlines() if line.startswith("build ")]

    def test_504_is_retried_before_verified_fetch(self):
        self.responses = [504, 200]
        result = self.run_prefetch()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.requests, 2)
        self.assertEqual(self.build_calls(), ["build fuzz-smoke fuzz-wal-crash --fetch=needed"])
        self.assertEqual(len(self.calls.read_text().splitlines()), 2)

    def test_persistent_http_failure_is_bounded_and_stops_resolution(self):
        self.responses = [504]
        result = self.run_prefetch()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.requests, 6)
        self.assertFalse(self.calls.exists())

    def test_sha256_mismatch_stops_before_zig(self):
        result = self.run_prefetch(sha256="0" * 64)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected archive SHA-256 for fixture", result.stderr)
        self.assertEqual(self.requests, 1)
        self.assertFalse(self.calls.exists())

    def test_package_hash_mismatch_stops_before_resolution(self):
        self.env["ACTUAL_HASH"] = "wrong-package-hash"
        result = self.run_prefetch()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected Zig package hash", result.stderr)
        self.assertEqual(self.requests, 1)
        self.assertEqual(self.build_calls(), [])

    def test_fetch_only_resolution_can_retry(self):
        self.env["BUILD_FAILURES"] = "2"
        result = self.run_prefetch(sha256="-")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.build_calls(), ["build fuzz-smoke fuzz-wal-crash --fetch=needed"] * 3)

    def test_persistent_resolution_failure_propagates(self):
        self.env["BUILD_FAILURES"] = "3"
        result = self.run_prefetch()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.build_calls(), ["build fuzz-smoke fuzz-wal-crash --fetch=needed"] * 3)

    def test_root_dependency_hashes_are_prefetched(self):
        manifest = (ROOT / "build.zig.zon").read_text()
        hashes = re.findall(r'\.hash = "([^\"]+)"', manifest)
        self.assertGreater(len(hashes), 0)
        source = SCRIPT.read_text()
        for package_hash in hashes:
            self.assertIn(package_hash, source)


if __name__ == "__main__":
    unittest.main()
