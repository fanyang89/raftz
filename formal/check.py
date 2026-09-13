#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parent
UPSTREAM = ROOT / "upstream" / "etcd"
CACHE = ROOT.parent / ".cache" / "tla"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_upstream():
    manifest = json.loads((UPSTREAM / "manifest.json").read_text())
    for name, expected in manifest["files"].items():
        path = UPSTREAM / name
        if not path.is_file() or digest(path) != expected:
            raise RuntimeError(f"Upstream file missing or changed: {name}")
    print(f"Verified {len(manifest['files'])} upstream files at {manifest['revision']}", flush=True)


def tools():
    return json.loads((ROOT / "tools.lock.json").read_text())


def prepare():
    CACHE.mkdir(parents=True, exist_ok=True)
    for name, artifact in tools().items():
        target = CACHE / name
        if target.is_file() and digest(target) == artifact["sha256"]:
            continue
        print(f"Downloading {artifact['url']}", flush=True)
        with tempfile.TemporaryDirectory(dir=CACHE) as temporary:
            download = Path(temporary) / name
            with urllib.request.urlopen(artifact["url"], timeout=120) as response:
                with download.open("wb") as output:
                    shutil.copyfileobj(response, output)
            if digest(download) != artifact["sha256"]:
                raise RuntimeError(f"Checksum mismatch: {name}")
            download.replace(target)
    print("TLA+ tools verified against pinned upstream release digests.", flush=True)


def classpath():
    paths = []
    for name, artifact in tools().items():
        path = CACHE / name
        if not path.is_file() or digest(path) != artifact["sha256"]:
            raise RuntimeError(f"Missing or changed {name}; run mise run prepare-tla")
        paths.append(str(path))
    return os.pathsep.join(paths)


def check(profile, timeout):
    cp = classpath()
    profiles = {
        "single": (ROOT / "models" / "SingleNode.cfg", []),
        "smoke": (ROOT / "models" / "ThreeNode.cfg", ["-simulate", "num=100", "-depth", "100", "-seed", "20260912"]),
        "model": (ROOT / "models" / "ThreeNode.cfg", []),
    }
    config, extra = profiles[profile]
    runs = CACHE / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=f"{profile}-", dir=runs))
    for path in (UPSTREAM / "tla").glob("*.tla"):
        shutil.copy2(path, run / path.name)
    shutil.copy2(ROOT / "models" / "RaftzMC.tla", run / "RaftzMC.tla")
    shutil.copy2(config, run / config.name)
    command = [
        "java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", cp, "tlc2.TLC",
        "-workers", "1", "-metadir", str(run / "states"),
        "-config", config.name, *extra, "RaftzMC.tla",
    ]
    print(f"{profile}: {'sampled simulation' if extra else 'bounded exhaustive model checking'}", flush=True)
    print(f"Artifacts: {run}", flush=True)
    (run / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    with (run / "tlc.log").open("w") as log:
        try:
            result = subprocess.run(command, cwd=run, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"TLC timed out after {timeout}s; NOT a pass. See {run / 'tlc.log'}") from error
    text = (run / "tlc.log").read_text()
    print("\n".join(text.splitlines()[-18:]), flush=True)
    if result.returncode != 0:
        raise RuntimeError(f"TLC failed ({result.returncode}); see {run / 'tlc.log'}")
    if extra:
        if "100 traces generated" not in text or "Simulation using seed" not in text:
            raise RuntimeError(f"TLC did not report completed simulation; see {run / 'tlc.log'}")
        print("Simulation passed for sampled behaviors only; this is not exhaustive verification.", flush=True)
    elif "Model checking completed. No error has been found." not in text:
        raise RuntimeError(f"TLC did not report completed model checking; see {run / 'tlc.log'}")


def main():
    parser = argparse.ArgumentParser(description="Check the pinned etcd TLA+ baseline, not the Zig implementation.")
    parser.add_argument("action", choices=["verify", "prepare", "smoke", "single", "model"])
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    verify_upstream()
    if args.action == "prepare":
        prepare()
    elif args.action != "verify":
        if args.action == "smoke":
            check("single", args.timeout)
        check(args.action, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
