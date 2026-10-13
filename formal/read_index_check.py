#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import check


PROFILES = {
    "single": ({"Nodes = {1, 2, 3}": "Nodes = {1}"}, None),
    "safe": ({}, None),
    "no-current-term": ({"RequireCurrentTerm = TRUE": "RequireCurrentTerm = FALSE"}, "ReadIndexSafety"),
    "no-quorum": ({"RequireQuorum = TRUE": "RequireQuorum = FALSE"}, "ReadIndexSafety"),
    "no-apply": ({"RequireApplied = TRUE": "RequireApplied = FALSE"}, "AppliedBarrier"),
    "completion-witness": ({}, "NoCompletedRead"),
    "batch-witness": ({}, "NoBatchedRelease"),
}


def validate_result(returncode, text, expected):
    violations = re.findall(r"^Error: Invariant (\w+) is violated\.$", text, re.MULTILINE)
    if expected:
        if (returncode == 12 and violations == [expected]
                and "Error: The behavior up to this point is:" in text
                and re.search(r"^State 2: ", text, re.MULTILINE)):
            return
        raise RuntimeError(f"Expected a concrete {expected} counterexample; incomplete runs and tool errors do not count")
    summaries = re.findall(
        r"^([0-9]+) states generated, ([0-9]+) distinct states found, ([0-9]+) states left on queue\.$",
        text, re.MULTILINE,
    )
    if (returncode != 0 or violations
            or "Model checking completed. No error has been found." not in text
            or not summaries or summaries[-1][2] != "0" or int(summaries[-1][1]) <= 1):
        raise RuntimeError("TLC did not complete a nontrivial exhaustive check with an empty queue")


def run(profile, timeout):
    cp = check.classpath()
    replacements, expected = PROFILES[profile]
    runs = check.CACHE / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=f"read-index-{profile}-", dir=runs))
    model = check.ROOT / "models" / "RaftzReadIndex.tla"
    shutil.copy2(model, directory / model.name)
    config = (check.ROOT / "models" / "ReadIndex.cfg").read_text()
    for old, new in replacements.items():
        if config.count(old) != 1:
            raise RuntimeError(f"Ambiguous or missing configuration setting: {old}")
        config = config.replace(old, new)
    if profile.endswith("-witness"):
        config += f"\nINVARIANT {expected}\n"
    (directory / "ReadIndex.cfg").write_text(config)
    command = [
        "java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", cp, "tlc2.TLC",
        "-workers", "1", "-metadir", str(directory / "states"),
        "-config", "ReadIndex.cfg", model.name,
    ]
    (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    print(f"{profile}: artifacts in {directory}", flush=True)
    with (directory / "tlc.log").open("w") as log:
        try:
            result = subprocess.run(command, cwd=directory, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"TLC timed out after {timeout}s; NOT a pass: {directory}") from error
    text = (directory / "tlc.log").read_text()
    print("\n".join(text.splitlines()[-8:]), flush=True)
    validate_result(result.returncode, text, expected)
    print(f"{profile}: {'expected counterexample found' if expected else 'bounded exhaustive check passed'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Check the Safe ReadIndex design abstraction, not Zig refinement.")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout per profile in seconds")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    check.verify_upstream()
    for profile in PROFILES:
        run(profile, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
