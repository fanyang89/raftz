#!/usr/bin/env bash
# Runs a command and uploads fuzz reproducers only when it fails, matching the
# GitHub Actions `if: failure()` artifact semantics.
set -uo pipefail

if [[ $# -eq 0 ]]; then
    printf 'A command is required\n' >&2
    exit 1
fi

"$@"
status=$?
if [[ $status -ne 0 ]]; then
    buildkite-agent artifact upload \
        "crash-*" \
        "leak-*" \
        "oom-*" \
        "timeout-*" \
        ".zig-cache/f/**/*" \
        ".zig-cache/tmp/libfuzzer.log" || true
fi
exit "$status"
