#!/usr/bin/env bash
set -euo pipefail

tools=(cc c++ make cmake ninja pkg-config)
missing=()
for tool in "${tools[@]}"; do
    command -v "$tool" >/dev/null || missing+=("$tool")
done
if [[ ${#missing[@]} -eq 0 ]]; then
    exit 0
fi

printf 'Missing native build tools: %s\n' "${missing[*]}" >&2
if ! command -v apt-get >/dev/null; then
    printf 'Install the missing tools in the agent image; automatic setup requires apt-get.\n' >&2
    exit 1
fi
if [[ $(id -u) -eq 0 ]]; then
    elevate=()
else
    if ! command -v sudo >/dev/null; then
        printf 'Native dependency installation requires root or passwordless sudo.\n' >&2
        exit 1
    fi
    elevate=(sudo -n)
fi

"${elevate[@]}" apt-get update
"${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
    build-essential cmake ninja-build pkg-config

for tool in "${tools[@]}"; do
    if ! command -v "$tool" >/dev/null; then
        printf 'Native build tool still missing after installation: %s\n' "$tool" >&2
        exit 1
    fi
done
