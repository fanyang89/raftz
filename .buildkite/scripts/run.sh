#!/usr/bin/env bash
set -euo pipefail

expected_arch=${1:?expected architecture is required}
shift
if [[ $(uname -s) != Linux || $(uname -m) != "$expected_arch" ]]; then
    printf 'Expected Linux %s, got %s %s\n' "$expected_arch" "$(uname -s)" "$(uname -m)" >&2
    exit 1
fi
if [[ $# -eq 0 ]]; then
    printf 'A command is required\n' >&2
    exit 1
fi

case "$expected_arch" in
    x86_64)
        mise_arch=x64
        mise_sha256=063dda9149ab6be53da877c2d176afe0eac68e64cf8ca295bd0528720701c65d
        ;;
    aarch64)
        mise_arch=arm64
        mise_sha256=98d2ea7b82dd966afdb8a9f4e9edbca771acf2a30d2842bfc0efdb7b61c886a3
        ;;
    *)
        printf 'Unsupported architecture: %s\n' "$expected_arch" >&2
        exit 1
        ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
export CI=true MISE_YES=1
export MISE_TRUSTED_CONFIG_PATHS="$root"
# These paths live on the Buildkite cache volume when one is mounted; without a
# volume they are ordinary temporary directories and caching is simply lost.
export MISE_DATA_DIR=/tmp/raftz-ci-cache/mise
export ZIG_GLOBAL_CACHE_DIR=/tmp/raftz-ci-cache/zig
temporary_root=${TMPDIR:-$HOME/tmp}/pi
mkdir -p "$temporary_root"
work=$(mktemp -d "$temporary_root/raftz-ci.XXXXXX")
trap 'rm -rf "$work"' EXIT

mise_version=2026.9.1
curl --fail --location --silent --show-error --retry 3 \
    --output "$work/mise.tar.gz" \
    "https://github.com/jdx/mise/releases/download/v$mise_version/mise-v$mise_version-linux-$mise_arch.tar.gz"
printf '%s  %s\n' "$mise_sha256" "$work/mise.tar.gz" | sha256sum --check
tar --extract --gzip --file "$work/mise.tar.gz" --directory "$work"
export PATH="$work/mise/bin:$PATH"
mise install
mise exec -- "$@"
