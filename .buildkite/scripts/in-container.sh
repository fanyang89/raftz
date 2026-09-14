#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
    printf 'A command is required\n' >&2
    exit 1
fi
case "$(docker info --format '{{.OSType}}/{{.Architecture}}')" in
    linux/x86_64|linux/amd64) ;;
    *)
        printf 'A Linux AMD64 Docker daemon is required on the hosted agent.\n' >&2
        exit 1
        ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
temporary_root=${TMPDIR:-$HOME/tmp}/pi
mkdir -p "$temporary_root"
work=$(mktemp -d "$temporary_root/raftz-container.XXXXXX")
cleanup() {
    if [[ -s "$work/container-id" ]]; then
        docker rm --force "$(< "$work/container-id")" >/dev/null 2>&1 || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

python3 .buildkite/container/seccomp.py "$work/seccomp.json"
docker build --platform linux/amd64 --iidfile "$work/image-id" .buildkite/container
test -s "$work/image-id"
docker run --rm --init --platform linux/amd64 \
    --cidfile "$work/container-id" \
    --security-opt "seccomp=$work/seccomp.json" \
    --ulimit memlock=67108864:67108864 \
    --volume "$root:$root" \
    --volume /tmp/raftz-ci-cache:/tmp/raftz-ci-cache \
    --workdir "$root" \
    --env CI=true --env PYTHONDONTWRITEBYTECODE=1 \
    "$(< "$work/image-id")" \
    bash -euc 'raftz-ci-preflight; exec "$@"' -- "$@"
