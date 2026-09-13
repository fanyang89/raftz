#!/usr/bin/env bash
set -euo pipefail

if [[ $(id -u) -eq 0 ]]; then
    elevate=()
else
    elevate=(sudo -n)
fi
"${elevate[@]}" apt-get update
"${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
    build-essential binutils-dev cmake libcurl4-openssl-dev libdw-dev \
    libiberty-dev libssl-dev ninja-build zlib1g-dev

temporary_root=${TMPDIR:-$HOME/tmp}/pi
mkdir -p "$temporary_root"
work=$(mktemp -d "$temporary_root/raftz-kcov.XXXXXX")
trap 'rm -rf "$work"' EXIT
curl --fail --location --silent --show-error --retry 3 \
    --output "$work/source.tar.gz" \
    https://github.com/SimonKagstrom/kcov/archive/a39874f938ce13f7a65f253120d1ec946b349ffe.tar.gz
printf '%s  %s\n' \
    dac01569171979477b500924be264d2a1bc649dae6010536228cbb319344d516 \
    "$work/source.tar.gz" | sha256sum --check
mkdir "$work/source"
tar --extract --gzip --file "$work/source.tar.gz" --directory "$work/source" --strip-components=1
cmake -S "$work/source" -B "$work/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$work/install"
cmake --build "$work/build" --parallel 2
cmake --install "$work/build"
export PATH="$work/install/bin:$PATH"
mise run coverage
test -s zig-out/coverage/kcov-merged/cobertura.xml
