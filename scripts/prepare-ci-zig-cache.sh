#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-"$HOME/tmp"}
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/raftz-zig-deps.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

fetch_package() {
    local name=$1
    local url=$2
    local sha256=$3
    local package_hash=$4
    local archive="$work_dir/$name.tar.gz"

    printf 'Fetching Zig package %s\n' "$name"
    curl \
        --connect-timeout 20 \
        --fail \
        --location \
        --max-time 120 \
        --retry 5 \
        --retry-max-time 180 \
        --retry-all-errors \
        --retry-delay 2 \
        --show-error \
        --silent \
        --output "$archive" \
        "$url"
    if [[ "$sha256" != - ]]; then
        if ! printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check --status; then
            printf 'unexpected archive SHA-256 for %s\n' "$name" >&2
            return 1
        fi
    fi

    local source="$work_dir/$name"
    mkdir "$source"
    tar --extract --gzip --file "$archive" --directory "$source" --strip-components=1

    local actual_hash
    actual_hash=$(zig fetch "$source")
    if [[ "$actual_hash" != "$package_hash" ]]; then
        printf 'unexpected Zig package hash for %s: got %s, expected %s\n' \
            "$name" "$actual_hash" "$package_hash" >&2
        return 1
    fi
}

fetch_package \
    grpc-lite-db3c400b \
    https://codeload.github.com/fanyang89/grpc-lite/tar.gz/db3c400bf70419742ebcb42fe316ba8b754b4975 \
    - \
    grpc_lite-0.4.0-BcwY0LD8KQDV9CoDiLNJ5OaDpyPBiV-4P5H_VD3Bsr_J

fetch_package \
    crc32c-2bbb3be4 \
    https://codeload.github.com/google/crc32c/tar.gz/2bbb3be42e20a0e6c0f7b39dc07dc863d9ffbc07 \
    - \
    N-V-__8AAPSAAQAtvOlJae6eMS2dtyeTGalfGt9JsXB9mYZj

fetch_package \
    zeit-06bb7b46 \
    https://codeload.github.com/rockorager/zeit/tar.gz/06bb7b462b4218770fb5438ebe6b0c0c5ef18888 \
    - \
    zeit-0.10.0-5I6bkxS-AgAaI3lI3RsXsWnPw7sMCqJKoFnSB8_QRAoo

fetch_package \
    nghttp2-68cb6900 \
    https://codeload.github.com/nghttp2/nghttp2/tar.gz/68cb6900fde14c77f0cd7add0e094a862960eb99 \
    - \
    N-V-__8AAPOqVwAHvwAVJJjhhX72DyDtjWw--9WUZf3-uKRX

fetch_package \
    cares-v1.34.8 \
    https://github.com/c-ares/c-ares/releases/download/v1.34.8/c-ares-1.34.8.tar.gz \
    - \
    N-V-__8AADDhTgDOiesa_sidmxGBzfPdF3OWU2HXS2GNZmVp

fetch_package \
    libcpucycles-20260625 \
    https://github.com/fanyang89/libcpucycles-mirror/releases/download/v20260625/libcpucycles-20260625.tar.gz \
    74a815bfb5ab645e5d07617125824c946ce5039db139e5233467d1ff33f69afa \
    N-V-__8AAHSUBAA_Vn8NXM2L9F21QFvrTIxbH9yvxs5cO-lY

fetch_package \
    nanozlog-a1ad3d52 \
    https://codeload.github.com/wyzdwdz/nanozlog/tar.gz/a1ad3d52aec355c2dba7568fe18ec440937d4f81 \
    - \
    nanozlog-0.1.0-5UtdH6f6AAACQMkivAT4xIQtfBDFG3JACCJRpnY5Up3P

fetch_package \
    zig-protobuf-e9e18c76 \
    https://codeload.github.com/Arwalk/zig-protobuf/tar.gz/e9e18c7664b8c950e5fc679259c968dd66ccc48b \
    - \
    protobuf-7.0.0-0e82aqFhNADVgEgPYPI7ddEg_dzhphBToYgkjQTrMKWv

for attempt in 1 2 3; do
    if zig build fuzz-smoke fuzz-wal-crash --fetch=needed; then
        exit 0
    fi
    if [[ "$attempt" == 3 ]]; then
        exit 1
    fi
    sleep $((attempt * 5))
done
