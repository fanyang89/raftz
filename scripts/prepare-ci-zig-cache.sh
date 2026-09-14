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

    curl \
        --connect-timeout 20 \
        --fail \
        --location \
        --max-time 300 \
        --retry 5 \
        --retry-all-errors \
        --retry-delay 2 \
        --show-error \
        --silent \
        --output "$archive" \
        "$url"
    if [[ "$sha256" != - ]]; then
        printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check --status
    fi

    local actual_hash
    actual_hash=$(zig fetch "$archive")
    if [[ "$actual_hash" != "$package_hash" ]]; then
        printf 'unexpected Zig package hash for %s: got %s, expected %s\n' \
            "$name" "$actual_hash" "$package_hash" >&2
        return 1
    fi
}

fetch_package \
    grpc-lite-c0d2207b \
    https://codeload.github.com/fanyang89/grpc-lite/tar.gz/c0d2207bdb426243327a0f4f41d3983ae4e53e30 \
    - \
    grpc_lite-0.4.0-BcwY0BKEFQCZukihgaM1osxJ0WrNVtc5nBTtjU3V9PKW

fetch_package \
    crc32c-2bbb3be4 \
    https://codeload.github.com/google/crc32c/tar.gz/2bbb3be42e20a0e6c0f7b39dc07dc863d9ffbc07 \
    - \
    N-V-__8AAPSAAQAtvOlJae6eMS2dtyeTGalfGt9JsXB9mYZj

fetch_package \
    marionette-v0.6.0 \
    https://codeload.github.com/sb2bg/marionette/tar.gz/refs/tags/v0.6.0 \
    - \
    marionette-0.6.0-jTrEFDEmGwAsls0lMrAb9uRDOWApe_5DSJkDfE6pTQwb

fetch_package \
    zeit-b1c1c2fc \
    https://codeload.github.com/rockorager/zeit/tar.gz/b1c1c2fcbc71fd7799a316bbcf0ff88d06d80ccc \
    - \
    zeit-0.9.0-5I6bk2m9AgBSMH8-L6rYJkwuQAyhXplnfxnvTSGzVHUR

fetch_package \
    libxev-b0650f08 \
    https://codeload.github.com/mitchellh/libxev/tar.gz/b0650f082458226860ed7ab0fc7c9c73823c8950 \
    - \
    libxev-0.0.0-86vtcxkOFACqPXUTAPuq5i0xpDYWU5G5RfrYQXxlUT26

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
    nanozlog-e693c119 \
    https://codeload.github.com/wyzdwdz/nanozlog/tar.gz/e693c11976d55ba0a5b8deeaaaf9f1c5cc30eba9 \
    dc8335550f242a7db3c1c8d80ac832bd1777ffeaea7da65187411fa4c8928328 \
    nanozlog-0.1.0-5UtdH535AADW7HUBpfLboKJkdB1IVYaqz7YFQ2dHIcqL

fetch_package \
    zig-protobuf-b794f993 \
    https://codeload.github.com/Arwalk/zig-protobuf/tar.gz/b794f99323cead7f1794ae68554d0311cc309857 \
    - \
    protobuf-5.0.0-0e82ahZiKwC5Yrh4psANoUzrV_H4CQU1EsOIY9Zdyap_

for attempt in 1 2 3; do
    if zig build fuzz-smoke fuzz-wal-crash --fetch=needed; then
        exit 0
    fi
    if [[ "$attempt" == 3 ]]; then
        exit 1
    fi
    sleep $((attempt * 5))
done
