FROM ubuntu:24.04@sha256:a61567bd31828687156d735ea8eb01ba4e37636e225dd6a48ba94136a70d9d61

ENV TZ=Etc/UTC TMPDIR=/root/tmp
RUN mkdir -p /root/tmp \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        build-essential ca-certificates cmake curl git ninja-build pkg-config python3 tzdata \
    && ln -snf /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && rm -rf /var/lib/apt/lists/*

COPY preflight.c /tmp/raftz-ci-preflight.c
RUN cc -std=c11 -Wall -Wextra -Werror -O2 \
        /tmp/raftz-ci-preflight.c -o /usr/local/bin/raftz-ci-preflight \
    && rm /tmp/raftz-ci-preflight.c
