# GitHub Actions CI

GitHub Actions is the repository's CI provider. Workflows use GitHub-hosted
Ubuntu 24.04 x86_64 runners and the Zig version pinned in `mise.toml`.
Linux ARM64 and macOS are not currently covered.

## Pull requests and main

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on pull requests,
pushes to `main`, and manual dispatches. Pull requests use the default checkout
of GitHub's test merge commit. Superseded runs for the same event and ref are
canceled.

| Check | Coverage |
| --- | --- |
| Workflow lint | Formatting, actionlint, shell syntax, dependency-prefetch regression tests |
| Core | Debug and ReleaseSafe |
| Coverage | kcov reports and Codecov upload using GitHub OIDC |
| Examples | raft-sqlite and libelection C SDK |
| Sanitizers | Separate TSan and UBSan jobs |
| Gperftools | Profiling integration tests |
| Bounded fuzzing | Codec, WAL, and confchange at 100K runs; simulation at 10K |
| WAL durability | Durability tests and WAL crash fuzzing at 1K runs |

The `Required` job runs even when dependencies fail or are canceled and succeeds
only when every prerequisite succeeds. Configure branch protection or a ruleset
to require this check from GitHub Actions. Keep its name stable when changing
individual jobs.

Coverage reports are archived as the `coverage` artifact, including partial
reports when generation fails. Fuzz failures upload crash, leak, OOM, and timeout
reproducers, `.zig-cache/f/`, and the libFuzzer log. Download artifacts from the
workflow run's summary page.

## Nightly fuzzing

[`.github/workflows/fuzz.yml`](../.github/workflows/fuzz.yml) runs daily at
03:17 UTC on the default branch and supports manual dispatch. It runs codec,
WAL, and confchange fuzzing at 1M iterations, simulation at 100K, and WAL crash
fuzzing at 10K. Each fuzz job has a 60-minute timeout.

A preparation job verifies and caches Zig packages before the five fuzz jobs
restore that cache. Failures preserve the same reproducer artifacts as PR CI.

## Local validation

```bash
mise install
mise run ci-lint-all
```

This checks Zig formatting, workflow syntax with actionlint, shell syntax, and
`tests/ci/` using Python 3's standard-library unittest runner. Dependency-prefetch
tests use a local HTTP fixture and a stub Zig executable; they do not download
real dependencies or run compilation. The suite also requires Bash and curl.

CI compilation jobs use `scripts/prepare-ci-zig-cache.sh` to download pinned
packages and verify their Zig package hashes before building.

## Repository settings

Removing old CI configuration does not change external service settings. Disable
the retired provider's pipelines, schedules, and webhooks separately, and remove
its required status checks after enabling GitHub Actions' `Required` check.
Do not remove the active merge gate before its replacement is working.

Codecov must be configured to accept this repository's GitHub OIDC uploads.
Coverage upload failures remain CI failures.
