# Buildkite CI

Buildkite runs alongside GitHub Actions during the migration. The workflows in
`.github/workflows/` and the existing GitHub required checks remain unchanged.
Creating these files does not create queues, pipelines, schedules, or GitHub
integration settings in Buildkite.

## Pipelines

| File | Workloads | Expanded jobs |
| --- | --- | --- |
| `.buildkite/pipeline.yml` | Lint, dual-architecture core tests, coverage, both examples, sanitizers, gperftools, bounded fuzzing, WAL durability | 16 |
| `.buildkite/nightly.yml` | Codec/WAL/confchange at 1M runs, simulation at 100K, WAL crash at 10K | 5 |

The regular pipeline preserves the existing test commands, optimization modes,
fuzz budgets, and job timeouts. It uses the overall Buildkite pipeline status
instead of a separate GitHub Actions `Required` aggregation job. No test is
soft-failed, and a failing test does not cancel its siblings.
`.buildkite/scripts/with-fuzz-artifacts.sh` uploads fuzz reproducers only when
the wrapped command fails, matching the GitHub Actions `if: failure()`
semantics; the coverage report is always archived through `artifact_paths`.
Forced cancellation or agent loss can prevent uploads; verify the failure path
on the hosted agents before cutover.

## Hosted queues

Create two Linux hosted queues in the same Buildkite cluster:

| Queue key | Architecture | Suggested starting shape |
| --- | --- | --- |
| `raftz-linux-amd64` | AMD64 | `LINUX_AMD64_4X16` |
| `raftz-linux-arm64` | ARM64 | `LINUX_ARM64_4X16` |

These names are referenced by the YAML files and initial upload steps below.
If using different names, update both pipeline files and their configuration
tests. Restrict pipeline access to these queues and set concurrency/budget limits
before enabling all jobs. Each job should get an isolated hosted environment;
do not share a writable checkout between concurrent jobs.

The image needs Bash, Git, curl, CA certificates, tar, sha256sum, Python 3, and
`buildkite-agent` on PATH. Coverage additionally requires apt-get and either root
or passwordless `sudo -n` for installing build dependencies. It compiles the same
pinned, SHA-256-verified kcov source as GitHub Actions, installs it into a unique
temporary directory, and checks that the Cobertura report is nonempty. kcov must
be permitted to trace child processes; verify ptrace/seccomp restrictions on the
actual hosted image. TSan must also be validated against its kernel/security
configuration.

Buildkite currently documents its default Linux image as Ubuntu 22.04, whereas
the GitHub jobs use Ubuntu 24.04. The pipeline does not assume these are identical.
Record the selected image and run the acceptance checks below; use a custom
Ubuntu 24.04 hosted image if matching the existing OS is necessary. Do not
silently disable sanitizer or coverage failures to accommodate an image.

`.buildkite/scripts/run.sh` checks the actual OS/architecture before downloading
anything. It downloads mise 2026.9.1 with architecture-specific hashes taken from
the upstream `SHASUMS256.txt`, installs the tools in `mise.toml`, and uses
`mise exec` to supply PATH to every command. Zig remains pinned to 0.16.0.
Downloads require access to GitHub releases/codeload and the configured tool
registries. Temporary directories respect an existing `TMPDIR`, otherwise use
`$HOME/tmp`.

## Caching

Both pipelines define a Buildkite hosted-agent cache volume holding
`.zig-cache` and `/tmp/raftz-ci-cache`. `run.sh` points `MISE_DATA_DIR` and
`ZIG_GLOBAL_CACHE_DIR` into that directory, so tool downloads (mise, Zig,
actionlint, zigcli), Zig package fetches (for example the pinned gperftools
fork), and the local build cache survive across jobs. Without a mounted volume
these are ordinary temporary directories and builds simply run cold.

Volume names interpolate `${BUILDKITE_BRANCH}`, so each branch gets its own
volume and one branch's jobs cannot read or replace another branch's cache.
The ARM64 core steps use an `-arm64-` volume because `MISE_DATA_DIR` contains
architecture-specific binaries; the Zig caches are content-addressed and would
be safe to share. A fork PR built from a branch whose name matches a trusted
branch (for example `main`) shares that branch's volume, so fork PR builds
must stay approval-gated as described below. Volumes are best-effort and can
be evicted; every job must still pass on a cold cache.

## Connect the regular pipeline

1. Install/connect the Buildkite GitHub App for this repository. With hosted
   agents, the full-access App provides checkout access. If using the limited
   App, configure checkout credentials separately. Do not place credentials in
   the repository or expose them to test commands.
2. Create a pipeline, for example `raftz-ci`, with `main` as its default branch.
   Put this initial step in its Buildkite pipeline settings:

   ```yaml
   steps:
     - label: "Upload CI pipeline"
       agents:
         queue: raftz-linux-amd64
       command: buildkite-agent pipeline upload .buildkite/pipeline.yml
       timeout_in_minutes: 5
   ```

3. Enable branch builds for `main`, and **Build when pull request is opened or
   updated**. Pipeline-level branch filters apply to branch builds; PR builds
   bypass them. Confirm that pushes to an existing PR trigger the expected
   builds without duplicating ordinary branch builds. Keep manual builds enabled.
4. Enable **Cancel Intermediate Builds** and **Skip Intermediate Builds** for
   superseded builds. The separate nightly pipeline keeps its cancellation
   scope independent of regular CI. These settings are branch-scoped, not an
   exact implementation of GitHub's workflow/event/ref concurrency expression.
5. Enable GitHub commit-status reporting. Leave GitHub branch protection/rulesets
   unchanged during the parallel run. Do not guess the required context name;
   record the actual context on a completed PR build before eventual cutover.

### PR checkout semantics and security

GitHub Actions currently checks out a PR's test merge commit by default.
Buildkite normally checks out its head commit. Buildkite's **Build the test merge
commit** setting is currently a private-preview feature requiring support
activation and agent v3.137.1 or newer. Enable it, if available, and verify
`BUILDKITE_PULL_REQUEST_USING_MERGE_REFSPEC=true` and the actual checkout commit.
Do not merely set that variable yourself as a substitute for provider support.
If this feature is unavailable, treat Buildkite's head-only PR result as
supplementary: retain the GitHub merge-commit check until an equivalent checkout
strategy is implemented and tested. This is a cutover blocker, not test parity.

Third-party fork PR builds require a separate provider setting. Enable them only
after configuring an appropriate approval/isolation policy. They execute
untrusted repository code, including mise configuration and pipeline changes.
Restrict queue/cluster access, do not attach deployment/cloud secrets, and do
not provide a Codecov token. Cache volumes are branch-scoped, but a fork branch
named like a trusted branch shares that branch's volume; keep fork builds
approval-gated so untrusted code cannot poison a trusted cache. A repository
shell script is not a security boundary against a malicious PR.

## Connect nightly fuzzing

Create a second pipeline, for example `raftz-nightly`, with this initial step:

```yaml
steps:
  - label: "Upload nightly pipeline"
    agents:
      queue: raftz-linux-amd64
    command: buildkite-agent pipeline upload .buildkite/nightly.yml
    timeout_in_minutes: 5
```

Disable automatic branch/PR builds for this pipeline. Configure a Buildkite
schedule with cron `17 3 * * *`, timezone **UTC**, and branch `main`. Manual builds
should use the same file. Set cancellation/skipping for superseded builds here
as well. The cron is a service-side setting; the YAML does not create it.
GitHub nightly remains enabled during the comparison period, so budget for both
services until cutover.

## Coverage during the parallel run

Buildkite generates and archives `zig-out/coverage/**/*`, including
`zig-out/coverage/kcov-merged/cobertura.xml`. It does **not** upload to Codecov.
GitHub Actions remains the sole uploader and retains its existing GitHub OIDC
configuration. This avoids introducing credentials or mixing duplicate reports
while evaluating Buildkite.

Before retiring GitHub Actions, implement and validate Codecov CLI/plugin
uploading with an authentication method Codecov accepts for Buildkite. A
Buildkite-issued OIDC token is not a GitHub-issued token. Preserve upload error
handling, correct commit/PR metadata, and a safe fork-PR policy. This remaining
integration is a cutover blocker.

## Validation and cutover checklist

Run locally with `buildkite-agent` and Python 3 installed:

```bash
mise run ci-lint-all
```

The Buildkite task checks shell syntax, uses native agent `pipeline upload
--dry-run --no-interpolation` parsing, and runs Python standard-library tests for
matrix coverage, architecture routing, failure/artifact settings, and bootstrap
failure handling. Dry runs use a dummy access token and never upload a pipeline.
They are not a substitute for server-side pipeline acceptance or hosted runs.

Before changing required checks or removing any GitHub workflow:

- Run all 16 regular jobs and all 5 nightly jobs on the hosted queues. Confirm
  that the ARM jobs really report `aarch64` and TSan runs on `x86_64`.
- Validate main push, PR open/update, manual builds, the UTC schedule, and rapid
  successive pushes/cancellation. Verify fork policy separately.
- Resolve PR merge-commit parity and the OS/kernel differences described above.
- Use a temporary test branch to force a command failure and create a fuzz
  artifact. Confirm the pipeline fails, siblings still run, and the hidden-cache
  reproducer can be downloaded. Test missing-artifact behavior as well.
- Compare coverage reports and implement the replacement Codecov upload before
  removing the GitHub uploader.
- Record the actual GitHub status context and change required checks only after
  successful comparison. Then disable old triggers and update the README badge
  in a separate change. Roll back by restoring the existing GitHub required
  check and disabling Buildkite triggers if needed.

## References

- [GitHub Actions migration](https://buildkite.com/docs/pipelines/migration/from-githubactions)
- [Linux hosted agents](https://buildkite.com/docs/agent/buildkite-hosted/linux)
- [GitHub integration and PR checkout](https://buildkite.com/docs/pipelines/source-control/github)
- [Build artifacts](https://buildkite.com/docs/pipelines/configure/artifacts)
- [Cache volumes](https://buildkite.com/docs/pipelines/hosted-agents/cache-volumes)
- [Canceling builds](https://buildkite.com/docs/pipelines/configure/canceling-builds)
