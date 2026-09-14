# Buildkite CI

Buildkite runs alongside GitHub Actions during the migration. The workflows in
`.github/workflows/` and the existing GitHub required checks remain unchanged.
Creating these files does not create queues, pipelines, schedules, or GitHub
integration settings in Buildkite.

Buildkite temporarily runs Linux AMD64 only on the existing `linux-medium`
queue. Linux ARM64 Debug and ReleaseSafe coverage remains in GitHub Actions.
The available macOS ARM64 queues are not substitutes for Linux ARM64 jobs.

## Pipelines

| File | Workloads | Expanded jobs |
| --- | --- | --- |
| `.buildkite/pipeline.yml` | Lint, AMD64 core tests, coverage, both examples, sanitizers, gperftools, bounded fuzzing, WAL durability | 14 |
| `.buildkite/nightly.yml` | Codec/WAL/confchange at 1M runs, simulation at 100K, WAL crash at 10K | 5 |

Apart from the deferred Linux ARM64 jobs, the regular pipeline preserves the
existing test commands, optimization modes, fuzz budgets, and job timeouts. It uses the overall Buildkite pipeline status
instead of a separate GitHub Actions `Required` aggregation job. No test is
soft-failed, and a failing test does not cancel its siblings.
`.buildkite/scripts/with-fuzz-artifacts.sh` uploads fuzz reproducers only when
the wrapped command fails, matching the GitHub Actions `if: failure()`
semantics; the coverage report is always archived through `artifact_paths`.
Forced cancellation or agent loss can prevent uploads; verify the failure path
on the hosted agents before cutover.

## Hosted queues

Both pipelines use this existing hosted queue in the pipeline's cluster:

| Queue key | OS / architecture | Shape |
| --- | --- | --- |
| `linux-medium` | Linux AMD64 | 4 vCPU / 16 GB RAM |

This name is referenced by the YAML files and initial upload steps below.
If using a different name, update both pipeline files and their configuration
tests. Restrict pipeline access to this queue and set concurrency/budget limits
before enabling all jobs. Each job should get an isolated hosted environment;
do not share a writable checkout between concurrent jobs.

The agent image needs Bash, Git, curl, CA certificates, tar, sha256sum, Python 3,
`buildkite-agent`, and a working Linux AMD64 Docker daemon/client. The bootstrap checks for C/C++ compilers, Make, CMake,
Ninja, and pkg-config before downloading mise. If any are missing, it installs
`build-essential`, `cmake`, `ninja-build`, and `pkg-config` with apt-get, requiring
root or passwordless `sudo -n`. Installation errors stop the job before building;
images that already supply all these tools do not require package installation.
The native grpc-lite dependencies need these tools even when Zig is installed.

The eight expanded runtime jobs (Core, Coverage, both examples, sanitizers, and
gperftools) use `in-container.sh` to build and run an Ubuntu 24.04 test container
**inside the existing Buildkite hosted job**. There is no self-hosted agent,
external VM, new queue, or service installation. The Linux AMD64 Ubuntu base is
pinned by its upstream registry digest in `.buildkite/container/Dockerfile`.
The image includes native build tools and UTC timezone data; the default hosted
image's missing `/etc/localtime` otherwise breaks the logger tests. Lint, fuzz,
and WAL durability jobs retain their direct hosted execution and artifact wrapper.

The container uses a hash-verified, pinned Moby default seccomp profile with only
two additional rules: `personality(ADDR_NO_RANDOMIZE)` for the native AMD64
personality, and the three io_uring syscalls. All upstream restrictions remain;
there is no `--privileged`, `seccomp=unconfined`, extra Linux capability, host ASLR
change, or Docker socket mount. Only the checkout and CI cache are bind-mounted;
agent credentials are not forwarded. The memlock limit is 64 MiB. See the
[upstream profile provenance](../.buildkite/container/moby/README.md).

Before downloading mise or compiling tests, a native probe checks `/etc/localtime`,
setting and restoring the process personality, opening an io_uring instance, and
tracing a child process. Any failed probe, container build, or test fails the job.
Container permissions cannot override a hosted kernel restriction: if the probe
still fails, retain its exact error and consult Buildkite rather than disabling
checks or falling back to a different execution environment. The full hosted tests,
not this small probe alone, establish whether the environment is compatible.

Coverage installs its development libraries inside this container and compiles
the same pinned, SHA-256-verified kcov source as GitHub Actions. It installs kcov
into a unique temporary directory and checks that the Cobertura report is
nonempty. Reports remain in the mounted checkout for the hosted agent to upload.
The container is removed on completion; ordinary wrapper exits also attempt
cleanup. Forced host loss relies on Buildkite destroying the job environment.

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

Volume names use the resolved `${BUILDKITE_COMMIT}` SHA rather than the branch
name. Buildkite permits only letters, numbers, and hyphens in cache names;
branches such as `formal/etcd-tla-baseline` would otherwise fail server-side
pipeline upload even though the agent's local dry run succeeds. Tests check
interpolated names as well as native YAML parsing.

Jobs and retries for the same commit can reuse these volumes, including across
branches at that commit. New commits start with a separate cache; this deliberately
trades cross-commit reuse for simple, collision-free source-revision keys.
Nightly uses a separate `-nightly-` volume. If Linux ARM64 jobs are restored,
give them a separate cache because `MISE_DATA_DIR` contains architecture-specific
binaries.
Cache names are not an authorization boundary; fork builds remain approval-gated.
Volumes are best-effort and can be evicted; every job must pass on a cold cache.

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
         queue: linux-medium
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
not provide a Codecov token. Cache volumes are commit-scoped, not trust-scoped;
keep fork builds approval-gated so untrusted execution cannot poison a trusted
cache. A repository
shell script is not a security boundary against a malicious PR.

## Connect nightly fuzzing

Create a second pipeline, for example `raftz-nightly`, with this initial step:

```yaml
steps:
  - label: "Upload nightly pipeline"
    agents:
      queue: linux-medium
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

- Run all 14 regular jobs and all 5 nightly jobs on `linux-medium`; confirm
  their actual OS/architecture is Linux `x86_64`.
- Restore Linux ARM64 Debug and ReleaseSafe jobs on a Linux ARM64 queue and
  validate their architecture before replacing GitHub's dual-architecture CI.
  The current AMD64-only Buildkite pipeline is not full architecture parity.
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
