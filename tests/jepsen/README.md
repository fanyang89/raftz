# raftz Jepsen registers (opt-in)

A real Clojure Jepsen/Knossos adaptation of the MIT-licensed
[wildarch/jepsen.rqlite register suite](UPSTREAM.md), testing the unmodified
`examples/raft-sqlite` executable. This is not the in-process chaos simulator
and does not add dependencies to default Zig tests.

## Run

From the repository root:

```sh
mise run test-jepsen-unit
mise run test-jepsen-smoke
mise run test-jepsen-full
```

Equivalent commands are `bash tests/jepsen/run.sh unit|smoke|full`.
Unit tests need Docker/Compose only; the controller image contains Java and
Leiningen. With an existing compatible Leiningen installation, `cd
 tests/jepsen && lein test` also works without starting database nodes.

Smoke runs **three fresh clusters**, sequentially: no faults, Raft partition,
and kill/restart, with 20 seconds of random workload per scenario. Full uses
120 seconds per scenario. Both include serialized warmup probes, healing,
a five-second recovery wait, and serialized recovery probes. They fail fast:
a failed baseline prevents later scenarios from running. Each controller run
has a 600-second wall-clock deadline (including setup, teardown, and checking).
Image builds have separate 600-second deadlines. Exit zero requires an
explicit `:valid? true` from all composed checkers in every scenario.

```sh
RAFTZ_JEPSEN_RESULTS="$HOME/tmp/raftz-results/run-1" \
  bash tests/jepsen/run.sh smoke

# Optional: use an already built, executable Linux binary compatible with
# the Docker daemon's architecture. Its SHA-256 is saved with results.
RAFTZ_JEPSEN_SKIP_BUILD=1 \
RAFTZ_JEPSEN_BINARY="$PWD/examples/raft-sqlite/zig-out/bin/raft-sqlite" \
  bash tests/jepsen/run.sh smoke
```

Prerequisites for real runs:

- Linux Docker daemon, Compose with `create`, `cp`, and `up --wait`; Bash,
  `timeout`, `ssh-keygen`, `sha256sum`, and Zig 0.16.0 on the runner.
- Network access to pinned base images, Maven Central/Clojars, Ubuntu packages,
  and raftz's pinned build dependencies on first build.
- Sufficient resources for Java (`-Xmx2g`), three nodes, and Zig compilation.
- Runner and daemon must have compatible CPU architectures. The default build
  is ReleaseSafe, statically linked `x86_64-linux-musl` or `aarch64-linux-musl`.
  Only x86_64 builds were checked in the initial integration.
- Kernel and container policy must permit libxev's `io_uring` backend,
  including `io_uring_setup`, `io_uring_enter`, and `io_uring_register`, for
  **both nodes and controller CLI subprocesses**. Docker defaults may deny
  these calls. Obtain administrator approval for any narrowly scoped policy
  changes; this harness does not change daemon/kernel policy, disable seccomp,
  or use `privileged`/`unconfined` as a workaround.

## Isolation and artifacts

Every invocation creates a unique `raftz-jepsen-<timestamp>-<pid>` Compose
project, a private bridge network, project-owned named volumes, and disposable
containers. No ports are published; no Docker socket, host PID/network
namespace, host credentials, or host firewall is exposed to the controller.
Only node containers receive `NET_ADMIN`. SSH controls the node-local
`node-control` script; its iptables rules affect only TCP port 9001 in those
containers, leaving API port 8001 and SSH reachable. A start fault targets the
observed leader, or a test node if no leader can be discovered.

Files use **create / Compose cp / start**, not host bind mounts, so a remote
Docker daemon need not see the runner filesystem. Only public keys enter
nodes; a fresh throwaway RSA/PEM private key enters the controller. The old
pinned Jepsen/JSch version needs RSA/SHA1 enabled on these disposable sshd
servers. Host-key checking is disabled only inside the test controller, for
the three test names. Never reuse these settings for real servers.

Jepsen teardown heals and restarts nodes. The host runner's EXIT/INT/TERM trap
also stops the controller, attempts healing/restart/shutdown, exports artifacts,
and removes only its own project/volumes. No shell can guarantee cleanup after
host power loss, SIGKILL, or an unavailable Docker daemon: inspect the printed
project name and `cleanup.log` and manually remove that project when necessary.
Artifact-copy or cleanup failures are reported, and do not mask an earlier
test failure. Temporary private keys are deleted and are never exported.
Images/build cache remain for reuse; node storage does not.

Results default to a unique directory beneath `${TMPDIR:-$HOME/tmp}/pi` and
are printed before execution. A supplied `RAFTZ_JEPSEN_RESULTS` should be a
new directory. Retained output includes:

- `controller-build.log`, `nodes-build.log`, `zig-build.log` when applicable;
- `raftz-commit.txt`, `binary.sha256`, Compose config, and `cleanup.log`;
- per-scenario `console.log`, `compose.log`, and node `/data` copies containing
  WAL, SQLite, and `raft-sqlite.log`;
- Jepsen `history.edn`, `history.txt`, `results.edn`, `test.jepsen`, `jepsen.log`,
  and independent per-key histories/results, if the test reached those stages.

Node data copies are diagnostic artifacts, not a certified backup protocol.
Crashes before workload startup can leave logs/storage without a checked
history. Always inspect top-level `results.edn`; log creation is not a pass.

## Semantics and coverage

Initialization commits a table and registers 0..8 at zero before recording
operations. Random workers contend over keys 0..7, with values 0..4. Key 8 is
reserved for warmup/recovery write, successful CAS, failed CAS, and read probes.
An independent CAS-register model starts each key at zero.

Every logical operation gets one UUID. Status probes may try other nodes, but
there is **at most one Query/Execute submission per workload operation**.
Execute uses atomic conditional `UPDATE` for CAS. There is no write retry or
leader forwarding, so no retry can duplicate a logical operation. A missing
leader before submission or failure to launch the CLI is a definite failure.
SQL rollback/invalid-request responses are definite failures. Timeouts,
connection loss, malformed responses, and other potentially submitted write
errors are `:info` (unknown), including `failed_precondition: not leader`:
raft-sqlite can use that same status for post-proposal `LostLeadership`.
Reads have no side effect and may fail. Initialization alone retries the same
UUID and identical idempotent SQL, before other operations run; it cannot
exceed the example's 10,000-entry deduplication window.

The composed checker requires:

- real independent Knossos linearizability, not a custom consistency checker;
- successful read, write, and CAS in **each** of warmup, random workload, and
  recovery, plus at least one definite CAS mismatch;
- fault scenarios with successful injection/healing and workload invocations
  between the completed start and stop events.

Coverage checks supplement, rather than replace, Knossos. Unit tests cover
protobuf JSON parsing, ambiguous write outcomes (including leadership loss),
subprocess deadlines, single submission/UUID retention, initialization retries,
nemesis target/healing calls, and checker acceptance/rejection of valid,
stale-read, impossible-CAS, indeterminate-write, and vacuous histories.

This is a bounded safety sample, not proof of Raft correctness or availability.
It does not test membership changes, multi-Raft, follower reads, weak reads,
clock faults, disk corruption, power-loss/fsync behavior, authentication/TLS,
long deduplication-window retries, or snapshot installation. Process SIGKILL
reuses the same WAL/SQLite volume, but is not a hardware power failure. Unknown
writes may or may not take effect; failed operations are not asserted to have
succeeded. Longer runs can be inconclusive or exceed the checker deadline.

## Initial validation status

Adapter/checker unit tests and native/static raft-sqlite builds passed. Real
three-node smoke was attempted, but did **not** produce a checked history:
node transport initialization failed with `ConnectionClosed`. A single-node,
no-peer diagnostic of the same binary identified:

```text
io_uring_setup(256, {flags=0, sq_thread_cpu=0, sq_thread_idle=1000})
  = -1 EPERM (Operation not permitted)
```

The error passes through pinned libxev `Loop.init`, grpc-lite
`LoopInitializationFailed`, and raftz's `mapStartError` to `ConnectionClosed`.
This demonstrates an io_uring permission blocker, not a Raft counterexample;
it does not distinguish seccomp from host kernel/LSM policy. No policy was
relaxed. Baseline startup was blocked, so partition/kill histories and the
full command remain **unvalidated end-to-end**. Re-run them only on approved
infrastructure; do not interpret passing unit tests as a Jepsen pass.
