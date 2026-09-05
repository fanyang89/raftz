# Operations

raftz ships dependency-free operational surfaces: a human-readable report for
runbooks, metric exporters for dashboards, and fail-closed format-version
handling for upgrades.

## Ops Report

```zig
const report = try raft.encodeOpsReport(host, allocator);
defer allocator.free(report);
```

The report is stable, greppable `key=value` text with one line per entity. It
is safe to call while the Host run loop is active and is intended for CLI
output, logs, and incident triage. Machine-readable consumers should prefer the
[Prometheus and OpenTelemetry exporters](observability.md).

Sections:

- one `raftz` line with node and group lifecycle counts
- `host` lines for event-loop, queue, transport, operation, snapshot,
  migration, preparation, and drop counters
- five `group id=...` lines per Group covering role and indexes, pending
  ingress, quota saturation, scheduler counters, and flags including the last
  error name
- one `migration id=...` line per active migration with stage, elapsed versus
  timeout ticks, stability progress, and replication positions

`MultiRaftHost.listReplicaMigrations(allocator)` returns the same migration
statuses as an owned slice sorted by Group ID for programmatic inspection.

## Upgrade Compatibility

Durable artifacts carry a magic marker and a format version:

| Artifact | Magic | Version |
| --- | --- | --- |
| WAL segments | `WAL1` | 1 |
| WAL metadata | versioned record | current and older versions |
| Durable cluster membership | `RCLS` | 1 |
| Membership context | `RMC1` | 1 |
| Migration intents | `RMIG` | 1 |

Decoders fail closed:

- unknown or damaged data surfaces as the artifact's corruption error
- a known magic with a different format version surfaces as `UnsupportedVersion`,
  distinct from corruption, and is checked before the checksum

`UnsupportedVersion` means the artifact was written by a different raftz
release. The current binary refuses to open or rewrite it, so downgrades fail
safely instead of destroying newer data. Resolve it by running a release that
supports the stored version. The WAL metadata decoder additionally understands
older metadata versions and the operator-supplied
`LegacyMembershipMigration` and `LegacySnapshotMigration` inputs upgrade
pre-durable-membership storage in place.

Upgrade guidance:

1. Roll out binaries that both read and write the new format first.
2. Only then allow artifacts in the new format to be produced.
3. Downgrades stop with `UnsupportedVersion` and leave the data directory
   untouched.

`raft.version` exposes the library semantic version for embedding in reports
and logs.
