# Upstream provenance

This suite adapts the independent read/write/CAS register test from
[wildarch/jepsen.rqlite](https://github.com/wildarch/jepsen.rqlite), pinned at
commit `20ec621810553e7a3833f010d7326660abeb26b5`.

Source files consulted at that exact revision:

- [`src/jepsen/rqlite/register.clj`](https://github.com/wildarch/jepsen.rqlite/blob/20ec621810553e7a3833f010d7326660abeb26b5/src/jepsen/rqlite/register.clj)
- [`project.clj`](https://github.com/wildarch/jepsen.rqlite/blob/20ec621810553e7a3833f010d7326660abeb26b5/project.clj)
- [`LICENSE`](https://github.com/wildarch/jepsen.rqlite/blob/20ec621810553e7a3833f010d7326660abeb26b5/LICENSE)

The upstream MIT license, including `Copyright (c) 2022 wildarch`, is preserved
verbatim in [LICENSE](LICENSE). It covers the adaptation in this directory;
it does not relicense raftz, Jepsen, Knossos, or their dependencies.

`src/raftz/register.clj` retains the upstream workload's independent tuples,
read/write/CAS operation semantics, five-value random domain,
`knossos.model/cas-register 0`, and the composition of
`independent/checker` with `checker/linearizable` (`:algorithm :linear`).
`src/raftz/cli.clj` replaces the rqlite Java/HTTP client with parameterized SQL
through the existing raft-sqlite CLI. `cluster.clj`, `core.clj`, the container
harness, and unit tests provide raftz-specific deployment and validation.

Intentional changes from upstream:

- All reads use raft-sqlite's leader ReadIndex Query, not configurable weak reads.
- A finite initialized domain replaces the upstream mismatch between 100
  preinitialized registers and an unbounded new-key generator.
- Status-only routing precedes a single submission. All potentially submitted
  write failures, including `failed_precondition`, remain indeterminate.
- Container-local SSH nemeses replace host process/firewall manipulation.
- Mandatory workload/recovery/coverage checks reject vacuous histories.
- There is no floating `rqlite-java master` dependency or JitPack repository.

Jepsen APIs were checked against primary source at
[Jepsen v0.2.6](https://github.com/jepsen-io/jepsen/tree/v0.2.6/jepsen/src/jepsen),
including `generator.clj`, `independent.clj`, `checker.clj`, `db.clj`,
`control.clj`, and `control/scp.clj`. Context7 was unavailable in this execution;
no replacement APIs were invented. Direct Clojure dependencies and base image
digests are pinned. OS packages are installed from the base distribution's
current package repositories, not a byte-for-byte package snapshot.
