#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mode=${1:-smoke}
case "$mode" in unit|smoke|full) ;; *) echo 'usage: run.sh unit|smoke|full' >&2; exit 2 ;; esac
root=$(cd ../.. && pwd)
project="raftz-jepsen-$(date +%s)-$$"
tmpbase="${TMPDIR:-$HOME/tmp}/pi"
mkdir -p "$tmpbase"
results=${RAFTZ_JEPSEN_RESULTS:-$(mktemp -d "$tmpbase/raftz-jepsen-results.XXXXXX")}
mkdir -p "$results"
results=$(realpath "$results")
keys=$(mktemp -d "$tmpbase/raftz-jepsen-keys.XXXXXX")
binary=${RAFTZ_JEPSEN_BINARY:-$root/examples/raft-sqlite/zig-out/bin/raft-sqlite}
compose=(docker compose -p "$project" -f compose.yaml)
active=0
artifact_error=0
current="$results/unit"
export RAFTZ_JEPSEN_COMMAND=test
echo "Project: $project; results: $results"
collect() {
  mkdir -p "$current"
  if ! timeout 30 "${compose[@]}" cp controller:/suite/store/. "$current" >>"$results/cleanup.log" 2>&1; then
    echo "ERROR: failed to export controller results to $current" >&2
    artifact_error=1
  fi
  if [[ "$mode" != unit ]]; then
    for node in n1 n2 n3; do
      if ! timeout 30 "${compose[@]}" cp "$node:/data" "$current/$node" >>"$results/cleanup.log" 2>&1; then
        echo "ERROR: failed to export $node data/logs to $current" >&2
        artifact_error=1
      fi
    done
  fi
  timeout 30 "${compose[@]}" logs --no-color >"$current/compose.log" 2>&1 || artifact_error=1
}
cleanup() {
  rc=$?
  trap - EXIT INT TERM
  set +e
  if ((active)); then
    timeout 30 "${compose[@]}" stop -t 10 controller >>"$results/cleanup.log" 2>&1
    if [[ "$mode" != unit ]]; then
      for node in n1 n2 n3; do
        {
          timeout 25 "${compose[@]}" exec -T "$node" node-control heal
          timeout 25 "${compose[@]}" exec -T "$node" node-control start
          timeout 25 "${compose[@]}" exec -T "$node" node-control stop
        } >>"$results/cleanup.log" 2>&1
      done
    fi
    collect
  fi
  timeout 60 "${compose[@]}" down --volumes --remove-orphans >>"$results/cleanup.log" 2>&1
  clean_rc=$?
  rm -rf "$keys"
  if ((clean_rc != 0)); then
    echo "ERROR: cleanup failed for Compose project $project (see $results/cleanup.log)" >&2
  fi
  if ((rc == 0 && (clean_rc != 0 || artifact_error != 0))); then rc=2; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
git -C "$root" rev-parse HEAD >"$results/raftz-commit.txt"
"${compose[@]}" config >"$results/compose-config.yaml"
timeout 600 "${compose[@]}" build controller 2>&1 | tee "$results/controller-build.log"
if [[ "$mode" != unit ]]; then
  if [[ -z ${RAFTZ_JEPSEN_SKIP_BUILD:-} ]]; then
    case "$(uname -m)" in x86_64) target=x86_64-linux-musl ;; aarch64) target=aarch64-linux-musl ;; *) exit 2 ;; esac
    (cd "$root/examples/raft-sqlite" && timeout 300 zig build -Dtarget="$target" -Doptimize=ReleaseSafe --summary all) 2>&1 | tee "$results/zig-build.log"
  fi
  [[ -f "$binary" && -x "$binary" ]]
  sha256sum "$binary" >"$results/binary.sha256"
  ssh-keygen -q -t rsa -b 3072 -m PEM -N '' -f "$keys/id_rsa"
  timeout 600 "${compose[@]}" build n1 n2 n3 2>&1 | tee "$results/nodes-build.log"
fi
seconds=20
[[ "$mode" != full ]] || seconds=120
scenarios=(unit)
[[ "$mode" == unit ]] || scenarios=(none partition kill)
for fault in "${scenarios[@]}"; do
  current="$results/$fault"
  mkdir -p "$current"
  if [[ "$mode" == unit ]]; then
    export RAFTZ_JEPSEN_COMMAND=test
    active=1
    timeout 60 "${compose[@]}" create controller
  else
    export RAFTZ_JEPSEN_COMMAND="run $fault $seconds"
    active=1
    timeout 60 "${compose[@]}" create
    for node in n1 n2 n3; do
      "${compose[@]}" cp "$keys/id_rsa.pub" "$node:/keys/id_rsa.pub"
      "${compose[@]}" cp "$binary" "$node:/usr/local/bin/raft-sqlite"
    done
    "${compose[@]}" cp "$keys/id_rsa" controller:/keys/id_rsa
    "${compose[@]}" cp "$binary" controller:/usr/local/bin/raft-sqlite
    timeout 90 "${compose[@]}" up -d --wait n1 n2 n3
  fi
  id=$("${compose[@]}" ps -aq controller)
  timeout 600 docker start -a "$id" 2>&1 | tee "$current/console.log"
  rc=$(docker inspect --format '{{.State.ExitCode}}' "$id")
  if ((rc != 0)); then exit "$rc"; fi
  collect
  ((artifact_error == 0))
  timeout 60 "${compose[@]}" down --volumes --remove-orphans
  active=0
done
