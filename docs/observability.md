# Observability

raftz exposes lock-safe status snapshots and dependency-free metric adapters.
It does not create an HTTP listener or own an OpenTelemetry SDK.

## Prometheus

```zig
const body = try raft.encodePrometheusMetrics(host, allocator);
defer allocator.free(body);
```

Return the owned bytes from an application endpoint with:

```text
Content-Type: text/plain; version=0.0.4; charset=utf-8
```

The encoder emits one `HELP` and `TYPE` record per metric family. Counters use a
`_total` Prometheus name; gauges retain their base name. Label values are escaped
for backslashes, quotes, and line feeds.

## OpenTelemetry

```zig
const Sink = struct {
    fn emit(_: *anyopaque, point: raft.MetricPoint) raft.Error!void {
        try recordPoint(point);
    }
};

var sink_context: u8 = 0;
const sink = raft.OpenTelemetryMetricSink{
    .ctx = &sink_context,
    .function = Sink.emit,
};
try raft.exportOpenTelemetryMetrics(host, allocator, sink);
```

Each point contains:

- a dotted OpenTelemetry name
- a Prometheus-compatible alias
- description and unit
- cumulative monotonic counter or gauge kind
- integer value
- typed attributes

Point attributes are valid only during the callback. SDK adapters must record or
copy them synchronously. The adapter supplies timestamps, resource attributes,
export interval, aggregation, and OTLP transport.

## Metric Families

Host families cover:

- Group lifecycle counts
- management and wake queues
- event-loop and scheduling activity
- shared transport routing and drops
- management operation results
- snapshot results
- active, recovered, and completed migration activity
- target preparation and local retirement events

Per-Group families cover:

- lifecycle, role, preparation, and retirement information
- term, leader, commit, applied, and incarnation values
- proposal, read-index, message, and peer-event backlog
- effective ingress limits and retained message bytes
- quota drop counts and dropped-byte totals
- scheduling and snapshot counters

`node_id` is present on every point. Per-Group points also include `group_id`.
State and event dimensions use bounded string values. Applications should still
apply normal cardinality controls when hosting a large number of Groups.

Exporters collect `getHostStatus` and `listGroupStatuses` sequentially. The
result is thread-safe and internally valid, but may span adjacent Host iterations
rather than represent one atomic global instant. See
[Operations](operations.md) for the human-readable ops report.
