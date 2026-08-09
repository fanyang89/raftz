//! Optional gperftools integration enabled with `-Dgperftools=true`.

const raftz = @import("raftz");
const grpc_gperftools = @import("grpc_lite_gperftools");

comptime {
    _ = raftz;
}

pub const allocator = grpc_gperftools.allocator;
pub const CpuProfilerError = grpc_gperftools.CpuProfilerError;
pub const startCpuProfiler = grpc_gperftools.startCpuProfiler;
pub const flushCpuProfiler = grpc_gperftools.flushCpuProfiler;
pub const stopCpuProfiler = grpc_gperftools.stopCpuProfiler;
pub const cpuProfilerRunning = grpc_gperftools.cpuProfilerRunning;
pub const startHeapProfiler = grpc_gperftools.startHeapProfiler;
pub const dumpHeapProfile = grpc_gperftools.dumpHeapProfile;
pub const stopHeapProfiler = grpc_gperftools.stopHeapProfiler;
pub const heapProfilerRunning = grpc_gperftools.heapProfilerRunning;
pub const getNumericProperty = grpc_gperftools.getNumericProperty;
pub const owns = grpc_gperftools.owns;
pub const releaseFreeMemory = grpc_gperftools.releaseFreeMemory;
pub const setGuardedSamplingInterval = grpc_gperftools.setGuardedSamplingInterval;
pub const guardedSamplingInterval = grpc_gperftools.guardedSamplingInterval;
pub const activateGuardedSampling = grpc_gperftools.activateGuardedSampling;
