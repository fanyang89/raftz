# Marionette compatibility snapshot

Source: <https://github.com/sb2bg/marionette/releases/tag/v0.6.0>

Original Zig package hash:
`marionette-0.6.0-jTrEFDEmGwAsls0lMrAb9uRDOWApe_5DSJkDfE6pTQwb`.
The upstream MIT license is preserved in `LICENSE`.

Only the source tree is included. The consumer-only build exports the
`marionette` module without upstream development dependencies. Upstream 0.7.2
still targets Zig 0.16, so a local snapshot is necessary for Zig 0.17.

Local changes:

- Migrate array repetition, enum builtins, and function reflection.
- Adapt the simulated I/O vtable, network operations, socket closing, and
  connection timeout errors to Zig 0.17.
- Fall back to allocate/copy/free when the backing allocator cannot shrink a
  remapped block, keeping successful shrink traces deterministic.

Update this snapshot from upstream once it supports Zig 0.17, preserving the
replay and WAL durability tests.
