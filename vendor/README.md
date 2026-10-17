# Vendored Dependencies

`grpc-lite` is pinned to commit `db3c400bf70419742ebcb42fe316ba8b754b4975`
for Zig 0.17 compatibility.

Its nanozlog dependency is overridden with a copy from commit
`e693c11976d55ba0a5b8deeaaaf9f1c5cc30eba9` with local fixes for sentinel
slices, synchronized background queue polling, and Zig 0.17 reflection APIs.
The local copy uses zeit 0.10.0 at commit
`06bb7b462b4218770fb5438ebe6b0c0c5ef18888`.

Marionette 0.6.0 is vendored with Zig 0.17 compatibility changes; see
[`marionette/README.raftz.md`](marionette/README.raftz.md).
