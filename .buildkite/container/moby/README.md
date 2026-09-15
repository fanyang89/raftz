# Upstream seccomp profile

`default-seccomp.json` and `LICENSE` are unchanged files from
[moby/profiles](https://github.com/moby/profiles) at commit
`3c28324314729dbade8287e868eef6338c42807a`:

- Source: `seccomp/default.json`
- SHA-256: `536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74`
- License: Apache-2.0 (`LICENSE`)

`../seccomp.py` verifies the source hash and generates a temporary derived profile.
It preserves every upstream rule and adds only `personality(ADDR_NO_RANDOMIZE)`
for the native AMD64 personality and the three io_uring syscalls. The existing
kernel-dependent child-ptrace permissions are retained. No capabilities are
added, and no global Docker or hosted-agent security setting is modified.
