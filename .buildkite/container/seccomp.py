import hashlib
import json
from pathlib import Path
import sys


UPSTREAM = Path(__file__).resolve().parent / "moby/default-seccomp.json"
UPSTREAM_SHA256 = "536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74"


def make_profile():
    data = UPSTREAM.read_bytes()
    if hashlib.sha256(data).hexdigest() != UPSTREAM_SHA256:
        raise ValueError("Upstream Moby seccomp profile checksum mismatch")
    profile = json.loads(data)
    profile["syscalls"].extend([
        {
            "names": ["personality"],
            "action": "SCMP_ACT_ALLOW",
            "args": [{"index": 0, "value": 0x40000, "op": "SCMP_CMP_EQ"}],
        },
        {
            "names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
            "action": "SCMP_ACT_ALLOW",
        },
    ])
    return profile


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: seccomp.py OUTPUT")
    Path(sys.argv[1]).write_text(json.dumps(make_profile(), indent=2) + "\n")
