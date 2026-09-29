"""Digest helpers (OCI digest = ``<algorithm>:<hex>``)."""
from __future__ import annotations

import hashlib
import re
from typing import BinaryIO

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# Tag: [a-zA-Z0-9_] plus . - in non-leading position; 1..128 chars.
TAG_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.\-]{0,127}$")
# OCI distribution-spec repository grammar: components separated by '/',
# each component 1..128 lowercase chars, '.', '_', '-' (no '__' sequences).
_REPO_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*"
REPO_RE = re.compile(rf"^{_REPO_COMPONENT}(?:/{_REPO_COMPONENT})*$")


def is_digest(ref: str) -> bool:
    return bool(DIGEST_RE.match(ref or ""))


def is_tag(ref: str) -> bool:
    return bool(TAG_RE.match(ref or ""))


def valid_repo(repo: str) -> bool:
    return bool(repo) and len(repo) <= 2048 and bool(REPO_RE.match(repo))


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return "sha256:" + h.hexdigest(), size


def sha256_stream(stream: BinaryIO) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        h.update(chunk)
        size += len(chunk)
    return "sha256:" + h.hexdigest(), size


def parse_digest(digest: str) -> tuple[str, str]:
    """Validate and return (algorithm, hex). Raises ValueError."""
    parts = (digest or "").split(":", 1)
    if len(parts) != 2 or parts[0] != "sha256" or not re.fullmatch(r"[0-9a-f]{64}", parts[1]):
        raise ValueError("invalid digest, expected sha256:<64 lowercase hex chars>")
    return parts[0], parts[1]
