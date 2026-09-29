"""Manifest parsing and referential validation.

Supported media types:

* ``application/vnd.oci.image.manifest.v1+json``
* ``application/vnd.docker.distribution.manifest.v2+json``
* ``application/vnd.oci.image.index.v1+json``
* ``application/vnd.docker.distribution.manifest.list.v2+json``
* ``application/vnd.oci.artifact.manifest.v1+json``

Publishing is *referentially closed*: every descriptor must point at content
already present in the same repository (blobs linked in ``repo_blobs``;
sub-manifests present in ``manifests``), and descriptor sizes must match.
"""
from __future__ import annotations

import json
from typing import Any

from .digest import is_digest
from .errors import OCIError, digest_invalid, manifest_invalid, unsupported_media

IMAGE_MANIFEST_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
INDEX_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}
ARTIFACT_MANIFEST_TYPE = "application/vnd.oci.artifact.manifest.v1+json"
SUPPORTED_TYPES = IMAGE_MANIFEST_TYPES | INDEX_TYPES | {ARTIFACT_MANIFEST_TYPE}


def parse_body(media_type: str | None, body: bytes) -> dict[str, Any]:
    """Parse + structural validation of a manifest document."""
    if not media_type or media_type not in SUPPORTED_TYPES:
        # Strip parameters (e.g. "; charset=utf-8") before rejecting.
        base = (media_type or "").split(";", 1)[0].strip()
        if base in SUPPORTED_TYPES:
            media_type = base
        else:
            raise unsupported_media(media_type)

    try:
        doc = json.loads(body)
    except ValueError as exc:
        raise manifest_invalid("manifest body is not valid JSON", MediaType=media_type) from exc
    if not isinstance(doc, dict):
        raise manifest_invalid("manifest must be a JSON object", MediaType=media_type)
    if doc.get("schemaVersion") != 2:
        raise manifest_invalid("schemaVersion must be 2", MediaType=media_type)

    return doc


def _descriptor(name: str, raw: Any) -> tuple[str, int]:
    if not isinstance(raw, dict):
        raise manifest_invalid(f"{name} is not a descriptor")
    digest = raw.get("digest")
    size = raw.get("size")
    if not isinstance(digest, str) or not is_digest(digest):
        raise digest_invalid(digest if isinstance(digest, str) else None,
                             f"{name} descriptor digest invalid")
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise manifest_invalid(f"{name} descriptor size must be a non-negative integer",
                               Digest=digest)
    return digest, size


def referenced_descriptors(media_type: str, doc: dict[str, Any]) -> dict[str, list[tuple[str, int]]]:
    """Return descriptors grouped by the kind of content they reference.

    ``blobs``     -> config/layers/blobs descriptors (must be repo blobs)
    ``manifests`` -> index children / artifact manifests (must be manifests)
    ``subject``   -> at most one manifest descriptor
    """
    blobs: list[tuple[str, int]] = []
    manifests: list[tuple[str, int]] = []
    subject: list[tuple[str, int]] = []

    if "subject" in doc and doc["subject"] is not None:
        subject.append(_descriptor("subject", doc["subject"]))

    if media_type in IMAGE_MANIFEST_TYPES:
        config = doc.get("config")
        if not isinstance(config, dict):
            raise manifest_invalid("image manifest requires a 'config' descriptor")
        blobs.append(_descriptor("config", config))
        layers = doc.get("layers", [])
        if not isinstance(layers, list):
            raise manifest_invalid("image manifest 'layers' must be an array")
        for i, layer in enumerate(layers):
            blobs.append(_descriptor(f"layers[{i}]", layer))

    elif media_type in INDEX_TYPES:
        entries = doc.get("manifests", [])
        if not isinstance(entries, list):
            raise manifest_invalid("index 'manifests' must be an array")
        for i, entry in enumerate(entries):
            manifests.append(_descriptor(f"manifests[{i}]", entry))

    elif media_type == ARTIFACT_MANIFEST_TYPE:
        for key, target in (("blobs", blobs), ("manifests", manifests)):
            entries = doc.get(key, [])
            if not isinstance(entries, list):
                raise manifest_invalid(f"artifact manifest '{key}' must be an array")
            for i, entry in enumerate(entries):
                target.append(_descriptor(f"{key}[{i}]", entry))

    return {"blobs": blobs, "manifests": manifests, "subject": subject}


def check_references(
    conn,
    repo: str,
    media_type: str,
    doc: dict[str, Any],
) -> None:
    """Ensure every referenced content exists *in this repository*.

    Uses the transaction's snapshot so a concurrent GC/publish cannot change
    membership underneath the check.
    """
    groups = referenced_descriptors(media_type, doc)

    for digest, size in groups["blobs"]:
        row = conn.execute(
            "SELECT b.size FROM blobs b JOIN repo_blobs r ON r.digest = b.digest "
            "WHERE r.repo = ? AND r.digest = ?",
            (repo, digest),
        ).fetchone()
        if row is None:
            raise OCIError(400, "BLOB_UNKNOWN",
                           "Referenced blob is not present in repository.",
                           {"Repository": repo, "Digest": digest})
        if row["size"] != size:
            raise manifest_invalid(
                "descriptor size does not match blob size",
                Digest=digest, DescriptorSize=size, ActualSize=row["size"],
            )

    for digest, size in groups["manifests"] + groups["subject"]:
        row = conn.execute(
            "SELECT size FROM manifests WHERE repo = ? AND digest = ?",
            (repo, digest),
        ).fetchone()
        if row is None:
            raise OCIError(400, "MANIFEST_UNKNOWN",
                           "Referenced manifest is not present in repository.",
                           {"Name": repo, "Digest": digest})
        if row["size"] != size:
            raise manifest_invalid(
                "descriptor size does not match manifest size",
                Digest=digest, DescriptorSize=size, ActualSize=row["size"],
            )
