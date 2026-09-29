"""OCI distribution-spec style error type.

Errors are serialized as::

    {"errors": [{"code": "DIGEST_INVALID", "message": "...", "detail": {...}}]}
"""
from __future__ import annotations

from typing import Any


class OCIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        detail: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail
        self.headers = headers

    def body(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.detail is not None:
            error["detail"] = self.detail
        return {"errors": [error]}


def unsupported() -> OCIError:
    return OCIError(404, "UNSUPPORTED", "The operation is unsupported.")


def unknown_name(repo: str) -> OCIError:
    return OCIError(404, "NAME_UNKNOWN", "Repository name not known to registry.",
                    {"Name": repo})


def name_invalid(repo: str) -> OCIError:
    return OCIError(400, "NAME_INVALID", "Invalid repository name.", {"Name": repo})


def blob_unknown(digest: str) -> OCIError:
    return OCIError(404, "BLOB_UNKNOWN", "Blob unknown to registry.",
                    {"Digest": digest})


def blob_unknown_in_repo(digest: str, repo: str) -> OCIError:
    return OCIError(404, "BLOB_UNKNOWN", "Blob unknown to registry in this repository.",
                    {"Digest": digest, "Repository": repo})


def digest_invalid(digest: str | None, reason: str = "") -> OCIError:
    detail: dict[str, Any] = {"Digest": digest}
    if reason:
        detail["Reason"] = reason
    return OCIError(400, "DIGEST_INVALID", "The provided digest did not match uploaded content.", detail)


def size_invalid(expected: int, actual: int) -> OCIError:
    return OCIError(400, "SIZE_INVALID", "The provided size did not match uploaded content.",
                    {"Expected": expected, "Actual": actual})


def range_invalid(offset: int, actual: int) -> OCIError:
    return OCIError(
        416, "RANGE_INVALID",
        "The Content-Range does not match the current upload position.",
        {"RangeErrorExpectedStart": actual, "ActualStart": offset},
        headers={"Range": f"0-{max(actual - 1, 0)}"},
    )


def upload_unknown(uuid: str) -> OCIError:
    return OCIError(404, "UPLOAD_UNKNOWN", "Upload session unknown to registry.",
                    {"UUID": uuid})


def manifest_unknown(repo: str, ref: str) -> OCIError:
    return OCIError(404, "MANIFEST_UNKNOWN", "Manifest unknown.",
                    {"Name": repo, "Reference": ref})


def manifest_invalid(reason: str, **detail: Any) -> OCIError:
    detail["Reason"] = reason
    return OCIError(400, "MANIFEST_INVALID", "Manifest invalid.", detail)


def unsupported_media(media_type: str | None) -> OCIError:
    return OCIError(415, "UNSUPPORTED_MEDIA_TYPE",
                    "The manifest media type is not supported.",
                    {"MediaType": media_type})


def tag_conflict(current_digest: str, expected: str, repo: str, tag: str) -> OCIError:
    return OCIError(
        409, "TAG_CONFLICT",
        "The expected digest in If-Match does not match the current tag target; "
        "refetch and retry (compare-and-swap).",
        {"Name": repo, "Tag": tag, "CurrentDigest": current_digest,
         "ExpectedDigest": expected},
        headers={
            "Location": f"/v2/{repo}/manifests/{tag}",
            "Docker-Content-Digest": current_digest,
            "ETag": f'"{current_digest}"',
        },
    )


def unauthorized(message: str = "Authentication required.") -> OCIError:
    return OCIError(
        401, "UNAUTHORIZED", message,
        headers={"WWW-Authenticate": "Bearer"},
    )


def plan_unknown(plan_id: str) -> OCIError:
    return OCIError(404, "PLAN_UNKNOWN", "GC plan unknown or expired.",
                    {"PlanID": plan_id})


def plan_stale(plan_id: str) -> OCIError:
    return OCIError(409, "PLAN_STALE",
                    "The plan was created before the most recent GC; create a new dry-run.",
                    {"PlanID": plan_id})


def gc_running() -> OCIError:
    return OCIError(409, "GC_IN_PROGRESS", "A garbage collection run is already in progress.")
