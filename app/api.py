"""HTTP API: OCI Distribution v2 style endpoints."""
from __future__ import annotations

import json
import re
from typing import Any

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .config import Settings
from .errors import (
    OCIError,
    blob_unknown_in_repo,
    manifest_unknown,
    unauthorized,
)
from .service import IterStream, RegistryService, _http_iso

router = APIRouter()


def _get_service(request: Request) -> RegistryService:
    return request.app.state.service


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings


def _error(exc: OCIError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.body(),
                        headers=exc.headers, media_type="application/json")


def _upload_headers(repo: str, uuid: str, offset: int, expires_at: float,
                    extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Location": f"/v2/{repo}/blobs/uploads/{uuid}",
        "Docker-Upload-UUID": uuid,
        "Upload-Expires-At": _http_iso(expires_at),
    }
    if offset > 0:
        headers["Range"] = f"0-{offset - 1}"
    if extra:
        headers.update(extra)
    return headers


def _iter_stream(request: Request) -> IterStream:
    return IterStream(request.stream())


def _has_body(request: Request) -> bool:
    """True when the request plausibly carries a body (content-length>0 or
    chunked transfer encoding)."""
    cl = request.headers.get("content-length")
    if cl is not None and cl != "0":
        return True
    te = request.headers.get("transfer-encoding", "").lower()
    return "chunked" in te


# ------------------------------------------------------------------- version

@router.get("/v2/")
def version() -> dict[str, str]:
    return {}


# --------------------------------------------------------------------- blobs

@router.post("/v2/{repo:path}/blobs/uploads/")
async def initiate_upload(
    request: Request,
    repo: str,
    mount: str | None = Query(default=None),
    from_repo: str | None = Query(default=None, alias="from"),
    digest: str | None = Query(default=None),
):
    service = _get_service(request)
    body: IterStream | None = None
    # Reading the stream is only meaningful when content was sent.
    if _has_body(request):
        body = _iter_stream(request)
    try:
        result = await service.create_upload(repo, body, mount=mount,
                                             from_repo=from_repo, digest=digest)
    except OCIError as exc:
        return _error(exc)

    if result.kind == "created":
        return Response(
            status_code=201,
            headers={"Location": f"/v2/{repo}/blobs/{result.data['digest']}",
                     "Docker-Content-Digest": result.data["digest"]},
        )
    d = result.data
    return Response(status_code=202, headers=_upload_headers(
        d["repo"], d["uuid"], d["offset"], d["expires_at"]))


@router.get("/v2/{repo:path}/blobs/uploads/{uuid}")
def get_upload(request: Request, repo: str, uuid: str):
    service = _get_service(request)
    try:
        d = service.upload_status(repo, uuid)
    except OCIError as exc:
        return _error(exc)
    return Response(status_code=204,
                    headers=_upload_headers(repo, uuid, d["offset"], d["expires_at"]))


@router.patch("/v2/{repo:path}/blobs/uploads/{uuid}")
async def patch_upload(
    request: Request,
    repo: str,
    uuid: str,
    content_range: str | None = Header(default=None, alias="Content-Range"),
):
    service = _get_service(request)
    try:
        d = await service.append_upload(repo, uuid, _iter_stream(request), content_range)
    except OCIError as exc:
        return _error(exc)
    return Response(status_code=202,
                    headers=_upload_headers(repo, uuid, d["offset"], d["expires_at"]))


@router.put("/v2/{repo:path}/blobs/uploads/{uuid}")
async def complete_upload(
    request: Request,
    repo: str,
    uuid: str,
    digest: str | None = Query(default=None),
    content_range: str | None = Header(default=None, alias="Content-Range"),
):
    service = _get_service(request)
    body: IterStream | None = None
    if _has_body(request):
        body = _iter_stream(request)
    try:
        d = await service.finalize_upload(repo, uuid, digest, body, content_range)
    except OCIError as exc:
        return _error(exc)
    return Response(
        status_code=201,
        headers={"Location": f"/v2/{repo}/blobs/{d['digest']}",
                 "Docker-Content-Digest": d["digest"],
                 "Content-Length": str(d["size"])},
    )


@router.head("/v2/{repo:path}/blobs/{digest}")
def head_blob(request: Request, repo: str, digest: str):
    service = _get_service(request)
    try:
        stat = service.blob_stat(repo, digest)
    except OCIError as exc:
        return _error(exc)
    if stat is None:
        return _error(blob_unknown_in_repo(digest, repo))
    size, dgst = stat
    return Response(status_code=200, headers={
        "Content-Type": "application/octet-stream",
        "Content-Length": str(size),
        "Docker-Content-Digest": dgst,
        "ETag": f'"{dgst}"',
        "Accept-Ranges": "bytes",
    })


@router.get("/v2/{repo:path}/blobs/{digest}")
def get_blob(
    request: Request,
    repo: str,
    digest: str,
    range_header: str | None = Header(default=None, alias="Range"),
):
    service = _get_service(request)
    try:
        stat = service.blob_stat(repo, digest)
    except OCIError as exc:
        return _error(exc)
    if stat is None:
        return _error(blob_unknown_in_repo(digest, repo))
    size, dgst = stat
    headers = {
        "Docker-Content-Digest": dgst,
        "ETag": f'"{dgst}"',
        "Accept-Ranges": "bytes",
    }
    offset = 0
    if range_header:
        m = re.match(r"^bytes=(\d+)-(\d*)$", range_header.strip())
        if not m:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        offset = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else size - 1
        if offset >= size or end >= size or offset > end:
            return Response(status_code=416,
                            headers={"Content-Range": f"bytes */{size}"})
        headers["Content-Range"] = f"bytes {offset}-{end}/{size}"
        headers["Content-Length"] = str(end - offset + 1)
        return StreamingResponse(
            service.storage.stream_blob(dgst, offset, end),
            status_code=206,
            media_type="application/octet-stream",
            headers=headers,
        )
    headers["Content-Length"] = str(size)
    return StreamingResponse(
        service.storage.stream_blob(dgst),
        media_type="application/octet-stream",
        headers=headers,
    )


# ----------------------------------------------------------------- manifests

@router.put("/v2/{repo:path}/manifests/{ref}")
async def put_manifest(
    request: Request,
    repo: str,
    ref: str,
    content_type: str | None = Header(default=None, alias="Content-Type"),
    if_match: str | None = Header(default=None, alias="If-Match"),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
):
    service = _get_service(request)
    body = await request.body()
    try:
        d = service.put_manifest(repo, ref, content_type, body, if_match, if_none_match)
    except OCIError as exc:
        return _error(exc)
    return Response(
        status_code=200 if (not d["created"] or d["tag_moved"]) else 201,
        headers={"Location": f"/v2/{repo}/manifests/{ref}",
                 "Docker-Content-Digest": d["digest"],
                 "ETag": f'"{d["digest"]}"'},
    )


@router.head("/v2/{repo:path}/manifests/{ref}")
def head_manifest(request: Request, repo: str, ref: str):
    service = _get_service(request)
    try:
        d = service.head_manifest(repo, ref)
    except OCIError as exc:
        return _error(exc)
    if d is None:
        return _error(manifest_unknown(repo, ref))
    return Response(status_code=200, headers={
        "Content-Type": d["media_type"],
        "Content-Length": str(d["size"]),
        "Docker-Content-Digest": d["digest"],
        "ETag": f'"{d["digest"]}"',
    })


@router.get("/v2/{repo:path}/manifests/{ref}")
def get_manifest(request: Request, repo: str, ref: str):
    service = _get_service(request)
    try:
        d = service.get_manifest(repo, ref)
    except OCIError as exc:
        return _error(exc)
    if d is None:
        return _error(manifest_unknown(repo, ref))
    return Response(
        content=d["body"],
        media_type=d["media_type"],
        headers={"Docker-Content-Digest": d["digest"],
                 "ETag": f'"{d["digest"]}"'},
    )


@router.get("/v2/{repo:path}/tags/list")
def list_tags(request: Request, repo: str):
    service = _get_service(request)
    try:
        tags = service.list_tags(repo)
    except OCIError as exc:
        return _error(exc)
    return JSONResponse({"name": repo, "tags": tags})


# ------------------------------------------------------------------------ gc

@router.post("/gc")
async def gc(request: Request):
    settings = _get_settings(request)
    service = _get_service(request)
    if settings.gc_token:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer ") or auth[len("Bearer "):].strip() != settings.gc_token:
            return _error(unauthorized("GC requires a bearer token (REGISTRY_GC_TOKEN)."))

    payload: dict[str, Any] = {}
    if request.headers.get("content-length", "0") != "0":
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

    execute = payload.get("execute", False) or payload.get("confirm", False)
    plan_id = payload.get("plan_id")
    force = bool(payload.get("force", False))

    try:
        if not execute:
            plan = service.gc_dry_run()
            return JSONResponse(status_code=200, content=plan)
        if not plan_id:
            return _error(OCIError(400, "PLAN_REQUIRED",
                                   "Execution requires 'plan_id' from a prior dry-run."))
        result = service.gc_execute(plan_id, force=force)
        return JSONResponse(status_code=200, content=result)
    except OCIError as exc:
        return _error(exc)


@router.get("/gc/plans/{plan_id}")
def get_gc_plan(request: Request, plan_id: str):
    service = _get_service(request)
    try:
        return JSONResponse(service.get_plan(plan_id))
    except OCIError as exc:
        return _error(exc)


# -------------------------------------------------------------------- health

@router.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
