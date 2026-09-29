"""Blob uploads: chunked, monolithic, resume, digest/size verification, mount."""
from __future__ import annotations

import hashlib
import os

from tests.conftest import upload_blob_chunked

OCI = "application/vnd.oci.image.manifest.v1+json"


def test_version_and_health(client):
    assert client.get("/v2/").status_code == 200
    assert client.get("/healthz").json() == {"status": "ok"}


def test_chunked_upload_and_download(client, tmp_data):
    data = b"hello " * 5000
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    got = upload_blob_chunked(client, "acme/app", data, chunk=12345)
    assert got == digest

    head = client.head("/v2/acme/app/blobs/" + digest)
    assert head.status_code == 200
    assert int(head.headers["Content-Length"]) == len(data)

    r = client.get("/v2/acme/app/blobs/" + digest)
    assert r.status_code == 200
    assert r.content == data

    # Range request
    r = client.get("/v2/acme/app/blobs/" + digest, headers={"Range": "bytes=10-19"})
    assert r.status_code == 206
    assert r.content == data[10:20]
    assert r.headers["Content-Range"] == f"bytes 10-19/{len(data)}"


def test_upload_resume_after_interruption(client):
    data = b"0123456789" * 1000
    digest = "sha256:" + hashlib.sha256(data).hexdigest()

    r = client.post("/v2/acme/app/blobs/uploads/")
    uuid = r.headers["Location"].rsplit("/", 1)[-1]

    # Send first chunk
    r = client.patch(f"/v2/acme/app/blobs/uploads/{uuid}",
                     content=data[:4000],
                     headers={"Content-Range": f"bytes 0-3999/{len(data)}"})
    assert r.status_code == 202

    # Client wrongly tries to resume at a stale offset -> 416, bytes rolled back
    r = client.patch(f"/v2/acme/app/blobs/uploads/{uuid}",
                     content=b"x" * 10,
                     headers={"Content-Range": "bytes 3000-3009/*"})
    assert r.status_code == 416
    assert r.json()["errors"][0]["code"] == "RANGE_INVALID"

    # Status reports the acknowledged offset and the session survives
    r = client.get(f"/v2/acme/app/blobs/uploads/{uuid}")
    assert r.status_code == 204
    assert r.headers["Range"] == "0-3999"

    # Continue correctly
    r = client.patch(f"/v2/acme/app/blobs/uploads/{uuid}",
                     content=data[4000:7000],
                     headers={"Content-Range": f"bytes 4000-6999/{len(data)}"})
    assert r.status_code == 202
    r = client.put(f"/v2/acme/app/blobs/uploads/{uuid}",
                   content=data[7000:],
                   headers={"Content-Range": f"bytes 7000-{len(data) - 1}/{len(data)}"},
                   params={"digest": digest})
    assert r.status_code == 201
    assert client.get(f"/v2/acme/app/blobs/{digest}").content == data


def test_monolithic_upload_digest_mismatch(client):
    r = client.post(
        "/v2/acme/app/blobs/uploads/",
        params={"digest": "sha256:" + "a" * 64},
        content=b"payload",
    )
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"


def test_monolithic_upload_success(client):
    data = b"single shot body"
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    r = client.post("/v2/acme/app/blobs/uploads/",
                    params={"digest": digest}, content=data)
    assert r.status_code == 201
    assert client.get(f"/v2/acme/app/blobs/{digest}").content == data


def test_dedup_same_content_one_file(client, tmp_data):
    data = b"dedup-me"
    d1 = upload_blob_chunked(client, "repo/a", data)
    d2 = upload_blob_chunked(client, "repo/b", data)
    assert d1 == d2
    # Exactly one physical file under blobs
    blobs = []
    for shard in os.listdir(os.path.join(tmp_data, "blobs", "sha256")):
        d = os.path.join(tmp_data, "blobs", "sha256", shard)
        if os.path.isdir(d):
            blobs += [os.path.join(d, f) for f in os.listdir(d)]
    assert len(blobs) == 1


def test_mount_known_and_unknown(client):
    data = b"mountable"
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    upload_blob_chunked(client, "source/repo", data)

    # Mount from source repository
    r = client.post("/v2/other/repo/blobs/uploads/",
                    params={"mount": digest, "from": "source/repo"})
    assert r.status_code == 201
    assert client.head(f"/v2/other/repo/blobs/{digest}").status_code == 200

    # Unknown digest -> normal session (OCI spec behavior)
    r = client.post("/v2/other/repo/blobs/uploads/",
                    params={"mount": "sha256:" + "b" * 64, "from": "source/repo"})
    assert r.status_code == 202
    assert "Location" in r.headers


def test_blob_unknown_and_repo_scope(client):
    data = b"scoped"
    digest = upload_blob_chunked(client, "only/here", data)
    assert client.head(f"/v2/elsewhere/blobs/{digest}").status_code == 404


def test_invalid_repo_name(client):
    r = client.post("/v2/BadName/blobs/uploads/")
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "NAME_INVALID"


def test_expired_upload_rejected(client, settings, tmp_data):
    import time
    data = b"will-expire"
    r = client.post("/v2/exp/app/blobs/uploads/")
    uuid = r.headers["Location"].rsplit("/", 1)[-1]
    # Force expiry in the database
    db = client.app.state.db
    with db.tx(immediate=True) as conn:
        conn.execute("UPDATE uploads SET expires_at = ? WHERE uuid = ?",
                     (time.time() - 1, uuid))
    r = client.patch(f"/v2/exp/app/blobs/uploads/{uuid}", content=data[:1],
                     headers={"Content-Range": "bytes 0-0/*"})
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "UPLOAD_UNKNOWN"
