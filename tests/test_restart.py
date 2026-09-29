"""Recovery after process restart: resumed uploads, orphan CAS adoption."""
from __future__ import annotations

import hashlib
import os

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _new_client(tmp_data, ttl=3600):
    settings = Settings(
        data_dir=tmp_data,
        db_path=os.path.join(tmp_data, "registry.db"),
        upload_ttl_seconds=ttl,
        plan_ttl_seconds=3600,
        gc_token=None,
    )
    app = create_app(settings)
    # Lifespan (startup reconciliation) runs on context entry.
    client = TestClient(app)
    with client:
        return client


def test_inprogress_upload_truncated_and_resumed(tmp_data):
    client = _new_client(tmp_data)
    with client:
        data = b"abcdefghij" * 500
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        r = client.post("/v2/acme/web/blobs/uploads/")
        uuid = r.headers["Location"].rsplit("/", 1)[-1]
        client.patch(f"/v2/acme/web/blobs/uploads/{uuid}",
                     content=data[:2000],
                     headers={"Content-Range": "bytes 0-1999/5000"})

        # Simulate a chunk that hit disk but whose ACK (offset update) never
        # landed: append bytes directly to the part file, then restart.
        part = os.path.join(tmp_data, "uploads", f"{uuid}.part")
        with open(part, "ab") as fh:
            fh.write(b"ghost-bytes-not-acked")

    # Restart: reconciliation truncates back to offset 2000.
    client2 = _new_client(tmp_data)
    with client2:
        r = client2.get(f"/v2/acme/web/blobs/uploads/{uuid}")
        assert r.status_code == 204
        assert r.headers["Range"] == "0-1999"

        r = client2.patch(f"/v2/acme/web/blobs/uploads/{uuid}",
                          content=data[2000:3000],
                          headers={"Content-Range": "bytes 2000-2999/5000"})
        assert r.status_code == 202, r.text
        r = client2.put(f"/v2/acme/web/blobs/uploads/{uuid}",
                        content=data[3000:],
                        headers={"Content-Range": f"bytes 3000-{len(data)-1}/{len(data)}"},
                        params={"digest": digest})
        assert r.status_code == 201, r.text
        assert client2.get(f"/v2/acme/web/blobs/{digest}").content == data


def test_orphan_cas_blob_adopted(tmp_data):
    # A correct CAS file whose DB row never committed (crash between link and tx).
    content = b"orphaned but valid"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    shard_dir = os.path.join(tmp_data, "blobs", "sha256", digest[7:9])
    os.makedirs(shard_dir, exist_ok=True)
    with open(os.path.join(shard_dir, digest[9:]), "wb") as fh:
        fh.write(content)

    client = _new_client(tmp_data)
    with client:
        # The blob row exists after reconciliation; linking membership works
        # and the file is not deleted.
        row = client.app.state.db.query_one(
            "SELECT size FROM blobs WHERE digest=?", (digest,))
        assert row is not None and row["size"] == len(content)
        assert os.path.exists(os.path.join(shard_dir, digest[9:]))


def test_corrupt_orphan_removed(tmp_data):
    shard_dir = os.path.join(tmp_data, "blobs", "sha256", "ab")
    os.makedirs(shard_dir, exist_ok=True)
    bogus = os.path.join(shard_dir, "c" * 62)
    with open(bogus, "wb") as fh:
        fh.write(b"content does not match the path digest")
    client = _new_client(tmp_data)
    with client:
        assert not os.path.exists(bogus)


def test_existing_state_survives_restart(tmp_data):
    import json
    from tests.conftest import image_manifest, upload_blob_chunked

    client = _new_client(tmp_data)
    with client:
        config = b'{"os":"linux"}'
        layer = b"persistent-layer"
        cd = upload_blob_chunked(client, "acme/web", config)
        ld = upload_blob_chunked(client, "acme/web", layer)
        mt, body = image_manifest(cd, len(config), [(ld, len(layer))])
        r = client.put("/v2/acme/web/manifests/v1", content=body,
                       headers={"Content-Type": mt})
        manifest_digest = r.headers["Docker-Content-Digest"]

    client2 = _new_client(tmp_data)
    with client2:
        assert client2.get("/v2/acme/web/manifests/v1").status_code == 200
        assert client2.head(f"/v2/acme/web/blobs/{ld}").status_code == 200
        assert client2.get(f"/v2/acme/web/manifests/{manifest_digest}").status_code == 200
        # A fresh GC must find nothing unreachable (state graph intact).
        plan = client2.post("/gc").json()
        assert plan["unreachable"]["blobs"] == []
        assert plan["unreachable"]["manifests"] == []
