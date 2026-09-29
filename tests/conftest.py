"""Pytest fixtures: each test gets an isolated data directory + app."""
from __future__ import annotations

import os
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient

# Point the module-level app at a writable directory before importing the app.
os.environ.setdefault("REGISTRY_DATA_DIR", tempfile.mkdtemp(prefix="registry-default-"))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.service import RegistryService
from app.storage import Storage


@pytest.fixture()
def tmp_data():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture()
def settings(tmp_data):
    return Settings(
        data_dir=tmp_data,
        db_path=os.path.join(tmp_data, "registry.db"),
        upload_ttl_seconds=3600,
        plan_ttl_seconds=3600,
        gc_token=None,
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        c.settings = settings  # type: ignore[attr-defined]
        yield c


@pytest.fixture()
def registry(settings):
    db = Database(settings.db_path)
    storage = Storage(settings.data_dir)
    return RegistryService(settings, db, storage)


# --------------------------------------------------------------------- helpers

def upload_blob_chunked(client, repo: str, data: bytes, chunk: int = 4096):
    """Full chunked upload; returns final digest."""
    import hashlib
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    r = client.post(f"/v2/{repo}/blobs/uploads/")
    assert r.status_code == 202, r.text
    location = r.headers["Location"]
    uuid = location.rsplit("/", 1)[-1]

    offset = 0
    while offset < len(data):
        part = data[offset:offset + chunk]
        end = offset + len(part) - 1
        r = client.patch(
            f"/v2/{repo}/blobs/uploads/{uuid}",
            content=part,
            headers={"Content-Range": f"bytes {offset}-{end}/{len(data)}"},
        )
        assert r.status_code == 202, r.text
        offset += len(part)

    r = client.put(f"/v2/{repo}/blobs/uploads/{uuid}", params={"digest": digest})
    assert r.status_code == 201, r.text
    return digest


def image_manifest(config_digest: str, config_size: int,
                   layers: list[tuple[str, int]]) -> tuple[str, bytes]:
    import json
    doc = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "size": config_size, "digest": config_digest},
        "layers": [
            {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
             "size": size, "digest": dgst}
            for dgst, size in layers
        ],
    }
    body = json.dumps(doc, separators=(",", ":")).encode()
    return doc["mediaType"], body
