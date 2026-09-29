"""GC endpoint authentication when REGISTRY_GC_TOKEN is set."""
from __future__ import annotations

import os

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _client(tmp_data, token):
    settings = Settings(
        data_dir=tmp_data,
        db_path=os.path.join(tmp_data, "registry.db"),
        upload_ttl_seconds=3600,
        plan_ttl_seconds=3600,
        gc_token=token,
    )
    return TestClient(create_app(settings))


def test_gc_requires_bearer_token(tmp_data):
    with _client(tmp_data, "secret") as c:
        r = c.post("/gc")
        assert r.status_code == 401
        assert r.headers["WWW-Authenticate"] == "Bearer"

        r = c.post("/gc", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401

        r = c.post("/gc", headers={"Authorization": "Bearer secret"})
        assert r.status_code == 200
        assert r.json()["dry_run"] is True
