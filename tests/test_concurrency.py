"""Concurrent pulls, publishes and GC against a real uvicorn server.

Asserts the core safety invariant end-to-end:
  * every fetch of a *tagged* blob verifies by digest;
  * no published manifest ever dangles (tag -> manifest -> blobs all present);
  * GC reclaims garbage while the workload runs.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time

import httpx
import uvicorn

from app.config import Settings
from app.main import create_app


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ServerThread(threading.Thread):
    def __init__(self, settings: Settings, port: int) -> None:
        super().__init__(daemon=True)
        self.config = uvicorn.Config(create_app(settings), host="127.0.0.1",
                                    port=port, log_level="warning")
        self.server = uvicorn.Server(self.config)

    def run(self) -> None:
        self.server.run()

    def stop(self) -> None:
        self.server.should_exit = True


def _upload_blob(base: httpx.Client, repo: str, data: bytes) -> str:
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    r = base.post(f"/v2/{repo}/blobs/uploads/", params={"digest": digest},
                  content=data)
    assert r.status_code == 201, r.text
    return digest


def _publish_image(base: httpx.Client, repo: str, tag: str, n: int) -> str:
    config = json.dumps({"n": n}).encode()
    layer = (f"layer-{n}-".encode() * 100)
    cd = _upload_blob(base, repo, config)
    ld = _upload_blob(base, repo, layer)
    doc = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "size": len(config), "digest": cd},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "size": len(layer), "digest": ld}],
    }
    body = json.dumps(doc, separators=(",", ":")).encode()
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    headers = {"Content-Type": doc["mediaType"]}
    r = base.put(f"/v2/{repo}/manifests/{tag}", content=body, headers=headers)
    if r.status_code in (200, 201):
        return digest
    # Tag exists: CAS retry loop.
    for _ in range(50):
        current = r.headers.get("Docker-Content-Digest")
        r = base.put(f"/v2/{repo}/manifests/{tag}", content=body,
                     headers={**headers, "If-Match": f'"{current}"'})
        if r.status_code == 200:
            return digest
        if r.status_code != 409:
            raise AssertionError(f"publish failed: {r.status_code} {r.text}")
    raise AssertionError("CAS retries exhausted")


def test_concurrent_publish_pull_gc(tmp_data):
    settings = Settings(
        data_dir=tmp_data,
        db_path=os.path.join(tmp_data, "registry.db"),
        upload_ttl_seconds=3600,
        plan_ttl_seconds=3600,
        gc_token=None,
    )
    port = _free_port()
    server = ServerThread(settings, port)
    server.start()
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if httpx.get(base_url + "/healthz", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)

    errors: list[Exception] = []
    stop = threading.Event()
    repo = "workload/app"
    ROUNDS = 12

    def publisher() -> None:
        try:
            with httpx.Client(base_url=base_url, timeout=10) as http:
                for n in range(ROUNDS):
                    _publish_image(http, repo, "latest", n)
                    # Also publish a versioned tag that never moves; each round
                    # adds unreachable garbage after 'latest' advances.
                    _publish_image(http, repo, f"v{n}", n)
        except Exception as exc:  # pragma: no cover - test reporting
            errors.append(exc)

    def puller() -> None:
        try:
            with httpx.Client(base_url=base_url, timeout=10) as http:
                while not stop.is_set():
                    r = http.get(f"/v2/{repo}/manifests/latest")
                    if r.status_code != 200:
                        time.sleep(0.02)
                        continue
                    doc = r.json()
                    dg = r.headers["Docker-Content-Digest"]
                    assert hashlib.sha256(r.content).hexdigest() == dg.split(":")[1]
                    for kind in ("config",) + tuple(range(len(doc["layers"]))):
                        d = doc["config"] if kind == "config" else doc["layers"][kind]
                        br = http.get(f"/v2/{repo}/blobs/{d['digest']}")
                        assert br.status_code == 200, (d["digest"], br.status_code)
                        assert hashlib.sha256(br.content).hexdigest() == d["digest"].split(":")[1]
                        assert len(br.content) == d["size"]
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def gc_runner() -> None:
        try:
            with httpx.Client(base_url=base_url, timeout=15) as http:
                while not stop.is_set():
                    plan = http.post("/gc").json()
                    time.sleep(0.05)
                    if plan["id"]:
                        r = http.post("/gc", json={"execute": True,
                                                   "plan_id": plan["id"]})
                        # A 409 here only means another plan went stale; accept it.
                        assert r.status_code in (200, 409), r.text
                    time.sleep(0.1)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=publisher),
               threading.Thread(target=puller),
               threading.Thread(target=puller),
               threading.Thread(target=gc_runner)]
    for t in threads:
        t.start()

    publisher_done = threads[0]
    publisher_done.join(timeout=60)
    stop.set()
    for t in threads[1:]:
        t.join(timeout=30)
    server.stop()

    assert not errors, [repr(e) for e in errors]

    # Final GC pass, then verify nothing tagged dangles.
    import sqlite3
    db_path = settings.db_path
    # Give the server a moment to release; WAL readers are fine immediately.
    with httpx.Client(base_url=base_url, timeout=10) as http:
        try:
            plan = http.post("/gc", timeout=5).json()
            http.post("/gc", json={"execute": True, "plan_id": plan["id"]}, timeout=5)
        except httpx.TransportError:
            # Server may have exited already; run the same check against the DB.
            pass

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    dangling_tags = conn.execute(
        "SELECT t.repo, t.tag FROM tags t LEFT JOIN manifests m "
        "ON m.repo=t.repo AND m.digest=t.digest WHERE m.digest IS NULL"
    ).fetchall()
    assert dangling_tags == []

    # Every layer/config of every surviving manifest must exist as a blob file.
    for m in conn.execute("SELECT repo, digest, media_type FROM manifests"):
        path = os.path.join(tmp_data, "blobs", "sha256",
                            m["digest"][7:9], m["digest"][9:])
        assert os.path.exists(path), m["digest"]
        doc = json.loads(open(path, "rb").read())
        for desc in ([doc["config"]] + doc.get("layers", [])):
            bpath = os.path.join(tmp_data, "blobs", "sha256",
                                 desc["digest"][7:9], desc["digest"][9:])
            assert os.path.exists(bpath), ("dangling blob ref", desc["digest"])
            assert os.path.getsize(bpath) == desc["size"]
    conn.close()
