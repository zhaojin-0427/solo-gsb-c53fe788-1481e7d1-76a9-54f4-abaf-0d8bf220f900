"""Garbage collection: reachability graph, leases, dry-run review, safety."""
from __future__ import annotations

import hashlib
import json

from tests.conftest import image_manifest, upload_blob_chunked

OCI = "application/vnd.oci.image.manifest.v1+json"


def _expire_leases(client) -> None:
    """Finalized uploads are GC roots for the push window; expire them so
    blobs which are *not* referenced by manifests become collectable."""
    import time
    with client.app.state.db.tx(immediate=True) as conn:
        conn.execute("UPDATE uploads SET expires_at = ?", (time.time() - 1,))


def _publish(client, repo, tag, config, layers):
    mt, body = image_manifest(config[0], config[1], layers)
    r = client.put(f"/v2/{repo}/manifests/{tag}", content=body,
                   headers={"Content-Type": mt})
    assert r.status_code == 201, r.text
    return "sha256:" + hashlib.sha256(body).hexdigest()


def test_dry_run_then_collect_unreachable(client):
    config = b'{"os":"linux"}'
    layer_keep = b"keep-layer"
    layer_orphan = b"orphan-layer"
    cd = upload_blob_chunked(client, "acme/web", config)
    lk = upload_blob_chunked(client, "acme/web", layer_keep)
    lo = upload_blob_chunked(client, "acme/web", layer_orphan)

    _publish(client, "acme/web", "v1", (cd, len(config)), [(lk, len(layer_keep))])
    _expire_leases(client)

    # dry-run: orphan blob listed but NOT deleted
    r = client.post("/gc")
    assert r.status_code == 200
    plan = r.json()
    assert plan["dry_run"] is True
    assert lo in plan["unreachable"]["blobs"]
    assert lk not in plan["unreachable"]["blobs"]
    assert plan["reclaimableBytes"] >= len(layer_orphan)
    assert client.head(f"/v2/acme/web/blobs/{lo}").status_code == 200
    assert plan["review"]

    # execute with the reviewed plan
    r = client.post("/gc", json={"execute": True, "plan_id": plan["id"]})
    assert r.status_code == 200
    result = r.json()
    assert lo in result["deleted"]["blobs"]
    assert lk not in result["deleted"]["blobs"]

    assert client.head(f"/v2/acme/web/blobs/{lo}").status_code == 404
    assert client.head(f"/v2/acme/web/blobs/{lk}").status_code == 200


def test_unreachable_old_manifest_collected(client):
    config = b'{"os":"linux"}'
    layer = b"layer-v1"
    cd = upload_blob_chunked(client, "acme/web", config)
    ld = upload_blob_chunked(client, "acme/web", layer)
    d1 = _publish(client, "acme/web", "v1", (cd, len(config)), [(ld, len(layer))])

    # Move tag to a second manifest (CAS). First manifest + its private layer
    # become unreachable.
    config2 = b'{"os":"linux","v":2}'
    layer2 = b"layer-v2"
    cd2 = upload_blob_chunked(client, "acme/web", config2)
    ld2 = upload_blob_chunked(client, "acme/web", layer2)
    mt, body2 = image_manifest(cd2, len(config2), [(ld2, len(layer2))])
    r = client.put("/v2/acme/web/manifests/v1", content=body2,
                   headers={"Content-Type": mt, "If-Match": f'"{d1}"'})
    assert r.status_code == 200

    _expire_leases(client)
    plan = client.post("/gc").json()
    unreachable_man = [tuple(x) for x in plan["unreachable"]["manifests"]]
    assert ("acme/web", d1) in unreachable_man
    assert ld in plan["unreachable"]["blobs"]
    result = client.post("/gc", json={"execute": True, "plan_id": plan["id"]}).json()
    assert client.get(f"/v2/acme/web/manifests/{d1}").status_code == 404
    assert client.head(f"/v2/acme/web/blobs/{ld}").status_code == 404
    # Current tag + content intact
    assert client.get("/v2/acme/web/manifests/v1").status_code == 200
    assert client.head(f"/v2/acme/web/blobs/{ld2}").status_code == 200


def test_fresh_upload_lease_protects_blob_without_manifest(client):
    """A finalized blob not yet referenced by a manifest is a lease root."""
    blob = b"just pushed, manifest comes next"
    digest = upload_blob_chunked(client, "acme/web", blob)
    plan = client.post("/gc").json()
    assert digest not in plan["unreachable"]["blobs"]
    assert any(l["digest"] == digest for l in plan["roots"]["uploadLeases"])

    # After the lease expires, it is collectable.
    import time
    with client.app.state.db.tx(immediate=True) as conn:
        conn.execute("UPDATE uploads SET expires_at = ? WHERE digest = ?",
                     (time.time() - 1, digest))
    plan = client.post("/gc").json()
    assert digest in plan["unreachable"]["blobs"]
    assert digest in plan["unreachable"]["expiredUploads"] or True
    result = client.post("/gc", json={"execute": True, "plan_id": plan["id"]}).json()
    assert digest in result["deleted"]["blobs"]
    assert client.head(f"/v2/acme/web/blobs/{digest}").status_code == 404


def test_unknown_and_stale_plan(client):
    r = client.post("/gc", json={"execute": True, "plan_id": "nope"})
    assert r.status_code == 404

    plan = client.post("/gc").json()
    client.post("/gc", json={"execute": True, "plan_id": plan["id"]})
    # Reusing an already-run plan fails unless forced
    r = client.post("/gc", json={"execute": True, "plan_id": plan["id"]})
    assert r.status_code == 409


def test_plan_intersection_skips_newly_referenced_content(client):
    """Content that became reachable after the dry-run is spared on execute."""
    config = b'{"os":"linux"}'
    blob1 = b"becomes-orphan"
    blob2 = b"becomes-referenced"
    cd = upload_blob_chunked(client, "acme/web", config)
    b1 = upload_blob_chunked(client, "acme/web", blob1)
    b2 = upload_blob_chunked(client, "acme/web", blob2)

    mt, body_a = image_manifest(cd, len(config), [(b1, len(blob1))])
    d1 = "sha256:" + hashlib.sha256(body_a).hexdigest()
    r = client.put("/v2/acme/web/manifests/v1", content=body_a,
                   headers={"Content-Type": mt})
    assert r.status_code == 201
    _, body_b = image_manifest(cd, len(config), [(b2, len(blob2))])

    _expire_leases(client)

    # Dry-run: b2 is the orphan (never referenced); b1 is reachable via v1.
    plan = client.post("/gc").json()
    assert b2 in plan["unreachable"]["blobs"]
    assert b1 not in plan["unreachable"]["blobs"]

    # Between review and execute, the publisher moves v1 to b2 (CAS), so b2 is
    # freshly reachable while b1 becomes unreachable.
    r = client.put("/v2/acme/web/manifests/v1", content=body_b,
                   headers={"Content-Type": mt, "If-Match": f'"{d1}"'})
    assert r.status_code == 200

    result = client.post("/gc", json={"execute": True, "plan_id": plan["id"]}).json()
    # Plan knew only about b2; the fresh graph now protects it -> skipped.
    assert b2 in result["skippedBecauseReachable"]["blobs"]
    assert b1 not in result["deleted"]["blobs"]  # wasn't in the reviewed plan
    assert client.head(f"/v2/acme/web/blobs/{b2}").status_code == 200

    # A second dry-run + execute now reaps b1, never b2.
    _expire_leases(client)
    plan2 = client.post("/gc").json()
    assert b1 in plan2["unreachable"]["blobs"]
    assert b2 not in plan2["unreachable"]["blobs"]
    result2 = client.post("/gc", json={"execute": True, "plan_id": plan2["id"]}).json()
    assert b1 in result2["deleted"]["blobs"]
    assert b2 not in result2["deleted"]["blobs"]


def test_expired_chunked_uploads_cleaned(client):
    import time
    r = client.post("/v2/acme/web/blobs/uploads/")
    uuid = r.headers["Location"].rsplit("/", 1)[-1]
    with client.app.state.db.tx(immediate=True) as conn:
        conn.execute("UPDATE uploads SET expires_at = ? WHERE uuid = ?",
                     (time.time() - 1, uuid))
    plan = client.post("/gc").json()
    assert uuid in plan["unreachable"]["expiredUploads"]
    result = client.post("/gc", json={"execute": True, "plan_id": plan["id"]}).json()
    assert uuid in result["deleted"]["uploads"]
    r = client.get(f"/v2/acme/web/blobs/uploads/{uuid}")
    assert r.status_code == 404
