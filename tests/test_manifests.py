"""Manifest publishing: reference validation, immutability, tag CAS."""
from __future__ import annotations

import hashlib
import json

from tests.conftest import image_manifest, upload_blob_chunked

OCI = "application/vnd.oci.image.manifest.v1+json"


def _push_config_and_layer(client, repo: str):
    config = b'{"architecture":"amd64","os":"linux"}'
    layer = b"\x1f\x8b layer content"
    cd = upload_blob_chunked(client, repo, config)
    ld = upload_blob_chunked(client, repo, layer)
    return (cd, len(config)), [(ld, len(layer))]


def test_publish_manifest_and_fetch_by_tag_and_digest(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    mt, body = image_manifest(cd, cs, layers)
    digest = "sha256:" + hashlib.sha256(body).hexdigest()

    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": mt})
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == digest

    r = client.get("/v2/acme/web/manifests/v1")
    assert r.status_code == 200
    assert r.headers["Content-Type"] == mt
    assert r.content == body

    r = client.head(f"/v2/acme/web/manifests/{digest}")
    assert r.status_code == 200
    assert int(r.headers["Content-Length"]) == len(body)

    assert client.get("/v2/acme/web/tags/list").json()["tags"] == ["v1"]


def test_missing_layer_reference_rejected(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    fake_layer = "sha256:" + "c" * 64
    _, body = image_manifest(cd, cs, [(fake_layer, 99)])
    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": OCI})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "BLOB_UNKNOWN"


def test_wrong_descriptor_size_rejected(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    _, body = image_manifest(cd, cs + 1, layers)
    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": OCI})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "MANIFEST_INVALID"


def test_digest_publish_requires_content_match(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    _, body = image_manifest(cd, cs, layers)
    wrong = "sha256:" + "d" * 64
    r = client.put(f"/v2/acme/web/manifests/{wrong}", content=body,
                   headers={"Content-Type": OCI})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"


def test_immutable_digest_content_and_tag_cas(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    mt, body1 = image_manifest(cd, cs, layers)
    d1 = "sha256:" + hashlib.sha256(body1).hexdigest()
    client.put("/v2/acme/web/manifests/v1", content=body1,
               headers={"Content-Type": mt})

    # Same content PUT by digest again is idempotent (content immutable).
    r = client.put(f"/v2/acme/web/manifests/{d1}", content=body1,
                   headers={"Content-Type": mt})
    assert r.status_code in (200, 201)
    # Repeating the very same digest PUT now hits the existing row -> 200.
    r = client.put(f"/v2/acme/web/manifests/{d1}", content=body1,
                   headers={"Content-Type": mt})
    assert r.status_code == 200

    # Tag move without If-Match -> 409 with current digest
    other_config = b'{"architecture":"arm64","os":"linux"}'
    od = upload_blob_chunked(client, "acme/web", other_config)
    mt2, body2 = image_manifest(od, len(other_config), layers)
    d2 = "sha256:" + hashlib.sha256(body2).hexdigest()

    r = client.put("/v2/acme/web/manifests/v1", content=body2,
                   headers={"Content-Type": mt2})
    assert r.status_code == 409
    err = r.json()["errors"][0]
    assert err["code"] == "TAG_CONFLICT"
    assert err["detail"]["CurrentDigest"] == d1
    assert r.headers["Docker-Content-Digest"] == d1

    # Stale If-Match -> 409
    r = client.put("/v2/acme/web/manifests/v1", content=body2,
                   headers={"Content-Type": mt2, "If-Match": f'"sha256:{"e"*64}"'})
    assert r.status_code == 409
    assert r.json()["errors"][0]["detail"]["CurrentDigest"] == d1

    # Correct If-Match -> tag moves
    r = client.put("/v2/acme/web/manifests/v1", content=body2,
                   headers={"Content-Type": mt2, "If-Match": f'"{d1}"'})
    assert r.status_code == 200
    assert r.headers["Docker-Content-Digest"] == d2

    assert client.get("/v2/acme/web/manifests/v1").content == body2
    # Old digest content remains retrievable (immutable)
    assert client.get(f"/v2/acme/web/manifests/{d1}").status_code == 200


def test_if_none_match_star(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    mt, body = image_manifest(cd, cs, layers)
    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": mt, "If-None-Match": "*"})
    assert r.status_code == 201
    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": mt, "If-None-Match": "*"})
    assert r.status_code == 409


def test_unsupported_media_type(client):
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    _, body = image_manifest(cd, cs, layers)
    r = client.put("/v2/acme/web/manifests/v1", content=body,
                   headers={"Content-Type": "text/plain"})
    assert r.status_code == 415


def test_index_manifest_child_validation(client):
    """Image index children must be manifests in the same repo."""
    (cd, cs), layers = _push_config_and_layer(client, "acme/web")
    mt, child = image_manifest(cd, cs, layers)
    child_digest = "sha256:" + hashlib.sha256(child).hexdigest()
    client.put(f"/v2/acme/web/manifests/{child_digest}", content=child,
               headers={"Content-Type": mt})

    index = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": mt, "size": len(child), "digest": child_digest,
             "platform": {"architecture": "amd64", "os": "linux"}},
        ],
    }
    body = json.dumps(index).encode()
    r = client.put("/v2/acme/web/manifests/latest", content=body,
                   headers={"Content-Type": index["mediaType"]})
    assert r.status_code == 201, r.text

    # Unknown child
    index["manifests"][0]["digest"] = "sha256:" + "f" * 64
    body = json.dumps(index).encode()
    r = client.put("/v2/acme/web/manifests/broken", content=body,
                   headers={"Content-Type": index["mediaType"]})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "MANIFEST_UNKNOWN"
