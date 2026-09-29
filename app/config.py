"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    data_dir: str
    db_path: str
    # Uploads which have not received a chunk/finalize within this window are
    # garbage collected and their leases no longer count as GC roots.
    upload_ttl_seconds: int
    # A dry-run plan must be confirmed within this window; on execute the
    # reachability graph is recomputed and intersected with the plan.
    plan_ttl_seconds: int
    # When set, POST /gc requires `Authorization: Bearer <token>`.
    gc_token: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = os.environ.get("REGISTRY_DATA_DIR")
        if not data_dir:
            # Prefer /data (container mount); fall back to ./data locally when
            # /data is not writable.
            if os.path.isdir("/data") and os.access("/data", os.W_OK):
                data_dir = "/data"
            else:
                data_dir = os.path.join(os.getcwd(), "data")
        return cls(
            data_dir=data_dir,
            db_path=os.environ.get("REGISTRY_DB_PATH", os.path.join(data_dir, "registry.db")),
            upload_ttl_seconds=_env_int("REGISTRY_UPLOAD_TTL_SECONDS", 24 * 3600),
            plan_ttl_seconds=_env_int("REGISTRY_GC_PLAN_TTL_SECONDS", 2 * 3600),
            gc_token=os.environ.get("REGISTRY_GC_TOKEN") or None,
        )
