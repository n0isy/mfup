from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from mfup_core.contracts import Authorize, MapFile, OnCommitted


def resolve_hook(value):
    if value is None or callable(value):
        return value
    module, name = value.rsplit(":", 1)
    hook = getattr(importlib.import_module(module), name)
    if not callable(hook):
        raise ValueError(f"Hook is not callable: {value}")
    return hook


@dataclass
class MfupConfig:
    base_dir: str | Path
    authorize: Authorize | str
    map_file: MapFile | str | None = None
    on_committed: OnCommitted | str | None = None
    limits: dict[str, int] | None = None
    ttl_ms: int = 86400000
    sweep_interval_ms: int = 60000
    max_meta_bytes: int = 16384
    max_context_bytes: int = 65536
    auto_publish: bool = False
    client_publish: bool = True
    prefix: str = ""
    on_error: Any = None

    def engine_options(self):
        self.prefix = self.prefix.rstrip("/")
        if self.prefix and not re.fullmatch(r"/(?:[A-Za-z0-9_-]+/?)+", self.prefix):
            raise ValueError("Invalid prefix")
        result = {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name not in ("prefix", "sweep_interval_ms")
        }
        for name in ("authorize", "map_file", "on_committed"):
            result[name] = resolve_hook(result[name])
        if not callable(result["authorize"]):
            raise ValueError("MFUP_AUTHORIZE is required")
        if type(self.sweep_interval_ms) is not int or self.sweep_interval_ms < 0:
            raise ValueError("Invalid sweep_interval_ms")
        return result

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        values = dict(
            base_dir=env.get("MFUP_BASE_DIR", "./data"), authorize=env.get("MFUP_AUTHORIZE", "")
        )
        for name in ("map_file", "on_committed", "prefix"):
            if "MFUP_" + name.upper() in env:
                values[name] = env["MFUP_" + name.upper()]
        for name in ("ttl_ms", "sweep_interval_ms", "max_meta_bytes", "max_context_bytes"):
            if "MFUP_" + name.upper() in env:
                values[name] = int(env["MFUP_" + name.upper()])
        for name in ("auto_publish", "client_publish"):
            raw = env.get("MFUP_" + name.upper())
            if raw is not None:
                if raw.lower() not in ("true", "false", "1", "0"):
                    raise ValueError(f"Invalid boolean: {name}")
                values[name] = raw.lower() in ("true", "1")
        limits = {}
        for field, variable in dict(
            concurrency="CONCURRENCY",
            maxParts="MAX_PARTS",
            batchBytes="BATCH_BYTES",
            partBytes="PART_BYTES",
        ).items():
            if "MFUP_" + variable in env:
                limits[field] = int(env["MFUP_" + variable])
        if limits:
            values["limits"] = limits
        return cls(**values)
