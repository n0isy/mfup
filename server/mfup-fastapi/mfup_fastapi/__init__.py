"""mfup-fastapi — FastAPI integration for the MFUP/2 upload engine.

    from mfup_fastapi import MfupConfig, MfupEngine, create_app
"""

from .config import MfupConfig
from .engine import (
    MapFileHookError,
    MfupEngine,
    NotCommitted,
    PublishError,
    SessionNotFound,
    TargetEscapes,
    reconcile_orphans,
)


def create_app(config: "MfupConfig | None" = None):
    """Lazy re-export: builds the standalone app without importing
    mfup_fastapi.app (which reads the environment at import time)."""
    from fastapi import FastAPI

    engine = MfupEngine(config or MfupConfig.from_env())
    app = FastAPI(title="MFUP/2 Server", lifespan=engine.lifespan)
    app.include_router(engine.router)
    app.state.mfup_engine = engine
    return app


__all__ = [
    "create_app",
    "MapFileHookError",
    "MfupConfig",
    "MfupEngine",
    "NotCommitted",
    "PublishError",
    "reconcile_orphans",
    "SessionNotFound",
    "TargetEscapes",
]
