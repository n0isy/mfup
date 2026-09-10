import os

from mfup_fastapi import create_app

limits = None
if os.getenv("MFUP_PART_BYTES"):
    limits = dict(
        partBytes=int(os.environ["MFUP_PART_BYTES"]),
        batchBytes=int(os.getenv("MFUP_BATCH_BYTES", "33554432")),
        maxParts=int(os.getenv("MFUP_MAX_PARTS", "128")),
    )
app = create_app(os.getenv("MFUP_DATA_DIR", "data"), authorize=lambda request: {}, limits=limits)
