"""Run the standalone MFUP/2 server: python -m mfup_fastapi"""

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "mfup_fastapi.app:app",
        host=os.environ.get("MFUP_HOST", "0.0.0.0"),
        port=int(os.environ.get("MFUP_PORT", "8070")),
        log_level="info",
    )
