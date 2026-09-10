import os

import uvicorn

from . import MfupConfig, create_app


def main():
    app = create_app(MfupConfig.from_env())
    uvicorn.run(
        app,
        host=os.environ.get("MFUP_HOST", "127.0.0.1"),
        port=int(os.environ.get("MFUP_PORT", "3001")),
    )


if __name__ == "__main__":
    main()
