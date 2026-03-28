"""Run the MFUP/2 server: python -m mfup"""

import logging
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

if __name__ == "__main__":
    uvicorn.run("mfup.app:app", host="0.0.0.0", port=8070, log_level="info")
