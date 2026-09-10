"""Run after installing the built wheels and httpx into the current interpreter."""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="mfup3-wheel-check-") as tmp:
    target = Path(tmp)
    shutil.copyfile(root / "scripts/consumer-python-check.py", target / "check.py")
    shutil.copyfile(root / "examples/multiuser-scopes/server/app.py", target / "example_app.py")
    subprocess.run([sys.executable, "-I", str(target / "check.py")], cwd=target, check=True)
