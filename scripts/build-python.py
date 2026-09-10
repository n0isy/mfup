"""Build Python distributions from only the package inputs, outside the checkout."""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
output = Path(sys.argv[1]).resolve()
output.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix="mfup-python-build-") as temporary:
    for name in ("core", "fastapi"):
        source = root / "server" / f"mfup-{name}"
        project = Path(temporary) / f"mfup-{name}"
        project.mkdir()
        for filename in ("pyproject.toml", "README.md", "README_ru.md", "LICENSE"):
            shutil.copyfile(source / filename, project / filename)
        shutil.copytree(
            source / f"mfup_{name}",
            project / f"mfup_{name}",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        subprocess.run(
            [sys.executable, "-m", "build", str(project), "--outdir", str(output)],
            check=True,
        )
