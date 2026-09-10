import json
import sys

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path

from packaging.version import Version

version = sys.argv[1]
for name in ("client", "react", "server"):
    assert json.loads(Path(f"packages/{name}/package.json").read_text())["version"] == version
for name in ("mfup-core", "mfup-fastapi"):
    data = tomllib.loads(Path(f"server/{name}/pyproject.toml").read_text())
    assert Version(data["project"]["version"]) == Version(version)
print("All five package versions match the tag.")
