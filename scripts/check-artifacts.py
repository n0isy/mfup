"""Validate the exact npm and Python distribution contents before publication."""

import json
import re
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

CYRILLIC = re.compile(r"[\u0400-\u052f\u2de0-\u2dff\ua640-\ua69f]")
CREDENTIAL = re.compile(
    rb"(?:gh[pousr]_|github_pat_|npm_)[A-Za-z0-9_]{30,}|-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----"
)
EXPECTED_NPM = {"@mfup/client", "@mfup/react", "@mfup/server"}
EXPECTED_PYTHON = {"mfup_core", "mfup_fastapi"}


def members(archive):
    if archive.suffix == ".whl":
        with zipfile.ZipFile(archive) as source:
            return {
                item.filename: source.read(item) for item in source.infolist() if not item.is_dir()
            }
    with tarfile.open(archive, "r:gz") as source:
        result = {}
        for item in source.getmembers():
            assert item.isfile() or item.isdir(), f"Unexpected archive entry: {item.name}"
            if item.isfile():
                result[item.name] = source.extractfile(item).read()
        return result


def validate(archive):
    files = members(archive)
    assert files, f"Empty archive: {archive}"
    npm = archive.suffix == ".tgz"
    wheel = archive.suffix == ".whl"
    for name, content in files.items():
        p = PurePosixPath(name)
        assert not p.is_absolute() and ".." not in p.parts, f"Invalid member: {name}"
        parts = p.parts if wheel else p.parts[1:]
        rel = PurePosixPath(*parts)
        if npm:
            allowed = str(rel) in {"package.json", "README.md", "README_ru.md", "LICENSE"} or (
                len(parts) == 2
                and parts[0] == "dist"
                and (rel.suffix == ".js" or str(rel).endswith(".d.ts"))
            )
        else:
            allowed = (
                (parts[0] in EXPECTED_PYTHON and (rel.suffix == ".py" or rel.name == "py.typed"))
                or (
                    not wheel
                    and str(rel)
                    in {"pyproject.toml", "README.md", "README_ru.md", "LICENSE", "PKG-INFO"}
                )
                or (
                    wheel
                    and parts[0].endswith(".dist-info")
                    and (
                        rel.name in {"METADATA", "WHEEL", "RECORD", "entry_points.txt"}
                        or parts[1:] == ("licenses", "LICENSE")
                    )
                )
            )
        assert allowed, f"Unexpected distribution file: {archive.name}: {name}"
        assert not CREDENTIAL.search(content), f"Credential-like content: {archive.name}: {name}"
        text = content.decode("utf-8")
        assert rel.name == "README_ru.md" or not CYRILLIC.search(text), (
            f"Non-English distribution content: {name}"
        )
    if npm:
        package = json.loads(files["package/package.json"])
        assert package["name"] in EXPECTED_NPM and package["version"] == "3.0.0"
        assert package["repository"]["url"] == "git+https://github.com/n0isy/mfup.git"
        assert package["main"].removeprefix("./") in {
            str(PurePosixPath(*PurePosixPath(name).parts[1:])) for name in files
        }
    print(
        f"{archive.name}: {len(files)} allowed files, {sum(map(len, files.values()))} unpacked bytes"
    )


archives = [
    p
    for directory in sys.argv[1:]
    for p in Path(directory).iterdir()
    if p.name.endswith((".tgz", ".whl", ".tar.gz"))
]
assert archives, "No distributions found"
for archive in sorted(archives):
    validate(archive)
