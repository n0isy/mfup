"""Verify the released versions and default npm tags from the public registries."""

import json
import sys
import time
import urllib.request

version = sys.argv[1]
packages = [
    (name, f"https://registry.npmjs.org/{name}")
    for name in ("@mfup/client", "@mfup/react", "@mfup/server")
]
packages += [
    (name, f"https://pypi.org/pypi/{name}/{version}/json") for name in ("mfup-core", "mfup-fastapi")
]
for name, url in packages:
    for attempt in range(12):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = json.load(response)
            if name.startswith("@"):
                assert version in data["versions"]
                assert data["dist-tags"]["latest"] == version
            else:
                assert data["info"]["version"] == version
                assert {item["packagetype"] for item in data["urls"]} == {"sdist", "bdist_wheel"}
            print(f"Verified {name} {version}")
            break
        except Exception:
            if attempt == 11:
                raise
            time.sleep(10)
