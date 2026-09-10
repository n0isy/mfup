"""Select artifacts only from a successful build of an unchanged release tag."""

import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path


def validate_release(run, jobs, artifacts, tag_sha):
    assert run["event"] == "push", "Artifacts must originate from a tag push"
    assert run["path"].split("@")[0] == ".github/workflows/release.yml", "Unexpected workflow"
    tag = run["head_branch"]
    assert re.fullmatch(r"v3\.\d+\.\d+(?:-[A-Za-z0-9.]+)?", tag), "Expected a version 3 tag"
    assert run["head_sha"] == tag_sha, "The release tag does not match the artifact source"
    assert any(j["name"] == "build" and j["conclusion"] == "success" for j in jobs), (
        "Build is not successful"
    )
    checks = [j for j in jobs if j["name"].startswith("checks / ")]
    assert checks and all(j["conclusion"] == "success" for j in checks), (
        "Release checks are not successful"
    )
    names = {a["name"] for a in artifacts if not a["expired"]}
    assert {"npm", "python"} <= names, "Release artifacts are missing or expired"
    return tag[1:]


def main():
    def emit(**values):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            for key, value in values.items():
                output.write(f"{key}={value}\n")

    previous = os.environ.get("ARTIFACT_RUN_ID", "")
    if not previous and os.environ["GITHUB_REF_TYPE"] != "tag":
        emit(publish="false")
        return
    run_id = previous or os.environ["GITHUB_RUN_ID"]
    assert run_id.isdecimal(), "Invalid artifact run ID"
    repository = os.environ["GITHUB_REPOSITORY"]

    def api(path):
        request = urllib.request.Request(
            f"https://api.github.com/repos/{repository}/{path}",
            headers={
                "Authorization": "Bearer " + os.environ["GH_TOKEN"],
                "Accept": "application/vnd.github+json",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def items(path, key):
        result = []
        page = 1
        while True:
            separator = "&" if "?" in path else "?"
            current = api(f"{path}{separator}per_page=100&page={page}")[key]
            result.extend(current)
            if len(current) < 100:
                return result
            page += 1

    run = api(f"actions/runs/{run_id}")
    tag = run["head_branch"]
    assert re.fullmatch(r"v3\.\d+\.\d+(?:-[A-Za-z0-9.]+)?", tag), "Expected a version 3 tag"
    ref = api("git/ref/tags/" + urllib.parse.quote(tag, safe=""))["object"]
    for _ in range(5):
        if ref["type"] == "commit":
            break
        assert ref["type"] == "tag", "Invalid release tag"
        ref = api("git/tags/" + ref["sha"])["object"]
    assert ref["type"] == "commit", "Cannot resolve release commit"
    version = validate_release(
        run,
        items(f"actions/runs/{run_id}/jobs?filter=latest", "jobs"),
        items(f"actions/runs/{run_id}/artifacts", "artifacts"),
        ref["sha"],
    )
    emit(publish="true", run_id=run_id, version=version, tag=tag)
    print(f"Verified artifacts from run {run_id}, tag {tag}, commit {ref['sha']}")


if __name__ == "__main__":
    main()
