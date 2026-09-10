import importlib.util
from pathlib import Path

import pytest

source = Path(__file__).resolve().parents[2] / "scripts/release-source.py"
spec = importlib.util.spec_from_file_location("release_source", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def inputs():
    return (
        dict(
            event="push",
            path=".github/workflows/release.yml",
            head_branch="v3.0.0",
            head_sha="release-commit",
            conclusion="failure",
        ),
        [
            dict(name="checks / unit", conclusion="success"),
            dict(name="checks / browsers", conclusion="success"),
            dict(name="build", conclusion="success"),
        ],
        [dict(name="npm", expired=False), dict(name="python", expired=False)],
    )


def test_completed_build_can_resume_after_publisher_failure():
    run, jobs, artifacts = inputs()
    assert module.validate_release(run, jobs, artifacts, "release-commit") == "3.0.0"


@pytest.mark.parametrize(
    "case", ["branch", "workflow", "event", "tag", "build", "checks", "no_checks", "artifacts"]
)
def test_unverified_artifacts_cannot_be_published(case):
    run, jobs, artifacts = inputs()
    commit = "release-commit"
    if case == "branch":
        run["head_branch"] = "main"
    elif case == "workflow":
        run["path"] = ".github/workflows/ci.yml"
    elif case == "event":
        run["event"] = "pull_request"
    elif case == "tag":
        commit = "other-commit"
    elif case == "build":
        jobs[-1]["conclusion"] = "failure"
    elif case == "checks":
        jobs[0]["conclusion"] = "failure"
    elif case == "no_checks":
        jobs = jobs[-1:]
    else:
        artifacts[0]["expired"] = True
    with pytest.raises(AssertionError):
        module.validate_release(run, jobs, artifacts, commit)
