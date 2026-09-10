import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

source = Path(__file__).resolve().parents[2] / "scripts/retry-command.py"
spec = importlib.util.spec_from_file_location("retry_command", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def clock(monkeypatch):
    state = SimpleNamespace(now=0, sleeps=[])

    def sleep(seconds):
        state.sleeps.append(seconds)
        state.now += seconds

    monkeypatch.setattr(module.time, "monotonic", lambda: state.now)
    monkeypatch.setattr(module.time, "sleep", sleep)
    return state


@pytest.mark.parametrize("exits", [[0], [1, 1, 0]])
def test_installation_stops_as_soon_as_it_succeeds(monkeypatch, clock, exits):
    command = ["installer", "package==3.1.0", "literal;argument"]
    calls = []

    def run(args, *, check, timeout):
        assert args == command and check is False
        calls.append(timeout)
        clock.now += 1
        return SimpleNamespace(returncode=exits[len(calls) - 1])

    monkeypatch.setattr(module.subprocess, "run", run)
    assert module.retry(command) == 0
    assert len(calls) == len(exits)
    assert clock.sleeps == [10] * (len(exits) - 1)


def test_failed_attempts_and_waits_share_one_deadline(monkeypatch, clock):
    budgets = []

    def run(args, *, check, timeout):
        budgets.append(timeout)
        clock.now += 15
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(module.subprocess, "run", run)
    assert module.retry(["installer"]) == 7
    assert clock.now == 120
    assert budgets == [120, 95, 70, 45, 20]
    assert clock.sleeps == [10, 10, 10, 10, 5]


def test_a_stalled_installation_cannot_exceed_the_deadline(monkeypatch, clock):
    def run(args, *, check, timeout):
        assert timeout == 120
        raise subprocess.TimeoutExpired(args, timeout)

    monkeypatch.setattr(module.subprocess, "run", run)
    assert module.retry(["installer"]) == 124
    assert clock.sleeps == []
