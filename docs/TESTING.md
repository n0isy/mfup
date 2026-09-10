# Testing

The repository uses the same HTTP contract and browser scenarios for Node and Python. CI runs on Linux, Windows and macOS; the source of record is [.github/workflows/ci.yml](../.github/workflows/ci.yml).

## Local commands

Use Node 22.13+ and Python 3.10+:

```bash
npm ci --no-audit --no-fund
npm run build
npm run check
npm test
python -m pip install ./server/mfup-core './server/mfup-fastapi[dev]'
python -m pytest server/tests -q
npm run pack:check
npx playwright install --with-deps chromium firefox webkit chrome msedge
```

Linux/macOS:

```bash
MFUP_EXTENDED=1 npm run test:e2e
MFUP_EXTENDED=1 MFUP_BACKEND=python PYTHON_BIN=python npm run test:e2e
MFUP_EXTENDED=1 npm run test:examples
MFUP_EXTENDED=1 MFUP_BACKEND=python PYTHON_BIN=python npm run test:examples
```

On PowerShell, set environment variables separately, for example `$env:MFUP_EXTENDED = '1'` and `$env:MFUP_BACKEND = 'python'`, then run the same npm commands.

Protocol tests use local ports 20063–20065; example tests use 20067–20068. Supervisors start and stop the required backends. Data is under ignored `.tmp/` directories. PYTHON_BIN selects the Python interpreter. Without MFUP_EXTENDED, projects are Chromium, Firefox and WebKit; with it they also include installed Chrome/Edge, Pixel 7 and iPhone 13 emulation.

## Coverage

- Bounded incremental enumeration, empty directories, grouped files and six active requests.
- Native File/Blob payloads without application arrayBuffer reads; exact published bytes.
- Long POST upload events before receipts, pause/resume and confirmation counters.
- Truncated requests, lost receipts, retransmission decisions, epochs and range validation.
- Restart, retained metadata, mapping plans and partial publication recovery.
- One overwrite permission, approval/cancellation during and after reception, concurrent cancellation and late responses.
- Application cookies, own-file listing/download, roots and extension hooks through installed packages.
- Quotas, path/case collisions, coordinated sweeping and a single database owner.
- Processing failure, explicit retry, server/client publication policy and callback cancellation.

Real filesystem cases use Linux Docker: `npm run test:storage`. Unprivileged containers exercise a full 4 MiB tmpfs, a read-only mount and denied write access. Tests restore space and verify resumed content. SQLite FULL is exercised with a page limit. EIO handling uses an injected file-operation failure. Hardware faults, power loss and physical mobile devices are not simulated by these checks.

Playwright Firefox is an automation build. WebKit testing and iPhone emulation do not establish behavior in installed Safari or on a physical iPhone. See [Playwright browsers](https://playwright.dev/docs/browsers).

## Distribution checks

`npm run pack:check` builds tarballs, installs them into a temporary project, checks archive contents and runs a copied consumer example. Python wheels are installed before running `python scripts/pack-python-check.py`; wheel and sdist metadata is checked with twine. `scripts/check-artifacts.py` enforces distribution file allowlists and checks for credential-like strings and Cyrillic outside translated README files.

CI has a unit/packaging matrix for Node 22.13/Python 3.10 and Node 24/Python 3.12 on all three operating systems. A separate browser matrix covers both backends and seven browser configurations on each OS. Logs, traces and screenshots are retained as CI artifacts. npm audit is not part of this test suite.

[Russian](ru/TESTING.md)
