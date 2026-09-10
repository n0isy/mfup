# Package release workflow

The five distributions share version **3.0.0**:

| npm          | Python       |
| ------------ | ------------ |
| @mfup/client | mfup-core    |
| @mfup/react  | mfup-fastapi |
| @mfup/server |              |

Python imports are mfup_core and mfup_fastapi. The protocol identifier is MFUP/3. Repository and package metadata point to https://github.com/n0isy/mfup.

## Build and inspect

```bash
npm ci --no-audit --no-fund
npm run build
npm run check
npm test
npm run pack:check
python -m pip install build twine ./server/mfup-core './server/mfup-fastapi[dev]'
python -m pytest server/tests -q
mkdir -p dist-npm dist-py
npm pack -w @mfup/client -w @mfup/react -w @mfup/server --pack-destination dist-npm
python scripts/build-python.py dist-py
twine check dist-py/*
python scripts/check-artifacts.py dist-npm dist-py
python scripts/check-version.py 3.0.0
```

Npm tarballs contain compiled JavaScript, TypeScript declarations, package metadata, license and English/Russian READMEs. Python wheels contain the module, py.typed and distribution metadata/license; source tarballs also contain pyproject.toml and both READMEs. Tests, demos, benchmarks, upload data, logs, development environments and local reports are excluded.

## CI/CD

[release.yml](../.github/workflows/release.yml) runs on v3.* tags or manual dispatch. It first runs the complete reusable CI matrix. The build job checks versions, builds and validates archives, then retains the npm/Python artifacts.

Only tag runs publish. Npm and PyPI jobs use the GitHub release environment and OIDC with id-token:write. The existing trusted-publisher identity is repository n0isy/mfup, workflow release.yml, environment release. No registry passwords are stored in the repository. Npm uses Node 24 and npm 11; stable artifacts publish to latest with provenance. Existing versions are skipped on reruns. PyPI uses pypa/gh-action-pypi-publish and skips existing files.

The verification job queries both public registries, checks all versions and npm latest tags, installs the registry packages and executes the same independent consumer examples. Package publication is confirmed by this job, not by a successful build alone.

The registry requirements are described by [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) and [PyPI publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

[Russian](ru/RELEASE.md)
