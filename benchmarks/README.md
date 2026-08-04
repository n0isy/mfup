# Benchmarks

## Baseline: `trivial-server/`

A deliberately naive upload server — the industry-default pattern MFUP/2
exists to beat: one `multipart/form-data` POST **per file** (FastAPI +
`python-multipart`). No sessions, no resume, no integrity checks; paths are
sanitised and that is all. It is deployed by the root `docker-compose.yaml`
as the `trivial` service and proxied at `/trivial/*`.

## Method

The demo app ships a side-by-side race page — `/compare.html` on the dev
stack (`demo/src/compare.ts`): the SAME locally-generated file tree is
uploaded twice in the same browser, once through MFUP/2, once through the
baseline, with per-phase timings.

## Why this baseline

The motivating measurement (docs/MFUP_RU.md): a 188-file, 1.0 MB project
folder over POST-per-file took **17.2 s** — ~13 s of it pure request
round-trips and ~2.9 s of `FormData` construction; actual transfer was
~1.3 s. The protocol was the bottleneck, not the bytes. MFUP/2 collapses
the tree to one WebSocket + two HTTP requests; on the same tree the race
page lands around the transfer-bound floor.

Numbers vary with RTT — run `/compare.html` over a real network path, not
localhost, to see the gap the way users do.
