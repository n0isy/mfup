# Performance and transport costs

MFUP/3 uses native File/Blob parts to keep payload processing out of browser JavaScript. Its additional cost over a trivial upload comes from session creation, receipt metadata, range validation, commit and publication. The benefit is resumable ranges, bounded discovery, grouped requests and upload-wide overwrite approval.

## Measurement method

The tables measure the 3.1.0 implementation on 10 September 2026: Linux x86_64, AMD EPYC 9654, 12 visible vCPUs, Docker Node 24.20.0/Python 3.12.14 and Chromium 151.0.7922.34. There are 66 verified runs: three repetitions per backend, payload and applicable mode. The local browser talks plain HTTP/1.1 through Caddy with no imposed WAN delay. Disk cache is not cleared. The environment is virtual and shared; three repetitions do not establish statistical significance.

The browser receives real disk files through a file input. File generation, picker enumeration, JavaScript download and final SHA-256 verification are outside the timed interval. Each published file is verified after timing. Mode order rotates. The MFUP configuration uses six requests, 128 parts, 32 MiB baskets, 16 MiB ranges and an 8 ms collection window, without application processing/mapping hooks or destination conflicts.

- Total time runs from session/request creation to published files, including MFUP connect, upload, commit and publish.
- Backend CPU is user plus system CPU of the receiver process; Node worker threads are included. It is not total machine CPU. Caddy and other processes are excluded.
- Main-thread time is Chromium Performance.TaskDuration during the operation, not total browser CPU or native networking/file-read cost.
- Network sizes count HTTP bodies plus JSON WebSocket message bytes. HTTP headers, WebSocket framing/handshake and TCP/IP overhead are excluded. These are not network-interface counters.

The trivial FormData baseline appends the full file list and performs one fetch. The per-file baseline uses six worker loops with one FormData per file. Raw File uses a single request body and applies only to single files. Baselines save files without durable sessions, approval or range resume. Node uses streamed Busboy writes. Python's ordinary request.form/UploadFile first spools large files, then copies them to the destination; MFUP writes ranges directly. Performance differences therefore include receiver implementation and disk I/O, not just protocol framing.

The measured lists are flat. A browser folder picker can supply relative file paths to ordinary FormData; transporting a file tree does not by itself require MFUP. Empty directories need separate metadata, as FileList does not include them.

## Time and computation

### Node

| Payload | Mode | Total ms (median) | Min-max ms | Backend | Payload | Mode | Upload bytes | Download bytes | HTTP requests |
| --- | --- | --- | ---: | ---: | ---: |
| node | 1 x 1 KiB | One FormData list | 1222 | 21 | 1 |
| node | 1 x 1 KiB | MFUP/3 | 1621 | 2299 | 4 |
| node | 1000 x 1 KiB | One FormData list | 1178044 | 24 | 1 |
| node | 1000 x 1 KiB | MFUP/3 | 1216653 | 11167 | 11 |
| node | 1 x 64 MiB | One FormData list | 67109062 | 21 | 1 |
| node | 1 x 64 MiB | MFUP/3 | 67110568 | 3497 | 7 |
| python | 1 x 1 KiB | One FormData list | 1222 | 21 | 1 |
| python | 1 x 1 KiB | MFUP/3 | 1621 | 2299 | 4 |
| python | 1000 x 1 KiB | One FormData list | 1178044 | 24 | 1 |
| python | 1000 x 1 KiB | MFUP/3 | 1216653 | 10036 | 11 |
| python | 1 x 64 MiB | One FormData list | 67109062 | 21 | 1 |
| python | 1 x 64 MiB | MFUP/3 | 67110568 | 2929 | 7 |

### Python

| Payload      | Mode                          | Total ms (median) |    Min-max ms | Backend CPU ms | Main-thread ms |
| ------------ | ----------------------------- | ----------------: | ------------: | -------------: | -------------: |
| 1 x 1 KiB    | Raw File                      |               3.0 |       2.8-3.6 |            1.3 |            2.4 |
| 1 x 1 KiB    | One FormData list             |               3.6 |       3.3-3.7 |            1.3 |            2.8 |
| 1 x 1 KiB    | One file/request, six workers |               3.2 |       3.2-6.3 |            1.3 |            2.6 |
| 1 x 1 KiB    | MFUP/3                        |              29.0 |     28.2-34.4 |           10.9 |            7.3 |
| 1000 x 1 KiB | One FormData list             |             425.4 |   419.0-431.5 |          109.8 |          212.8 |
| 1000 x 1 KiB | One file/request, six workers |            1128.2 |  973.1-1154.4 |          704.1 |         1006.4 |
| 1000 x 1 KiB | MFUP/3                        |            1275.9 | 1258.5-1340.4 |         1202.1 |          266.8 |
| 1 x 64 MiB   | Raw File                      |             140.3 |   108.8-141.2 |          121.5 |            4.1 |
| 1 x 64 MiB   | One FormData list             |             341.5 |   323.8-363.5 |          352.9 |            3.9 |
| 1 x 64 MiB   | One file/request, six workers |             408.5 |   323.7-416.0 |          365.5 |            4.4 |
| 1 x 64 MiB   | MFUP/3                        |             138.4 |   136.3-155.2 |          117.7 |           11.0 |

A large file benefits from native browser serialization and direct range writes. A small upload still pays session/control costs. For many small files, one trivial FormData can be faster because it has no range journal or publication plan. Six workers do not justify one request per small file: grouping avoids repeated request and multipart setup. Results are local measurements, not throughput guarantees for other disks, devices or networks.

## Transferred data

Median body bytes, with HTTP request count excluding the WebSocket handshake:

| Backend | Payload      | Mode              | Upload bytes | Download bytes | HTTP requests |
| ------- | ------------ | ----------------- | -----------: | -------------: | ------------: |
| node    | 1 x 1 KiB    | One FormData list |         1222 |             21 |             1 |
| node    | 1 x 1 KiB    | MFUP/3            |         1621 |           1849 |             4 |
| node    | 1000 x 1 KiB | One FormData list |      1178044 |             24 |             1 |
| node    | 1000 x 1 KiB | MFUP/3            |      1216653 |          27944 |            11 |
| node    | 1 x 64 MiB   | One FormData list |     67109062 |             21 |             1 |
| node    | 1 x 64 MiB   | MFUP/3            |     67110568 |           2768 |             7 |
| python  | 1 x 1 KiB    | One FormData list |         1222 |             21 |             1 |
| python  | 1 x 1 KiB    | MFUP/3            |         1621 |           1849 |             4 |
| python  | 1000 x 1 KiB | One FormData list |      1178044 |             24 |             1 |
| python  | 1000 x 1 KiB | MFUP/3            |      1216653 |          27719 |            11 |
| python  | 1 x 64 MiB   | One FormData list |     67109062 |             21 |             1 |
| python  | 1 x 64 MiB   | MFUP/3            |     67110568 |           2543 |             7 |

Multipart boundaries and headers scale with part count. MFUP adds a manifest tuple per range, session JSON, receipts and snapshots. Large payloads amortize these bytes; small files do not. The published snapshot contains at most 256 paths plus a total and cursor. Resume checks only current basket ranges using a boolean list, without downloading a full session inventory. Fresh uploads do not require that extra status request. Metadata control traffic is proportional to transferred ranges across the operation, while each response is bounded.

The scheduler groups 1,000 small files into eight data baskets under the default 128-part limit; create, commit and publish add three control requests. A 64 MiB file uses four 16 MiB data ranges. The exact distribution of ranges across baskets depends on available work and limits.

## Latency, memory and application work

Relative to one ordinary POST, create, commit and publish add three sequential control exchanges when client publication is used. At 50 ms RTT this dependency alone can add roughly 150 ms, before processing and the collection window. This is a dependency estimate, not a measured WAN test. Preconnecting can hide creation latency; server autoPublish can remove the separate client publication request.

Six concurrent requests share connection and link bandwidth. HTTP/2 can multiplex them onto one connection; TCP loss can still affect all streams. Multipart alone does not provide range resume. On an interrupted basket, MFUP first checks the receipt and retransmits only unconfirmed data; checking adds a control request and a lost basket can cost up to its unconfirmed payload size.

The SDK retains at most 10000 pending work records including active baskets, and resumes discovery at 5000. It releases confirmed references and has no full-session entry set or resume index. Native FileList and browser directory-page buffers belong to the browser; the SDK does not duplicate FileList. A drop can contain at most 10000 selected roots, whose contents are enumerated incrementally. Payload stays in native File/Blob objects. Database metadata and indexes occupy O(files + ranges) persistent space. Path checks use indexed lookups per path prefix and at most one basket of new mappings; no whole-session map is built in RAM. Snapshots use persisted counters. Commit and publication walk 256-node pages; manifest metadata and range receipts each use a transaction per basket. The mapper adds callback latency before that basket body is consumed, and starts overwrite questions early. Content reads in onCommitted add application I/O. There is no extra payload pass or external storage service. The demo lists 256 published files per page with bounded selection memory; obtaining a sorted page scans the destination directory.

Overwrite uses one conflict flag and one permission flag. The first detected conflict and first approval each require a session update. Approval is one POST with `{"overwrite":true}`: 18 JSON bytes before HTTP framing. The prompt and first-error object are constant-size with respect to conflict count; result path pages are capped at 256. Retry backoff uses one timer per waiting operation and no payload processing. At default settings, 1000 waits add 9 h 57 min 27 s plus request time. Receipt probes add control traffic during failures, and retransmission adds at most the unconfirmed basket payload per attempt.

XHR byte notifications are coalesced at 50 ms and use native upload events; they add no polling channel. They estimate active payload rather than confirming durable writes. Unconfirmed estimates can fall after interruption. The measured timing mode uses fetch; no timing claim for application-specific XHR rendering, conflicts, physical devices, peak RSS or power-loss recovery is made.

[Russian](ru/PERFORMANCE.md)
