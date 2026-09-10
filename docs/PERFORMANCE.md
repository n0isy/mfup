# Performance and transport costs

MFUP/3 uses native File/Blob parts to keep payload processing out of browser JavaScript. Its additional cost over a trivial upload comes from session creation, receipt metadata, range validation, commit and publication. The benefit is resumable ranges, bounded discovery, grouped requests and upload-wide overwrite approval.

## Measurement method

The tables measure the 3.0.0 implementation on 10 September 2026: Linux x86_64, AMD EPYC 9654, 12 visible vCPUs, Docker Node 24.20.0/Python 3.12.14 and Chromium 151.0.7922.34. There are 66 verified runs: three repetitions per backend, payload and applicable mode. The local browser talks plain HTTP/1.1 through Caddy with no imposed WAN delay. Disk cache is not cleared. The environment is virtual and shared; three repetitions do not establish statistical significance.

The browser receives real disk files through a file input. File generation, picker enumeration, JavaScript download and final SHA-256 verification are outside the timed interval. Each published file is verified after timing. Mode order rotates. The MFUP configuration uses six requests, 128 parts, 32 MiB baskets, 16 MiB ranges and an 8 ms collection window, without application processing/mapping hooks or destination conflicts.

- Total time runs from session/request creation to published files, including MFUP connect, upload, commit and publish.
- Backend CPU is user plus system CPU of the receiver process; Node worker threads are included. It is not total machine CPU. Caddy and other processes are excluded.
- Main-thread time is Chromium Performance.TaskDuration during the operation, not total browser CPU or native networking/file-read cost.
- Network sizes count HTTP bodies plus JSON WebSocket message bytes. HTTP headers, WebSocket framing/handshake and TCP/IP overhead are excluded. These are not network-interface counters.

The trivial FormData baseline appends the full file list and performs one fetch. The per-file baseline uses six worker loops with one FormData per file. Raw File uses a single request body and applies only to single files. Baselines save files without durable sessions, approval or range resume. Node uses streamed Busboy writes. Python's ordinary request.form/UploadFile first spools large files, then copies them to the destination; MFUP writes ranges directly. Performance differences therefore include receiver implementation and disk I/O, not just protocol framing.

The measured lists are flat. A browser folder picker can supply relative file paths to ordinary FormData; transporting a file tree does not by itself require MFUP. Empty directories need separate metadata, as FileList does not include them.

## Time and computation

### Node

| Payload      | Mode                          | Total ms (median) |    Min-max ms | Backend CPU ms | Main-thread ms |
| ------------ | ----------------------------- | ----------------: | ------------: | -------------: | -------------: |
| 1 x 1 KiB    | Raw File                      |               4.2 |       3.5-9.7 |            2.4 |            2.9 |
| 1 x 1 KiB    | One FormData list             |               4.6 |       4.3-9.6 |            3.5 |            3.9 |
| 1 x 1 KiB    | One file/request, six workers |               4.8 |       4.7-4.9 |            2.9 |            3.5 |
| 1 x 1 KiB    | MFUP/3                        |              37.8 |     29.0-43.1 |           14.0 |            8.2 |
| 1000 x 1 KiB | One FormData list             |             354.6 |   350.1-632.7 |          214.4 |          201.9 |
| 1000 x 1 KiB | One file/request, six workers |            1074.5 | 1023.3-1100.4 |          806.4 |          959.6 |
| 1000 x 1 KiB | MFUP/3                        |             526.0 |   454.0-532.8 |          485.8 |          233.2 |
| 1 x 64 MiB   | Raw File                      |             133.8 |   120.2-165.8 |          219.8 |            3.3 |
| 1 x 64 MiB   | One FormData list             |             262.8 |   238.1-282.6 |          359.3 |            4.0 |
| 1 x 64 MiB   | One file/request, six workers |             244.8 |   229.2-321.4 |          332.9 |            3.6 |
| 1 x 64 MiB   | MFUP/3                        |             148.1 |   145.1-152.0 |          347.1 |           11.0 |

### Python

| Payload      | Mode                          | Total ms (median) |    Min-max ms | Backend CPU ms | Main-thread ms |
| ------------ | ----------------------------- | ----------------: | ------------: | -------------: | -------------: |
| 1 x 1 KiB    | Raw File                      |               3.3 |       3.0-4.1 |            1.3 |            2.5 |
| 1 x 1 KiB    | One FormData list             |               3.9 |       3.1-4.6 |            1.4 |            2.9 |
| 1 x 1 KiB    | One file/request, six workers |               3.6 |       3.1-5.3 |            1.3 |            3.1 |
| 1 x 1 KiB    | MFUP/3                        |              27.5 |     27.3-30.0 |           10.0 |            6.8 |
| 1000 x 1 KiB | One FormData list             |             446.8 |   427.0-476.3 |          141.4 |          221.5 |
| 1000 x 1 KiB | One file/request, six workers |            1089.0 | 1029.0-1123.8 |          689.6 |          985.6 |
| 1000 x 1 KiB | MFUP/3                        |             521.4 |   499.0-562.8 |          434.2 |          253.7 |
| 1 x 64 MiB   | Raw File                      |             121.7 |   111.1-158.6 |          115.7 |            3.5 |
| 1 x 64 MiB   | One FormData list             |             367.7 |   365.0-407.9 |          373.9 |            4.4 |
| 1 x 64 MiB   | One file/request, six workers |             352.7 |   347.2-426.6 |          369.7 |            4.0 |
| 1 x 64 MiB   | MFUP/3                        |             141.6 |   139.6-144.4 |          124.3 |           11.0 |

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

Multipart boundaries and headers scale with part count. MFUP adds a manifest tuple for each range, session JSON, receipts and snapshots. Large payloads amortize these bytes; small files do not. The published snapshot includes a path list, and resume carries file/range metadata, so total control data is not constant in file count.

The scheduler groups 1,000 small files into eight data baskets under the default 128-part limit; create, commit and publish add three control requests. A 64 MiB file uses four 16 MiB data ranges. The exact distribution of ranges across baskets depends on available work and limits.

## Latency, memory and application work

Relative to one ordinary POST, create, commit and publish add three sequential control exchanges when client publication is used. At 50 ms RTT this dependency alone can add roughly 150 ms, before processing and the collection window. This is a dependency estimate, not a measured WAN test. Preconnecting can hide creation latency; server autoPublish can remove the separate client publication request.

Six concurrent requests share connection and link bandwidth. HTTP/2 can multiplex them onto one connection; TCP loss can still affect all streams. Multipart alone does not provide range resume. On an interrupted basket, MFUP first checks the receipt and retransmits only unconfirmed data; checking adds a control request and a lost basket can cost up to its unconfirmed payload size.

The ready queue is bounded by entry count. Payload stays in native File/Blob objects; browser networking still reads the bytes. Persistent metadata is O(files + ranges). The mapping plan is O(files/paths), is validated before moves and is saved in one transaction. ListStaged reads metadata in pages of 256; openStaged adds payload I/O only when the application consumes the stream. A processing hook can add arbitrary CPU, storage and service latency.

Overwrite uses one conflict flag and one permission flag. The first detected conflict and first approval each require a session update. Approval is one POST with `{"overwrite":true}`: 18 JSON bytes before HTTP framing. The prompt and first-error object are constant-size with respect to conflict count, while other snapshot fields can grow. There is no additional payload pass or separate storage service.

XHR byte notifications are coalesced at 50 ms and use native upload events; they add no polling channel. They estimate active payload rather than confirming durable writes. Unconfirmed estimates can fall after interruption. The measured timing mode uses fetch; no timing claim for application-specific XHR rendering, conflicts, physical devices, peak RSS or power-loss recovery is made.

[Russian](ru/PERFORMANCE.md)
