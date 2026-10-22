# Fault-tolerant topic-based file distribution service

IT305 Computer Networks Lab, Socket Programming Project.
**Lab Group 006 + 043** (C, gnu11, POSIX threads, TCP/IPv4, Linux).

| # | Name | Roll number |
|---|---|---|
| 1 | Madhav Chauhan | 202401106 |
| 2 | Dhruva Bhatiya | 202401029 |
| 3 | Tasvi Bhalani | 202401027 |
| 4 | Tanvi Kakadiya | 202401078 |
| 5 | Bhagy Parmar | 202401137 |
| 6 | Kanika Pal | 202401082 |

A server maps each *topic* (a sub-folder of `topics_root_dir`) to its files. A client names a
topic and receives every file of it. Part I is the plain service (single- and multi-threaded
server). Part II injects connection failures and recovers from them in three ways.

## The four versions

| Folder | What it is | Recovery after a cut connection |
|---|---|---|
| `Part1/` | Plain service, `single` or `multi` (default) server | none (no faults injected) |
| `Part2_Case1/` | Fault injection, **no** session management | client reconnects and the whole topic is sent again |
| `Part2_Case2/` | Fault injection + session management | client resumes from a checkpoint (file and byte offset) |
| `Part2_Case2_Enhanced/` | Case 2 + parallel connections | K connections, dynamic work queue, failed file is re-queued |

## Requirements

Linux, `gcc` and `make` (clang is optional). Nothing else is needed to build and run.
Sanitizer builds (`make SAN=...`) also need the sanitizer runtimes (Fedora:
`libasan libubsan libtsan`; Ubuntu: part of gcc).
<!-- repo-only -->
Tests need `bash` and `python3`. The graphs need `python3-matplotlib`.
<!-- /repo-only -->

## Folder layout

<!-- repo-only -->
```
README.md  HOW_TO_USE.md  FinalReport_Group_006_043.pdf   (the report is only in the submission)
Part1/  Part2_Case1/  Part2_Case2/  Part2_Case2_Enhanced/   one Makefile, server.c, client.c each
Results/                       raw CSV logs and graphs
common/                        canonical shared modules (repository only, not in the archive)
tools/  tests/  docs/          scripts, tests, assignment, design document, DEVIATIONS.md
```

Every Part folder is self-contained: the shared modules (`util`, `net`, `protocol`, `log`,
`topic_map`, `transfer`, `server_core`, plus `fault`, `session`, `checkpoint`, `workq` where
needed) are copies of `common/` made by `tools/sync_common.sh`.
<!-- /repo-only -->
<!-- archive-only
```
FinalSubmission_Group_006_043/
  FinalReport_Group_006_043.pdf   the report (design, deviations, performance analysis)
  README.md                       this file
  Part1/                          Makefile, server.c, client.c + shared modules
  Part2_Case1/                    Makefile, server.c, client.c + shared modules
  Part2_Case2/                    Makefile, server.c, client.c + shared modules
  Part2_Case2_Enhanced/           Makefile, server.c, client.c, client_poll.c + shared modules
  Results/                        raw CSV logs (raw/) and graphs (*.png), summary tables
```

Every Part folder is self-contained: the shared modules (`util`, `net`, `protocol`, `log`,
`topic_map`, `transfer`, `server_core`, plus `fault`, `session`, `checkpoint`, `workq` where
needed) are identical copies in each folder, so `make` in one folder needs nothing outside it.
archive-only -->

## Build

Run inside any Part folder (`Part1/`, `Part2_Case1/`, `Part2_Case2/`, `Part2_Case2_Enhanced/`):

```
make                  # builds exactly two executables here: ./server and ./client (no warnings)
make strict           # clean rebuild with -Werror
make SAN=address      # AddressSanitizer + UBSan build (run make clean first)
make SAN=thread       # ThreadSanitizer build (run make clean first)
make CC=clang         # build with clang
make clean            # remove objects and executables
```

To build all four: `for d in Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced; do make -C $d; done`

## Run

`<topics_root_dir>` contains one sub-folder per topic; the sub-folder name is the topic key.
Stop a server with Ctrl-C (SIGINT): it finishes active transfers (up to 5 s) and prints a summary.
Ports, paths and IPs are never hard-coded; they come only from the arguments below.

### Part I (`Part1/`)

```
./server <port> <topics_root_dir> [single|multi]        # default: multi
./client <server_ip> <server_port> <topic> <output_dir>
```
`single` serves clients one after another; `multi` uses a thread per client (at most 256 at a time). A
client that is refused with `ERROR 503` (server busy) waits with back-off and asks again, up to 100 times;
every other error is fatal (exit 1).

```
# terminal 1                                   # terminal 2
cd Part1                                       cd Part1
./server 5050 ../dataset/raw-img multi         ./client 127.0.0.1 5050 gatto ../out
```

### Part II, all cases: server

```
./server <port> <topics_root_dir> <failure_probability> [seed]
```
`failure_probability` (p) is a real number in [0, 1]. The optional `seed` makes the fault
positions repeatable. The server is multi-threaded in all Part II cases. A server reads each request within an absolute
30 s deadline and accepts only small requests (at most 512 bytes), so a stalled or hostile peer cannot hold a
thread or make it allocate memory.

### Part II Case 1 (`Part2_Case1/`)

```
./client <server_ip> <server_port> <topic> <output_dir> [max_retries=100]
```
```
# terminal 1                                   # terminal 2
cd Part2_Case1                                 cd Part2_Case1
./server 5050 ../dataset/raw-img 0.001 42      ./client 127.0.0.1 5050 gatto ../out
```

### Part II Case 2 (`Part2_Case2/`)

```
./client <server_ip> <server_port> <topic> <output_dir> [max_retries=20]
```
```
# terminal 1                                   # terminal 2
cd Part2_Case2                                 cd Part2_Case2
./server 5050 ../dataset/raw-img 0.01 42       ./client 127.0.0.1 5050 gatto ../out
```
`max_retries` counts consecutive attempts that made no progress (1..1000000).

### Part II Case 2 Enhanced (`Part2_Case2_Enhanced/`)

```
./client <server_ip> <server_port> <topic> <output_dir> [connections=4] [max_retries=20] [threads|poll]
```
`threads` (default) uses one blocking thread per connection. `poll` is one thread with
non-blocking sockets (`io=threads` / `io=poll` are accepted too). The optional arguments are
positional: to choose the engine you must also give the first two.
```
# terminal 1                                   # terminal 2
cd Part2_Case2_Enhanced                        cd Part2_Case2_Enhanced
./server 5050 ../dataset/raw-img 0.01 42       ./client 127.0.0.1 5050 gatto ../out 4 20 poll
```

### Output location and exit codes

The client writes to `<output_dir>/<topic exactly as typed>/` (created with `mkdir -p`).
When it succeeds, that folder holds **only** the topic's files: no `.part`, `.session` or
temporary files. Case 2 and Enhanced keep `<name>.part` and `.session` files while a transfer is
unfinished; they are what makes a restart of the client resumable. A fresh Enhanced run (no `.session`) first
deletes any old file of the topic's names and any `*.part` in that folder, so stale files are never trusted.

| Exit code | Meaning |
|---|---|
| 0 | success |
| 1 | fatal or usage error (a usage line is printed), e.g. unknown topic, server unreachable |
| 2 | the client gave up after `max_retries` (Part II) |

### What the programs print, and how to check a run

The client prints **one summary line on stdout** when it succeeds, and its progress
(reconnects, resumes) on stderr. The server prints the topics it loaded, one line per
injected fault, and a summary when it is stopped with Ctrl-C. Real output, topic
`elefante` (1446 files), `p = 0.5`, seed 1:

```
$ ./client 127.0.0.1 5050 elefante out              # Part2_Case1
[client] attempt 1/100 failed after 24439539 data bytes: connection lost (closed by the server); retrying in 50 ms
...
[client] 5 attempt(s), 144421820 data bytes received in total (100375023 redundant)
elefante: 1446 files, 44046797 bytes in 0.573 s (76.86 MB/s)          <- stdout

$ ./client 127.0.0.1 5050 elefante out              # Part2_Case2
[client] session started: 1446 files, 44046797 bytes
[client] connection lost at file 824 offset 15371; resuming in 50 ms
...
[client] attempts=5 reconnects=4 rejects=0
elefante: 1446 files, 44046797 bytes in 0.418 s (105.25 MB/s)         <- stdout

$ ./client 127.0.0.1 5050 elefante out              # Part2_Case2_Enhanced (defaults: k=4, threads)
[client] elefante: 1446 files, 44046797 bytes; new session, k=4
[client] [conn 2] lost at file 1 offset 12360 (closed by the server); re-queued file 1 at offset 12360, retry in 50 ms
...
[client] 9 connection(s) opened (4 after a failure), 0 FETCH(es) rejected, 44046797 data bytes received
elefante: 1446 files, 44046797 bytes in 0.152 s (289.96 MB/s), k=4, io=threads   <- stdout

server side (Part2_Case1 run above):
[server] listening on port 5050: 10 topic(s), multi mode, p = 0.5
[server] conn 1: FAULT injected: cut in file #824 'OIP-PjYC6SscebOCVOzccwaFnYwtUeH.jpeg' at byte 15371, connection closed
...
[server] summary: 5 connection(s) served (0 refused busy), 144421820 data bytes sent (144823892 on the wire), 4 fault(s) injected
```

To check a run: the exit code is 0, and the received folder is an exact copy of the topic
folder, with nothing else in it:

```
./client 127.0.0.1 5050 elefante out; echo "exit code $?"
diff -r <topics_root_dir>/elefante out/elefante && echo "identical"
```
`FDS_QUIET=1` silences the progress lines (the stdout summary stays).

### Optional environment variables

Logging and tuning come only from the environment; all are optional.

| Variable | Effect |
|---|---|
| `FDS_LOG=<file.csv>` | CSV log: one row per connection (server) or per run (client) |
| `FDS_EVENTS=<file.csv>` | server only: one row per FAULT / RESUME / FETCH event |
| `FDS_RUN_ID=<text>` | copied into every CSV row |
| `FDS_CLIENT_ID=<text>` | client name in the CSV (default: the pid) |
| `FDS_SNDBUF=<bytes>`, `FDS_RCVBUF=<bytes>` | set `SO_SNDBUF` / `SO_RCVBUF` (buffer-size experiment) |
| `FDS_QUIET=1` | no progress lines on stderr |

## Fault-injection mechanism (Part II)

Defined in `fault.h` / `fault.c` (in every Part II folder).

- **Meaning of p.** p is the probability that a **connection** is interrupted. Every
  connection is cut with probability p, independently of the others and of the topic
  size. `p = 0` never injects a fault. `p = 1` cuts every connection before the last byte
  of its data, so nothing can finish: the clients give up with exit code 2 after
  `max_retries` attempts.
- **When the decision is made.** Once per connection, when the server has accepted the
  connection's first data request and knows how many file bytes it will send for it:
  the whole topic for a Case 1 `GET`, the rest of the topic from the checkpoint for a
  Case 2 `GET` or `RESUME`, the rest of the requested file for the first `FETCH` of an
  Enhanced connection. Control messages (headers, manifest, ...) never get a decision,
  so every fault interrupts file data. A request with 0 data bytes leaves the decision
  to the connection's next request.
- **How the decision is made.** Every connection owns a `drand48_r` generator
  (thread-safe, no global state). Its 48-bit state is a hash of `(seed, conn_id)`, so one
  `[seed]` reproduces the same faults run after run, and every connection gets its own
  sequence. Without a seed, the hash mixes wall time, thread id, connection id and
  monotonic time. One draw `r` in [0,1): the connection will be cut iff `r < p`. If so, a
  second draw picks the cut byte uniformly among the data bytes of that request.
- **What happens at the cut.** The file-send loop checks before every 64 KB chunk whether
  the cut byte falls inside it. If so, the server sends only the bytes before the cut,
  prints and logs a `FAULT` line (connection, file, offset), then calls `shutdown()` and
  `close()`. The client sees the connection die at a random byte, not only at file or
  chunk boundaries, which is what exercises byte-offset resume. Only that connection
  ends; the server keeps serving everyone else.
- **Consequences.** Case 1: an attempt succeeds with probability `1-p`, so a client needs
  `1/(1-p)` attempts on average and every cut wastes on average half the topic. Case 2:
  about `p/(1-p)` cuts per transfer and no byte is sent twice. Enhanced: each of the K
  connections is cut with probability p (during its first file), no byte is sent twice.

## How each case recovers

- **Case 1.** No state. After a cut the client waits (50 ms, doubling up to 1 s when attempts
  receive no data at all), reconnects, sends `GET` again and the whole topic is re-sent. It
  gives up (exit 2) after `max_retries` attempts in total (default 100).
- **Case 2.** The client is the authority. For the file in progress it keeps `<name>.part`; its
  **size is the byte offset**; a `.session` file stores the session id. After a cut, the client
  reconnects and sends a RESUME carrying that checkpoint; the server validates it and continues
  from the offset, so no byte is sent twice (zero redundant bytes). A restarted client process
  resumes the same way from the files on disk.
- **Case 2 Enhanced.** The client opens K parallel connections (default 4). A shared work queue
  hands out one file per `FETCH`, so fast connections take more files (dynamic balancing). If a
  connection fails, its file is put back in the queue together with the offset reached, and any
  connection resumes it. Two client engines: `threads` (one blocking thread per connection) and
  `poll` (one thread, non-blocking sockets, `poll()`). The server sends with zero-copy
  `sendfile()`.

<!-- repo-only -->
## Testing

```
make -C tests/unit run                 # C unit tests of common/ (no sanitizer; SAN=address|thread needs the runtimes)
tests/integration/run_all.sh           # builds everything, then every integration test
tests/integration/run_all.sh --quick   # skips the slow checks
tests/integration/run_all.sh --only case2     # one of: build cli part1 case1 case2 enhanced
```
Each check prints `PASS`, `FAIL` or `SKIP`; the run ends with a table and exits 1 on any failure.
Helpers: `tests/gen_dataset.py` (deterministic test datasets), `tests/verify_output.sh <src_topic>
<out_topic>` (exact copy check), `tests/faultproxy.py` (cuts or throttles a connection at chosen
bytes). See `tests/README.md`.

## Experiments

The performance experiments (Part I single vs multi, Part II p-sweeps, socket buffers), run on
three PCs or emulated on one, and the scripts that turn the raw CSV files into the tables and
graphs in `Results/` are described step by step in `HOW_TO_USE.md` (repository) and
`tools/exp/README.md`.

## Deviations from the design document

The implementation differs from the submitted design in a few reviewed places (for example the
per-connection fault probability, the random cut byte and the hashed seed). All of them, with
reasons, are listed in `docs/DEVIATIONS.md` and are repeated in the Design section of the report.
<!-- /repo-only -->
<!-- archive-only
## Results

`Results/` holds the raw CSV logs of every measured run (`Results/raw/`), the summary tables
computed from them and the graphs (`Part1_*`, `Part2_Case1_*`, `Part2_Case2_*`,
`Part2_Case2_Enhanced_*`, `Part2_Comparison_*`: `*_throughput.png` and `*_overhead.png`). The
experiment setup (three PCs, sweeps, repetitions) and the analysis are in the report.

## Deviations from the design document

The implementation differs from the submitted design in a few reviewed places (for example the
per-connection fault probability, the random cut byte and the hashed seed). All of them, with
reasons, are listed in the Design section of `FinalReport_Group_006_043.pdf`.
archive-only -->
