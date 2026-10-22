# Code walkthrough for the viva (IT305 FDS, group 006 + 043)

Purpose: after reading this document, any team member can open any file of the
project and explain what it does and why. The guidelines warn of a "rigorous
viva about the implementation", and every one of the six of us is responsible
for ALL the code, not only our own part.

How to use it:

- Every function is cited as `file:line`. Lines refer to the originals in
  `common/` (the copies in `Part*/` are byte-identical, checked by
  `tools/check_common.sh`) and to the `server.c` / `client.c` files of each
  Part folder. If you edit a file, the line numbers move; use `grep -n`.
- "Deviation N" means row N of `docs/DEVIATIONS.md`. "Design §x" means section x
  of `docs/design_document.txt`. Where they disagree, DEVIATIONS.md wins.
- Everything here was checked against the code. Where the code and an older
  comment disagree, the code is described (see section 10, "Known loose ends").

Contents:

1. Big picture
2. Wire protocol in one page
3. Module by module (util, net, protocol, log, topic_map, transfer, server_core,
   fault, session, checkpoint, workq)
4. Life of a request, per case
5. Concurrency and safety
6. Measurement
7. Design decisions and alternatives
8. Likely viva questions with model answers
9. Study plan per team member
10. Known loose ends (things that are slightly inconsistent)

---------------------------------------------------------------------------

## 1. Big picture

### 1.1 What the system does

A client says `GET <topic>`. The server sends every file of that topic
(a sub-folder of the Animals-10 dataset), and the client stores them in
`output_dir/<topic exactly as typed>/`. Part II adds random disconnections
injected by the server, and three ways of recovering from them.

### 1.2 The four versions

| Folder | Server | Client | What it adds |
|---|---|---|---|
| `Part1/` | single or multi (`argv[3]`, default multi) | one `GET`, no retry of the transfer; only `ERROR 503` (server busy) is retried with back-off (Deviation 26) | The basic service. Exit 1 on any other failure. |
| `Part2_Case1/` | multi only + fault injection (`fault.c`) | retries the whole topic | No state on either side. After a fault the client sends `GET` again and gets every file again (`O_TRUNC`). Exit 2 after `max_retries` (default 100) attempts. |
| `Part2_Case2/` | Case 1 + session table (`session.c`) | `.part` files + `.session` checkpoint (`checkpoint.c`) | After a fault the client sends `RESUME` with its checkpoint; the server validates it and sends only the missing bytes. Zero redundant bytes. |
| `Part2_Case2_Enhanced/` | Case 2 + `MANIFEST_REQ` and `FETCH` handling | K parallel connections, shared work queue (`workq.c`), two engines: `threads` and `poll` (`client_poll.c`) | Parallelism, one file per `FETCH`, per-connection recovery. |

Executables are always exactly `server` and `client`. The Part II servers take
`<port> <topics_root_dir> <failure_probability> [seed]`.

### 1.3 Architecture diagram: who calls whom

```
                              SERVER PROCESS
  server.c (per Part)                       main(): parse args, fill server_cfg_t
     |  server_run(&cfg)                    -> server_core.c:657
     v
  server_core.c                             accept loop, 256-thread cap, signal thread,
     |   main thread: accept_loop (508)     CSV row per connection
     |   signal thread: signal_thread (560)
     |   worker thread: conn_thread -> run_conn (392) -> cfg->serve_client(c)
     |
     |   serve_client(conn_t *)  <- the ONLY part that differs per case (server.c)
     |        |  uses: conn_recv_request (server_core.c:346) / dec_*  (protocol.c)
     |        |        topic_find_or_rescan     (topic_map.c)
     |        |        session_* (Case 2, Enh)  (session.c)
     |        |        fault_init / hook        (fault.c, Part II)
     |        v
     |   transfer.c: serve_topic (341) -> send_file_range (268) -> send_chunks (234)
     |        |         sendfile() loop in send_data (132); fault hook before each chunk
     |        v
     |   protocol.c enc_* + conn_send (server_core.c:161) -> send_encoded -> sendall (net.c:41)
     v
  net.c, util.c, log.c are used by everybody

                              CLIENT PROCESS
  client.c (per Part)   main(): parse args, retry loop (differs per case)
     |   uses: tcp_connect (net.c:223), enc_get / enc_resume / enc_manifest_req (protocol.c)
     |         recv_expect (transfer.c:394), recv_topic (600), recv_file (497)
     |         checkpoint.c (Case 2, Enhanced), workq.c + client_poll.c (Enhanced)
     v
  transfer.c client side writes the file bytes, renames .part -> final name
```

Layering (lower layers never call upper layers):

```
 util  <-  net  <-  protocol  <-  transfer  <-  server_core / client.c / server.c
 log (used by all)      topic_map, session, checkpoint, fault, workq are leaf helpers
```

### 1.4 How `common/` is copied into every folder

The grader requires each Part folder to build on its own with `make` and to
contain exactly two executables. So there is no shared library and no `-I../common`.
Instead `tools/sync_common.sh` (the script, 49 lines) copies the shared modules into
each folder:

- `base_mods="util net protocol log topic_map transfer server_core"` (copied to all four).
- Extra per folder: Part1 none; Case 1 `fault`; Case 2 `fault session checkpoint`;
  Enhanced `fault session checkpoint workq`. `common/fds.h` goes everywhere.
- `tools/sync_common.sh --check` (also run as `tools/check_common.sh`) exits 1 if any
  copy differs from `common/`. Rule from `CLAUDE.md`: edit only `common/`, then run the
  sync. Never edit a copy inside `Part*/`.
- Each folder keeps its own `Makefile`, `server.c`, `client.c` (Enhanced also
  `enh_client.h` and `client_poll.c`).

One consequence you may be asked about: `transfer.c` is compiled into Part I
as well, but Part I never installs a fault hook (`conn_t.fault_hook` stays NULL,
`transfer.c:249`), and `fault.c` is not even linked there.

### 1.5 Build and flags

`CFLAGS = -std=gnu11 -Wall -Wextra -O2 -pthread -MMD -MP` (Part1/Makefile).
`make strict` adds `-Werror` (Deviation 5: the default `make` has no `-Werror`
so that a new warning on the grader's compiler cannot prevent the build).
`make SAN=address` and `make SAN=thread` build with the sanitizers.

---------------------------------------------------------------------------

## 2. Wire protocol in one page

### 2.1 Why binary and length-prefixed

TCP is a byte stream. One `send()` of 100 bytes may arrive as two `recv()`s of
40 and 60 bytes, and two `send()`s may arrive in one `recv()`. So the receiver
needs a rule that tells it where a message ends. We use a fixed 16-byte header that
carries the payload length: read exactly 16 bytes, then exactly `payload_len` bytes.
We did not use newline-terminated text (as in HTTP/FTP) because image data can
contain any byte value, so a delimiter is unsafe (design §9). The request is still
the text `GET <topic>`, but it travels inside the binary frame.

### 2.2 The 16-byte header

Defined in `common/fds.h:17-19`, written by `hdr_encode` (`protocol.c:277`), read
and validated by `hdr_decode` (`protocol.c:289`).

```
offset  size  field        value
 0      u16   magic        0x5446 ("TF")
 2      u8    version      1
 3      u8    type         msg_type_t
 4      u32   reserved     0 (ignored by the decoder, for future flags)
 8      u64   payload_len  number of payload bytes that follow
```

All integers are big-endian (network byte order). For `GET gatto` the 16 header
bytes are `54 46 01 01 00 00 00 00 00 00 00 00 00 00 00 09`, followed by the 9
payload bytes `GET gatto`.

`hdr_decode` rejects: wrong magic or version; unknown type; `payload_len` greater
than `max_payload_for_type(type)` (`protocol.c:268`). That last check happens
BEFORE `recv_msg` calls `malloc`, so a hostile header cannot make us allocate
gigabytes (`protocol.c:308` comment).

Size caps (Deviation 2): 64 KiB for every type, except `MANIFEST` which may be up
to 64 MiB (the manifest of `cane` with 4 863 files is about 250 KB). These are the caps
of `hdr_decode`, which the CLIENTS use. A SERVER is stricter (Deviation 25): it reads a
request with `conn_recv_request()` (`server_core.c:346`), which accepts only the four
client->server types (GET, RESUME, MANIFEST_REQ, FETCH) and a payload of at most
`FDS_MAX_REQUEST_PAYLOAD` = 512 bytes (`fds.h:30`; the largest real request, RESUME/FETCH,
is 349 bytes). The check is made on the header, BEFORE the payload buffer is allocated, so
a peer announcing a 64 MiB "MANIFEST" gets `ERROR 400` at once and costs us no memory.

### 2.3 Message table

| Code | Name | Dir | Payload | Used by |
|---|---|---|---|---|
| 0x01 | GET | C->S | ASCII `"GET <topic>"` (topic 1..63 bytes) | Part I, Case 1, Case 2 |
| 0x02 | RESUME | C->S | session_id[16] file_index u32 offset u64 topic_len u8 topic name_len u16 name | Case 2 |
| 0x03 | MANIFEST_REQ | C->S | topic_len u8 topic connections u8 | Enhanced |
| 0x04 | FETCH | C->S | same layout as RESUME | Enhanced |
| 0x10 | TOPIC_BEGIN | S->C | session_id[16] file_count u32 total_bytes u64 | Part I, Case 1, Case 2 |
| 0x11 | FILE_HDR | S->C | file_index u32 file_size u64 start_offset u64 name_len u16 name; then raw bytes | all |
| 0x12 | FILE_END | S->C | file_index u32 | all |
| 0x13 | TOPIC_END | S->C | file_count u32 total_bytes u64 | Part I, Case 1, Case 2 |
| 0x14 | RESUME_OK | S->C | file_index u32 offset u64 | Case 2 |
| 0x15 | RESUME_REJECT | S->C | reason u16 (1..5) | Case 2, Enhanced |
| 0x16 | MANIFEST | S->C | session_id[16] file_count u32 total_bytes u64, then per file: size u64 name_len u16 name | Enhanced |
| 0x7F | ERROR | S->C | code u16 msg_len u16 msg (400, 404, 500, 503) | all |

Types are listed in one table, `msg_types[]` (`protocol.c:34`), which both
`msg_type_name` and `hdr_decode` use, so a type cannot be known to one and not
the other. Payload structs are in `protocol.h` (`m_topic_begin_t`, `m_file_hdr_t`,
`m_resume_t`, ...). RESUME and FETCH share `m_resume_t` and `dec_resume`
(`protocol.c:634`), and `enc_resume` (`protocol.c:423`) takes the type as an argument.

RESUME_REJECT reasons (`fds.h:66-70`): 1 unknown topic, 2 file index out of range,
3 file name mismatch, 4 offset beyond file size, 5 checkpoint ahead of data sent.

### 2.4 Big-endian packing: put_u* / get_u*

We never send a struct raw, for two reasons: the compiler may insert padding, and
the host byte order may differ. Every field is written one byte at a time, most
significant byte first, through a bounds-checked writer:

- `wbuf_t` and `put_u8/16/32/64/bytes` (`protocol.c:128-168`). `put_u16`
  (`protocol.c:135`) writes `v >> 8` then `v`.
- `rbuf_t` and `get_u8/16/32/64/bytes/str` (`protocol.c:193-255`).
- Both are "sticky": the first overflow/underflow sets `err = 1` and every later call
  does nothing. A decoder can do all its `get_*` calls and check `err` once. The check is
  written `n > cap - len` instead of `len + n > cap` so that a huge `n` cannot overflow
  the addition (`wb_room`, `protocol.c:119`).
- `rb_done` (`protocol.c:261`) is the strict end check: no error AND all bytes consumed.
  Every decoder returns -1 if there are trailing bytes.
- `get_str` (`protocol.c:237`) refuses a string that does not fit (instead of truncating
  it: a cut-off file name would name another file), and refuses an embedded NUL.

### 2.5 Why FILE_HDR is followed by raw, unframed bytes

`FILE_HDR`'s `payload_len` covers only its metadata (index, size, start_offset, name).
Right after it come exactly `file_size - start_offset` raw file bytes, with no
per-chunk framing. Reasons:

- Zero per-chunk overhead: a 64 KB chunk costs 64 KB on the wire, not 64 KB + 16.
- The server can hand the bytes straight to `sendfile()`, which copies from the page
  cache to the socket inside the kernel. If every chunk needed a header, we would have to
  interleave `send(header)` and `sendfile()`.
- The receiver knows the count from the header (`file_size - start_offset`), so it needs
  no terminator. `recv_msg` (`protocol.c:815`) deliberately stops after the metadata for
  FILE_HDR; `recv_file` (`transfer.c:500`) then reads exactly `remaining` bytes with plain
  `recv()` and expects `FILE_END`.
- The price: if a fault ends the stream in the middle of the raw bytes, the receiver
  sees EOF inside a file. That is exactly how it detects the failure (`RX_LOST`).

Also: because the server may not send an `ERROR` in the middle of raw bytes (the client
would read it as file data), `data_failed` (`transfer.c:194`) just closes the connection
on a file error in mid-stream.

---------------------------------------------------------------------------

## 3. Module by module

Order: util, net, protocol, log, topic_map, transfer, server_core, fault, session,
checkpoint, workq. The shared constants are all in `common/fds.h`.

### 3.0 `fds.h` (constants)

Magic/version/header length (`fds.h:17-19`), name limits (`:21-22`), payload caps
(`:25-26`), `FDS_CHUNK_SIZE = 65536` (`:29`), `FDS_MAX_THREADS = 256` (`:33`),
`FDS_MAX_SESSIONS = 256` (`:34`), `FDS_SESSION_EXPIRY_S = 300` (`:35`), server
timeouts 30 s (`:36`), client timeout 15 s (`:37`), connect timeout 10 s (`:38`),
shutdown grace 5 s (`:39`), re-scan limit 1 s (`:40`), back-off 50 ms to 1000 ms
(`:43-44`), default retries 100 (Case 1) and 20 (Case 2/Enhanced) (`:45-46`),
default K = 4, max K = 16 (`:47-48`), exit codes 0/1/2 (`:51-53`). `fds_chunks_of`
(`:75`) is ceil(size / 65536): a 0-byte file has 0 chunks.

### 3.1 util (`util.h`, `util.c`)

**Responsibility.** Argument parsing, name validation, paths, time, back-off, random
bytes, hex, environment. No networking.

**Key functions.**

- `parse_long` (`util.c:16`): `strtol` with `errno` check, rejects trailing junk and
  range violations. `parse_port` (`:31`) is 1..65535. `parse_prob` (`:41`) uses `strtod`
  and the test `!(v >= 0 && v <= 1)`, which also rejects NaN.
- `topic_is_valid` (`util.c:65`): 1..63 bytes, no `/`, no control characters, not `.` or `..`.
- `name_is_safe` (`util.c:79`): 1..255 bytes, no `/`, no control characters, and does NOT
  start with `.`. This is the path-safety rule: a name from the network becomes a path on
  the client's disk. It is used by the server when loading a topic, by every decoder
  (`dec_file_hdr`, `dec_resume`, `dec_manifest`), and by the checkpoint code.
- `path_join` / `path_join3` (`:91`, `:98`): `snprintf` and a length check, return -1 on
  truncation.
- `mkdir_p` (`util.c:105`): creates each prefix, accepts `EEXIST`, and finally `stat`s to
  make sure the result is a directory.
- `now_ns` (`:146`) is `CLOCK_MONOTONIC`; `wall_ns` (`:147`) is `CLOCK_REALTIME`, used only
  for rough cross-machine alignment in the CSV.
- `sleep_ms` (`:149`): `nanosleep` loop that restarts on `EINTR` with the remaining time.
- `backoff_ms(streak)` (`util.c:157`): 50, 100, 200, 400, 800, 1000, 1000, ...
- `random_bytes` (`:166`): `getrandom()` loop (EINTR-safe), with a non-cryptographic
  fallback; only needs to be unique (session ids).
- `env_str` (`:229`) treats unset and empty alike; `fds_quiet` (`:246`) reads `FDS_QUIET`.

**Tricky details.**

- Monotonic vs wall clock: every interval is measured with `CLOCK_MONOTONIC` because
  NTP/`date` adjustments can move the wall clock backwards (see Q22).
- `backoff_ms` takes a *streak* (consecutive attempts without progress), not the attempt
  number. See Deviation 18 and the retry policy in section 4.

### 3.2 net (`net.h`, `net.c`)

**Responsibility.** The lowest layer: every byte that touches a socket goes through it.

**Key functions.**

- `sendall(fd, buf, n, flags)` (`net.c:41`): loops `send(..., flags | MSG_NOSIGNAL)` until
  all `n` bytes are sent. `EINTR` -> retry. Any other error (including `EAGAIN`, which on a
  blocking socket means `SO_SNDTIMEO` expired) -> return -1. `r == 0` (should never happen
  for `left > 0`) is turned into `EPIPE` so the loop cannot spin.
- `recvall(fd, buf, n, &got)` (`net.c:70`): loops `recv` until `n` bytes. Returns `NET_OK`,
  `NET_EOF` (peer closed before `n` bytes) or `NET_ERR`. `*got` is set on EVERY path, so
  callers can count wire bytes and know how much of a header arrived before the stream ended.
- `tcp_listen(port)` (`net.c:106`): `socket(SOCK_CLOEXEC)`, `SO_REUSEADDR`, bind `INADDR_ANY`,
  `listen(SOMAXCONN)`.
- `tcp_connect(host, port, timeout)` (`net.c:223`): `getaddrinfo` (IPv4, numeric service),
  tries each address with `connect_one` (`:192`), which uses a NON-blocking `connect` plus
  `poll` for the time-out (`wait_connected`, `:146`), then switches the socket back to blocking.
- `set_timeouts` (`:263`) sets `SO_RCVTIMEO`/`SO_SNDTIMEO`; `set_nodelay` (`:276`) sets
  `TCP_NODELAY`; `set_nonblocking` (`:286`) is a read-modify-write of `O_NONBLOCK`;
  `apply_env_bufsizes` (`:310`) applies `FDS_SNDBUF`/`FDS_RCVBUF` only if set;
  `shutdown_close` (`:320`) is `shutdown(SHUT_RDWR)` then `close`, preserving `errno`;
  `peer_to_str` (`:336`) gives `a.b.c.d:port` for the CSV.

**Tricky details a viva may probe.**

- *Short reads/writes.* TCP does not preserve message boundaries; `send` may accept fewer
  bytes than asked and `recv` may return fewer. That is why both are loops. A single `recv`
  of 16 bytes may return 7.
- *EINTR.* A signal can interrupt a blocking `send`/`recv` before any byte moves; the call
  fails with `EINTR` and you simply call it again. The code never treats it as an error.
- *MSG_NOSIGNAL and SIGPIPE.* Writing to a socket whose peer has closed raises `SIGPIPE`,
  whose default action kills the process. A fault-injecting server and a retrying client must
  survive this, so every `send` carries `MSG_NOSIGNAL` and gets `EPIPE` as an ordinary error.
- *sendfile has no flags.* `sendfile()` cannot take `MSG_NOSIGNAL`, so for the data path we
  rely on `signal(SIGPIPE, SIG_IGN)` in `server_run` (`server_core.c:680`). See `transfer.c:13-16`.
- *EAGAIN meaning.* On a blocking socket with `SO_RCVTIMEO` set, `recv` fails with
  `EAGAIN`/`EWOULDBLOCK` when the time-out expires; the client reports it as "no data for 15 s".
  On a non-blocking socket (poll engine) the same errno means "not ready, wait for poll()".
- *Non-blocking connect.* Result is `EINPROGRESS`; the socket becomes writable when the
  handshake finishes, and `getsockopt(SO_ERROR)` tells whether it worked (`net.c:182`). The
  deadline is absolute (monotonic), so repeated `EINTR` cannot extend the wait.
- *SO_REUSEADDR* (`net.c:117`): lets the server re-bind a port that still has connections in
  `TIME_WAIT`, so it can be restarted at once between experiments. It does NOT let two live
  servers share a port.
- *Why shutdown() before close() (`shutdown_close`).* `shutdown` sends the FIN immediately and
  wakes any thread blocked on that socket; the fault injector needs the FIN to go out right
  after the last data byte.

### 3.3 protocol (`protocol.h`, `protocol.c`)

**Responsibility.** Turn messages into bytes and back, with validation. Two layers: pure
buffer encoders/decoders (`enc_*`, `dec_*`, usable by blocking code and by the poll engine,
which parses incrementally) and blocking fd helpers (`send_encoded`, `send_error`, `recv_msg`).

**Key data structures.** `msg_hdr_t` {type, payload_len}; `wbuf_t`/`rbuf_t` (cursor + `err`
flag); one struct per message (`m_topic_begin_t`, `m_file_hdr_t`, `m_resume_t`,
`m_resume_ok_t`, `m_manifest_req_t`, `m_manifest_t` with a malloc'd `files` array,
`m_error_t`); `file_entry_t {name, size}` from `fds.h`.

**Key functions.**

- `enc_begin` (`protocol.c:321`) leaves 16 bytes for the header and `enc_finish` (`:331`) fills
  it once the payload length is known; it refuses a payload larger than the type's cap.
- Encoders: `enc_get` (`:359`), `enc_topic_begin` (`:372`), `enc_file_hdr` (`:385`),
  `enc_file_end` (`:402`), `enc_topic_end` (`:411`), `enc_resume` (`:423`), `enc_resume_ok`
  (`:442`), `enc_resume_reject` (`:454`), `enc_manifest_req` (`:463`), `enc_error` (`:476`).
  They return the total length or 0 on error ("encoders refuse to produce anything the
  decoders would reject").
- `enc_manifest_alloc` (`:490`): two passes. First validates names and sums sizes (checks no
  64-bit wrap with `add_u64`, `:82`; checks the sum equals `total_bytes`), then `malloc`s exactly
  the needed size and fills it.
- Decoders: `dec_get` (`:556`: exactly `"GET "` + valid topic, no embedded NUL),
  `dec_topic_begin` (`:579`), `dec_file_hdr` (`:590`: also `name_is_safe` and
  `start_offset <= file_size`), `dec_file_end` (`:615`), `dec_topic_end` (`:624`),
  `dec_resume` (`:634`), `dec_resume_ok` (`:659`), `dec_resume_reject` (`:669`),
  `dec_manifest_req` (`:680`), `dec_manifest` (`:696`), `dec_error` (`:754`).
- `dec_manifest` defends against a hostile count: `count > (len - fixed) / min_entry` is rejected
  BEFORE `calloc` (otherwise 28 bytes claiming 4 billion files would allocate ~1 TB, `:714`).
  It checks every name with `name_is_safe`, that no trailing bytes remain, and that the sizes add
  up to `total_bytes`.
- `dec_error` (`:754`) replaces control characters in the text with `?` so a server cannot inject
  terminal escape sequences into the client's console.
- `send_encoded(fd, buf, len, more)` (`:788`): refuses `len < 16` (catches the bug of passing the
  `0` an encoder returned on failure); `more != 0` adds `MSG_MORE`.
- `recv_msg(fd, &h, &payload, &wire)` (`:815`): `recvall` 16 bytes, `hdr_decode`, `malloc`
  `payload_len`, `recvall` the payload. Return values: `NET_OK`; `NET_EOF` only if the peer closed
  BETWEEN messages (zero bytes of a header); `NET_ERR` for errors or EOF inside a message (sets
  `errno = ECONNRESET`); `NET_PROTO` for a bad header. `*wire` accumulates received bytes even
  when the read fails. `recv_msg` is what the CLIENTS use to read server replies. The SERVERS no longer
  call it for requests: they use `conn_recv_request` (section 3.7), which adds an absolute deadline and
  a much smaller payload cap.

**Tricky details.**

- EOF at a message boundary is "normal", EOF inside a message is a failure. The server relies on
  this: the Enhanced FETCH loop treats `NET_EOF` as "client's queue is empty" (`Part2_Case2_Enhanced/server.c:343`).
- The free-the-payload convention: `*payload` is set to NULL on entry (`protocol.c:821`), so
  `free(*payload)` is safe on every path.
- Validation is symmetrical: encoders and decoders apply the same checks, so a bug on the sending
  side is caught where it happens.

### 3.4 log (`log.h`, `log.c`)

**Responsibility.** Human-readable stderr lines and optional raw CSV for the performance analysis.
Controlled only by environment variables (Deviation 8: the guidelines forbid hard-coded paths):
`FDS_LOG`, `FDS_EVENTS`, `FDS_RUN_ID`, `FDS_CLIENT_ID`, `FDS_QUIET`.

**Key data structures.** `log_ctx_t` (role, case name, mode, p, seed); three row structs:
`log_conn_row_t` (server, one per connection), `log_client_row_t` (client, one per run), plus the
event arguments of `log_event` (FAULT, RESUME_OK, ...).

**Key functions.**

- `log_init` (`log.c:57`): copies the context, reads the env vars, opens the CSV files with
  `O_WRONLY | O_CREAT | O_APPEND` (`open_csv`, `:34`). `FDS_EVENTS` is opened only for the server.
- `append_row` (`log.c:44`): under `flock(LOCK_EX)`, writes the header if the file is empty, then the
  row. Several processes can append to the same CSV (all clients of a run on one machine).
- `log_conn` (`:159`), `log_event` (`:184`), `log_client` (`:208`): format one row with `snprintf` into a
  1024-byte buffer and call `append_row` under the in-process mutex `g_mu`.
- `log_info` (`:100`) honours `FDS_QUIET`; `log_warn` (`:111`) always prints. `vlog` (`:87`) builds the
  whole line first and prints it under `g_mu`, so lines from several threads do not interleave.
- `csv_str` (`:129`) quotes topic names (they may contain commas) and doubles quotes.

**Column lists** are in `log.h:14-21` and `log.c:25-32`; see section 6 for their meaning.

**Tricky details.**

- Two layers of locking: `g_mu` (threads of one process) and `flock` (processes on one machine).
  `O_APPEND` makes each write land at the end even without the lock.
- Time columns are `CLOCK_MONOTONIC` ns (comparable between processes on the same machine) and
  `*_wall_ns` `CLOCK_REALTIME` ns.
- Nothing is written unless the env var is set; otherwise only the summary goes to stderr.

### 3.5 topic_map (`topic_map.h`, `topic_map.c`)

**Responsibility.** The server's map: topic key -> absolute directory -> sorted file list
(design §2). Built once at start-up; extended (never modified) when a new folder appears.

**Key data structures.**

```c
typedef struct topic {            /* topic_map.h:28 */
    char key[64]; char abs_path[PATH_MAX];
    file_entry_t *files;          /* sorted by name (strcmp) */
    uint32_t n_files; uint64_t total_bytes, total_chunks;
} topic_t;
typedef struct {                  /* topic_map.h:37 */
    topic_t **topics;             /* array of POINTERS, sorted by key */
    size_t n_topics; char root[PATH_MAX];
    pthread_rwlock_t lock;        /* guards topics and n_topics only */
    pthread_mutex_t rescan_mu;    /* serialises re-scans, guards last_rescan_ns */
    int64_t last_rescan_ns;
} topic_map_t;
```

**Key functions.**

- `topic_map_load` (`topic_map.c:432`): `realpath(root)`, init the locks, `scan_root`, install the array,
  back-date `last_rescan_ns` so the first request for a new topic is not rate-limited (`:482`).
- `scan_root` (`:327`): reads `root`; skips names beginning with `.`, non-directories, invalid topic names,
  and keys already present (`key_present`, `:155`); calls `load_topic`; sorts the new topics by key and
  drops duplicates.
- `load_topic` (`:181`): `realpath` of the folder, `readdir` it, skips unsafe names (`name_is_safe`) and
  non-regular files (`fstatat` follows symlinks; FIFOs are skipped because opening one would block a
  worker), records `{name, st_size}`, **sorts by name with `qsort`/`strcmp`** (`:296`), computes
  `total_bytes` and `total_chunks`. A topic whose folder cannot be read completely is skipped as a whole,
  because serving a partial list would let a client "succeed" with files missing.
- `topic_find` (`:499`): read lock; exact `bsearch` first (`find_exact`, `:139`), then a case-insensitive
  linear scan with `strcasecmp` (the assignment's table writes "Cane", the folder is "cane").
- `topic_find_or_rescan` (`:583`): `topic_find`; on a miss, if the key is valid, take `rescan_mu`, and re-scan
  at most once per `FDS_RESCAN_MIN_MS` (1 s), then look again.
- `rescan_locked` (`:522`): scan outside the rwlock; build a NEW merged array (`malloc`, `memcpy` old
  pointers + new pointers, `qsort`); take the WRITE lock for just two stores (`m->topics`, `m->n_topics`);
  then free the OLD POINTER ARRAY only.
- `topic_file_index` (`:618`): `bsearch` by name -> index.
- `topic_map_free` (`:633`): only at exit.

**The pointer-stability rule (Deviation 3).** The design said "array of topics under an rwlock; a re-scan
rebuilds it". That would let a re-scan `realloc` or reorder `topic_t` structs that worker threads are using
mid-transfer. So: every `topic_t` is `calloc`'d once, completely filled while still private to the building
thread, and **never moved, modified or freed until process exit**. The map holds an array of *pointers* to
them. A re-scan only adds topics: it builds a new pointer array and swaps it in. Consequences:

- A worker may keep `const topic_t *` for a whole transfer without any lock (it is just a pointer to
  immutable data).
- Sessions also keep `const topic_t *`, and pointer equality is an exact "same topic" test (`session.c:216`).
- The rwlock is held for one `bsearch` (readers) or two stores (the writer), never across disk or network
  I/O, so a re-scan never waits for a long transfer (a reader-preferring rwlock would otherwise starve the
  writer).
- Freeing `old` after the swap is safe: readers touch `m->topics` only under the read lock, and the write lock
  waited for all of them to leave (`:564-570`).
- A consequence worth knowing: files added to an EXISTING topic after load are not picked up. A topic is an
  immutable snapshot. `send_file_range` therefore uses the size from the map, not `fstat` (`transfer.c:26-28`).

**Other tricky details.**

- Why sort files by name: `readdir` order is arbitrary, and Case 2/Enhanced identify a file by its index. The
  index must mean the same file in every `MANIFEST`, `RESUME`, `FETCH` (`topic_map.c:293`).
- Why `topic_find_or_rescan` checks `topic_is_valid` before taking the mutex (`:591`): a key that no folder could
  have costs no disk access, so a flood of garbage keys cannot make the server hammer the disk.
- Lock failure aborts (`LOCK_OK`, `:93`): carrying on without mutual exclusion would be worse.

### 3.6 transfer (`transfer.h`, `transfer.c`)

Two halves: server streaming and client receiving.

#### Server side

**Key data structures.** `src_t` (`transfer.c:76`): the open file descriptor, whether `sendfile` is still in use,
whether it ever worked, and a lazily allocated fallback buffer. `xfer_rc_t` (`transfer.h`): `XFER_OK`, `XFER_FAULT`,
`XFER_PEER`, `XFER_IOERR`.

**Key functions.**

- `serve_topic(c, t, session_id, from_idx, from_off, send_begin)` (`transfer.c:344`): validates the resume point
  (`from_idx == n_files` is allowed); optionally sends `TOPIC_BEGIN` with `MSG_MORE` (`:367`); loops
  `send_file_range` from `from_idx` (first file at `from_off`, later ones at 0); sends `TOPIC_END` (no `MSG_MORE`).
- `send_file_range(c, t, idx, off)` (`:268`): checks the range; builds the path as `abs_path/name` (never a
  client-supplied string); `open(O_RDONLY|O_CLOEXEC)`; encodes `FILE_HDR` with `start_offset = off`; sends it
  with `MSG_MORE` (`:313`); calls `progress_hook(c, idx, off, 0)` right after the header (`:320`); runs
  `send_chunks`; on success sends `FILE_END` WITHOUT `MSG_MORE` (flush now, `:326`); closes the file and frees
  the fallback buffer on every path (`:335-337`).
- `send_chunks` (`:234`): the heart of the server. For each chunk of at most 64 KB:
  1. `c->fault_hook(c->fault_state, n, &prefix)` (`:246`). Returns 1 with a prefix `< n` on a fault. A NULL hook
     (Part I) means no draw.
  2. `send_data` for `n` bytes (or `prefix` bytes if faulted).
  3. `account(...)` the bytes ACTUALLY accepted by the kernel (`:257`), even if the send failed half-way.
  4. On error, `data_failed` (`:193`). On a fault, `inject_fault` (`:215`): logs the `FAULT` event, sets
     `faulted=1`, `status="FAULT"`, calls `conn_abort` (shutdown + close), returns `XFER_FAULT`.
- `send_data` (`:132`): the sendfile loop. `sendfile(c->fd, s->in, &o, left)` with an explicit offset `o`, so the
  file's own position is untouched and resuming at any offset is natural. A partial return is normal: advance
  `*pos` and `*sent`, loop. `EINTR` -> retry. `EINVAL`/`ENOSYS` on the FIRST call -> switch to the fallback for this
  file (`:147`). `r == 0` -> `SEND_SHORT` (file shrank). `EAGAIN` -> `SO_SNDTIMEO` expired -> peer lost.
- `copy_piece` (`:95`): fallback `pread` + `send(MSG_NOSIGNAL)` loop. It uses `send` directly instead of `sendall`
  because a failed `sendall` does not say how much was accepted.
- `account` (`:175`): adds to `data_bytes` AND `wire_bytes`, counts a chunk, calls the progress hook.

**Tricky details.**

- *The fault hook and the sendfile chunk loop.* The draw happens BEFORE a chunk. If faulted, only the random
  prefix is sent through the same `send_data`, then the connection is cut. So the cut can land at any byte of the
  chunk (Deviation 9), `data_bytes` stays exact (it counts what `sendfile` returned), and the client has exactly
  those bytes in its `.part`.
- *Prefix is always less than the chunk* (`:250`): at least one byte is lost, so every fault really forces a
  recovery. That is also why p = 1 can never finish.
- *Why the progress hook fires even for an empty prefix* (`:257`, `faulted && rc == SEND_OK`): the Case 2 session
  must always know "how far was handed to the socket" so a correct RESUME is not wrongly rejected (check 5).
- *SIGPIPE with sendfile*: see `net` above; `transfer.c:13-16` says every process that calls `send_file_range` must
  ignore SIGPIPE.
- *Error semantics in mid-file*: if the file cannot be read after FILE_HDR went out, the client is in the middle of
  raw bytes, so ERROR would be read as data. `data_failed` closes the connection instead and sets `ERR_500`.
- *Size from the map*: the announced `file_size` must match what is sent; a file that shrank gives `SEND_SHORT`.

#### Client side

**Key data structures.** `rx_ctx_t` (`transfer.h`): `fd`, `dir`, mode (`RX_DIRECT` or `RX_PART`), cumulative counters
`data_bytes`, `wire_bytes`, `files_done`, `next_index`, `stored_bytes`, the optional `on_file_hdr` hook (Case 2 saves
`.session` there), and room for `err` / `reject_reason`. `rx_rc_t`: `RX_OK`, `RX_LOST`, `RX_PROTO`, `RX_IOERR`,
`RX_SERVER_ERR`, `RX_REJECT`.

**Key functions.**

- `recv_expect(rx, want, &payload, &len)` (`transfer.c:397`): `recv_msg`; EOF/error -> `RX_LOST`; bad header ->
  `RX_PROTO`; `ERROR` -> `RX_SERVER_ERR`; `RESUME_REJECT` -> `RX_REJECT`; a type other than `want` -> `RX_PROTO`.
- `recv_file(rx, &hdr)` (`:497`): validates the header; calls `on_file_hdr`; opens the target:
  - `RX_DIRECT` or `start_offset == 0`: `open(O_TRUNC)` of `<name>` (direct) or `<name>.part`;
  - else `reopen_part` (`:454`): the `.part` must exist and have at least `start_offset` bytes; it is
    `ftruncate`d to exactly `start_offset` and the file position set there. If no `.part` exists but the final file
    is complete with that size, return "-2" (only `FILE_END` remains);
  - then loops `recv(rx->fd, buf, min(64 KB, remaining))` and `write_all` (`:431`) each piece BEFORE the next `recv`;
  - then `recv_expect(MSG_FILE_END)`, checks the index, `close` (checked: write-back errors show up here), and in
    `RX_PART` mode `rename(.part, final)` (atomic).
- `recv_topic(rx, &end)` (`:600`): loop over messages: `FILE_HDR` (index must equal `next_index`, then `recv_file`),
  `TOPIC_END` (done), `ERROR`; anything else is `RX_PROTO`. EOF before `TOPIC_END` -> `RX_LOST`.

**Tricky details.**

- *Why `recv` and not `recvall` in the data loop.* `recvall` would wait for the whole 64 KB buffer before returning;
  we want to write each piece as it arrives so the `.part` size equals the bytes received at every instant. That
  property is the checkpoint (see `checkpoint`).
- *Why a final name appears only after `FILE_END`.* A file with its final name is always complete, so after a
  crash a complete `<name>` is trustworthy and a `<name>.part` is a partial.
- *No `fsync`.* Deliberate, for speed (`transfer.h`, `checkpoint.c:112-120`): the fault model is a lost connection or
  a killed process, and the kernel's page cache survives both. Only an OS crash or power loss could lose data, which is
  outside our fault model.
- *Files must arrive in index order* (`recv_topic`, `:628`); this is what makes "next index + `.part` size" a complete
  checkpoint in Case 2.

### 3.7 server_core (`server_core.h`, `server_core.c`)

**Responsibility.** The part of the server that is identical in all four versions (Deviation 14): listening,
accept loop, single vs multi scheduling, the 256-thread cap, the signal thread and graceful shutdown, the per-connection
CSV row. Each `server.c` supplies `main()` and `serve_client(conn_t *)`.

**Key data structures.**

- `conn_t` (`server_core.h`): everything a connection needs: `fd`, `conn_id`, `peer`, `map`, `cfg`, start times, `kind`,
  `status` (default "OK"), `topic`, `session_id`/`have_session`, `k`, byte counters (`data_bytes`, `wire_bytes`, `chunks`,
  `files_completed`, `faulted`), the hooks (`fault_hook`, `fault_state`, `progress_hook`) and a `user` pointer. It is
  `calloc`'d on the main thread (`server_core.c:446`) and owned by exactly one thread afterwards, so it needs no lock.
- `server_cfg_t`: port, root, `multi`, `p` (-1 in Part I), seed, `case_name`, `serve_client`.
- Globals (`server_core.c:70-84`): `g_mu` + `g_cv` (condition variable on `CLOCK_MONOTONIC`), `g_active`, `g_main_done`,
  `g_listen_fd`, the totals, atomics `g_stopping` and `g_next_conn_id`, the map `g_map`, thread attribute `g_detached`.

**Key functions.**

- `server_run(cfg)` (`:657`): `log_init`; `signal(SIGPIPE, SIG_IGN)` (`:680`); **block SIGINT/SIGTERM in the main thread
  BEFORE any thread is created** (`pthread_sigmask`, `:685`) so every thread inherits the mask; `init_state`; `topic_map_load`;
  `tcp_listen`; create the signal thread; `accept_loop`; then the main-thread part of shutdown; `pthread_join` of the signal
  thread; cleanup.
- `accept_loop` (`:508`): `accept4(SOCK_CLOEXEC)`; on success `handle_new_conn`. On failure: if `g_stopping` -> break (the
  normal way out); `EINTR`/`ECONNABORTED` and pending network errors -> continue; `EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM` ->
  warn and sleep 100 ms (so it does not spin at 100% CPU); `EBADF`/`EINVAL`/... -> fatal.
- `handle_new_conn` (`:432`): sets 30 s send/receive time-outs, `TCP_NODELAY`, optional buffer sizes; allocates the `conn_t`;
  `conn_id = atomic_fetch_add(&g_next_conn_id, 1)`; records `start_ns` (the moment of accept). Then:
  - **single mode** (`cfg->multi == 0`, `:465`): `g_active++` and `run_conn(c)` INLINE on the main thread, so the next `accept()`
    happens only after this client is completely served;
  - **multi mode**: reserve a slot under `g_mu` (`g_active < FDS_MAX_THREADS`, `:481`), then `pthread_create` a detached thread
    running `conn_thread` -> `run_conn`. If the cap is reached or `pthread_create` fails -> `refuse_busy`.
- `refuse_busy` (`:413`): sends `ERROR 503 "server busy"`, `shutdown(SHUT_WR)`, then drains whatever request bytes already arrived
  (non-blocking, up to 8 reads) before `finish_conn`.
- `conn_recv_request(c, &h, &payload, &wire)` (`:346`): THE way every server reads a client request (GET, RESUME,
  MANIFEST_REQ, FETCH); `recv_msg` is not used by servers any more. Two protections (Deviation 25):
  1. *Absolute deadline.* `deadline = now_ns() + FDS_SRV_TIMEOUT_S` (30 s) is computed once and the WHOLE request
     (header + payload) must arrive before it. `read_by_deadline` (`:315`) loops on `wait_readable` (`:276`, a
     `poll()` on the time left) and a `MSG_DONTWAIT` `recv`. `SO_RCVTIMEO` alone is not enough: it restarts on
     every byte, so a peer sending one byte every 29 s would hold a thread for days (and, in `single` mode, the
     whole server).
  2. *Small, typed requests.* After the 16-byte header is decoded, anything that is not one of the four request
     types, or has `payload_len > FDS_MAX_REQUEST_PAYLOAD` (512), returns `NET_PROTO` BEFORE `malloc`; the caller
     answers `ERROR 400`. A hostile 64 MiB length therefore allocates nothing.
  Return values: `NET_OK`; `NET_EOF` (clean close before a header); `NET_ERR` (`errno` = `EAGAIN` for the deadline,
  `ECONNRESET` for a close mid-message); `NET_PROTO`.
- `run_conn` (`:392`): calls `serve_client(c)`; if `c->status` is still "OK", `drain_until_peer_closes`; then `finish_conn`.
- `finish_conn` (`:215`): writes the CSV row (`log_conn`), closes the socket if still open, updates the totals under `g_mu`,
  decrements `g_active`, **broadcasts** `g_cv`, frees the `conn_t`.
- `conn_abort` (`:149`): `shutdown_close` and set `fd = -1`, so a second call and the later `close` in `finish_conn` are harmless.
- `conn_send` (`:161`): `send_encoded` then `c->wire_bytes += len` (counted only if the whole message went out).
  `conn_send_error` (`:189`): encodes and sends an ERROR and sets `c->status` through `status_for_code` (`:178`).
- `signal_thread` (`:560`): see below.

**The 256-thread cap and ERROR 503.** `FDS_MAX_THREADS = 256`. The count `g_active` is incremented under `g_mu` BEFORE the thread
is created, so it can never overshoot. The 257th simultaneous client gets `ERROR 503` and its connection is closed by the main
thread; the CSV status is "BUSY". The Part II clients treat 503 as a retriable failure (Case 1 `classify`, `Part2_Case1/client.c:264`;
Case 2 `failure_outcome`, `Part2_Case2/client.c:478`). The session table has the same idea: when all 256 slots hold fresh ACTIVE
sessions, `session_create` fails and the server answers 503 "session table full".

**Graceful shutdown (sigwait + shutdown() of the listener).**

1. SIGINT and SIGTERM are blocked in every thread, so the kernel can deliver them only to the thread sitting in `sigwait()`.
   The shutdown logic is ordinary code in that thread, NOT an async-signal handler (where locks, `malloc` and stdio are forbidden).
2. `signal_thread` (`:560`) wakes, sets `g_stopping`, and under `g_mu` calls `shutdown(g_listen_fd, SHUT_RDWR)` (`:593`). On Linux that
   makes the `accept()` blocked in the main thread fail (`EINVAL`), and new connections are refused. `g_listen_fd` is read under
   `g_mu`, and the main thread closes it under `g_mu` too, so the signal thread never shuts down a descriptor number that was closed
   and reused.
3. The main thread leaves `accept_loop`, closes the listener, and waits on `g_cv` until `g_active == 0` or `FDS_SHUTDOWN_GRACE_S` (5 s)
   (`:719-733`). It logs a summary line, `log_close()`, frees the map (only if no worker is left), sets `g_main_done`, broadcasts, and
   returns exit code 0.
4. The signal thread waits for `g_main_done` up to 6 s. If it is not set (single mode in the middle of a long transfer: the main thread
   IS the worker) it logs, `log_close()`s and `_exit(0)`s so a shutdown never hangs (`:604-611`).
5. If the accept loop ends by itself (fatal accept error), `server_run` wakes the signal thread with `pthread_kill(sig_tid, SIGTERM)`
   (`:757`) so `pthread_join` does not hang. SIGINT/SIGTERM stay blocked on purpose.
6. The condition variable uses `CLOCK_MONOTONIC` (`pthread_condattr_setclock`, `:633`) so a wall-clock change cannot stretch the grace.
7. The Enhanced server's FETCH loop checks `server_stopping()` between files: "finish the file in flight, take no new one"
   (`Part2_Case2_Enhanced/server.c:339`).

**The drain before `end_ns`.** `drain_until_peer_closes` (`:292`) runs after a normal end of service (status "OK"): `shutdown(SHUT_WR)` (we
are done sending), then wait with `wait_readable` and a non-blocking `recv` until it returns 0 (the client closed), data/an error, or the
deadline. The 5 s deadline is ABSOLUTE (`now_ns() + FDS_SHUTDOWN_GRACE_S`, computed once), so a peer trickling bytes cannot stretch it (it
would otherwise hold the single-mode server's only thread). Why (Deviation 21):
`send`/`sendfile` return when the kernel has QUEUED the data, not when the client has received it. If `end_ns` were taken right after the
last `send`, the last few MB still in the socket buffers would be outside the measured interval and the throughput would be inflated
(about 10% at N = 1). Every client closes right after its last message, so the wait is about one round trip; the 5 s absolute cap bounds a misbehaving
client. It also guarantees that the final `close()` is a clean FIN with no unread data (never an RST). `finish_conn` takes `end_ns = now_ns()`
only afterwards (`:227`).

**Other tricky details.**

- Detached threads: nobody joins workers; the shutdown waits on the `g_active` counter instead.
- Why `g_next_conn_id` is atomic: it is read without the lock; also the fault seeding uses `conn_id`.
- The `_Static_assert` checks in each server.c make sure `conn_t.topic` and `topic_t.key` have the same size, so `memcpy` is in bounds.
- `refuse_busy` drains the unread request before closing: closing a socket with unread data makes Linux send an RST, which can discard the
  ERROR that was just queued.

### 3.8 fault (`fault.h`, `fault.c`) - Part II servers only

**Responsibility.** Decide, once per connection, whether and where to cut it (Deviation 28).

**Key data structure.** `fault_state_t` (`fault.h:41`): `p`, a `struct drand48_data rng`, the plan (`planned`, `doomed`, `cut_at`), the
byte counter `pos` and two counters. One per connection, on the worker thread's stack (`case1_serve`, `Part2_Case1/server.c:177`;
`serve_client`, `Part2_Case2/server.c:254`; `enhanced_serve`, `Part2_Case2_Enhanced/server.c:411`), installed in `conn_t.fault_hook` /
`fault_state`, and cleared before the function returns so nothing can reach the dead stack frame.

**Key functions.**

- `fault_init(f, p, have_seed, seed, conn_id)` (`fault.c:101`): seeds the 48-bit state with `seed48_r`. No decision yet.
- `fault_plan(f, bytes)` (`fault.c:140`): the connection's ONE decision, called by the server when it accepts the first data request
  and knows how many file bytes it will send for it:
  - does nothing if already planned or if `bytes == 0` (a 0-byte request leaves the decision to the next one);
  - `!(p > 0)`: planned, never cut, no draw (so p = 0 behaves exactly like Part I; the form also treats NaN as 0);
  - `drand48_r(&rng, &r)`: uniform `r` in [0,1); cut iff `r < p`;
  - a SECOND draw `r2` picks `cut_at = floor(r2 * bytes)`, clamped to `bytes - 1`: a uniform byte of the request.
- `fault_should_fail(state, chunk_len, &prefix)` (`fault.c:175`): the hook, called before every chunk. It counts the bytes offered so
  far (`pos`); for the chunk that contains `cut_at` it returns 1 with `*prefix = cut_at - pos`. At most one fault per connection.
- `mix64` (`fault.c:91`): the splitmix64 finaliser (a bijection where every input bit affects every output bit).

**When and how (what to say if asked "document when and how the failure decision is made").**

- WHEN: once per connection, when its first data request is accepted: Case 1 `GET` -> the whole topic (`Part2_Case1/server.c:162`);
  Case 2 `GET` -> the whole topic (`Part2_Case2/server.c:170`), `RESUME` -> the rest of the topic from the checkpoint
  (`bytes_from`, `Part2_Case2/server.c:243`); Enhanced: the connection's first `FETCH` -> the rest of that file
  (`Part2_Case2_Enhanced/server.c:319`). Headers and control messages never get a decision. A reconnection is a new connection
  with its own generator and a fresh decision.
- HOW: one uniform draw, cut iff `r < p`, so p is the probability that a connection is interrupted. The cut byte is uniform in the
  request's data. `send_chunks` (`transfer.c:249`) asks the hook before every chunk; the hook says which chunk holds the cut and how
  many of its bytes still go out. Then: log `FAULT`, `shutdown(SHUT_RDWR)` + `close()`, end that connection's thread; the server
  keeps running.
- Why per connection and not per chunk (the design's choice): the guidelines define p as "how frequently the client-server
  session is interrupted". Per chunk, a Case 1 attempt survives with probability `(1 - p)^N`; for gatto (N about 1 800) Case 1 gave
  up at every p >= 0.01, so an automated test at p = 0.1 would fail Case 1. Per connection, every case finishes for any p < 1.
- Why Enhanced cuts in the FIRST FETCH: the server cannot know which later FETCHes the client will send on that connection, so it
  places the cut in the one request it knows. Each of the K connections is cut with probability p.

**drand48_r and hashed seeding (Deviation 20).**

- `rand()` and `drand48()` keep hidden global state shared by all threads (not thread-safe, and one thread's draws would shift another's).
  `drand48_r` keeps all its state in the struct we pass, so each connection has its own private generator and no locks are needed.
- Seeding: the design said `srand48_r(seed ^ conn_id)`. Problem found in our emulated dry run: the repetitions use seeds 1001, 1002, ..., which differ
  only in low bits, so `{1001 ^ c}` and `{1002 ^ c}` over conn_ids 1..32 share 30 of 32 values. Two "independent" repetitions replayed the same fault
  sequences in a different order (both Case 1 runs at p = 0.01 gave exactly 28 faults). The code now uses
  `h = mix64(mix64(seed) ^ conn_id)` and loads three 16-bit words of `h` into `seed48_r` (`fault.c:109-114`). Same `seed` -> same fault positions
  (repeatable); different seeds or conn_ids -> unrelated sequences.
- Without a seed: wall time, thread id, conn_id and monotonic time are each mixed in with the same hash (`fault.c:126-130`).

**Tricky details.** The cut byte depends only on the two draws and the request size, not on how the data is split into chunks
(a unit test checks 64 KB chunks against 333-byte chunks). `cut_at < bytes` guarantees that at least one byte of the request is lost,
so every fault really forces a recovery; at p = 1 every connection is cut before its last byte, so nothing can finish.
`_Static_assert(__builtin_types_compatible_p(...))` (`fault.c:84`) proves at compile time that `fault_should_fail` has the hook's type.

### 3.9 session (`session.h`, `session.c`) - Case 2 and Enhanced servers

**Responsibility.** The server-side session table: lets a NEW connection resume work started on an OLD one, and validates the client's checkpoint.

**Key data structures** (`session.c:31-49`).

```c
typedef struct { uint8_t id[16]; const topic_t *topic; sess_state_t state;  /* FREE/ACTIVE/INTERRUPTED/DONE */
                 int enhanced; int adopted;
                 uint32_t next_file; uint64_t file_off_sent;   /* Case 2: position of the single stream */
                 uint64_t total_sent; uint32_t reconnects; int64_t last_seen_ns;
                 uint64_t *sent_off; } slot_t;                  /* Enhanced: highest offset of file i handed to any socket */
static slot_t slot[FDS_MAX_SESSIONS];  static pthread_mutex_t table_mu;
```

One static array of 256 slots behind ONE mutex. No pointer to a slot ever leaves the lock: all functions take an id and finish inside the critical section.

**Key functions.**

- `session_table_init` (`:145`), `session_create(t, enhanced, id_out)` (`:156`): picks a slot, generates a random 128-bit id (never all zero, never a duplicate).
  Returns -1 if the table is full -> the server sends ERROR 503.
- `pick_slot_locked` (`:82`) (Deviation 12): 1. a FREE slot; 2. the oldest DONE slot; 3. any slot idle longer than 300 s (even an ACTIVE one: a live
  connection updates `last_seen` after every chunk and the server times silent peers out after 30 s, so such a slot belongs to a dead thread); 4. the oldest
  INTERRUPTED. A fresh ACTIVE slot is NEVER evicted.
- `session_validate_resume(id, t, file_index, name, offset, enhanced)` (`:250`): the checkpoint validation (below). Returns 0 = OK, 1..5 = reject reason,
  -1 = table full when adopting.
- `session_progress(id, file_index, new_off, n)` (`:330`): called by the progress hook after each chunk. Case 2: set `(next_file, file_off_sent)` (the latest
  call IS the stream position). Enhanced: `sent_off[file] = max(sent_off[file], new_off)`. Also `total_sent`, `last_seen`; INTERRUPTED -> ACTIVE.
- `session_set_state` (`:376`), `session_all_sent` (`:405`: Enhanced, true when every `sent_off[f] >= size`), `session_get_info` (`:428`), `session_count` (`:446`).

**The 5 validation checks (design §5.5; `session.h` comment, implemented in `check_static_locked` `:204` and `ahead_of_sent` `:242`).**

| # | Check | Reject reason |
|---|---|---|
| 1 | topic exists; if the session is known it belongs to this topic (and is the same kind: Case 2 vs Enhanced) | 1 unknown topic |
| 2 | `file_index < t->n_files` | 2 index out of range |
| 3 | `name == t->files[file_index].name` (detects a changed folder) | 3 name mismatch |
| 4 | `offset <= t->files[file_index].size` | 4 offset beyond file size |
| 5 | known (non-adopted) session only: checkpoint not ahead of what the server sent. Case 2: `file_index < next_file`, or `file_index == next_file && offset <= file_off_sent`. Enhanced (Deviation 11): `offset == 0 \|\| offset <= sent_off[file_index]` | 5 ahead of data sent |

Why check 5 is sound: the server's record is updated after every send and a byte cannot reach the client before it is sent, so the client's checkpoint can
never legitimately exceed it. One that does comes from a corrupted or unrelated `.session`.

**Adopted sessions (Deviation 22).** If the id is unknown (server restarted, or the slot was evicted/expired), checks 1-4 are enough to make the resume safe
(they prove the checkpoint names a real byte position of a real file of this topic). The server then creates a new ACTIVE slot under the CLIENT's id and marks
it `adopted = 1` (`:281-299`). An adopted session skips check 5 permanently (`:273`), because the server has no record of what was sent before. For Enhanced
this matters: a client may legitimately hold a `.part` of a different file whose `sent_off` is 0 in the new record, and check 5 would wrongly throw it away.
A rejected checkpoint only causes a fresh start, never wrong data, so skipping check 5 is safe. After a successful validation the progress is set to the checkpoint:
Case 2 `next_file = file_index, file_off_sent = offset, reconnects++` (record moves even backwards if needed); Enhanced `sent_off[file] = max(.., offset)`.

**Tricky details.**

- The session pointer `slot->topic` is a `const topic_t *`; pointer equality is the "same topic" test (works because of Deviation 3).
- Memory: the `sent_off` array is allocated before locking in `session_create` and freed after unlocking (`:169`, `:193`), so the allocator does not run inside
  the critical section. `fill_slot_locked` returns the old array for the caller to free.
- The Enhanced MANIFEST creates the session INTERRUPTED until the first FETCH arrives (`Part2_Case2_Enhanced/server.c:198`), so an unused session does not pin a slot.
- A session released with `SESS_FREE` frees its array (`:388`).

### 3.10 checkpoint (`checkpoint.h`, `checkpoint.c`) - Case 2 and Enhanced clients

**Responsibility.** The client's authoritative record of progress. What it contains, in `output_dir/<topic>/`:

- `<name>.part` - the partial data. **Its size IS the byte offset** of the checkpoint (every received byte is written before the next `recv`).
- `<name>` - a completed file (renamed from `.part` on `FILE_END`).
- `.session` - a small text `key=value` record: `session=<32 hex>`, `topic=<as typed>`, `file_index=<n or -1>`, `file_name=<name>`, optionally `k=<K>`.
  The offset is deliberately NOT stored.

**Key functions.**

- `ckpt_save(dir, ck)` (`checkpoint.c:76`) - **atomic write via rename**: writes the whole record to `.session.tmp` with `fopen`/`fprintf`, `fclose` (checked:
  a full disk shows up when stdio flushes), then `rename(".session.tmp", ".session")`. `rename` replaces the target atomically, so a crash at any instant leaves
  either the complete old record or the complete new one, never a torn mix that could point at the wrong file. On failure the tmp file is unlinked. `ckpt_valid`
  (`:62`) refuses to write anything `ckpt_load` would reject (a name with a newline would otherwise inject a line).
- `ckpt_load(dir, ck)` (`:197`) - strict: returns 0 (loaded), 1 (none), -1 (corrupt). Rejects files over 4096 bytes, NUL bytes, malformed lines, repeated keys,
  an invalid topic or name, and a record missing `session` or `topic`. Parses into a local copy so `*ck` changes only on success. Unknown keys are skipped
  (forward compatible). A line is split at its FIRST `=` (file names may contain `=`).
- `ckpt_clear` (`:250`): unlinks `.session` and `.session.tmp`. `part_size`/`final_size` (`:288`, `:293`): `stat` of `<name>.part` / `<name>`, with
  `name_is_safe` and `S_ISREG` checks. `remove_partials` (`:298`): unlinks all `*.part` files and the checkpoint (fresh start after `RESUME_REJECT`); completed
  files stay and are overwritten later.

**Who is authoritative?** The client, for the stored-byte offset. The server knows the bytes it handed to its socket, not the bytes that reached the client's
disk. The server keeps its own record only to *validate* (checks 1-5) and to reject an impossible checkpoint. See Q15 for the trade-off table.

**Tricky details.**

- No `fsync` before the rename: speed; the fault model is a lost connection/killed process, not an OS crash (`checkpoint.c:112-120`).
- The record is written on `TOPIC_BEGIN` (`file_index = -1`) and on each `FILE_HDR` via the `on_file_hdr` hook, BEFORE the file's first byte is written, so it
  always names the file the `.part` belongs to (`Part2_Case2/client.c:269`). When resuming the same file nothing is rewritten (`:277`).
- Not thread-safe per directory (fixed tmp name). That is fine: only one thread writes the record (Case 2: the only thread; Enhanced: the manifest step before
  the workers start).

### 3.11 workq (`workq.h`, `workq.c`) - Enhanced client

**Responsibility.** The shared queue of `(file index, start offset)` items that K workers consume.

**Key data structure** (`workq.c:26`): a ring buffer `items[cap]` with `head`, `count`, plus `busy` (items popped and not yet `workq_done`), `aborted`,
one mutex, one condition variable.

**Key functions.**

- `workq_create(capacity)` (`:37`) - capacity is the file count; a re-queue always fits because an item is only re-queued by the worker holding it.
- `workq_push(q, idx, off)` (`:77`): enqueue (initial fill, or re-queue). `pthread_cond_signal` (one item can satisfy one waiter).
- `workq_pop(q, &out)` (`:107`) - **blocking**: `while (count == 0 && busy > 0 && !aborted) cond_wait`. Returns 1 with an item (caller counted busy),
  or 0 when (aborted) or (queue empty AND nobody busy = topic complete). When complete it also broadcasts so every waiting worker wakes and exits.
- `workq_trypop` (`:134`) - non-blocking, for the poll engine, which owns all sockets in one thread and must never sleep on the queue.
- `workq_done(q)` (`:148`): `busy--` (guarded against underflow) and **broadcast** (if busy just reached 0 with an empty queue, EVERY waiter must wake and see completion;
  a single signal would leave the others blocked forever).
- `workq_abort` (`:164`): set `aborted`, broadcast; every later pop returns 0. `workq_outstanding` (`:182`) = `count + busy` (0 = complete).

**The "busy" counter - why it prevents early exit.** An empty queue does NOT mean the topic is complete: a worker holding an item may still lose its connection and
push the item back. If idle workers quit as soon as the queue were empty, the re-queued file might be left with fewer (or no) connections to fetch it, or the
program could report completion early. So `pop` waits while the queue is empty but someone is busy, and reports "complete" only when the queue is empty AND `busy == 0`:
then nobody is left who could add work.

**Order matters on the failure path: push BEFORE done** (`Part2_Case2_Enhanced/client.c:567-574`, `client_poll.c:329-332`). In between, the item is either queued or
busy, so `count + busy` never drops to 0 while a file is missing and no other worker can see "complete" too early.

---------------------------------------------------------------------------

## 4. Life of a request, per case

Notation: `S:` server function, `C:` client function.

### 4.1 Part I: GET, single mode and multi mode

Common to both modes:

1. `C: main` (`Part1/client.c:345`) parses args (`parse_args`, `:102`), `mkdir_p output_dir/<topic>` (`make_output_dir`, `:124`), starts the clock
   (`start_ns`, just before `connect`).
2. `C: fetch_topic_when_free` (`:329`) calls `fetch_topic` (`:290`) in a loop (see "503 retry" below): `connect_to_server` (`:138`: `tcp_connect` with 10 s connect time-out, `TCP_NODELAY`; NO receive time-out, Deviation 1) ->
   `send_get` (`:153`: `enc_get`, `send_encoded`).
3. `S: accept_loop` (`server_core.c:508`) -> `handle_new_conn` (`:432`) sets timeouts/NODELAY, allocates `conn_t`.
4. `S: part1_serve` (`Part1/server.c:130`): `read_get_request` (`:73`: `conn_recv_request`, 30 s absolute deadline, <= 512 bytes, must be `MSG_GET`, `dec_get`; otherwise `ERROR 400`), `topic_find_or_rescan` (`:143`; unknown
   -> `ERROR 404`), copy the canonical key into `c->topic`, `serve_topic(c, t, zero_sid, 0, 0, 1)` (`:151`).
5. `S: serve_topic` (`transfer.c:344`): `TOPIC_BEGIN` (session id all zero), then for each file `send_file_range` (`FILE_HDR` with `MSG_MORE`, sendfile chunks, `FILE_END`),
   then `TOPIC_END`.
6. `C: receive_topic` (`:170`): `recv_expect(TOPIC_BEGIN)`, `dec_topic_begin`, `recv_topic` (`transfer.c:603`) -> for each `FILE_HDR`, `recv_file` (RX_DIRECT: `open O_TRUNC`, `recv`+`write` loop,
   `FILE_END`), until `TOPIC_END`.
7. `C: totals_agree` (`:221`): the three views must agree: TOPIC_BEGIN counts/bytes, TOPIC_END counts/bytes, what was stored. Prints one line, writes the CSV row, exit 0.
8. `S: run_conn` (`server_core.c:392`): status "OK" -> `drain_until_peer_closes` (client closes right after TOPIC_END) -> `finish_conn` (CSV row, close, free).

Difference between modes (the only one): `handle_new_conn` (`server_core.c:465`).

- **single** (`./server <port> <dir> single`): `run_conn(c)` runs on the MAIN thread. While it serves client 1, clients 2..N wait in the kernel's listen backlog (their `connect`
  succeeded but nobody has called `accept`). The client has no receive time-out so it can wait (Deviation 1). Aggregate throughput does not grow with N.
- **multi** (default): `g_active` slot reserved, `pthread_create(detached)` -> `conn_thread` -> `run_conn`. Many transfers overlap; the main thread immediately returns to `accept`.
  At 256 simultaneous workers the next client gets `ERROR 503`. The Part I client does not give up on that: `fetch_topic` returns
  `FETCH_BUSY` and `fetch_topic_when_free` waits (`backoff_ms`, 50 ms doubling to 1 s) and asks again, up to `PART1_BUSY_TRIES` = 100 times
  (Deviation 26). Every other error is still fatal (exit 1). With 300 simultaneous clients all 300 now finish.

### 4.2 Case 1 with a fault and restart

Server: `case1_serve` (`Part2_Case1/server.c:175`) does `fault_init(&f, p, have_seed, seed, conn_id)` and installs `fault_should_fail` as the hook, then `serve_get` (`:136`) - identical to Part I.

1. `C: fetch_with_retries` (`Part2_Case1/client.c:319`) loop: `r->attempts++`, `run_attempt` (`:290`): `connect_to_server` (`:169`; sets 15 s `SO_RCVTIMEO`/`SO_SNDTIMEO`), `send_get`,
   `receive_topic` (`:208`: resets `next_index = 0` and `stored_bytes = 0` for the new attempt).
2. `S: send_chunks` (`transfer.c:236`) draws before every chunk. Suppose the draw faults in file #57: the server sends a random prefix of that chunk, logs a `FAULT` event
   (`inject_fault`, `:215`), calls `conn_abort` (shutdown + close), `serve_client` returns, `finish_conn` writes the CSV row (`faulted=1`, status "FAULT").
3. `C:` the `recv` in `recv_file` returns 0 (FIN) -> `RX_LOST` -> `classify` (`:250`) -> `ATTEMPT_RETRY` ("connection lost (closed by the server)").
4. Retry policy in `fetch_with_retries`: if `attempts >= max_retries` -> print "giving up" and **exit 2**. Otherwise `streak = (data_bytes grew) ? 0 : streak + 1`;
   `wait = backoff_ms(max(streak, 1))`; `sleep_ms(wait)`. Because every attempt that received data resets the streak, the wait after an injected fault is a constant 50 ms
   (Deviation 18). `max_retries` counts EVERY attempt (default 100).
5. New attempt: new connection (new `conn_id`, new fault generator), `GET` again, **the whole topic from file 0**, files reopened with `O_TRUNC` (they overwrite the partial files).
   Bytes received by failed attempts are the *redundant* data.
6. Success: `totals_agree`, exit 0. Non-retriable: ERROR 404/400/500, protocol error, local file error -> exit 1. `503` -> retry.

Real run (`p = 0.01`, seed 1001, a 3.75 MB topic): 9 attempts, 14 241 917 data bytes received for 3 753 925 useful ones (10 487 992 redundant). That is the Case 1 cost.

### 4.3 Case 2: a fault, RESUME, validation, and a client `kill -9` + restart

Server `serve_client` (`Part2_Case2/server.c:252`) installs the fault state and the `on_progress` hook (`:84`), reads the first message with `conn_recv_request` and dispatches: `MSG_GET` -> `handle_get` (`:127`),
`MSG_RESUME` -> `handle_resume` (`:178`), anything else -> ERROR 400.

**First connection (phase 1).**

1. `C: main` (`Part2_Case2/client.c:669`): `prepare_dir`, `load_checkpoint` (`:213`), `run_transfer` (`:589`).
2. `C: run_attempt` (`:508`): `open_connection` (`:296`), no checkpoint yet -> `start_with_get` (`:321`): send GET, `recv_expect(TOPIC_BEGIN)`, then `save_checkpoint` with
   `file_index = -1` (the session id is stored BEFORE any file arrives, so even a cut inside the first file is resumable), `rx.next_index = 0`.
3. `S: handle_get`: `topic_find_or_rescan`, `session_create(t, 0, id)` (503 if the table is full), `bind_session`, log `SESSION_NEW`, `serve_topic(c, t, id, 0, 0, 1)`; afterwards
   `end_session` (`:111`): XFER_OK -> `SESS_DONE`, otherwise `SESS_INTERRUPTED`.
4. `C: recv_topic`: for each `FILE_HDR`, `recv_file` first calls `on_file_hdr` (`:269`) which rewrites `.session` (atomic: tmp + rename) with the new file index and name, and then writes the `.part`.
   On `FILE_END` the `.part` is renamed to the final name.

**A fault.** The server's `progress_hook` has been calling `session_progress` after each chunk and after the fault prefix, so the session knows `(next_file = 57, file_off_sent = bytes handed to the socket)`.
It cuts the connection. The client's `recv` returns 0 -> `RX_LOST` -> `ATT_RETRY` (`failure_outcome`, `:471`). The `.part` of file #57 has exactly the prefix that arrived.

**Reconnect and RESUME (phases 3-4).**

5. `C: run_transfer` (`:589`): was progress made? (`made_progress`, `:557`: more data bytes, or a later `file_index`). `streak = 0` if yes else `streak++`; if `streak >= max_retries` (default 20)
   -> exit 2. Back-off `backoff_ms`, `sleep_ms`.
6. `C: run_attempt`: `can_resume` (`:235`: checkpoint with a file name) -> `start_with_resume` (`:377`): the offset is `checkpoint_offset` (`:248`) = `stat(<name>.part).st_size` (or the size of the
   final file if the file was completed and the next FILE_HDR never arrived: `complete = 1`). Builds `RESUME(session_id, file_index, offset, topic, name)`, sends it, expects `RESUME_OK`, and checks that it echoes the
   same `(file_index, offset)`.
7. `S: handle_resume` (`:178`): `dec_resume`; an ALL-ZERO session id is refused with `ERROR 400` (`:192-199`; `session_create` never hands out id 0,
   but a zeroed `.session` looks like one, and two such clients must not share one adopted record; Enhanced already did this); `topic_find_or_rescan`, `session_validate_resume(...)` (the 5 checks). `rc > 0` -> `reject_resume` (`:159`: sends `RESUME_REJECT(reason)`, status "REJECT", logs
   `RESUME_REJECT`); `rc < 0` -> ERROR 503. `rc == 0` -> `bind_session`, send `RESUME_OK(file_index, offset)`, log `RESUME_OK`, `serve_topic(c, t, id, file_index, offset, 0)` (no TOPIC_BEGIN) - this sends
   `FILE_HDR(start_offset = offset)`, only the missing bytes (sendfile reads from that offset), then the later files and `TOPIC_END`.
8. `C: recv_file` with `start_offset > 0`: `reopen_part` (`transfer.c:457`) checks the `.part` is at least `start_offset` long, `ftruncate`s to exactly `start_offset`, appends. No stored byte is received again.
9. `TOPIC_END`: `finish_topic` (`:420`) compares counts and (if this process saw TOPIC_BEGIN) byte totals; `ckpt_clear` removes `.session`; exit 0. The directory then holds only the topic files.

**RESUME_REJECT (phase 5).** `handle_reject` (`:449`): count it, `discard_download` (`remove_partials`, reset counters) and `run_transfer` goes straight back to `GET` with NO back-off (`ATT_REJECTED`, `:610`).
The server answered, so it is not overloaded. A reject counts toward the no-progress streak.

**Client `kill -9` and restart.** The client keeps no state in memory that matters: `.part` files and `.session` are on disk.

- `kill -9` can strike anywhere; `ckpt_save` is atomic, so `.session` is either the old or the new complete record; the `.part` is whatever had been written.
- The server sees EOF/RST on its thread: `XFER_PEER` -> status "PEER_LOST", session `INTERRUPTED` (`end_session`).
- On restart with the same arguments, `load_checkpoint` (`:213`) loads `.session`; it is accepted only if the topic matches and `k == 0` (not written by the Enhanced client); otherwise
  `discard_download`. Then `run_attempt` takes the RESUME path. `saw_begin = 0` in this process, so `finish_topic` checks only the file count (the byte total can be checked only if this process saw
  `TOPIC_BEGIN`, `:427-438`).
- The offset sent in `RESUME` is always the real `stat(<name>.part).st_size` at that moment, so it cannot disagree with the data. `reopen_part` still `ftruncate`s the `.part` to exactly the
  offset in `FILE_HDR` (in case bytes were written after the checkpoint was taken), and refuses (I/O error, exit 1) a `.part` that is SHORTER than the resume offset instead of inventing bytes.
- If the server was restarted meanwhile, the session id is unknown: checks 1-4 pass, the server ADOPTS the id (skips check 5), and the transfer continues.

### 4.4 Case 2 Enhanced: MANIFEST, K workers, FETCH, a fault and re-queue

Server side: `enhanced_serve` (`Part2_Case2_Enhanced/server.c:409`) -> `serve_first` (`:359`), which dispatches on the first message: `MSG_MANIFEST_REQ` -> `serve_manifest` (`:124`), `MSG_FETCH` ->
`serve_fetches` (`:316`), else ERROR 400. Both `serve_first` and the FETCH loop read with `conn_recv_request`.

**Step 1: MANIFEST (client `enh_fetch_manifest`, `client.c:386`).**

1. `main` (`client.c:1062`) -> `run` (`:914`) loop. `enh_fetch_manifest` first loads any existing `.session`: `had_session = (ckpt_load == 0 && topic matches)`.
2. `manifest_attempt` (`:310`): short connection, `MANIFEST_REQ(topic, K)`, `recv_msg` up to 64 MiB, `dec_manifest` (validates every name and that sizes sum to `total_bytes`). Retries with back-off if the connection fails or 503.
3. `S: serve_manifest` (`server.c:124`): `dec_manifest_req`; K must be 1..16 (policy lives here, not in the decoder); `topic_find_or_rescan`; `session_create(t, 1, sid)` (Enhanced: allocates `sent_off[]`);
   `enc_manifest_alloc`, `conn_send`; then marks the session `INTERRUPTED` until the first FETCH (so an unused session does not pin a slot). The connection then ends.
4. Client picks the session id: keep the OLD id if `had_session`, otherwise the manifest's id; saves `.session` with `k` (`ckpt_save`).

**Step 2: queue (`enh_build_queue`, `client.c:482`).** For every file `i`: if a `.session` existed and the final file has the manifest size -> done (and any stale `.part` is unlinked); if a `.part` exists with size
<= file size -> queue `(i, part size)`; otherwise queue `(i, 0)` and remove an untrusted `.part` (an adopted session skips check 5, so foreign bytes could otherwise creep in). Without a previous `.session`
everything is fetched from 0 and stale files are never trusted. This is enforced in `enh_fetch_manifest` (`client.c:436-456`): on a
fresh start the client DELETES every manifest file and every `*.part` in the output folder BEFORE it writes `.session`
(Deviation 27). Reason: once `.session` exists, a restarted client trusts every complete file of the right size, so killing the
client in its first second used to leave stale same-size files that the next run accepted (73 corrupted files in the review's test).

**Step 3: K workers.**

Each worker repeatedly: pop an item, make sure it has a connection, send `FETCH(session id, idx, off, topic, name)`, receive `FILE_HDR` + bytes + `FILE_END`. One FETCH is outstanding per connection and is never pipelined
(Deviation 13; reason in Q27). The server's loop `serve_fetches` (`server.c:316`) answers one at a time: `serve_one_fetch` (`:262`): `dec_resume`, rejects an all-zero id, rejects a FETCH for a different session than this
connection's, `topic_find_or_rescan`, `session_validate_resume(..., enhanced = 1)` (reject -> `RESUME_REJECT`, connection stays open: `reject_fetch`, `:217`), `FETCH_RESUME` event if `offset > 0`, `send_file_range`.
`after_send` (`:233`): OK -> `session_all_sent` -> `DONE`; fault/peer/io error -> session `INTERRUPTED`, stop. The loop ends on `NET_EOF` (the client closed because its queue is empty; status stays "OK") or `NET_ERR` (status "PEER_LOST"). In both
cases, if the topic is not fully sent (`!session_all_sent`) the session is set `INTERRUPTED` (`server.c:330-346`), so a killed or DNF client
no longer pins a slot ACTIVE for 300 s (256 such sessions used to fill the table); another live connection's next chunk makes it ACTIVE again.

#### Threads engine (`enh_run_threads`, `client.c:806`)

- Starts K `worker_main` threads (`:778`). `worker_main`: `while (workq_pop(...)) { ensure_conn; do_item }`; `close_conn` at the end. Connections are opened lazily, so a worker that never gets an item never connects.
- `ensure_conn` (`:612`): if it has a socket but `conn_reusable` (`:563`, a `MSG_PEEK|MSG_DONTWAIT` recv: EAGAIN means alive and silent) says the server closed it while idle, reopen without counting a failure; else
  `enh_connect` (`:209`).
- `fetch_item` (`:637`): encode and send FETCH, `recv_expect(FILE_HDR)`, check that the reply matches the request AND the manifest (index, offset, size, name), then `recv_file` (RX_PART).
- `do_item` (`:718`): `RX_OK` -> `enh_file_completed`, `workq_done`, `streak = 0`. `RX_LOST` or `503` -> `connection_failed`. `RX_REJECT` -> `handle_reject`. Anything else -> fatal.
- **Fault and re-queue**: `connection_failed` (`:579`): `close_conn`; push `(idx, enh_part_offset)` back to the queue FIRST; then `workq_done`; update this worker's `streak` (any stored byte resets it);
  if `streak >= max_retries` -> `stop_run(gave_up)`; else sleep `backoff_ms`. ONLY this worker sleeps; the other K-1 keep fetching, and one of them may pick up the re-queued item immediately (so `reconnects` can be lower
  than the number of faults, Deviation 24). The next `FETCH` for this file is sent with `offset = .part size`, passes validation (check 5 per file) and the server sends only the missing bytes.
- `handle_reject` (`:684`): reason 4/5 (and `off > 0`): `unlink_part`, re-queue `(idx, 0)`, same connection stays open. Reason 1/2/3 (topic changed): `stop_run(restart)` -> `run` (`client.c:914`) removes partials
  and `.session` and starts again from step 1 (at most `max_retries` restarts). Unexpected reason -> fatal.
- `stop_run` (`:295`): set the flag under `e->mu` and `workq_abort`, so every worker's next `pop` returns 0.
- End: `pthread_join` of all workers (`:839`) on every path; `finish_success` (`client.c:996`): `verify_topic` (`:965`: every file `done[]` and size on disk equals the manifest; counts and byte total match), `ckpt_clear`, summary.

#### Poll engine (`enh_run_poll`, `client_poll.c:1133`)

ONE thread, K `O_NONBLOCK` sockets, `poll()`. It does the same job but each connection's progress must be stored explicitly in a **slot** (`slot_t`, `client_poll.c:170`). The states (`slot_state_t`, `:158`):

| State | Meaning |
|---|---|
| `S_IDLE` | no item; if `fd >= 0` the connection is kept open and watched with `POLLIN` only to notice the server closing it |
| `S_BACKOFF` | no item, no socket, waiting until `deadline` (after a failure) |
| `S_CONNECTING` | non-blocking `connect()` in progress (`EINPROGRESS`); waiting for `POLLOUT`; item held |
| `S_SENDING` | FETCH partly sent (`req_off` advances); item held |
| `S_RECV_HDR` | reading the 16-byte header incrementally (`hdr_got`) |
| `S_RECV_PAYLOAD` | reading `payload_len` bytes (`pay_got`), then dispatch by type |
| `S_RECV_DATA` | reading the raw file bytes (`remaining`), writing each piece to the `.part` |

Transitions (header comment `client_poll.c:50-87`): IDLE -(pop, no socket)-> CONNECTING -(POLLOUT, SO_ERROR == 0)-> SENDING -(all sent)-> RECV_HDR -> RECV_PAYLOAD -> {FILE_HDR: open the .part -> RECV_DATA; FILE_END: close,
rename, done -> IDLE; RESUME_REJECT 4/5: re-queue at 0 -> IDLE; RESUME_REJECT 1/2/3 -> restart; ERROR 503 -> lost; other ERROR -> fatal}; RECV_DATA -(remaining == 0)-> RECV_HDR (expecting FILE_END); any busy state -(lost)-> BACKOFF
-(deadline)-> IDLE.

Main loop (`enh_run_poll`, `client_poll.c:1133`), repeated until `workq_outstanding() == 0` or a flag is set:

1. `expire_timers` (`:928`): back-off over -> IDLE; connect time-out (10 s) or 15 s without I/O -> `slot_lost` (the engine's equivalent of `SO_RCVTIMEO`).
2. `assign_work` (`:968`): IDLE slots get items with `workq_trypop` (NEVER the blocking pop: this thread owns every socket). Slots with an open connection are served first (saves a handshake).
3. `build_pollset` (`:995`): what each slot waits for (`POLLIN` / `POLLOUT`), and the nearest deadline as the `poll()` time-out.
4. `poll()`, then `service_slot` (`:1055`) for each ready socket: `finish_connect` (`:514`, checks `SO_ERROR`), `do_send` (`:406`), `do_recv` (`:828`), `idle_readable` (`:904`).
   `do_recv` asks for exactly what the current state still needs (the rest of the header, or payload, or file), reads until `EAGAIN` but at most `RECV_BUDGET` (1 MiB) per wake-up so one fast socket cannot starve the others.

Failure: `slot_lost` (`:343`) closes the socket and the `.part` fd, flushes the byte counts, `requeue_item` at `enh_part_offset` (**push before done**), updates the streak (data stored -> 0), gives up at `max_retries`, else
`S_BACKOFF` with the deadline. The other slots carry on. Reconnecting is lazy: a slot connects only when it next holds an item. `on_file_hdr` (`:626`) checks the reply against the request and manifest, `open_output` (`:543`) mirrors
`recv_file` (offset 0 -> truncate; offset > 0 -> `ftruncate` to the offset; complete file -> nothing to write), `store_data` (`:606`) writes before the next `recv`, `on_file_end` (`:657`) renames and calls `workq_done`.
`cleanup` (`:1119`) closes every socket and `.part` fd and returns any held item to the queue.

Why poll gets an explicit state machine and the thread engine does not: a blocking worker keeps its progress in its stack and program counter (inside `recv_msg`, "7 of 16 header bytes read"). With non-blocking I/O any `recv`
can return any number of bytes and the rest may arrive after other sockets were serviced, so partial reads/writes and every timer must be stored per connection.

---------------------------------------------------------------------------

## 5. Concurrency and safety

### 5.1 Every shared variable and its lock

| Where | Variable | Protected by | Notes |
|---|---|---|---|
| `server_core.c:72-74` | `g_active`, `g_main_done`, `g_listen_fd` | `g_mu` | signalled through `g_cv` |
| `server_core.c:77` | `g_conns`, `g_busy`, `g_data_bytes`, `g_wire_bytes`, `g_faults` (totals for the summary) | `g_mu` | updated in `finish_conn` |
| `server_core.c:79-80` | `g_stopping`, `g_next_conn_id` | C11 atomics | read without the lock |
| `server_core.c:83` | `g_map` (`topic_map_t`) | see topic_map | |
| `topic_map.h:37-44` | `topics`, `n_topics` | `lock` (rwlock): readers = lookups, writer = swap | held for one bsearch or two stores |
| `topic_map.h` | re-scan, `last_rescan_ns` | `rescan_mu` | serialises re-scans |
| `topic_t` and its `files[]` | everything | nothing - immutable after publication | pointer-stability rule (Deviation 3) |
| `session.c:51` | `slot[256]` (all fields incl. `sent_off[]`) | `table_mu` | no slot pointer escapes |
| `log.c:18` | stderr output, CSV file descriptors | `g_mu` (threads) + `flock` (processes) | |
| `conn_t` | all fields | single owner thread | allocated by main, freed in `finish_conn` by the same worker |
| `fault_state_t` | `rng`, counters | single owner thread | on the worker's stack |
| `workq.c:26` | ring buffer, `count`, `busy`, `aborted` | `q->mu` + `q->cv` | |
| `enh_client.h` | `data_bytes`, `wire_bytes`, `files_done`, `attempts`, `reconnects`, `rejects`, `fatal`, `gave_up`, `restart`, `done[]` | `e->mu` | through `enh_add_bytes`, `enh_file_completed`, `stop_run`, ... |
| `enh_t` | `man`, `dir`, `topic`, `host`, `port`, `session_id`, `k`, `max_retries` | none - read-only while workers run | written before `pthread_create`, which orders those writes before the threads start |
| `worker_t` | `fd`, `streak`, `failed`, `rx_ctx_t` | single owner (the worker) | |
| poll engine | `engine_t`, all `slot_t` | single thread | still takes `e->mu` and the queue mutex because the shared helpers do |
| Case 1 / Case 2 client | everything | single-threaded | |
| checkpoint dir | `.session`, `.session.tmp` | one writer at a time | fixed tmp name |

### 5.2 Why there are no data races

- Each connection's mutable state lives in one `conn_t` owned by one thread; the only objects shared across threads are the topic map (immutable `topic_t`s behind an rwlock for the pointer array), the session table
  (one mutex), the log (one mutex), the thread counters (one mutex), and on the client the work queue and `enh_t` counters (mutexes).
- Locks are never nested in a way that can deadlock: `session.c` takes only `table_mu` and calls no other module while holding it; `topic_map.c` takes `rescan_mu` then `lock` (always in that order, never the reverse);
  `server_core.c` holds `g_mu` only for counter updates and `cond_wait`; `log.c` takes its mutex last. The slow work (disk reads, `qsort`, `malloc`) is done outside locks.
- No lock is held across network or disk I/O. The rwlock writer holds for two stores.
- Condition variables are always used in a `while (condition)` loop (`workq.c:116`, `server_core.c:596,725`), so spurious wake-ups are harmless.
- Thread sanitizer builds (`make SAN=thread`) and unit tests (`make -C tests/unit run`) are part of the build gate.

### 5.3 How fds and memory are released on fault paths

- Server: a fault calls `conn_abort` (`server_core.c:149`): `shutdown_close` and `fd = -1`. `finish_conn` closes only if `fd >= 0` and ALWAYS frees the `conn_t` and decrements `g_active`; that is on every
  path out of `run_conn` (served, faulted, error, refused). `send_file_range` closes the source file and frees the fallback buffer on every path (`transfer.c:338-340`). The fault state is on the stack and the hook
  pointers are cleared before return. `recv_msg` and `conn_recv_request` free their buffer on error and the callers free the payload (`free(payload)` is safe because it is initialised to NULL).
- Client: `recv_file` frees its buffer and `close`s the output on every failure path, leaving the `.part` on disk on purpose (it is the checkpoint). `enh_run_threads` joins every started worker on every path (`:839`),
  `workq_destroy` is called only after all threads are joined (`free_pass`, `client.c:899`); `enh_run_poll` calls `cleanup` (`client_poll.c:1119`) and frees `slots` and `buf`. `manifest_free` / `free(e->done)`.
- Shutdown: `server_run` frees the map only if no worker is left (`left == 0`, `:642`) and destroys the condition variable only then, because detached workers still hold pointers into them.

---------------------------------------------------------------------------

## 6. Measurement

### 6.1 Where the CSV rows come from

Enabled by environment variables only (no hard-coded path): `FDS_LOG=<file.csv>` (one row per server connection / per client run), `FDS_EVENTS=<file.csv>` (server events), `FDS_RUN_ID`, `FDS_CLIENT_ID`.
`finish_conn` (`server_core.c:215`) writes the server row at the END of each connection; each client writes one row just before exit (`write_csv`/`report`).

**Server connection CSV** (`log.h:14-16`): `run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status`.

**Server events CSV** (`log.h:17-18`): `run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail`. Events: `FAULT` (with `detail=prefix=a/b`), `SESSION_NEW`, `RESUME_OK`,
`RESUME_REJECT`, `FETCH_RESUME`, `FETCH_REJECT`.

**Client CSV** (`log.h:19-21`): `run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status`.

A real Case 2 run (`p = 0.02`, seed 1001, topic `gatto` of the synthetic dataset, 167 files, 3 753 925 bytes):

```
run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status
demo_case2,case2,multi,0.02,1001,1,get,00806606...,"gatto",0,127.0.0.1:43698,3469553696912,3469554144720,...,573612,575086,23,21,1,FAULT
demo_case2,case2,multi,0.02,1001,2,resume,00806606...,"gatto",0,127.0.0.1:43708,3469605509599,3469605990964,...,882753,885846,41,38,1,FAULT
...
demo_case2,case2,multi,0.02,1001,6,resume,00806606...,"gatto",0,127.0.0.1:43736,...,1050310,1055442,55,54,0,OK
```

Client row: `...,"gatto",1,-,...,3753925,3753925,3769006,167,6,5,0,OK` = `useful_bytes 3753925`, `data_bytes_rx 3753925`, `wire_bytes_rx 3769006`, `files 167`, `attempts 6`, `reconnects 5`, `rejects 0`, `status OK`.
Event rows: `FAULT,...,21,0,22,573612,"prefix=6951/18071"` (file 21, chunk began at offset 0, 22nd chunk of the connection, 573 612 data bytes sent on that connection, 6 951 of the 18 071 chunk bytes sent) and
`RESUME_OK,...,21,6951,...` (the next connection resumes file 21 at exactly byte 6 951).

### 6.2 data_bytes vs wire_bytes

- `data_bytes`: file payload bytes handed to the socket (what `sendfile` returned, including a fault's partial chunk). Updated in `account` (`transfer.c:176`).
- `wire_bytes`: `data_bytes` + every header/control byte sent. Control messages are added in `conn_send` (`server_core.c:172`); `account` adds the data. In the run above 573 612 data vs 575 086 wire: the difference of 1 474 is
  headers (TOPIC_BEGIN, FILE_HDRs, FILE_ENDs).
- Client side: `data_bytes_rx` counts file bytes received and written (including re-received ones in Case 1); `wire_bytes_rx` counts every byte read (`recv_msg` adds the header + payload, `recv_file` adds raw bytes).
- Consistency check we use (and quote in the viva): sum of server `wire_bytes` == sum of client `wire_bytes_rx` (3 769 006 on both sides in the Case 2 run; 3 775 322 on both in the Enhanced run), and in Case 2/Enhanced sum of
  server `data_bytes` == topic bytes x clients == client `useful_bytes` (no redundant bytes).

### 6.3 Server-side aggregate throughput (the assignment's Part I metric)

Definition (design §6.7, Deviation 21): `throughput = Σ data_bytes of all connections / (max end_ns − min start_ns)` over the connections of a run, measured with the server's single `CLOCK_MONOTONIC` clock.
`start_ns` is taken when the connection is accepted (`server_core.c:459`), `end_ns` in `finish_conn` (`:225`) AFTER `drain_until_peer_closes`, so the interval includes the delivery of the last bytes. We use the server clock because
the client machines' clocks are not precisely synchronised; client-side figures are only secondary (per-client completion time).

### 6.4 `tools/exp/analyze.py`

Reads every CSV under `--raw`, recognises the type by its header (`classify`, `analyze.py:123`), groups rows by `run_id`, and ignores warm-up runs (`_warmup` suffix). Per run (`run_metrics`, `:278`):

- `throughput = Σ server data_bytes / span`, where `span = max end_ns − min start_ns` over connections with `kind != "-"` (`:294-306`).
- `total_MB = Σ data_bytes`; `wire_MB = Σ wire_bytes`.
- `useful = Σ useful_bytes of clients that finished OK` (`:334`); falls back to `topic bytes × clients` if client CSVs are missing.
- `redundant = total − useful`; `overhead % = (data − useful) / useful × 100` (`:358-359`); `wire_overhead %` the same with `wire`.
- `goodput = useful / span` (server interval); per-client goodput = `useful_bytes / (end_ns − start_ns)` of that client; completion time mean and max (`:366-370`).
- `faults` = number of `FAULT` rows of the events CSV; `reconnects` and `attempts` from the clients.
- A run is OK only if every client finished OK; DNF (a client gave up or was killed by the time-out) and failed runs are counted but EXCLUDED from the means. `aggregate` (`:416`) reports mean and sample standard deviation
  (`statistics.stdev`) over the OK repetitions.

Fault-model predictions (`predictions`, `:211`; needs `--dataset` to know `T`). Deviation 28: every connection is cut with probability p,
at a uniform byte of its first request. `T` = topic bytes, `q = 1 − p`:

- Case 1: `pred_attempts = 1 / q`; `pred_faults = n_clients · p / q`; `pred_overhead_pct = (p / q) · ((T − 1) / 2) / T · 100` (each cut
  wastes on average half the topic); `pred_dnf_run_prob = 1 − (1 − p^100)^n_clients` (probability that some client has all 100 attempts cut).
- Case 2: `pred_faults = n_clients · p / q`; Enhanced: `n_clients · K · p / q` (an upper bound); `pred_overhead_pct = 0` for both.

It also prints a consistency note when `|faults − reconnects| > max(2, 10%)` (`:382-391`); Enhanced runs are skipped because their `reconnects` can legitimately be lower (Deviation 24). Outputs: `Part1_summary.csv`, `Part2_summary.csv`, `Buffers_summary.csv`, `all_runs.csv`, `summary.md`.

### 6.5 What the numbers should show (so you can explain results)

- Part I: single-threaded aggregate throughput stays flat as N grows (one client at a time); multi-threaded grows and then saturates at the network / disk limit.
- Case 1: expected attempts `1/(1 − p)`, redundant bytes about `p/(1 − p) × T/2`, so overhead grows steeply as p approaches 1 (about 50 % at p = 0.5, 450 % at p = 0.9); it only gives up at p = 1 (or with probability p^100).
- Case 2/Enhanced: overhead is zero in data bytes; the only cost is control messages of each reconnect plus back-off time. Faults per client `≈ p/(1 − p)` (Case 2) and up to `K·p/(1 − p)` (Enhanced, each of the K connections can be cut).

---------------------------------------------------------------------------

## 7. Design decisions and alternatives

Mirrors design §9 plus the deviations. Format: decision -> alternative -> why we chose this.

### 7.1 From design §9

| Decision | Alternatives considered | Why we chose this |
|---|---|---|
| Language: C + pthreads | Python, C++ | The assignment is built around pthreads and `clock_gettime`; C gives direct control of every system call; no interpreter dependency on the grader. |
| Framing: length-prefixed binary header | Newline-terminated text (HTTP/FTP style); delimiter bytes | Image data can contain any byte, so delimiters are unsafe; an explicit length says exactly where each message ends. Request is still the text "GET <topic>". |
| Topic map: sorted array with cached file lists | Re-scan the directory per request; hash table | About 10 topics: binary search is as fast as a hash and simpler; the cache avoids disk scans and gives stable file indices for Case 2. |
| Concurrency: thread per client (cap 256) | Thread pool; single-threaded event loop (epoll) | Required by Part II, simplest correct design for tens of clients. A pool bounds resources but adds queueing delay. An event loop only pays off with thousands of connections. 256 cap limits resources. |
| Single vs multi: runtime argument | Two programs; compile-time flag | The grader needs exactly one `server` per folder; one build serves both experiments and `./server <port> <dir>` still works. |
| Fault decision: one draw per connection, cut at a uniform byte of its first request (Deviation 28; the design drew per 64 KB chunk) | Independent draw before every 64 KB chunk; a separate killer thread | Matches the guidelines ("how frequently the session is interrupted"); every case finishes for p < 1, so Case 1 vs Case 2 differ in wasted bytes, not in whether they finish; uses only the worker's own state. Per chunk, Case 1 could not finish a real topic at p >= 0.01. A killer thread would race with the worker on the same socket. |
| RNG: `drand48_r` with per-thread state | `rand()`/`drand48()`; `rand_r()` | `rand`/`drand48` use hidden global state (not thread-safe); `rand_r` is obsolescent; `drand48_r` is thread-safe and seedable. |
| Interruption: `shutdown()` then `close()` | Abortive close with `SO_LINGER` (RST) | Deterministic: the client receives every byte sent before the cut and then end-of-stream, so byte accounting is exact. (Deviation 9 refined this: a random prefix of the faulted chunk is sent first.) |
| Case 1 restart: overwrite files in place (`O_TRUNC`) | Delete the output directory and restart | Simpler, and still retransmits the whole topic as Case 1 requires. |
| Case 2 checkpoint: both sides, client authoritative | Server only; client only | The server knows what it sent, not what the client stored. The client's `.part` size is exact; the server's record lets it reject impossible checkpoints. |
| Checkpoint frequency: continuous (the `.part` size) | After every file; every N KB | Costs nothing: no extra I/O; `.session` rewritten only when the file index changes. |
| Invalid checkpoint: `RESUME_REJECT` with a reason | Close the connection; resume from start of current file | Never delivers wrong data; client falls back to a clean GET. |
| Enhanced work split: dynamic file queue | Static round-robin; byte ranges within files | Files are small, so splitting inside a file adds overhead without benefit; dynamic queue balances load and makes recovery trivial (re-queue the file). |
| Enhanced connections: configurable, default 4 | Fixed 2; fixed 8 | Best value depends on the network; default 4 from reasoning (several connections hide per-file turnaround, many only compete for bandwidth), K = 1, 2, 4, 8 measured. |
| Socket mode: blocking + threads as main, `poll()` client as measured variant | Non-blocking only; epoll | Blocking is simplest and easiest to defend; the poll engine is the "non-blocking" comparison. epoll pays off only with hundreds of sockets; K <= 16. |

### 7.2 Deviations (implementation vs submitted design)

| # | Decision | Design said / alternative | Why |
|---|---|---|---|
| 1 | Part I client has no `SO_RCVTIMEO` | 15 s | In single-threaded mode later clients wait for all earlier ones; 32 clients would time out. |
| 2 | Size cap per message type (MANIFEST up to 64 MiB) | 64 KB for all control messages | The manifest of `cane` (4 863 files, ~250 KB) would have been rejected. |
| 3 | Topic map = sorted array of pointers; `topic_t` never moved/freed; re-scan only adds | Array of structs under an rwlock rebuilt by a re-scan | A re-scan could `realloc`/reorder structs in use by workers; reader-preferring rwlock could make the re-scan wait for every transfer. |
| 4 | Output to `output_dir/<topic exactly as typed>/` | - | The grader checks the folder named as it typed it, even if the server matched case-insensitively. |
| 5 | `make` without `-Werror`; `make strict` is our gate | `-Werror` always | One new warning on the grader's compiler would mean no executables at all. |
| 6 | No byte-range split of big files | Optional split with `ftruncate`+`pwrite` | Pre-sizing breaks the invariant ".part size = checkpoint offset"; files are tens of KB. |
| 7 | Text fixes (gatto has 1 668 files, cane 4 863; `006_043`) | - | Typos in the design. |
| 8 | CSV only when `FDS_LOG`/`FDS_EVENTS` set | Logs to a file | No hard-coded paths allowed. |
| 9 | Fault sends a random prefix (0..n-1 bytes) of the faulted chunk, then closes | Faulted chunk not sent at all | "Disconnection at any point in time"; most images are under 64 KB, so otherwise every cut would be at offset 0 and byte-offset resume would never be exercised. Accounting stays exact. At p = 1 no case can finish. |
| 10 | `TCP_NODELAY` on every data socket | - | Nagle + delayed ACK can stall each per-file FETCH turnaround by ~40 ms. `MSG_MORE` still coalesces headers with data. |
| 11 | Enhanced check 5 is per file: `offset == 0 \|\| offset <= sent_off[i]` | Rules of §5.5 | With K connections one `(next_file, offset)` pair cannot describe progress. |
| 12 | Session slot reuse order: FREE, oldest DONE, idle > 300 s, oldest INTERRUPTED; fresh ACTIVE never evicted (else 503) | 256 slots expiring after 300 s | Long experiment series must not run out of slots; a stale session must not pin one. |
| 13 | Enhanced restart rebuilds the queue from MANIFEST + disk; back-off resets after progress; FETCH never pipelined | Re-queue (file, offset) | Restart-safety; unread data at `close()` makes it an RST that discards queued bytes. |
| 14 | `accept_loop` and `signal_thread` in shared `server_core.c` | In each `server.c` | One implementation of the thread model. |
| 15 | p in {0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9} (per connection, see 28); K in {1, 2, 4, 8}; wire bytes also counted; throughput-vs-overhead plot | Smaller sets | With p per connection every case finishes for p < 1, so the sweep covers the whole range; K = 1 separates parallelism from per-file cost; the guidelines ask for throughput vs overhead. |
| 16 | `getaddrinfo`, 10 s connect time-out, backlog `SOMAXCONN` | `tcp_connect(ip, port)` | Robustness. |
| 17 | Added `util` and `server_core`; `transfer` holds the client `recv_file`/`recv_topic` | - | Shared code. |
| 18 | Back-off counter = consecutive attempts that received no file data; after a cut that received data wait 50 ms | Doubling between all attempts | After an injected fault the server is healthy; long waits would only inflate completion time. |
| 19 | Case 2/Enhanced can only resume files whose names are <= 250 bytes | - | `<name>.part` must fit Linux's 255-byte name limit. Known limitation. |
| 20 | Fault RNG state = `splitmix64(splitmix64(seed) ^ conn_id)` via `seed48_r` | `srand48_r(seed ^ conn_id)` | Repetitions with seeds 1001, 1002, ... shared 30 of 32 per-connection seeds. |
| 21 | Connection end time taken after `shutdown(SHUT_WR)` + waiting (<= 5 s, absolute deadline) for the client's close | End at last `send` | `send`/`sendfile` return when data is queued, which inflated throughput ~10% at N = 1. |
| 22 | Adopted sessions skip check 5 | Check 5 whenever the id is known | The server has no record of what was sent before it adopted the id; checking would wrongly reject a valid `.part`. |
| 23 | Third optional Enhanced argument selects `threads` or `poll` (`io=` prefix accepted) | `[connections] [max_retries]` | The "non-blocking socket" variant of §6.1. |
| 24 | In Enhanced, `reconnects` may be lower than the number of faults | Reconnects == faults | A re-queued file is often taken by another worker still connected. Consistency check: server data bytes = client data bytes = topic x clients (`analyze.py` skips the faults-vs-reconnects note for Enhanced). |
| 25 | Servers read requests with `conn_recv_request`: absolute 30 s deadline, only the four request types, payload <= 512 bytes, checked before allocation | `recv_msg` under `SO_RCVTIMEO` | A one-byte-per-29-s peer could hold a thread for days; a 64 MiB "MANIFEST" request was buffered. |
| 26 | Part I client retries `ERROR 503` (back-off, <= 100 tries) | Any error -> exit 1 | 503 means "busy", not a failed transfer; 44 of 300 simultaneous clients used to exit 1. |
| 27 | Enhanced fresh start deletes old manifest files and `*.part` BEFORE writing `.session`; a FETCH connection that ends early leaves the session INTERRUPTED | Overwrite stale files as they are fetched | A client killed in its first second left stale same-size files that the next run trusted (73 corrupted); abandoned sessions pinned slots ACTIVE for 300 s. |

---------------------------------------------------------------------------

## 8. Likely viva questions with model answers

Answers are short on purpose; the pointers say where to look.

**Q1. Why TCP and not UDP?**
The service must deliver files completely and in order, and the assignment is about TCP socket programming. TCP gives reliable, ordered delivery, flow and congestion control, so the application only has to handle the
failures TCP cannot hide (a connection closing). With UDP we would have to build retransmission, ordering and congestion control ourselves. Our injected faults are connection-level (`shutdown`+`close`), which only make
sense on a connection-oriented transport.

**Q2. Why a length-prefixed binary header instead of text lines?**
TCP is a byte stream with no message boundaries. Files contain arbitrary bytes, so a delimiter could appear in the data. A 16-byte header with `payload_len` says exactly how many bytes to read next
(`hdr_decode`, `protocol.c:289`; `recv_msg`, `:815`). The header also carries magic and version, which catch a peer that is not speaking our protocol, and a size cap that stops a hostile length from causing a huge `malloc`.

**Q3. What happens if `recv()` returns fewer bytes than I asked for?**
That is normal for TCP, so we never assume a single `recv` fills a buffer. `recvall` (`net.c:70`) loops until all `n` bytes have arrived, returning `NET_EOF` if the peer closes early. `sendall` (`net.c:41`) does the same for
`send`. The file-data loop in `recv_file` (`transfer.c:548`) deliberately uses plain `recv` and processes whatever arrives, because each piece must be written to disk immediately.

**Q4. Blocking vs non-blocking sockets: what is the difference and where do you use each?**
A blocking `recv`/`send`/`accept` puts the thread to sleep until it can complete; a non-blocking one returns at once with `EAGAIN` if it cannot. Everything uses blocking sockets with `SO_RCVTIMEO`/`SO_SNDTIMEO` except the
Enhanced `io=poll` client (`client_poll.c`), where one thread drives K `O_NONBLOCK` sockets with `poll()`. `tcp_connect` (`net.c:192`) uses a non-blocking `connect` only temporarily, to bound it with a time-out, and then
switches the socket back to blocking.

**Q5. Why `poll()` and not `epoll`?**
`poll` costs O(n) per call because the whole set is passed each time; `epoll` is O(1) per ready event but only pays off with hundreds or thousands of sockets. We have at most K = 16 sockets (`FDS_ENH_MAX_K`), so rebuilding the
`pollfd` array is a few nanoseconds (`build_pollset`, `client_poll.c:995`), and `poll` is plain POSIX (design §6.1; header comment `client_poll.c:33-38`).

**Q6. Why thread-per-client and not a thread pool?**
Part II requires one thread per client; it is the simplest correct design for tens of clients; a pool adds queueing delay for clients beyond the pool size. We cap threads at 256 (`FDS_MAX_THREADS`) and answer
`ERROR 503` beyond that (`server_core.c:481`), so resources stay bounded. An event loop would only be worth it for thousands of connections.

**Q7. What did you find comparing single- and multi-threaded servers?**
Single mode (`server_core.c:465`) serves one client at a time from the main thread; the other clients' connections sit in the listen backlog, so aggregate throughput stays about flat as N grows and per-client completion
time grows linearly. Multi mode overlaps transfers (one detached thread each), so aggregate throughput rises with N until the network or disk saturates. The exact numbers come from `Results/` (produced by
`tools/exp/run_part1.sh` and `analyze.py`); do not quote numbers you have not looked up. The mechanism is what the viva asks.

**Q8. How is the failure probability p applied, and why per connection?**
Once per connection: when the server accepts the connection's first data request, `fault_plan` (`fault.c:140`) draws `r` from
`drand48_r` and the connection will be cut iff `r < p`; a second draw picks the cut byte uniformly in that request's data. `send_chunks`
(`transfer.c:249`) asks the hook before every chunk, and the chunk holding the cut byte is sent only up to it. So p is the probability
that a connection is interrupted, as the guidelines put it ("how frequently the client-server session is interrupted"). The design drew
per 64 KB chunk instead; we changed it (Deviation 28) because then a Case 1 attempt over N chunks survives with probability `(1 − p)^N`,
and on a real topic (gatto, N about 1 800) Case 1 could never finish at p >= 0.01.

**Q9. What is the expected number of attempts in Case 1?**
Every attempt is a new connection, cut with probability p independently, so the number of attempts is geometric with mean `1/(1 − p)`
(`predictions` in `analyze.py:211`): 2 at p = 0.5, 10 at p = 0.9. Each failed attempt wastes a uniform part of the topic, (T − 1)/2 bytes
on average, so the redundant bytes are about `p/(1 − p) × T/2`. The client gives up only if all 100 attempts are cut: probability p^100
(about 3e-5 at p = 0.9). Case 2 needs the same number of connections, but redundant bytes are 0 because it resumes at the cut byte.

**Q10. Why does Case 2 have zero redundant bytes?**
Every chunk is sent exactly once: the client's `.part` size is the exact number of bytes stored (each `recv` is written before the next), the client reports it in `RESUME`, and `sendfile` starts at exactly that offset
(`send_data` uses an explicit offset, `transfer.c:139`). The faulted chunk's prefix that was sent is also stored, so it is not sent again. Files before the checkpoint are complete and skipped. We verify it: sum of server
`data_bytes` equals topic bytes (3 753 925 in our test run), and equals the client's `useful_bytes`. Only control messages and back-off time are overhead.

**Q11. What does the checkpoint contain, and who is authoritative?**
Session id, topic, current file index and name (in `.session`), plus the byte offset, which is not stored but derived as `stat(<name>.part).st_size`. The client is authoritative, because only it knows which bytes reached
its disk; the server keeps a session record (`session.c`) only to validate (five checks) and reject impossible checkpoints. See design §5.6's table: server-only cannot know what the client stored; client-only cannot
validate; "both, client authoritative" gets exactness plus validation.

**Q12. What happens if the server restarts?**
In Case 1 nothing is lost that mattered: the client's retry loop reconnects and starts over (exit 2 only after `max_retries`). In Case 2/Enhanced the session table is gone, so the client's `RESUME`/`FETCH` carries an
unknown id. If checks 1-4 pass, the server ADOPTS that id (`session.c:281-299`) and skips check 5 for it (Deviation 22), and the transfer continues from the client's offset. If they fail, `RESUME_REJECT` -> the client
discards partials and starts again from `GET`/`MANIFEST_REQ`. While the server is down the client keeps retrying with back-off and exits 2 after `max_retries` attempts without progress.

**Q13. What happens if the client crashes (kill -9)?**
The `.part` files and the atomic `.session` are on disk, so nothing is lost. On restart with the same arguments `load_checkpoint` (Case 2, `client.c:213`) or `enh_fetch_manifest` + `enh_build_queue` (Enhanced) pick up
the checkpoint. On the server, the thread sees EOF/RST (`XFER_PEER`), the session becomes `INTERRUPTED`, and the slot stays valid for the RESUME (until reused after idle). `ckpt_save` is atomic (tmp + `rename`), so a
kill between the two cannot leave a half-written record (`checkpoint.c:94-99`).

**Q14. Why is `.session` written with rename?**
`rename(".session.tmp", ".session")` is atomic on POSIX file systems: a reader or a crash sees either the entire old file or the entire new file, never a half-written one that could name the wrong file. `ckpt_save`
also checks `fclose` (a full disk is only reported when stdio flushes). We skip `fsync` because our fault model is a lost connection or killed process, not power loss.

**Q15. Why client-authoritative and not server-only checkpoints?**
The server only knows what it handed to its socket (`file_off_sent`), which is an upper bound of what the client has, not an exact value; after a fault the kernel may not have delivered everything. The client's
`.part` size is the exact stored amount and costs no extra I/O. The server's record is still used: check 5 rejects a checkpoint that is ahead of what was sent.

**Q16. What are the 5 validation checks?**
(1) topic exists and the known session belongs to it, (2) `file_index < n_files`, (3) `name` equals the server's `files[file_index].name`, (4) `offset <= size`, (5) for a known, non-adopted session, the checkpoint is not ahead
of the data sent (Case 2: `(index, offset) <= (next_file, file_off_sent)`; Enhanced: `offset == 0 || offset <= sent_off[index]`). Failures return `RESUME_REJECT` with reasons 1-5 (`session.c:204-248`).

**Q17. What is an "adopted" session and why does it skip check 5?**
If the id is unknown (server restarted, or slot evicted), `session_validate_resume` accepts a valid checkpoint (checks 1-4) and creates a slot under the client's own id with `adopted = 1`. The server has no history of what
was sent before the adoption, so check 5 would have nothing to compare against and could wrongly reject a legitimate `.part` (an Enhanced worker holding a `.part` of another file). Skipping it is safe: a bad checkpoint can only
cause a fresh start, never wrong data (Deviation 22).

**Q18. How did you choose K (number of connections)?**
By reasoning plus measurement. Animals-10 images are tens of KB, so a single connection spends a large share of its time on the per-file FETCH turnaround, and after a fault it sits idle during back-off and the new
handshake. K connections keep the link busy; beyond a few they only compete for bandwidth. So the default is 4 (`FDS_ENH_DEFAULT_K`), configurable 1..16, and we measure K = 1, 2, 4, 8 (Deviation 15; K = 1 separates
parallelism from the per-file FETCH cost). The justification should be taken from `Results/summary.md`, not from memory.

**Q19. How are files reassembled in Enhanced?**
They are not: nothing is ever split. Each file is owned by exactly one worker at a time (the one that popped it), written to its own `<name>.part` and renamed on `FILE_END`. K workers write K different files, so there is
no byte-stream reassembly, no ordering between connections, no shared write position (Deviation 6: no byte-range split). The manifest index fixes the order; `verify_topic` (`client.c:945`) checks every file exists with its
manifest size.

**Q20. Why TCP_NODELAY and why also MSG_MORE? Don't they conflict?**
They address different problems. `TCP_NODELAY` disables Nagle's algorithm so small requests (FETCH, RESUME) are not held back waiting for an ACK, avoiding a ~40 ms delayed-ACK stall per file (Deviation 10, `net.c:276`).
`MSG_MORE` is a per-call hint that more data follows immediately: the `FILE_HDR` is sent with `MSG_MORE` (`transfer.c:316`) so the kernel puts it in the same segment as the first data bytes instead of a tiny packet of its own.
The final `FILE_END`/`TOPIC_END` go without `MSG_MORE` so they are flushed at once.

**Q21. What is `sendfile` and what does zero-copy mean?**
`sendfile(out_fd, in_fd, &offset, count)` moves data from the file's page cache directly to the socket inside the kernel, avoiding the `read()` into a user buffer and `send()` from it (two copies and two syscalls per piece).
It takes an explicit offset (so resume is natural) and returns how many bytes it moved, possibly fewer than requested (so we loop, `transfer.c:133`). Fallback: `pread`+`send` if the file system rejects `sendfile`
(`EINVAL`/`ENOSYS`, `:147`). It is "zero-copy" in user space; the kernel still copies/DMA's into socket buffers.

**Q22. Why `clock_gettime(CLOCK_MONOTONIC)`?**
Intervals must not be affected by NTP steps or someone changing the date; `CLOCK_REALTIME` can jump backwards, giving negative or wrong durations. `now_ns` (`util.c:146`) is monotonic and used for every measurement and for
deadlines (also the condition-variable time-outs, `pthread_condattr_setclock`, `server_core.c:633`). `wall_ns` is recorded only for rough alignment between machines.

**Q23. What does `SO_REUSEADDR` do?**
It lets `bind()` succeed on a port even if earlier connections on it are still in `TIME_WAIT`, so we can restart the server immediately between experiments (`net.c:117`). It does not allow two live listeners on the same port.

**Q24. What is SIGPIPE and how do you avoid it?**
Writing to a socket whose peer has closed raises `SIGPIPE`, which kills the process by default. Fault injection creates closed connections on purpose. So every `send` uses `MSG_NOSIGNAL` (`sendall`, `copy_piece`, the poll engine's
`send`), and the server also runs `signal(SIGPIPE, SIG_IGN)` (`server_core.c:680`) because `sendfile` cannot take that flag.

**Q25. Explain graceful shutdown.**
SIGINT/SIGTERM are blocked in all threads before any thread is created; one dedicated thread waits in `sigwait`. On a signal it sets `g_stopping`, calls `shutdown()` on the listening socket (which wakes the blocked `accept`
with an error), the main thread stops accepting, waits up to 5 s for `g_active` to reach 0 (condition variable on the monotonic clock), prints a summary, closes the CSV files and exits 0. If the main thread is itself serving a
client in single mode, the signal thread forces `_exit(0)` after 6 s so a shutdown never hangs (`server_core.c:560-613`, `719-751`). Running logic in a normal thread instead of a signal handler means we may use mutexes and `malloc`.

**Q26. What if two threads call `accept` or `topic_find` at the same time?**
Only the main thread calls `accept`. `topic_find` is called from many worker threads under a read lock (concurrent readers); a re-scan takes `rescan_mu` to serialise re-scans, builds the new array outside the rwlock and
takes the write lock for two stores (`topic_map.c:522-575`). `topic_t` objects are immutable so nothing else needs protection.

**Q27. Why is only one FETCH outstanding per connection (no pipelining)?**
If the client sent FETCH #j while #i was still streaming and a fault then cut the connection during #i, the server's `close()` would find unread data (FETCH #j) in its receive buffer. On Linux that makes `close()` send an RST instead
of a FIN, and an RST makes the client's kernel discard bytes it had queued but not yet read. The prefix of #i that the server counted as sent would be lost and byte accounting would break (`Part2_Case2_Enhanced/server.c:36-47`,
Deviation 13). The cost is one RTT per file, hidden by K connections. For the same reason `refuse_busy` drains the unread request before closing.

**Q28. What is the "busy" counter in the work queue?**
It counts items that have been popped and not yet `workq_done`. `workq_pop` blocks while the queue is empty but `busy > 0`, because a busy worker might still re-queue its item after a connection failure; it reports "complete"
only when the queue is empty AND `busy == 0`. Without it, workers would exit while a failed file is still to be re-fetched, or the program would finish early (`workq.c:8-14`). The failure path pushes BEFORE calling `workq_done`
so `count + busy` never touches 0 in between.

**Q29. How do you verify that the byte counts are correct?**
Four ways: (1) the client checks TOPIC_BEGIN counts/bytes against TOPIC_END and against what it stored (`totals_agree`, `finish_topic`, `verify_topic`); the files on disk are compared with the manifest sizes; (2) server `wire_bytes`
summed over connections equals client `wire_bytes_rx` (3 769 006 = 3 769 006 in our Case 2 test); (3) in Case 2/Enhanced the sum of server `data_bytes` equals the topic bytes x clients; (4) `tests/verify_output.sh` compares the
downloaded tree with the dataset byte for byte. The server counts what `sendfile` returned, not what it intended to send, so the counters are exact even for a faulted chunk.

**Q30. Why does the server do the "drain" after the last message?**
Because `send`/`sendfile` return when data is queued in the kernel, not when the client has it. Taking `end_ns` there would leave the last MB of socket buffer outside the measured interval and overstate throughput by about
10% at N = 1. `drain_until_peer_closes` (`server_core.c:292`) half-closes and waits (max 5 s) for the client's own close, which arrives about one RTT after the client read the last message (Deviation 21). It also makes the final
`close` a clean FIN.

**Q31. What is the 256-thread cap, what happens with more than 256 clients, and what does the client do on ERROR 503?**
`handle_new_conn` increments `g_active` under `g_mu` before creating a thread; if it is already 256, or `pthread_create` fails, the main thread sends `ERROR 503 "server busy"` and closes (`server_core.c:480-501`, `refuse_busy`
`:413`). The 257th client is therefore refused, not queued, and nothing is allocated for it. EVERY client now treats 503 as "busy, try again": the Part II clients always did (back off and retry, counted against `max_retries`), and since
commit 57ff9fb the Part I client does too (`fetch_topic_when_free`, `Part1/client.c:329`: back-off, at most 100 tries, then exit 1; Deviation 26). Before that, 44 of 300 simultaneous Part I clients exited 1; now all 300 finish. The session
table has the same idea: when all 256 slots are fresh ACTIVE sessions, `session_create` fails and the server answers 503 "session table full".

**Q31b. What if a client sends garbage, a huge length, or sends one byte at a time?**
All three are handled by `conn_recv_request` (`server_core.c:346`, Deviation 25), which every server uses for requests. *Garbage*: `hdr_decode` rejects a wrong magic/version/type -> `NET_PROTO` -> `ERROR 400`, connection closed. *Huge length*: the header is checked before any allocation: only GET, RESUME, MANIFEST_REQ and FETCH are accepted and `payload_len` must be <= `FDS_MAX_REQUEST_PAYLOAD` = 512 bytes (the largest real request is 349), so a 64 MiB "MANIFEST" is refused at once with nothing buffered (before, three such connections added 190 MB of memory). *Slow drip*: the whole request (header + payload) must arrive within an ABSOLUTE 30 s deadline (`FDS_SRV_TIMEOUT_S`, `now_ns()` based, `poll()` on the time left); `SO_RCVTIMEO` alone restarts on every byte, so a peer sending one byte every 29 s could hold a thread for days, and in `single` mode the whole server. The same idea protects the end of a transfer: `drain_until_peer_closes` waits at most 5 s in total. *Unknown topic keys* only cost a directory re-scan at most once per second (section 3.5).

**Q32. What happens if the file list of a topic changes while the server runs?**
A topic is an immutable snapshot. New topic folders are picked up by a re-scan (when an unknown topic is requested, at most once per second); files added to an existing topic are not. The server sends the sizes from the map,
and if a file shrank, `send_data` detects `sendfile == 0` and closes the connection (`ERR_500`). In Case 2/Enhanced, check 3 (name at index) and check 4 (offset <= size) reject a checkpoint if the folder changed
(Enhanced then restarts from the manifest). Tip: prepare a new topic under a name starting with `.` and `rename` it into place.

**Q33. How does the client protect itself from a malicious server?**
Header: magic/version/type/length checked before allocation (`hdr_decode`). Payloads: strict decoders (exact length, no trailing bytes, strings bounded, no embedded NUL). File names from the network must pass
`name_is_safe` (no `/`, no leading `.`, no control characters, <= 255 bytes), so the client cannot be tricked into writing outside its topic folder. The manifest's count is bounded by its payload length before `calloc`, sizes are
summed with overflow checking, and ERROR text has control characters replaced. The Enhanced client also checks that every `FILE_HDR` matches the request and the manifest before writing a byte.

**Q34. What does each case log, and how do you tell a Case 1 run from a Case 2 run in the CSV?**
The `case` column (`part1`, `case1`, `case2`, `enhanced`) and the `kind` column (`get`, `resume`, `manifest`, `fetch`). Case 1: all rows `kind=get`, many with `faulted=1`, and the client's `data_bytes_rx` far larger than
`useful_bytes`. Case 2: first row `get`, the others `resume`, client `data_bytes_rx == useful_bytes`. Enhanced: one `manifest` row (data_bytes 0) then `fetch` rows (one per worker connection); `k` and `io` columns are set in
the client row.

**Q35. Why does the Part I client not use a receive time-out, while the Part II clients use 15 s?**
In single-threaded mode a connected client may legitimately wait silently until every earlier client was served; with 32 clients any fixed time-out would fire and make later clients fail (Deviation 1). In Part II the server is
always multi-threaded and a silent server means a dead one, so a 15 s `SO_RCVTIMEO` turns a hang into a retriable failure (`RX_LOST`, EAGAIN shown as "no data for 15 s").

**Q36. What does `max_retries` mean in each case?**
Case 1: the number of ATTEMPTS (every attempt counts); exit 2 when reached (default 100). Case 2: the number of CONSECUTIVE attempts WITHOUT progress (progress = more data bytes stored, or a later file index); default 20
(`run_transfer`, `Part2_Case2/client.c:589`). Enhanced: per connection, the streak of consecutive attempts that stored no byte; a connection reaching the limit aborts the whole run with exit 2. Exit codes: 0 ok, 1 fatal
(usage, 404, protocol error, local I/O error), 2 gave up.

**Q37. Why can the run never finish at p = 1?**
Every chunk draw faults, and each fault leaves at least one byte of the chunk unsent (`prefix < chunk_len`, `transfer.c:253`). Case 1 never gets a clean attempt. Case 2/Enhanced advance by a random prefix each time, but a chunk of
remaining length n shrinks to at most 1 byte left, where prefix is always 0 and no progress is made; after `max_retries` consecutive attempts without progress they exit 2 (Deviation 9; `test_case2.sh` checks p = 1 -> exit 2).

**Q38. What does `topic_find` do with "Cane" vs "cane"?**
The assignment's table writes "Cane" while the folder is "cane". `topic_find` tries an exact case-sensitive `bsearch` first, then a case-insensitive `strcasecmp` scan, and returns the topic with its canonical key. The client still
writes the files to `output_dir/<topic exactly as typed>/` (Deviation 4), because the grader looks for the folder named as it typed it.

**Q39. What do `-Wall -Wextra`, `make strict`, and the sanitizers give you?**
`-Wall -Wextra` catch common mistakes at compile time; `make strict` adds `-Werror` so a warning fails the build (our gate for gcc, and clang if installed). `make SAN=address` runs AddressSanitizer + UBSan to find memory errors and
leaks; `make SAN=thread` runs ThreadSanitizer to find data races. The shipped `make` has no `-Werror` (Deviation 5) so that a new warning on the grader's compiler cannot stop the executables from being built.

**Q40. Walk me through what happens between `connect()` and the first byte of file data in Case 2.**
`tcp_connect` -> `set_timeouts`, `set_nodelay` -> `start_with_get`: `enc_get`, `send_encoded`; server `accept4` -> `handle_new_conn` -> `serve_client` -> `conn_recv_request` -> `handle_get` -> `topic_find_or_rescan` ->
`session_create` -> `serve_topic`: `TOPIC_BEGIN` (with `MSG_MORE`) then `FILE_HDR #0` (with `MSG_MORE`) then `sendfile`. Client: `recv_expect(TOPIC_BEGIN)` -> `save_checkpoint` (`.session` with the session id) -> `recv_topic` ->
`recv_file` -> `on_file_hdr` writes `.session` with file #0 -> `open(.part)` -> `recv`/`write` loop.

---------------------------------------------------------------------------

## 9. Study plan per team member

Cover table of the design document (who built what), the sections to master FIRST, and a second pass. Everybody must still know everything: the examiner may ask any of you about any module, and "it is my teammate's part" is
not an answer. The best preparation is to take a module you did NOT write and explain it aloud to a teammate.

| # | Name | Area (design cover table) | Master first | Then |
|---|---|---|---|---|
| 1 | Madhav Chauhan | Part I, single/multi-threaded server modes, basic client-server communication | 3.7 server_core (accept loop, single vs multi, cap, shutdown); 4.1 Part I; Q6, Q7, Q23-Q25, Q30, Q31 | 3.2 net; 2 protocol overview; 6 measurement |
| 2 | Dhruva Bhatiya | Part I message handling, topic/file management, client-side file reception, testing single/multi | 2 wire protocol; 3.3 protocol; 3.5 topic_map; 3.6 transfer (client half); Q1-Q3, Q32, Q33, Q38 | 3.1 util; 3.4 log; tests (`tests/README.md`) |
| 3 | Tasvi Bhalani | Part II Case 1: fault injection, connection failure handling, retry logic, full retransmission | 3.8 fault; 4.2 Case 1; Q8, Q9, Q36, Q37; section 6.4 predictions | 3.6 transfer (server half: `send_chunks`); Q10 contrast with Case 2 |
| 4 | Tanvi Kakadiya | Part II Case 2: session management, checkpoints, RESUME/RESUME_OK, recovery, partial transfer | 3.9 session; 3.10 checkpoint; 4.3 Case 2; Q10-Q17, Q40 | 3.6 transfer (client half, `reopen_part`); 5 concurrency |
| 5 | Bhagy Parmar | Part II Case 2 Enhanced: parallel connections, dynamic work queue, concurrent transfers, enhanced recovery | 3.11 workq; 4.4 Enhanced; 5 concurrency; Q18, Q19, Q27, Q28 | 3.9 session (Enhanced `sent_off`, check 5 per file); poll engine state machine |
| 6 | Kanika Pal | Enhanced performance, fault recovery, zero-copy transfer, testing, performance evaluation | 3.6 transfer (sendfile, MSG_MORE); 4.4 poll engine; 6 measurement; Q5, Q20-Q22, Q29, Q30 | 3.7 drain/end_ns; `tools/exp/analyze.py`; 7 design table |

Suggested order for everybody: (1) section 1 and 2 in one sitting; (2) read `common/net.c` and `common/protocol.c` once end to end; (3) trace one request per case with the function lists of section 4, with the real
programs running (`make` in each folder, `tests/gen_dataset.py` for data); (4) read section 5 and draw the lock table from memory; (5) answer Q1-Q40 aloud without looking.

A good drill: run `FDS_LOG=/tmp/s.csv FDS_EVENTS=/tmp/e.csv` for a Case 2 server with `p = 0.02` and a seed, run a client, and explain every row of both CSVs.

---------------------------------------------------------------------------

## 10. Known loose ends (things that are slightly inconsistent)

Be ready to be honest about these if asked; none affects correctness of the transfers. (Two items of earlier
versions of this list are fixed and removed: the stale `seed ^ conn_id` header comments, and the `analyze.py`
faults-vs-reconnects note for Enhanced runs, which now skips Enhanced, Deviation 24.)

1. **Prediction formulas.** With the per-connection fault model (Deviation 28) the predictions in `predictions()` (`analyze.py:211`) are
   simple: attempts `1/(1−p)`, faults `p/(1−p)` per client (up to `K·p/(1−p)` for Enhanced), Case 1 overhead `p/(1−p) × (T−1)/2 ÷ T`.
   The Enhanced fault count is an upper bound: a worker whose queue is already empty does not reconnect after a cut.
2. **`sendfile` and SIGPIPE** rely on a process-wide `SIG_IGN` set in `server_run` (`server_core.c:680`). That is fine for our programs, but any new caller of `send_file_range` outside `server_run` must also ignore SIGPIPE
   (documented at `transfer.c:13-16` and in the comment block at `transfer.h:27-32`).
3. **Mixed return convention** of `session_validate_resume`: 0 = OK, 1..5 = reject reason, -1 = table full. The callers test `rc < 0` before `rc > 0` (`Part2_Case2/server.c:221-230`).
4. **Known limitation (Deviation 19):** Case 2 and Enhanced cannot resume files whose name is longer than 250 bytes (`<name>.part` must fit in 255).
5. **Case 1 is expected to give up on large topics at moderate p** (not a bug): an attempt of N chunks succeeds with probability (1−p)^N, so for example `many`/`big` at p = 0.05 end with exit 2 (`max_retries` = 100). Say so with the formula; Case 2 and Enhanced are the answer to it.
