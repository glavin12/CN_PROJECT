# IT305 FDS project: rules for every contributor (human or agent)

Fault-tolerant topic-based file distribution service in C (gnu11, pthreads, TCP/IPv4, Linux).
Group 006 + 043. **Always use `006_043`** in names (never `043_006`).

Sources of truth:
- `docs/assignment.txt`: the guidelines. They are binding and override everything else.
- `docs/design_document.txt`: our submitted design. Follow it.
- `docs/DEVIATIONS.md`: the reviewed changes to the design. These override the design document.
- The plan is in `~/.claude/plans/hey-so-i-want-merry-lemon.md`.

## Hard rules from the guidelines (an auto-grader checks these)
- Executables are named exactly `server` and `client`. `make` inside `Part1/`, `Part2_Case1/`, `Part2_Case2/` and `Part2_Case2_Enhanced/` produces **exactly these two executable files** in that folder, with **zero warnings**. Put no other executable file (scripts included) in those folders.
- Invocations, with no interactive prompts:
  - Part 1 server: `./server <port> <topics_root_dir>` plus optional `[single|multi]` (default multi)
  - Part 2 server, all cases: `./server <port> <topics_root_dir> <failure_probability>` plus optional `[seed]`
  - Client, all cases: `./client <server_ip> <server_port> <topic> <output_dir>`. Optional trailing arguments:
    - Case 1: `[max_retries=100]`
    - Case 2: `[max_retries=20]`
    - Enhanced: `[connections=4] [max_retries=20] [threads|poll]` (`io=poll` also accepted)
- The client writes files under `output_dir/<topic exactly as typed>/`. When it succeeds, that directory holds **only** the topic's files (no `.part`, `.session` or tmp files).
- **Never hard-code a path, port or IP address.** Optional logging comes only from env vars: `FDS_LOG`, `FDS_EVENTS`, `FDS_RUN_ID`, `FDS_CLIENT_ID`, `FDS_SNDBUF`, `FDS_RCVBUF`, `FDS_QUIET`.
- Exit codes: 0 = OK, 1 = fatal or usage error (print a usage line), 2 = the client gave up after max_retries.

## Code layout
- `common/` is the **only** place where you edit shared modules: fds.h, util, net, protocol, log, topic_map, transfer, server_core, fault, session, checkpoint, workq.
- `tools/sync_common.sh` copies them into each Part folder, and `tools/check_common.sh` detects drift. Never edit the copies inside `Part*/`.
- Each Part folder holds its own `Makefile`, `server.c` and `client.c`. Enhanced also has `enh_client.h` and `client_poll.c`.
- Unit tests live in `tests/unit/test_<module>.c` (run with `make -C tests/unit run`). Integration tests live in `tests/integration/`, generators and helpers in `tests/`, and scripts in `tools/`.
- The headers in `common/*.h` and `Part2_Case2_Enhanced/enh_client.h` are **frozen contracts**. Do not change a signature or a documented behaviour. If one is wrong, stop and report it instead of working around it.

## Coding conventions
- Style: K&R, 4-space indent, `snake_case`, and a block comment at the top of every file. Comments explain *why*: someone in the viva must be able to defend every function.
- Full-length socket I/O goes through `sendall`/`recvall` (EINTR-safe, `MSG_NOSIGNAL`). Integers go on the wire big-endian via `put_u*`/`get_u*`; never send raw structs.
- No global mutable state without a lock. Use `drand48_r` (never `rand`/`drand48`) and `now_ns()` (CLOCK_MONOTONIC) for intervals.
- Check every syscall's return value. Leak no fd, memory or thread on any path, including fault paths.
- Build gate: `make strict` (`-Werror`) is clean with gcc (and clang if installed), and `make clean && make SAN=address` / `SAN=thread` runs clean.

## Agent workflow
- Edit only the files you own (named in your task). If something outside your ownership is broken, report it in your final message.
- Commit your work on your own branch or worktree with a clear message. Do not push.
