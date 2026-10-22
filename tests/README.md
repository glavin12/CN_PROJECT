# Tests

Only `bash`, coreutils and the `python3` standard library are needed.

## Run everything

```bash
tests/integration/run_all.sh              # make -s in every folder, then all tests
tests/integration/run_all.sh --quick      # skip the slow checks (32 clients, big topics at high p, ...)
tests/integration/run_all.sh --only case2 # one file: build | cli | part1 | case1 | case2 | enhanced
tests/integration/run_all.sh --no-build   # use the binaries as they are
KEEP=1 tests/integration/run_all.sh       # keep logs, outputs and CSVs (path printed at the end)
SAN=address tests/integration/run_all.sh  # rebuild with a sanitizer first (also SAN=thread)
make -C tests/unit run                    # C unit tests of common/
```

Each test prints one `PASS` / `FAIL` / `SKIP` line per check and ends with a
`RESULT` line. `run_all.sh` prints a table and exits 1 if any check failed.
If a folder has no `server`/`client` yet, its tests print `SKIP: <folder> not built`
and exit 0. A single file can also be run on its own: `bash tests/integration/test_part1.sh`.

| File | What it checks |
|---|---|
| `test_build.sh` | `make` has zero warnings, exactly `server` + `client` are executable, `make strict` with gcc/clang, `tools/check_common.sh` |
| `test_cli.sh` | the auto-grader CLI: usage errors and exit codes 1/2, closed port, unknown topic, no hard-coded IP/path literals |
| `test_part1.sh` | every edge topic, `MIXED` → `output_dir/MIXED`, 32 concurrent clients, single mode, kill -9, CSV logs, re-scan, SIGINT |
| `test_case1.sh` | faults restart the topic (redundant bytes), p = 1 → exit 2, cuts at any byte |
| `test_case2.sh` | resume with **no redundant bytes**, cuts at any byte, kill -9 + restart, p = 1 → exit 2 |
| `test_enhanced.sh` | K ∈ {1,2,4,8} × threads/poll, cuts at any byte, kill -9 + restart, defaults |

Every test runs on `127.0.0.1` with random free ports and its own temp directory
(`${TMPDIR:-/tmp}/fds-tests.*`, removed on exit unless `KEEP=1`).

## Tools

```bash
# deterministic datasets (same arguments -> identical bytes)
python3 tests/gen_dataset.py /tmp/edge --profile edge                 # corner cases, ~26 MB
python3 tests/gen_dataset.py /tmp/animals --profile animals --scale 0.1 --seed 1

# exact copy? (names, sha256, and no .part/.session/extra files)
tests/verify_output.sh /tmp/edge/many out/many        # "OK 400 files ... bytes" or FAIL lines

# cut connections at random bytes / throttle, between a client and a p = 0 server
python3 tests/faultproxy.py --listen 9001 --target 127.0.0.1:9000 \
        --cut-prob 0.5 --window 200000 --max-cuts 200 --seed 1   # or --rate 2000000
./client 127.0.0.1 9001 many out
```

`faultproxy.py` prints `READY` once it listens, logs every `CUT conn=<n> at s2c byte <pos>`
on stderr and a `SUMMARY` line on SIGTERM.

Notes:
- The `edge` topic `names` has a 255-byte file name. Case 2 and Enhanced cannot store
  `<name>.part` for it (that is 260 bytes, more than NAME_MAX), so their tests use the
  topic `names250` instead (see `make_names250` in `integration/lib.sh`).
- p is per connection (DEVIATIONS 28), so the fault tests use p = 0.5 and 0.9: an attempt
  succeeds with probability 1 − p whatever the topic size. Every run starts a new server, so
  its first connection is always conn_id 1; the Case 1 seeds were picked because they cut
  that connection (a fault-free run would prove nothing about restarts).
