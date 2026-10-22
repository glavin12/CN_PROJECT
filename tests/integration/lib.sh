# shellcheck shell=bash
# lib.sh - helpers shared by every integration test (tests/integration/test_*.sh).
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# Sourced, never executed:   set -euo pipefail; source "$(dirname "$0")/lib.sh"
#
# What it gives a test:
#   ROOT, TESTS_DIR, HOST_IP, TEST_TMP   locations (TEST_TMP is removed on exit unless KEEP=1)
#   pass / fail / skip / info / section  coloured PASS/FAIL lines and counters
#   summary                              totals + "RESULT ..." line; non-zero if anything failed
#   require_bins <folder>                SKIP the rest of the file if server/client are missing
#   mktemp_dir, pick_port, now_ms, is_quick, wait_until
#   start_server / stop_server           background server, waits until it listens
#   start_proxy / stop_proxy             tests/faultproxy.py in front of a server
#   run_client                           foreground client with a time-out (sets CLIENT_RC)
#   start_client_bg / wait_client        background clients (concurrency, kill -9 tests)
#   run_with_timeout                     coreutils timeout wrapper
#   csv_sum / csv_count / csv_max / csv_header   read FDS_LOG / FDS_EVENTS files (python csv)
#   topic_bytes / topic_files / stored_bytes / has_part
#   gen_edge, verify_ok, client_ok, show_log
#
# Every process a test starts is recorded; the EXIT trap kills whatever is
# still running, so a failing or interrupted test never leaves servers behind.
#
# Note on `set -e`: bash ignores it inside any function that is called as an
# `if`/`until`/`&&` condition, so the helpers below check return codes
# explicitly instead of relying on it.

# ---- locations and settings ---------------------------------------------------
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TESTS_DIR=$ROOT/tests
HOST_IP=127.0.0.1               # every test runs server, proxy and clients on this machine
QUICK=${QUICK:-0}               # run_all.sh --quick exports QUICK=1
KEEP=${KEEP:-0}                 # KEEP=1: do not delete the temp dir (logs, outputs, CSVs)
CLIENT_TIMEOUT=${CLIENT_TIMEOUT:-120}   # seconds before a client run counts as hung
TEST_NAME=$(basename "$0" .sh)
export ROOT TESTS_DIR

# The exact CSV headers documented in common/log.h (the contract, not log.c).
SERVER_CSV_HEADER="run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status"
EVENTS_CSV_HEADER="run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail"
CLIENT_CSV_HEADER="run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status"

# ---- coloured output and counters ---------------------------------------------
if [[ -t 1 || ${FDS_COLOR:-0} == 1 ]]; then
    C_GREEN=$'\e[32m' C_RED=$'\e[31m' C_YELLOW=$'\e[33m' C_BOLD=$'\e[1m' C_RESET=$'\e[0m'
else
    C_GREEN='' C_RED='' C_YELLOW='' C_BOLD='' C_RESET=''
fi
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

pass()    { PASS_COUNT=$((PASS_COUNT + 1)); printf '%sPASS%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
fail()    { FAIL_COUNT=$((FAIL_COUNT + 1)); printf '%sFAIL%s %s\n' "$C_RED" "$C_RESET" "$*"; }
skip()    { SKIP_COUNT=$((SKIP_COUNT + 1)); printf '%sSKIP%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
info()    { printf '     %s\n' "$*"; }
section() { printf '\n%s== %s%s\n' "$C_BOLD" "$*" "$C_RESET"; }

# check <description> <command...>: PASS if the command succeeds, FAIL otherwise.
check() {
    local desc=$1
    shift
    if "$@"; then pass "$desc"; else fail "$desc"; fi
}

# check_eq <description> <actual> <expected>
check_eq() {
    if [[ "$2" == "$3" ]]; then
        pass "$1 ($2)"
    else
        fail "$1: got '$2', expected '$3'"
    fi
}

# summary: print the totals and a machine-readable line for run_all.sh.
# Returns non-zero if any check failed (so it can be a test's last command).
summary() {
    local colour=$C_GREEN
    if (( FAIL_COUNT > 0 )); then colour=$C_RED; fi
    printf '\n%s%s: %d passed, %d failed, %d skipped%s\n' \
        "$colour" "$TEST_NAME" "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT" "$C_RESET"
    printf 'RESULT %s pass=%d fail=%d skip=%d\n' "$TEST_NAME" "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"
    (( FAIL_COUNT == 0 ))
}

# abort <message>: FAIL, print the summary and stop this test file.
abort() {
    fail "$*"
    summary || true
    exit 1
}

is_quick() { [[ $QUICK == 1 ]]; }

# ---- binaries -------------------------------------------------------------------
have_bins() { [[ -x $ROOT/$1/server && -x $ROOT/$1/client ]]; }

# require_bins <folder>: skip the rest of this test file if the folder is not built.
require_bins() {
    if ! have_bins "$1"; then
        echo "SKIP: $1 not built"
        skip "$1 not built (no server/client executable in $1/)"
        summary || true
        exit 0
    fi
}

# ---- temp files, processes and cleanup ----------------------------------------------
TEST_TMP=$(mktemp -d "${TMPDIR:-/tmp}/fds-tests.XXXXXX")
_SEQ=0                    # counter for unique log file names
_FDS_PIDS=()              # every background process we started and have not reaped
declare -A _CLIENT_LOG_OF=()

# mktemp_dir [name]: a new empty directory inside TEST_TMP (safe to call in $(...)).
mktemp_dir() { mktemp -d "$TEST_TMP/${1:-dir}.XXXXXX"; }

_track()   { _FDS_PIDS+=("$1"); }
_untrack() {
    local p keep=()
    for p in "${_FDS_PIDS[@]}"; do [[ $p == "$1" ]] || keep+=("$p"); done
    _FDS_PIDS=("${keep[@]}")
}

_fds_cleanup() {
    local rc=$? pid
    trap - EXIT
    for pid in "${_FDS_PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [[ $KEEP == 1 ]]; then
        echo "KEEP=1: test files kept in $TEST_TMP"
    else
        rm -rf -- "$TEST_TMP"
    fi
    exit "$rc"
}
trap _fds_cleanup EXIT
trap 'echo "interrupted"; exit 130' INT TERM

# now_ms: wall-clock milliseconds (EPOCHREALTIME uses the locale's decimal mark).
now_ms() {
    local t=${EPOCHREALTIME/[.,]/}
    echo $(( t / 1000 ))
}

# _alive <pid>: true while the process exists and is not a zombie.
_alive() {
    local st
    { st=$(< "/proc/$1/stat"); } 2>/dev/null || return 1
    st=${st##*) }
    [[ ${st:0:1} != Z ]]
}

# wait_until <seconds> <command...>: poll every 50 ms until the command succeeds.
wait_until() {
    local secs=$1 deadline
    shift
    deadline=$(( $(now_ms) + secs * 1000 ))
    until "$@"; do
        (( $(now_ms) < deadline )) || return 1
        sleep 0.05
    done
}

# run_with_timeout <seconds> <command...>: exit status of the command, 124 on time-out.
# --foreground keeps the command in our process group, so Ctrl+C still reaches it.
run_with_timeout() {
    local secs=$1
    shift
    timeout --foreground -k 2 "$secs" "$@"
}

# A non-interactive bash starts background jobs with SIGINT ignored, and
# python ignores SIGPIPE/SIGXFSZ; both would be inherited across exec. This
# launcher restores the default dispositions so a server sees SIGINT exactly
# as after Ctrl+C, and a missing SIGPIPE handler is NOT hidden by the tests.
_SIGRESET_PY='import os, signal, sys
for name in ("SIGINT", "SIGQUIT", "SIGTERM", "SIGPIPE", "SIGXFSZ"):
    signal.signal(getattr(signal, name), signal.SIG_DFL)
os.execvp(sys.argv[1], sys.argv[1:])'

# _spawn <log> <command...>: start in the background (stdin /dev/null, stdout and
# stderr to <log>), with default signal dispositions. Sets LAST_PID.
_spawn() {
    local log=$1
    shift
    python3 -c "$_SIGRESET_PY" "$@" </dev/null >"$log" 2>&1 &
    LAST_PID=$!
    _track "$LAST_PID"
}

# _LISTEN_PY <port> <pid> <timeout>: exit 0 once <pid> owns a socket listening
# on <port> (read from /proc, so no probe connection is made: a probe would
# count as a client in the server's CSV). Exit 2 if <pid> died, 1 on time-out.
_LISTEN_PY='import os, sys, time
port, pid, timeout = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
want = ":%04X" % port
def alive():
    try:
        st = open("/proc/%d/stat" % pid).read()
    except OSError:
        return False
    return st[st.rfind(")") + 2] != "Z"
def listening_inodes():
    found = set()
    for fn in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = open(fn).read().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            f = line.split()
            if f[1].endswith(want) and f[3] == "0A":      # 0A = TCP_LISTEN
                found.add(f[9])
    return found
deadline = time.time() + timeout
while time.time() < deadline:
    inodes = listening_inodes()
    if inodes:
        try:
            for fd in os.listdir("/proc/%d/fd" % pid):
                link = os.readlink("/proc/%d/fd/%s" % (pid, fd))
                if link.startswith("socket:[") and link[8:-1] in inodes:
                    sys.exit(0)
        except OSError:
            pass
    if not alive():
        sys.exit(2)
    time.sleep(0.05)
sys.exit(1)'

# pick_port: print a free TCP port in 20000-60000, never one this test already picked.
pick_port() {
    python3 - "$TEST_TMP/.ports" <<'EOF'
import random, socket, sys
path = sys.argv[1]
try:
    used = set(int(x) for x in open(path).read().split())
except OSError:
    used = set()
for _ in range(500):
    p = random.randint(20000, 60000)
    if p in used:
        continue
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("0.0.0.0", p))        # free if we can bind it (no SO_REUSEADDR)
    except OSError:
        continue
    finally:
        s.close()
    with open(path, "a") as f:
        f.write("%d\n" % p)
    print(p)
    sys.exit(0)
sys.exit("pick_port: no free port found")
EOF
}

# ---- server ------------------------------------------------------------------------
# start_server <folder> <port> <more server args...>
#   Runs $ROOT/<folder>/server <port> <more args...> in the background and waits
#   (max 5 s) until it listens. Environment variables set on the call
#   (FDS_LOG=... start_server ...) reach the server.
#   Sets SERVER_PID, SERVER_PORT, SERVER_LOG. Aborts the test file on failure.
start_server() {
    local folder=$1 port=$2 rc=0
    shift
    SERVER_LOG=$TEST_TMP/server.$folder.$port.$((++_SEQ)).log
    _spawn "$SERVER_LOG" "$ROOT/$folder/server" "$@"
    SERVER_PID=$LAST_PID
    SERVER_PORT=$port
    python3 -c "$_LISTEN_PY" "$port" "$SERVER_PID" 5 || rc=$?
    if (( rc != 0 )); then
        show_log "$SERVER_LOG"
        abort "$folder/server $* did not start listening ($( ((rc == 2)) && echo exited || echo 'time-out 5 s'))"
    fi
    export SERVER_PID SERVER_PORT SERVER_LOG
}

# stop_server [pid]: SIGINT, wait up to 6 s, then SIGKILL.
#   Sets STOP_RC (exit status), STOP_MS (time to exit), STOP_FORCED (1 = needed SIGKILL).
stop_server() {
    local pid=${1:-$SERVER_PID} t0 deadline
    STOP_RC=0
    STOP_FORCED=0
    t0=$(now_ms)
    deadline=$(( t0 + 6000 ))
    kill -INT "$pid" 2>/dev/null || true
    while _alive "$pid" && (( $(now_ms) < deadline )); do
        sleep 0.05
    done
    if _alive "$pid"; then
        STOP_FORCED=1
        kill -9 "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || STOP_RC=$?
    STOP_MS=$(( $(now_ms) - t0 ))
    _untrack "$pid"
}

# server_alive [pid]
server_alive() { _alive "${1:-$SERVER_PID}"; }

# ---- fault proxy -------------------------------------------------------------------
# start_proxy <listen_port> <target_port> [faultproxy options...]
#   Starts tests/faultproxy.py and waits for its READY line.
#   Sets PROXY_PID, PROXY_PORT, PROXY_LOG (its stderr: CUT and SUMMARY lines).
start_proxy() {
    local lport=$1 tport=$2 out
    shift 2
    out=$TEST_TMP/proxy.$lport.$((++_SEQ)).out
    PROXY_LOG=${out%.out}.log
    python3 "$TESTS_DIR/faultproxy.py" --listen "$lport" --target "$HOST_IP:$tport" "$@" \
        </dev/null >"$out" 2>"$PROXY_LOG" &
    PROXY_PID=$!
    PROXY_PORT=$lport
    _track "$PROXY_PID"
    if ! wait_until 5 grep -q '^READY' "$out"; then
        show_log "$PROXY_LOG"
        abort "faultproxy on port $lport did not become READY"
    fi
}

# stop_proxy [pid]: SIGTERM (prints its SUMMARY), then SIGKILL after 3 s.
stop_proxy() {
    local pid=${1:-$PROXY_PID} deadline
    deadline=$(( $(now_ms) + 3000 ))
    kill -TERM "$pid" 2>/dev/null || true
    while _alive "$pid" && (( $(now_ms) < deadline )); do sleep 0.05; done
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    _untrack "$pid"
}

# proxy_cuts [log]: number of connections the proxy cut.
proxy_cuts() { grep -c ' CUT ' "${1:-$PROXY_LOG}" || true; }

# ---- clients ---------------------------------------------------------------------
# run_client <folder> <port> <topic> <output_dir> [extra args...]
#   ./client 127.0.0.1 <port> <topic> <output_dir> [extra...] in the foreground,
#   stdin from /dev/null, killed after CLIENT_TIMEOUT seconds.
#   Sets CLIENT_RC (124 = timed out), CLIENT_MS, CLIENT_LOG.
run_client() {
    local folder=$1 port=$2 t0
    shift 2
    CLIENT_LOG=$TEST_TMP/client.$((++_SEQ)).log
    CLIENT_RC=0
    t0=$(now_ms)
    run_with_timeout "$CLIENT_TIMEOUT" "$ROOT/$folder/client" "$HOST_IP" "$port" "$@" \
        </dev/null >"$CLIENT_LOG" 2>&1 || CLIENT_RC=$?
    CLIENT_MS=$(( $(now_ms) - t0 ))
}

# start_client_bg <folder> <port> <topic> <output_dir> [extra args...]
#   Same client, in the background. Sets LAST_PID (the client itself, so
#   `kill -9 $LAST_PID` really kills it) and CLIENT_LOG.
start_client_bg() {
    local folder=$1 port=$2 log
    shift 2
    log=$TEST_TMP/client.$((++_SEQ)).log
    _spawn "$log" "$ROOT/$folder/client" "$HOST_IP" "$port" "$@"
    CLIENT_LOG=$log
    _CLIENT_LOG_OF[$LAST_PID]=$log
}

# wait_client <pid> [timeout_s]: wait for a background client.
#   Sets WAIT_RC (124 = still running after the time-out, then killed) and CLIENT_LOG.
wait_client() {
    local pid=$1 secs=${2:-$CLIENT_TIMEOUT} deadline
    deadline=$(( $(now_ms) + secs * 1000 ))
    WAIT_RC=0
    while _alive "$pid"; do
        if (( $(now_ms) > deadline )); then
            kill -9 "$pid" 2>/dev/null || true
            WAIT_RC=124
            break
        fi
        sleep 0.05
    done
    if (( WAIT_RC == 124 )); then
        wait "$pid" 2>/dev/null || true
    else
        wait "$pid" 2>/dev/null || WAIT_RC=$?
    fi
    _untrack "$pid"
    CLIENT_LOG=${_CLIENT_LOG_OF[$pid]:-}
}

# ---- CSV logs (FDS_LOG / FDS_EVENTS) ----------------------------------------------
# All take optional filters "column=value" (exact match, after CSV unquoting).
#   csv_sum <file> <column> [col=val...]    sum of a numeric column
#   csv_max <file> <column> [col=val...]    maximum (0 if no row)
#   csv_count <file> [col=val...]           number of rows
#   csv_header <file>                       the first line
# A missing file counts as "no rows" (prints 0) so $(...) never trips set -e.
_CSV_PY='import csv, sys
op, path, rest = sys.argv[1], sys.argv[2], sys.argv[3:]
col = None
if op in ("sum", "max"):
    col, rest = rest[0], rest[1:]
flt = [a.split("=", 1) for a in rest]
try:
    with open(path, newline="") as f:
        rows = [r for r in csv.DictReader(f) if all(r.get(k) == v for k, v in flt)]
except OSError:
    rows = []
if op == "count":
    print(len(rows))
    sys.exit(0)
if rows and col not in rows[0]:
    sys.exit("csv: no column %r in %s" % (col, path))
vals = []
for r in rows:
    try:
        vals.append(int(r[col]))
    except ValueError:
        vals.append(float(r[col]))
if op == "sum":
    print(sum(vals))
else:
    print(max(vals) if vals else 0)'

csv_sum()    { python3 -c "$_CSV_PY" sum "$@"; }
csv_max()    { python3 -c "$_CSV_PY" max "$@"; }
csv_count()  { python3 -c "$_CSV_PY" count "$@"; }
csv_header() { head -n 1 -- "$1" 2>/dev/null || true; }

# ---- datasets and outputs -----------------------------------------------------------
# topic_bytes <dir> / topic_files <dir>: what the server serves from a topic folder
# (regular files directly inside, names not starting with '.').
topic_bytes() {
    find "$1" -mindepth 1 -maxdepth 1 -type f ! -name '.*' -printf '%s\n' \
        | awk '{ s += $1 } END { printf "%d\n", s }'
}
topic_files() {
    find "$1" -mindepth 1 -maxdepth 1 -type f ! -name '.*' -printf '.' | wc -c
}

# stored_bytes <dir>: bytes a client has on disk (completed files + .part files).
stored_bytes() {
    find "$1" -mindepth 1 -maxdepth 1 -type f ! -name '.session*' -printf '%s\n' \
        | awk '{ s += $1 } END { printf "%d\n", s }'
}

# has_part <dir>: true if at least one *.part file exists in <dir>.
has_part() { [[ -n $(find "$1" -maxdepth 1 -name '*.part' -print -quit 2>/dev/null) ]]; }

# gen_edge <dir> [seed]: generate the "edge" dataset (aborts the test on failure).
gen_edge() {
    local log=$TEST_TMP/gen.$((++_SEQ)).log
    if ! python3 "$TESTS_DIR/gen_dataset.py" "$1" --profile edge --seed "${2:-1}" >"$log" 2>&1; then
        show_log "$log"
        abort "gen_dataset.py --profile edge failed"
    fi
}

# make_names250 <data_root>: add topic "names250" = names/ without names longer
# than 250 bytes, plus one file with a 250-byte name. Case 2 and Enhanced
# store <name>.part, and "<255-byte name>.part" exceeds NAME_MAX (255), so the
# full "names" topic cannot work there (checkpoint.h contract); 250 + 5 fits.
make_names250() {
    local root=$1 f n tmp
    local LC_ALL=C                    # ${#n} counts bytes, not UTF-8 characters
    tmp=$root/.names250.tmp
    mkdir "$tmp"
    for f in "$root/names"/*; do
        n=${f##*/}
        if (( ${#n} <= 250 )); then cp -- "$f" "$tmp/"; fi
    done
    n=$(printf 'M%.0s' {1..246}).jpg  # 246 + 4 = 250 bytes
    head -c 70000 -- "$root/boundary/b_131073.bin" >"$tmp/$n"
    mv "$tmp" "$root/names250"        # appears atomically (hidden while filling)
}

# show_log <file> [n]: print the last n lines of a log, indented.
show_log() {
    local f=$1 n=${2:-15}
    [[ -s $f ]] || return 0
    printf '       --- last lines of %s\n' "$f"
    tail -n "$n" -- "$f" | sed 's/^/       | /'
}

# verify_ok <description> <source_topic_dir> <output_topic_dir>
verify_ok() {
    local desc=$1 out
    if out=$("$TESTS_DIR/verify_output.sh" "$2" "$3" 2>&1); then
        pass "$desc: $out"
    else
        fail "$desc"
        printf '%s\n' "$out" | sed -n '1,12s/^/       /p'
    fi
}

# client_ok <description> <source_topic_dir> <output_topic_dir>
#   The last run_client (or wait_client) succeeded AND its output is exact.
#   Uses CLIENT_RC, or WAIT_RC when called as `RC=$WAIT_RC client_ok ...`.
client_ok() {
    local desc=$1 rc=${RC:-$CLIENT_RC}
    if (( rc != 0 )); then
        if (( rc == 124 )); then
            fail "$desc: client timed out"
        else
            fail "$desc: client exit status $rc"
        fi
        show_log "$CLIENT_LOG"
        return 0
    fi
    verify_ok "$desc" "$2" "$3"
}
