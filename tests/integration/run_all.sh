#!/usr/bin/env bash
# run_all.sh - build every case folder and run the integration test suite.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
#   tests/integration/run_all.sh [--only build|cli|part1|case1|case2|enhanced]
#                                [--quick] [--no-build]
#
#   --only NAME   run just tests/integration/test_NAME.sh
#   --quick       skip the slow checks (big topics at high p, 32 clients, ...)
#   --no-build    do not run `make -s` in the case folders first
#
# Environment:
#   KEEP=1        keep every test's temp dir (logs, outputs, CSVs) for debugging
#   SAN=address|thread   build with that sanitizer first (make clean + make SAN=...)
#   TMPDIR        where the temp dirs go (default /tmp)
#
# A folder without server/client executables makes its tests print SKIP (not
# FAIL), so the suite can run while the cases are still being written.
# Exit status: 0 if no check failed, 1 otherwise.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
ALL_TESTS=(build cli part1 case1 case2 enhanced)
FOLDERS=(Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced)

usage() {
    sed -n '6,16s/^# \{0,1\}//p' "$0"
}

only=""
quick=0
build=1
while (( $# > 0 )); do
    case $1 in
        --only)     [[ $# -ge 2 ]] || { usage; exit 1; }; only=$2; shift 2 ;;
        --only=*)   only=${1#*=}; shift ;;
        --quick)    quick=1; shift ;;
        --no-build) build=0; shift ;;
        -h|--help)  usage; exit 0 ;;
        *)          echo "run_all.sh: unknown option '$1'" >&2; usage >&2; exit 1 ;;
    esac
done

tests=("${ALL_TESTS[@]}")
if [[ -n $only ]]; then
    if [[ ! -f $HERE/test_$only.sh ]]; then
        echo "run_all.sh: no test named '$only' (choose: ${ALL_TESTS[*]})" >&2
        exit 1
    fi
    tests=("$only")
fi

# ---- 1. build ------------------------------------------------------------------
if (( build )); then
    echo "== building every folder (make -s${SAN:+ SAN=$SAN})"
    for d in "${FOLDERS[@]}"; do
        if [[ ! -f $ROOT/$d/server.c || ! -f $ROOT/$d/client.c ]]; then
            echo "   $d: no server.c/client.c yet, its tests will SKIP"
            continue
        fi
        if [[ -n ${SAN:-} ]]; then
            make -s -C "$ROOT/$d" --no-print-directory clean >/dev/null 2>&1 || true
        fi
        # shellcheck disable=SC2086  # ${SAN:+...} must vanish entirely when unset
        if out=$(make -s -C "$ROOT/$d" --no-print-directory ${SAN:+SAN=$SAN} 2>&1); then
            echo "   $d: built"
        else
            echo "   $d: BUILD FAILED (its tests may run an old binary or SKIP)"
            printf '%s\n' "$out" | sed 's/^/      /'
        fi
    done
fi

# ---- 2. run the tests ---------------------------------------------------------------
LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/fds-suite.XXXXXX")
colour=0
[[ -t 1 ]] && colour=1
declare -A r_pass=() r_fail=() r_skip=() r_secs=() r_note=()

for t in "${tests[@]}"; do
    log=$LOG_DIR/test_$t.log
    echo
    echo "######## test_$t.sh"
    start=$SECONDS
    rc=0
    # tee: show the output live and keep a copy to read the RESULT line from.
    QUICK=$quick FDS_COLOR=$colour bash "$HERE/test_$t.sh" 2>&1 | tee "$log" || rc=$?
    r_secs[$t]=$(( SECONDS - start ))
    line=$(grep '^RESULT ' "$log" | tail -n 1 || true)
    if [[ $line =~ pass=([0-9]+)\ fail=([0-9]+)\ skip=([0-9]+) ]]; then
        r_pass[$t]=${BASH_REMATCH[1]}
        r_fail[$t]=${BASH_REMATCH[2]}
        r_skip[$t]=${BASH_REMATCH[3]}
        r_note[$t]=""
        if (( rc != 0 && r_fail[$t] == 0 )); then
            r_fail[$t]=1
            r_note[$t]="exit status $rc"
        fi
    else
        # the script died before its summary (syntax error, crash, ...)
        r_pass[$t]=0 r_fail[$t]=1 r_skip[$t]=0 r_note[$t]="no RESULT line (exit $rc)"
    fi
done

# ---- 3. summary -------------------------------------------------------------------
tp=0 tf=0 ts=0
echo
echo "================ integration summary$( (( quick )) && echo ' (--quick)')"
printf '  %-20s %6s %6s %6s %7s\n' "test" "pass" "fail" "skip" "time"
for t in "${tests[@]}"; do
    status=OK
    (( r_fail[$t] > 0 )) && status=FAILED
    (( r_fail[$t] == 0 && r_pass[$t] == 0 && r_skip[$t] > 0 )) && status=SKIPPED
    printf '  %-20s %6d %6d %6d %6ss  %s %s\n' "test_$t.sh" "${r_pass[$t]}" "${r_fail[$t]}" \
        "${r_skip[$t]}" "${r_secs[$t]}" "$status" "${r_note[$t]}"
    tp=$(( tp + r_pass[$t] ))
    tf=$(( tf + r_fail[$t] ))
    ts=$(( ts + r_skip[$t] ))
done
printf '  %-20s %6d %6d %6d\n' "TOTAL" "$tp" "$tf" "$ts"

if (( tf > 0 )) || [[ ${KEEP:-0} == 1 ]]; then
    echo "  per-test logs: $LOG_DIR"
else
    rm -rf -- "$LOG_DIR"
fi
(( tf == 0 ))
