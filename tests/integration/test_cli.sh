#!/usr/bin/env bash
# test_cli.sh - the Standard CLI Interface of the guidelines, for every folder.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# Contract (CLAUDE.md, design 1.3):
#   Part 1 server:  ./server <port> <topics_root_dir> [single|multi]
#   Part 2 server:  ./server <port> <topics_root_dir> <failure_probability> [seed]
#   client:         ./client <server_ip> <server_port> <topic> <output_dir> + optional:
#                     Case 1 [max_retries]   Case 2 [max_retries]
#                     Enhanced [connections 1..16] [max_retries] [threads|poll]
#   exit 0 = OK, 1 = fatal/usage error (with a usage line), 2 = gave up after max_retries.
# Every program runs with stdin from /dev/null, so an interactive prompt would
# fail the test instead of hanging it, and every run has a time-out.
# A static grep also rejects hard-coded addresses/paths in the C sources.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

FOLDERS=(Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced)

# expect_exit <description> <expected_rc> <want_usage 0|1> <timeout_s> <command...>
#   Runs the command (stdin /dev/null) and checks its exit status, and with
#   want_usage=1 that stderr contains "usage" (any case).
expect_exit() {
    local desc=$1 want=$2 want_usage=$3 secs=$4 rc=0 err t0 ms
    shift 4
    err=$TEST_TMP/cli.$((++_SEQ)).err
    t0=$(now_ms)
    run_with_timeout "$secs" "$@" </dev/null >"$err.stdout" 2>"$err" || rc=$?
    ms=$(( $(now_ms) - t0 ))
    if (( rc == 124 )); then
        fail "$desc: still running after ${secs} s (expected exit $want)"
    elif (( rc != want )); then
        fail "$desc: exit $rc, expected $want"
        show_log "$err" 6
    elif (( want_usage )) && ! grep -qi usage "$err"; then
        fail "$desc: exit $rc but no 'usage' line on stderr"
        show_log "$err" 6
    else
        pass "$desc: exit $rc ($ms ms)"
    fi
}

# A tiny topics root: one topic with one file.
DATA=$(mktemp_dir data)
mkdir -p "$DATA/alpha"
printf 'hello\n' >"$DATA/alpha/a.txt"
OUT=$(mktemp_dir out)
MISSING=$TEST_TMP/no/such/dir

for d in "${FOLDERS[@]}"; do
    section "$d"
    if ! have_bins "$d"; then
        echo "SKIP: $d not built"
        skip "$d not built (no server/client executable)"
        continue
    fi
    S=$ROOT/$d/server
    C=$ROOT/$d/client
    P=$(pick_port)          # a free port: nothing listens there
    if [[ $d == Part1 ]]; then
        need=("$DATA")      # required server arguments after <port>
    else
        need=("$DATA" 0)
    fi

    # ---- server ----
    expect_exit "$d server: no arguments"      1 1 5 "$S"
    expect_exit "$d server: only <port>"       1 1 5 "$S" "$P"
    if [[ $d != Part1 ]]; then
        expect_exit "$d server: no failure_probability" 1 1 5 "$S" "$P" "$DATA"
    fi
    for bad in abc 0 70000; do
        expect_exit "$d server: port '$bad'" 1 0 5 "$S" "$bad" "${need[@]}"
    done
    expect_exit "$d server: nonexistent topics_root_dir" 1 0 5 "$S" "$P" "$MISSING" "${need[@]:1}"
    if [[ $d == Part1 ]]; then
        expect_exit "$d server: mode 'bogus'" 1 0 5 "$S" "$P" "$DATA" bogus
    else
        for bad in 1.5 -0.1 x; do
            expect_exit "$d server: failure_probability '$bad'" 1 0 5 "$S" "$P" "$DATA" "$bad"
        done
        expect_exit "$d server: seed 'abc'" 1 0 5 "$S" "$P" "$DATA" 0 abc
    fi

    # ---- client: argument errors ----
    expect_exit "$d client: no arguments"      1 1 5 "$C"
    expect_exit "$d client: only 3 arguments"  1 1 5 "$C" "$HOST_IP" "$P" alpha
    for bad in abc 0 70000; do
        expect_exit "$d client: port '$bad'" 1 0 15 "$C" "$HOST_IP" "$bad" alpha "$OUT/x"
    done
    case $d in
        Part2_Case1|Part2_Case2)
            expect_exit "$d client: max_retries 'abc'" 1 0 15 "$C" "$HOST_IP" "$P" alpha "$OUT/x" abc
            ;;
        Part2_Case2_Enhanced)
            expect_exit "$d client: connections 0"  1 0 15 "$C" "$HOST_IP" "$P" alpha "$OUT/x" 0
            expect_exit "$d client: connections 17" 1 0 15 "$C" "$HOST_IP" "$P" alpha "$OUT/x" 17
            expect_exit "$d client: max_retries 'abc'" 1 0 15 "$C" "$HOST_IP" "$P" alpha "$OUT/x" 4 abc
            expect_exit "$d client: io 'bogus'" 1 0 15 "$C" "$HOST_IP" "$P" alpha "$OUT/x" 4 20 bogus
            ;;
    esac

    # ---- client: nothing listens on the port ----
    case $d in
        Part1)
            expect_exit "$d client: closed port (no retries)" 1 0 15 \
                "$C" "$HOST_IP" "$P" alpha "$OUT/closed1" ;;
        Part2_Case1|Part2_Case2)
            expect_exit "$d client: closed port, max_retries 3 -> gave up" 2 0 15 \
                "$C" "$HOST_IP" "$P" alpha "$OUT/closed$d" 3 ;;
        Part2_Case2_Enhanced)
            expect_exit "$d client: closed port, '2 3' -> gave up" 2 0 15 \
                "$C" "$HOST_IP" "$P" alpha "$OUT/closed$d" 2 3 ;;
    esac

    # ---- with a running server ----
    start_server "$d" "$P" "${need[@]}"
    expect_exit "$d server: port already in use" 1 0 5 "$S" "$P" "${need[@]}"
    expect_exit "$d client: unknown topic" 1 0 15 "$C" "$HOST_IP" "$P" no_such_topic "$OUT/unknown$d"
    run_client "$d" "$P" alpha "$OUT/ok$d"
    client_ok "$d client: the 4 required arguments work" "$DATA/alpha" "$OUT/ok$d/alpha"
    stop_server
done

section "no hard-coded addresses or paths in the C sources"
shopt -s nullglob
srcs=("$ROOT"/Part*/*.c "$ROOT"/Part*/*.h "$ROOT"/common/*.c "$ROOT"/common/*.h)
shopt -u nullglob
if (( ${#srcs[@]} == 0 )); then
    skip "no C sources found"
else
    # string literals that contain a loop-back address, a host name or an absolute path
    hits=$(grep -nH -E '"[^"]*(127\.0\.0\.1|localhost|/home/|/tmp/)[^"]*"' "${srcs[@]}" || true)
    if [[ -z $hits ]]; then
        pass "no \"127.0.0.1\", \"localhost\", \"/home/\" or \"/tmp/\" literal in ${#srcs[@]} files"
    else
        fail "hard-coded address or path in a string literal:"
        printf '%s\n' "$hits" | sed -n '1,20s/^/       /p'
    fi
fi

summary
