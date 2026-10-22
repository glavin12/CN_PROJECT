#!/usr/bin/env bash
# test_part1.sh - Part 1: the single- and multi-threaded topic server.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# Checks (design sections 1-3, 7; DEVIATIONS 1, 4):
#   - every edge-profile topic downloads byte-exact (empty topic -> empty dir);
#   - "MIXED" is served from folder Mixed into output_dir/MIXED (as typed);
#   - hidden folders, nested folders and root files are not topics;
#   - a nested output_dir that does not exist yet is created;
#   - 32 concurrent clients (multi mode; 8 with --quick) and 8 concurrent
#     clients against the single-threaded server all succeed;
#   - kill -9 of a client in the middle of a transfer does not hurt the server;
#   - FDS_LOG CSVs have the log.h header and exact byte counts;
#   - a topic folder created while the server runs is served (re-scan);
#   - SIGINT stops the server with exit 0 within 6 s (also mid-transfer).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

F=Part1
require_bins "$F"

DATA=$TEST_TMP/data
gen_edge "$DATA"
EDGE_TOPICS=(empty tiny boundary names big many Mixed)

# some_bytes <dir>: the client has stored at least one byte there.
some_bytes() { [[ -d $1 ]] && (( $(stored_bytes "$1") > 0 )); }

# ---------------------------------------------------------------------------------
section "multi-threaded server (default mode): every edge topic"
PORT=$(pick_port)
start_server "$F" "$PORT" "$DATA"          # only the 2 required arguments
MULTI_PID=$SERVER_PID
OUT=$(mktemp_dir out)
for t in "${EDGE_TOPICS[@]}"; do
    run_client "$F" "$PORT" "$t" "$OUT"
    client_ok "topic '$t'" "$DATA/$t" "$OUT/$t"
done

section "topic name matching and output directory"
OUT2=$(mktemp_dir case)
run_client "$F" "$PORT" MIXED "$OUT2"
client_ok "'MIXED' is served from folder Mixed" "$DATA/Mixed" "$OUT2/MIXED"
check_eq "output_dir holds only the directory named as typed" "$(ls -A "$OUT2" | tr '\n' ' ')" "MIXED "

NEST=$TEST_TMP/nested/a/b/c
run_client "$F" "$PORT" tiny "$NEST"
client_ok "nested output_dir that did not exist is created" "$DATA/tiny" "$NEST/tiny"

OUT3=$(mktemp_dir noise)
for t in .hidden README.txt sub; do
    run_client "$F" "$PORT" "$t" "$OUT3"
    check_eq "'$t' is not a topic (hidden dir / root file / nested dir): client exit" "$CLIENT_RC" 1
done
run_client "$F" "$PORT" no_such_topic "$OUT3"
check_eq "unknown topic: client exit" "$CLIENT_RC" 1

# ---------------------------------------------------------------------------------
N=32
is_quick && N=8
section "$N concurrent clients on 'many' (multi mode)"
CONC=$(mktemp_dir conc)
pids=()
for ((i = 0; i < N; i++)); do
    start_client_bg "$F" "$PORT" many "$CONC/c$i"
    pids+=("$LAST_PID")
done
ok=0
for i in "${!pids[@]}"; do
    wait_client "${pids[$i]}" 180
    if (( WAIT_RC == 0 )) && "$TESTS_DIR/verify_output.sh" "$DATA/many" "$CONC/c$i/many" >/dev/null 2>&1; then
        ok=$((ok + 1))
    else
        info "client $i: exit $WAIT_RC"
        show_log "$CLIENT_LOG" 5
    fi
done
check_eq "$N concurrent clients finished with exact output" "$ok" "$N"

# ---------------------------------------------------------------------------------
section "kill -9 of a client in the middle of a transfer"
PPORT=$(pick_port)
start_proxy "$PPORT" "$PORT" --rate 2000000      # ~2 MB/s: 'big' takes several seconds
KOUT=$(mktemp_dir kill)
start_client_bg "$F" "$PPORT" big "$KOUT"
victim=$LAST_PID
if wait_until 10 some_bytes "$KOUT/big"; then
    sleep 0.3
    kill -9 "$victim" 2>/dev/null || true
    wait_client "$victim" 5
    check_eq "client killed mid-transfer (exit status 128+9)" "$WAIT_RC" 137
else
    fail "the throttled client never stored a byte"
    show_log "$CLIENT_LOG"
fi
sleep 0.5                                         # let the server hit the dead socket
check "server survived the killed client" server_alive "$MULTI_PID"
KOUT2=$(mktemp_dir kill2)
run_client "$F" "$PORT" big "$KOUT2"
client_ok "a later client still succeeds" "$DATA/big" "$KOUT2/big"
stop_proxy

# ---------------------------------------------------------------------------------
section "re-scan: topic folder created while the server runs"
sleep 1.2                                  # re-scans are rate-limited to one per second
mkdir "$DATA/.fresh.tmp"                   # fill a hidden folder, then rename it, so
cp "$DATA/boundary/"* "$DATA/.fresh.tmp/"  # the server never sees a half-copied topic
mv "$DATA/.fresh.tmp" "$DATA/fresh"
run_client "$F" "$PORT" fresh "$OUT"
client_ok "new topic 'fresh' served without a restart" "$DATA/fresh" "$OUT/fresh"

section "SIGINT (idle server)"
stop_server "$MULTI_PID"
check_eq "multi server exit status after SIGINT" "$STOP_RC" 0
check "multi server exited within 6 s (${STOP_MS} ms)" test "$STOP_FORCED" -eq 0

# ---------------------------------------------------------------------------------
section "single-threaded server: 8 concurrent clients on 'big'"
SPORT=$(pick_port)
start_server "$F" "$SPORT" "$DATA" single
SINGLE_PID=$SERVER_PID
SOUT=$(mktemp_dir single)
pids=()
for ((i = 0; i < 8; i++)); do
    start_client_bg "$F" "$SPORT" big "$SOUT/c$i"
    pids+=("$LAST_PID")
done
ok=0
for i in "${!pids[@]}"; do
    wait_client "${pids[$i]}" 180
    if (( WAIT_RC == 0 )) && "$TESTS_DIR/verify_output.sh" "$DATA/big" "$SOUT/c$i/big" >/dev/null 2>&1; then
        ok=$((ok + 1))
    else
        info "client $i: exit $WAIT_RC$( ((WAIT_RC == 124)) && echo ' (timed out)')"
        show_log "$CLIENT_LOG" 5
    fi
done
check_eq "8 queued clients all served, none timed out" "$ok" 8
stop_server "$SINGLE_PID"
check_eq "single server exit status after SIGINT" "$STOP_RC" 0

# ---------------------------------------------------------------------------------
section "CSV logs (FDS_LOG on server and client)"
LPORT=$(pick_port)
SRV_CSV=$TEST_TMP/part1_server.csv
CLI_CSV=$TEST_TMP/part1_client.csv
FDS_LOG=$SRV_CSV FDS_RUN_ID=part1-log start_server "$F" "$LPORT" "$DATA"
LOUT=$(mktemp_dir log)
for i in 1 2 3; do
    FDS_LOG=$CLI_CSV FDS_RUN_ID=part1-log FDS_CLIENT_ID=c$i run_client "$F" "$LPORT" boundary "$LOUT/c$i"
    client_ok "logged client $i" "$DATA/boundary" "$LOUT/c$i/boundary"
done
stop_server                                # rows are complete once the server has exited
check_eq "server exit status after SIGINT" "$STOP_RC" 0
TB=$(topic_bytes "$DATA/boundary")
check_eq "server CSV header (log.h)" "$(csv_header "$SRV_CSV")" "$SERVER_CSV_HEADER"
check_eq "server CSV: one row per connection" "$(csv_count "$SRV_CSV")" 3
check_eq "server CSV: rows with case=part1 mode=multi" "$(csv_count "$SRV_CSV" case=part1 mode=multi)" 3
check_eq "server CSV: sum(data_bytes) = topic bytes x 3" "$(csv_sum "$SRV_CSV" data_bytes)" $((TB * 3))
check_eq "client CSV header (log.h)" "$(csv_header "$CLI_CSV")" "$CLIENT_CSV_HEADER"
check_eq "client CSV: rows with status=OK" "$(csv_count "$CLI_CSV" status=OK case=part1)" 3
check_eq "client CSV: sum(useful_bytes) = topic bytes x 3" "$(csv_sum "$CLI_CSV" useful_bytes)" $((TB * 3))

# ---------------------------------------------------------------------------------
if is_quick; then
    skip "SIGINT during an active transfer (--quick)"
else
    section "SIGINT while a transfer is active"
    APORT=$(pick_port)
    start_server "$F" "$APORT" "$DATA"
    ACTIVE_PID=$SERVER_PID
    PPORT=$(pick_port)
    start_proxy "$PPORT" "$APORT" --rate 1000000
    AOUT=$(mktemp_dir active)
    start_client_bg "$F" "$PPORT" big "$AOUT"
    slow=$LAST_PID
    if wait_until 10 some_bytes "$AOUT/big"; then
        stop_server "$ACTIVE_PID"
        check_eq "server exit status after SIGINT mid-transfer" "$STOP_RC" 0
        check "server exited within 6 s (${STOP_MS} ms; grace period is 5 s)" test "$STOP_FORCED" -eq 0
    else
        fail "the throttled client never stored a byte"
        stop_server "$ACTIVE_PID"
    fi
    wait_client "$slow" 15                 # it loses its server; any exit status is fine
    stop_proxy
fi

summary
