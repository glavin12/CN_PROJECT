#!/usr/bin/env bash
# test_case2.sh - Part 2 Case 2: fault injection WITH session management.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# After a fault the client sends RESUME(session, file, offset = .part size)
# and only the missing bytes are sent (design 3.7, 5; DEVIATIONS 9). Checks:
#   - p = 0 and p in {0.05, 0.2} x seeds {1, 2}: exact output, no .part/.session;
#   - NO redundant bytes: server sum(data_bytes) == topic bytes x clients, and
#     the client received exactly that (fault prefixes are stored, so it is exact);
#   - FAULT events exist, client reconnects > 0, FAULT events == reconnects;
#   - cut at ANY byte (faultproxy, p = 0): also mid-header, many cuts;
#   - kill -9 of the client, then a restart: .session survives, the rerun
#     completes, re-receives only the missing bytes, server logs kind=resume;
#   - p = 1 on 'tiny', max_retries 5 -> exit 2 (DEVIATIONS 9: a 1-byte chunk
#     can never make progress);
#   - 4 concurrent clients at p = 0.05.
# Topic "names250" replaces "names" here: see make_names250 in lib.sh.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

F=Part2_Case2
require_bins "$F"

DATA=$TEST_TMP/data
gen_edge "$DATA"
make_names250 "$DATA"

# ---------------------------------------------------------------------------------
section "p = 0: every topic, exact bytes"
PORT=$(pick_port)
SRV=$TEST_TMP/p0.server.csv
FDS_LOG=$SRV start_server "$F" "$PORT" "$DATA" 0     # only the 3 required arguments
OUT=$(mktemp_dir out)
P0_TOPICS=(empty tiny boundary names250 Mixed many big)
for t in "${P0_TOPICS[@]}"; do
    run_client "$F" "$PORT" "$t" "$OUT"              # only the 4 required arguments
    client_ok "p=0 topic '$t'" "$DATA/$t" "$OUT/$t"
done
stop_server
check_eq "server exit status after SIGINT" "$STOP_RC" 0
bad=""
for t in "${P0_TOPICS[@]}"; do
    [[ $(csv_sum "$SRV" data_bytes topic="$t") == "$(topic_bytes "$DATA/$t")" ]] || bad+=" $t"
done
check_eq "p=0: server data_bytes = topic bytes for every topic (mismatches:${bad:- none})" "$bad" ""

# ---------------------------------------------------------------------------------
# fault_run <p> <seed> <topic> <need_faults 0|1>: one server, one logged client.
fault_run() {
    local p=$1 seed=$2 t=$3 need=$4 port srv ev cli out tb sb rx faults rec tag
    tag="p=$p seed=$seed '$t'"
    port=$(pick_port)
    srv=$TEST_TMP/srv.$((++_SEQ)).csv
    ev=$TEST_TMP/events.$_SEQ.csv
    cli=$TEST_TMP/cli.$_SEQ.csv
    FDS_LOG=$srv FDS_EVENTS=$ev start_server "$F" "$port" "$DATA" "$p" "$seed"
    out=$(mktemp_dir run)
    CLIENT_TIMEOUT=180 FDS_LOG=$cli run_client "$F" "$port" "$t" "$out"
    stop_server
    client_ok "$tag" "$DATA/$t" "$out/$t"
    (( CLIENT_RC == 0 )) || return 0

    tb=$(topic_bytes "$DATA/$t")
    sb=$(csv_sum "$srv" data_bytes topic="$t")
    rx=$(csv_sum "$cli" data_bytes_rx)
    rec=$(csv_max "$cli" reconnects)
    faults=$(csv_count "$ev" event=FAULT)
    info "reconnects=$rec FAULT events=$faults resumes=$(csv_count "$srv" kind=resume) time=${CLIENT_MS} ms"
    check_eq "$tag: no redundant bytes, server data_bytes = topic bytes" "$sb" "$tb"
    check_eq "$tag: client data_bytes_rx = topic bytes" "$rx" "$tb"
    check_eq "$tag: FAULT events = client reconnects" "$faults" "$rec"
    if (( need )); then
        check "$tag: faults were injected ($faults) and the client reconnected ($rec)" \
            test "$faults" -gt 0 -a "$rec" -gt 0
    fi
}

section "fault injection: resume, no redundant bytes"
SEEDS=(1 2)
is_quick && SEEDS=(1)
for p in 0.05 0.2; do
    for seed in "${SEEDS[@]}"; do
        fault_run "$p" "$seed" many 1
        fault_run "$p" "$seed" boundary 0     # only 9 chunks: maybe no fault at all
        if is_quick && [[ $p == 0.2 ]]; then
            skip "p=$p seed=$seed 'big' (--quick)"
        else
            fault_run "$p" "$seed" big 1
        fi
    done
done
fault_run 0.05 1 names250 0                  # odd names in .session and RESUME

# ---------------------------------------------------------------------------------
section "4 concurrent clients at p = 0.05"
PORT=$(pick_port)
SRV=$TEST_TMP/conc.server.csv
EV=$TEST_TMP/conc.events.csv
CLI=$TEST_TMP/conc.client.csv
FDS_LOG=$SRV FDS_EVENTS=$EV start_server "$F" "$PORT" "$DATA" 0.05 3
CONC=$(mktemp_dir conc)
pids=()
for i in 0 1 2 3; do
    FDS_LOG=$CLI FDS_CLIENT_ID=c$i start_client_bg "$F" "$PORT" many "$CONC/c$i"
    pids+=("$LAST_PID")
done
for i in "${!pids[@]}"; do
    wait_client "${pids[$i]}" 180
    RC=$WAIT_RC client_ok "concurrent client $i" "$DATA/many" "$CONC/c$i/many"
done
stop_server
TB=$(topic_bytes "$DATA/many")
check_eq "4 clients: server data_bytes = topic bytes x 4" "$(csv_sum "$SRV" data_bytes topic=many)" $((TB * 4))
check_eq "4 clients: FAULT events = sum of reconnects" "$(csv_count "$EV" event=FAULT)" "$(csv_sum "$CLI" reconnects)"

# ---------------------------------------------------------------------------------
# proxy_run <topic> <faultproxy options...>: p = 0 server behind the proxy.
proxy_run() {
    local t=$1 sport pport out
    shift
    sport=$(pick_port)
    pport=$(pick_port)
    start_server "$F" "$sport" "$DATA" 0
    start_proxy "$pport" "$sport" "$@"
    out=$(mktemp_dir cut)
    CLIENT_TIMEOUT=180 run_client "$F" "$pport" "$t" "$out"
    stop_proxy
    stop_server
    LAST_CUTS=$(proxy_cuts)
    client_ok "'$t' through faultproxy $* ($LAST_CUTS cuts)" "$DATA/$t" "$out/$t"
}

section "cut at any byte (faultproxy in front of a p = 0 server)"
for seed in "${SEEDS[@]}"; do
    proxy_run many --cut-prob 0.5 --window 200000 --max-cuts 200 --seed "$seed"
    proxy_run big  --cut-prob 0.5 --window 200000 --max-cuts 200 --seed "$seed"
done
# every connection cut within 100 KB until 30 cuts: many resumes in a row
proxy_run many --cut-prob 1 --window 100000 --max-cuts 30 --seed 3
check_eq "dense cuts: the proxy made all 30 cuts" "$LAST_CUTS" 30
# every connection cut within 300 bytes: most cuts land inside RESUME_OK/FILE_HDR
proxy_run boundary --cut-prob 1 --window 300 --max-cuts 40 --seed 4
check_eq "header cuts: the proxy made all 40 cuts" "$LAST_CUTS" 40

# ---------------------------------------------------------------------------------
section "client restart: kill -9 mid-transfer, then run the client again"
SPORT=$(pick_port)
PPORT=$(pick_port)
SRV=$TEST_TMP/restart.server.csv
CLI=$TEST_TMP/restart.client.csv
FDS_LOG=$SRV start_server "$F" "$SPORT" "$DATA" 0
start_proxy "$PPORT" "$SPORT" --rate 2000000
OUT=$(mktemp_dir restart)
TB=$(topic_bytes "$DATA/big")
start_client_bg "$F" "$PPORT" big "$OUT"
victim=$LAST_PID
if wait_until 15 has_part "$OUT/big"; then
    sleep 0.5
    kill -9 "$victim" 2>/dev/null || true
    wait_client "$victim" 5
    check_eq "client killed mid-transfer (exit status 128+9)" "$WAIT_RC" 137
    check ".session survives kill -9" test -f "$OUT/big/.session"
    check ".part file survives kill -9" has_part "$OUT/big"
    STORED=$(stored_bytes "$OUT/big")
    info "stored before the restart: $STORED of $TB bytes"
    FDS_LOG=$CLI run_client "$F" "$SPORT" big "$OUT"       # direct, full speed
    client_ok "restarted client completes" "$DATA/big" "$OUT/big"
    check_eq "restart re-receives only the missing bytes (client data_bytes_rx)" \
        "$(csv_sum "$CLI" data_bytes_rx)" $((TB - STORED))
else
    fail "the throttled client never created a .part file"
    show_log "$CLIENT_LOG"
fi
stop_proxy
stop_server
check "server CSV has a kind=resume row ($(csv_count "$SRV" kind=resume))" \
    test "$(csv_count "$SRV" kind=resume)" -ge 1

# ---------------------------------------------------------------------------------
section "p = 1: no progress possible on 1-byte chunks"
PORT=$(pick_port)
CLI=$TEST_TMP/p1.client.csv
start_server "$F" "$PORT" "$DATA" 1 1
OUT=$(mktemp_dir p1)
CLIENT_TIMEOUT=60 FDS_LOG=$CLI run_client "$F" "$PORT" tiny "$OUT" 5
stop_server
check_eq "p=1 'tiny' max_retries=5: client exit status (2 = gave up)" "$CLIENT_RC" 2
check "p=1: gave up in reasonable time (${CLIENT_MS} ms < 30 s)" test "$CLIENT_MS" -lt 30000
check_eq "p=1: client CSV status" "$(csv_count "$CLI" status=DNF)" 1

summary
