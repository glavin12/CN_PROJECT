#!/usr/bin/env bash
# test_case1.sh - Part 2 Case 1: fault injection, no session management.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# After a fault the client reconnects and the WHOLE topic is sent again
# (design 3.6, 4.4). Checks:
#   - p = 0 (only the 3 required server arguments): every edge topic is exact;
#   - with faults: output exact, no leftovers, the client needed > 1 attempt in
#     at least one run, and then the server sent MORE data bytes than the topic
#     holds (redundant retransmission); FAULT events = client reconnects, and
#     client data_bytes_rx = server data_bytes;
#   - p = 1, max_retries 5 -> exit 2 (gave up) quickly, client CSV status DNF;
#   - a cut at any byte (faultproxy, also mid-header) is survived.
#
# Why p = 0.005 for 'many' and not 0.05: 'many' has 400 one-chunk files, so an
# attempt succeeds with probability (1-p)^400. At p = 0.05 that is 1e-9 and
# Case 1 can never finish (design 4.4); at p = 0.005 it is 0.13 (about 7
# attempts expected, P(100 attempts fail) < 1e-6).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

F=Part2_Case1
require_bins "$F"

DATA=$TEST_TMP/data
gen_edge "$DATA"
EDGE_TOPICS=(empty tiny boundary names big many Mixed)
MAX_ATTEMPTS=0

# ---------------------------------------------------------------------------------
section "p = 0: every edge topic"
PORT=$(pick_port)
start_server "$F" "$PORT" "$DATA" 0          # only the 3 required arguments
OUT=$(mktemp_dir out)
for t in "${EDGE_TOPICS[@]}"; do
    run_client "$F" "$PORT" "$t" "$OUT"      # only the 4 required arguments
    client_ok "p=0 topic '$t'" "$DATA/$t" "$OUT/$t"
done
stop_server
check_eq "server exit status after SIGINT" "$STOP_RC" 0

# ---------------------------------------------------------------------------------
# fault_run <p> <seed> <topic>: one server (FDS_LOG + FDS_EVENTS), one logged client.
fault_run() {
    local p=$1 seed=$2 t=$3 port srv ev cli out tb sb rx faults att rec tag
    tag="p=$p seed=$seed '$t'"
    port=$(pick_port)
    srv=$TEST_TMP/srv.$((++_SEQ)).csv
    ev=$TEST_TMP/events.$_SEQ.csv
    cli=$TEST_TMP/cli.$_SEQ.csv
    FDS_LOG=$srv FDS_EVENTS=$ev start_server "$F" "$port" "$DATA" "$p" "$seed"
    out=$(mktemp_dir run)
    FDS_LOG=$cli run_client "$F" "$port" "$t" "$out"
    stop_server
    client_ok "$tag" "$DATA/$t" "$out/$t"
    (( CLIENT_RC == 0 )) || return 0

    tb=$(topic_bytes "$DATA/$t")
    sb=$(csv_sum "$srv" data_bytes topic="$t")
    rx=$(csv_sum "$cli" data_bytes_rx)
    att=$(csv_max "$cli" attempts)
    rec=$(csv_max "$cli" reconnects)
    faults=$(csv_count "$ev" event=FAULT)
    info "attempts=$att reconnects=$rec FAULT events=$faults server data_bytes=$sb topic bytes=$tb"
    if (( att > MAX_ATTEMPTS )); then MAX_ATTEMPTS=$att; fi

    check_eq "$tag: client CSV status" "$(csv_count "$cli" status=OK case=case1)" 1
    check_eq "$tag: attempts = reconnects + 1" "$att" $((rec + 1))
    check_eq "$tag: FAULT events = client reconnects" "$faults" "$rec"
    check_eq "$tag: client data_bytes_rx = server data_bytes" "$rx" "$sb"
    if (( att > 1 )); then
        check "$tag: redundant bytes, server data_bytes $sb > topic bytes $tb" test "$sb" -gt "$tb"
    else
        check_eq "$tag: no fault, server data_bytes = topic bytes" "$sb" "$tb"
    fi
}

section "fault injection (whole topic re-sent after each fault)"
fault_run 0.05 7 boundary
fault_run 0.05 7 tiny
fault_run 0.005 7 many
if is_quick; then
    skip "second seed (--quick)"
else
    fault_run 0.05 8 boundary
    fault_run 0.005 8 many
fi
check "at least one faulty run needed more than one attempt (max $MAX_ATTEMPTS)" test "$MAX_ATTEMPTS" -gt 1

# ---------------------------------------------------------------------------------
section "p = 1: the client gives up"
PORT=$(pick_port)
EV=$TEST_TMP/p1.events.csv
CLI=$TEST_TMP/p1.client.csv
FDS_EVENTS=$EV start_server "$F" "$PORT" "$DATA" 1 1
OUT=$(mktemp_dir p1)
CLIENT_TIMEOUT=60 FDS_LOG=$CLI run_client "$F" "$PORT" boundary "$OUT" 5
stop_server
check_eq "p=1 max_retries=5: client exit status (2 = gave up)" "$CLIENT_RC" 2
check "p=1: gave up in reasonable time (${CLIENT_MS} ms < 30 s)" test "$CLIENT_MS" -lt 30000
check_eq "p=1: client CSV status" "$(csv_count "$CLI" status=DNF)" 1
check_eq "events CSV header (log.h)" "$(csv_header "$EV")" "$EVENTS_CSV_HEADER"
check "p=1: one FAULT event per attempt ($(csv_count "$EV" event=FAULT) >= 5)" \
    test "$(csv_count "$EV" event=FAULT)" -ge 5

# ---------------------------------------------------------------------------------
section "cut at any byte (faultproxy in front of a p = 0 server)"
SPORT=$(pick_port)
PPORT=$(pick_port)
start_server "$F" "$SPORT" "$DATA" 0
# every connection is cut within its first 300 bytes (often inside a header)
# until 20 cuts were made; Case 1 then has to restart from file 0 each time
start_proxy "$PPORT" "$SPORT" --cut-prob 1 --window 300 --max-cuts 20 --seed 5
OUT=$(mktemp_dir cut)
run_client "$F" "$PPORT" boundary "$OUT"
stop_proxy
stop_server
client_ok "boundary through 20 cuts in the first 300 bytes" "$DATA/boundary" "$OUT/boundary"
check_eq "proxy made all its cuts" "$(proxy_cuts)" 20

summary
