#!/usr/bin/env bash
# test_enhanced.sh - Part 2 Case 2 Enhanced: K parallel connections per topic.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
#   ./client <ip> <port> <topic> <output_dir> [connections=4] [max_retries=20] [threads|poll]
#
# One MANIFEST connection, then K workers FETCH one file at a time from a
# shared queue; a failed worker re-queues (file, .part size) and reconnects
# (design 3.8, 6.2; enh_client.h; DEVIATIONS 11, 13). Checks:
#   - K in {1,2,4,8} x io in {threads, poll} at p = 0.3: exact output and
#     server data_bytes == topic bytes (no redundant bytes);
#   - p = 0, K = 4: at least 4 worker rows (kind=fetch) in the server CSV;
#   - p = 0: empty topic -> empty dir; other edge topics; defaults (4 args);
#   - cut at any byte (faultproxy), both io modes;
#   - kill -9 + restart with K = 4: completes, re-receives only missing bytes;
#   - p = 1 on 'tiny' -> exit 2; 4 concurrent clients at p = 0.3.
# Topic "names250" replaces "names" here: see make_names250 in lib.sh.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

F=Part2_Case2_Enhanced
require_bins "$F"

DATA=$TEST_TMP/data
gen_edge "$DATA"
make_names250 "$DATA"
IO_MODES=(threads poll)

# ---------------------------------------------------------------------------------
section "K x io at p = 0.3 (seed 1): exact output, no redundant bytes"
MATRIX_FAULTS=0
for k in 1 2 4 8; do
    for io in "${IO_MODES[@]}"; do
        port=$(pick_port)
        srv=$TEST_TMP/matrix.$k.$io.server.csv
        FDS_LOG=$srv FDS_EVENTS=$srv.ev start_server "$F" "$port" "$DATA" 0.3 1
        out=$(mktemp_dir "m$k$io")
        declare -A rx=()
        for t in many boundary; do
            cli=$TEST_TMP/matrix.$k.$io.$t.client.csv
            FDS_LOG=$cli run_client "$F" "$port" "$t" "$out" "$k" 20 "$io"
            client_ok "K=$k io=$io '$t'" "$DATA/$t" "$out/$t"
            rx[$t]=$(csv_sum "$cli" data_bytes_rx)
            info "reconnects=$(csv_max "$cli" reconnects) time=${CLIENT_MS} ms"
        done
        stop_server
        for t in many boundary; do
            tb=$(topic_bytes "$DATA/$t")
            check_eq "K=$k io=$io '$t': server data_bytes = topic bytes" \
                "$(csv_sum "$srv" data_bytes topic="$t")" "$tb"
            check_eq "K=$k io=$io '$t': client data_bytes_rx = topic bytes" "${rx[$t]}" "$tb"
        done
        unset rx
        MATRIX_FAULTS=$((MATRIX_FAULTS + $(csv_count "$srv.ev" event=FAULT)))
    done
done
check "faults were injected in the K x io runs ($MATRIX_FAULTS)" test "$MATRIX_FAULTS" -gt 3

# ---------------------------------------------------------------------------------
section "p = 0: worker connections, edge topics, defaults"
PORT=$(pick_port)
SRV=$TEST_TMP/p0.server.csv
FDS_LOG=$SRV start_server "$F" "$PORT" "$DATA" 0     # only the 3 required arguments
OUT=$(mktemp_dir p0)
run_client "$F" "$PORT" many "$OUT" 4 20 threads
client_ok "p=0 K=4 'many'" "$DATA/many" "$OUT/many"
for io in "${IO_MODES[@]}"; do
    for t in empty tiny names250 Mixed big; do
        if [[ $t == big ]] && is_quick && [[ $io == poll ]]; then
            skip "p=0 io=$io 'big' (--quick)"
            continue
        fi
        run_client "$F" "$PORT" "$t" "$OUT/$io" 4 20 "$io"
        client_ok "p=0 K=4 io=$io '$t'" "$DATA/$t" "$OUT/$io/$t"
    done
done
run_client "$F" "$PORT" boundary "$OUT/defaults"      # only the 4 required arguments
client_ok "defaults (connections, max_retries, io omitted)" "$DATA/boundary" "$OUT/defaults/boundary"
stop_server
check_eq "server exit status after SIGINT" "$STOP_RC" 0
check "p=0 K=4 'many': at least 4 kind=fetch rows ($(csv_count "$SRV" kind=fetch topic=many))" \
    test "$(csv_count "$SRV" kind=fetch topic=many)" -ge 4
check "MANIFEST connections logged as kind=manifest" test "$(csv_count "$SRV" kind=manifest)" -ge 1

# ---------------------------------------------------------------------------------
section "4 concurrent clients, K = 4, p = 0.3"
PORT=$(pick_port)
SRV=$TEST_TMP/conc.server.csv
FDS_LOG=$SRV start_server "$F" "$PORT" "$DATA" 0.3 3
CONC=$(mktemp_dir conc)
pids=()
for i in 0 1 2 3; do
    start_client_bg "$F" "$PORT" many "$CONC/c$i" 4 20 "${IO_MODES[i % 2]}"
    pids+=("$LAST_PID")
done
for i in "${!pids[@]}"; do
    wait_client "${pids[$i]}" 180
    RC=$WAIT_RC client_ok "concurrent client $i (io=${IO_MODES[i % 2]})" "$DATA/many" "$CONC/c$i/many"
done
stop_server
TB=$(topic_bytes "$DATA/many")
check_eq "4 clients: server data_bytes = topic bytes x 4" "$(csv_sum "$SRV" data_bytes topic=many)" $((TB * 4))

# ---------------------------------------------------------------------------------
# proxy_run <io> <topic> <faultproxy options...>: p = 0 server behind the proxy, K = 4.
proxy_run() {
    local io=$1 t=$2 sport pport out
    shift 2
    sport=$(pick_port)
    pport=$(pick_port)
    start_server "$F" "$sport" "$DATA" 0
    start_proxy "$pport" "$sport" "$@"
    out=$(mktemp_dir cut)
    CLIENT_TIMEOUT=180 run_client "$F" "$pport" "$t" "$out" 4 20 "$io"
    stop_proxy
    stop_server
    LAST_CUTS=$(proxy_cuts)
    client_ok "io=$io '$t' through faultproxy $* ($LAST_CUTS cuts)" "$DATA/$t" "$out/$t"
}

section "cut at any byte (faultproxy in front of a p = 0 server, K = 4)"
for io in "${IO_MODES[@]}"; do
    proxy_run "$io" many --cut-prob 0.5 --window 200000 --max-cuts 200 --seed 1
    proxy_run "$io" big  --cut-prob 0.5 --window 200000 --max-cuts 200 --seed 1
    if ! is_quick; then
        proxy_run "$io" many --cut-prob 0.5 --window 200000 --max-cuts 200 --seed 2
    fi
    proxy_run "$io" many --cut-prob 1 --window 100000 --max-cuts 30 --seed 3
    check_eq "io=$io dense cuts: the proxy made all 30 cuts" "$LAST_CUTS" 30
    # within 300 bytes: lands in MANIFEST, FILE_HDR, FILE_END, ... most of the time
    proxy_run "$io" boundary --cut-prob 1 --window 300 --max-cuts 40 --seed 4
    check_eq "io=$io header cuts: the proxy made all 40 cuts" "$LAST_CUTS" 40
done

# ---------------------------------------------------------------------------------
# restart_run <io>: kill -9 the client mid-transfer, run it again.
restart_run() {
    local io=$1 sport pport out cli victim stored tb
    sport=$(pick_port)
    pport=$(pick_port)
    cli=$TEST_TMP/restart.$io.client.csv
    start_server "$F" "$sport" "$DATA" 0
    start_proxy "$pport" "$sport" --rate 2000000
    out=$(mktemp_dir "restart$io")
    tb=$(topic_bytes "$DATA/big")
    start_client_bg "$F" "$pport" big "$out" 4 20 "$io"
    victim=$LAST_PID
    if wait_until 15 has_part "$out/big"; then
        sleep 0.5
        kill -9 "$victim" 2>/dev/null || true
        wait_client "$victim" 5
        check_eq "io=$io: client killed mid-transfer (exit status 128+9)" "$WAIT_RC" 137
        check "io=$io: .session survives kill -9" test -f "$out/big/.session"
        stored=$(stored_bytes "$out/big")
        info "stored before the restart: $stored of $tb bytes"
        FDS_LOG=$cli run_client "$F" "$sport" big "$out" 4 20 "$io"    # direct, full speed
        client_ok "io=$io: restarted client completes" "$DATA/big" "$out/big"
        check_eq "io=$io: restart re-receives only the missing bytes (client data_bytes_rx)" \
            "$(csv_sum "$cli" data_bytes_rx)" $((tb - stored))
    else
        fail "io=$io: the throttled client never created a .part file"
        show_log "$CLIENT_LOG"
    fi
    stop_proxy
    stop_server
}

section "client restart: kill -9 mid-transfer, then run again (K = 4)"
restart_run threads
if is_quick; then skip "restart with io=poll (--quick)"; else restart_run poll; fi

# ---------------------------------------------------------------------------------
section "p = 1: every connection is cut, the topic never completes"
PORT=$(pick_port)
start_server "$F" "$PORT" "$DATA" 1 1
for io in "${IO_MODES[@]}"; do
    OUT=$(mktemp_dir "p1$io")
    CLIENT_TIMEOUT=60 run_client "$F" "$PORT" tiny "$OUT" 2 5 "$io"
    check_eq "p=1 'tiny' K=2 max_retries=5 io=$io: client exit status (2 = gave up)" "$CLIENT_RC" 2
    check "p=1 io=$io: gave up in reasonable time (${CLIENT_MS} ms < 30 s)" test "$CLIENT_MS" -lt 30000
done
stop_server

summary
