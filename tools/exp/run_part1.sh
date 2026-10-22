#!/usr/bin/env bash
#
# run_part1.sh - Part I experiment (design 6.5): aggregate throughput of the
# single-threaded vs the multi-threaded server against the number of clients.
#
# Default sweep: N in {1, 2, 4, 8, 16, 32} clients in total, split evenly over
# PC A and PC B (N = 1: one client on A), servers single and multi, topic
# elefante, 1 warm-up + 5 measured repetitions per (server, N).
# Run id: part1_<single|multi>_N<n>_r<r>  (warm-up: ..._w1_warmup)
#
# Examples:
#   tools/exp/run_part1.sh --mode emu --quick              # pipeline test on one PC
#   tools/exp/run_part1.sh --mode ssh --yes                # the real experiment (lab.conf)
#   tools/exp/run_part1.sh --mode manual --role A --server-ip 192.168.1.10 --start-at 14:30
#
set -euo pipefail
# shellcheck source=tools/exp/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SWEEP=part1
TOPIC_DEFAULT=elefante

usage() {
    cat <<'EOF'
usage: tools/exp/run_part1.sh --mode emu|local|ssh|manual [options]

Part I options:
  --ns <list>        total client counts (default 1,2,4,8,16,32; --quick: 1,4)
  --servers <list>   server modes (default single,multi)
  --topic <name>     topic (default elefante)

EOF
    exp_common_help
}

while (($#)); do
    case "$1" in
        --ns) OPT[ns]="${2:?--ns needs a value}"; shift 2 ;;
        --servers) OPT[servers]="${2:?--servers needs a value}"; shift 2 ;;
        *) exp_parse_opt "$@"; shift "$EXP_SHIFT" ;;
    esac
done

# --quick: a small sweep to test the whole pipeline (explicit flags still win).
if [[ "${OPT[quick]:-0}" == 1 ]]; then
    : "${OPT[ns]:=1,4}" "${OPT[reps]:=2}" "${OPT[warmup]:=1}"
fi
exp_finalize

NS_LIST="$(split_list "${OPT[ns]:-1,2,4,8,16,32}")"
SERVERS_LIST="$(split_list "${OPT[servers]:-single,multi}")"

# Default per-run client time-out and manual-mode slot length. Generous: a
# single-threaded server on a 100 Mbit/s link needs about N x 3 s for elefante.
part1_timeout() { echo $((60 + 10 * $1)); }
part1_slot() { echo $((20 + 4 * $1)); }

# One Part I run: FUNC RUN_ID SEED SERVER_MODE N (the seed is unused: no faults).
add_part1_run() {
    local rid=$1 _seed=$2 smode=$3 n=$4
    plan_add "$rid" Part1 "$n" "$TOPIC" "$smode" - - "$(part1_timeout "$n")" "$(part1_slot "$n")"
}

for smode in $SERVERS_LIST; do
    [[ "$smode" == single || "$smode" == multi ]] || die "bad server mode '$smode' (single or multi)"
    for n in $NS_LIST; do
        [[ "$n" =~ ^[1-9][0-9]*$ ]] || die "bad client count '$n'"
        exp_add_reps "part1_${smode}_N${n}" add_part1_run "$smode" "$n"
    done
done

exp_main
