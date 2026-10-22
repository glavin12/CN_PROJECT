#!/usr/bin/env bash
#
# run_part2.sh - Part II experiment (design 6.6 + deviation 15): Case 1,
# Case 2 and Case 2 Enhanced against the per-connection fault probability p (deviation 28).
#
# Default sweep:
#   p in {0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9}
#   implementations ("cases"):
#     case1          Part2_Case1,          client max_retries 100
#     case2          Part2_Case2,          client max_retries 20
#     enh_K1..enh_K8 Part2_Case2_Enhanced, K = 1, 2, 4, 8 connections, io=threads
#     enh_K4_poll    Part2_Case2_Enhanced, K = 4, io=poll (non-blocking client)
#   topic gatto, 4 clients per run (2 on A, 2 on B), 1 warm-up + 5 reps.
#   Server seed of repetition r: 1000 + r, identical for every case.
#   A Case 1 run is capped at 600 s (--case1-cap); a client killed by the cap
#   counts as DNF, like one that gave up after max_retries.
# Run ids: case1_p<p>_r<r>, case2_p<p>_r<r>, enh_K<k>_<threads|poll>_p<p>_r<r>,
#          p1cmp_multi_N4_r<r> (with --part1-compare)
#
# Examples:
#   tools/exp/run_part2.sh --mode emu --quick
#   tools/exp/run_part2.sh --mode ssh --part1-compare --yes
#   tools/exp/run_part2.sh --mode ssh --cases case2,enh --ps 0,0.001 --yes
#
set -euo pipefail
# shellcheck source=tools/exp/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SWEEP=part2
TOPIC_DEFAULT=gatto

usage() {
    cat <<'EOF'
usage: tools/exp/run_part2.sh --mode emu|local|ssh|manual [options]

Part II options:
  --ps <list>            fault probabilities (default 0,0.1,0.2,0.3,0.5,0.7,0.9)
  --cases <list>         which implementations (default: all). Names: case1 case2
                         enh_K1 enh_K2 enh_K4 enh_K8 enh_K4_poll, or the groups
                         enh (all Enhanced), enh_threads, enh_poll
  --ks <list>            Enhanced K values with io=threads (default 1,2,4,8)
  --poll-ks <list>       Enhanced K values with io=poll (default 4)
  --clients <n>          clients per run, split over A and B (default 4)
  --case1-retries <n>    Case 1 client max_retries (default 100)
  --case2-retries <n>    Case 2 / Enhanced client max_retries (default 20)
  --case1-cap <s>        time-out of a Case 1 run (default 600; counts as DNF)
  --part1-compare        also run the Part I multi-threaded server with the same
                         topic and client count (p = 0 comparison, design 6.6)
  --topic <name>         topic (default gatto)

EOF
    exp_common_help
}

while (($#)); do
    case "$1" in
        --ps | --cases | --ks | --poll-ks | --clients | --case1-retries | --case2-retries | --case1-cap)
            (($# >= 2)) || die "option $1 needs a value"
            OPT[${1#--}]="$2"
            shift 2
            ;;
        --part1-compare) OPT[part1-compare]=1; shift ;;
        *) exp_parse_opt "$@"; shift "$EXP_SHIFT" ;;
    esac
done

if [[ "${OPT[quick]:-0}" == 1 ]]; then
    : "${OPT[ps]:=0,0.3,0.7}" "${OPT[cases]:=case1,case2,enh_K4,enh_K4_poll}" \
        "${OPT[reps]:=2}" "${OPT[warmup]:=1}"
fi
exp_finalize

PS_LIST="$(split_list "${OPT[ps]:-0,0.1,0.2,0.3,0.5,0.7,0.9}")"
KS_LIST="$(split_list "${OPT[ks]:-1,2,4,8}")"
POLL_KS_LIST="$(split_list "${OPT[poll-ks]:-4}")"
CLIENTS="${OPT[clients]:-4}"
CASE1_RETRIES="${OPT[case1-retries]:-100}"
CASE2_RETRIES="${OPT[case2-retries]:-20}"
CASE1_CAP="${OPT[case1-cap]:-600}"
for v in "$CLIENTS" "$CASE1_RETRIES" "$CASE2_RETRIES" "$CASE1_CAP"; do
    [[ "$v" =~ ^[1-9][0-9]*$ ]] || die "'$v' is not a positive integer"
done

# All implementations in run order, then the --cases filter.
ALL_IMPLS="case1 case2"
for k in $KS_LIST; do ALL_IMPLS+=" enh_K$k"; done
for k in $POLL_KS_LIST; do ALL_IMPLS+=" enh_K${k}_poll"; done

impl_selected() {
    local impl=$1 c
    [[ -z "${OPT[cases]:-}" ]] && return 0
    for c in $(split_list "${OPT[cases]}"); do
        case "$c" in
            all) return 0 ;;
            enh) [[ "$impl" == enh_* ]] && return 0 ;;
            enh_threads) [[ "$impl" == enh_* && "$impl" != *_poll ]] && return 0 ;;
            enh_poll) [[ "$impl" == enh_*_poll ]] && return 0 ;;
            *) [[ "$impl" == "$c" ]] && return 0 ;;
        esac
    done
    return 1
}

# p <= x ? (floating point, via awk)
p_le() { awk -v p="$1" -v x="$2" 'BEGIN { exit !(p <= x) }'; }

# Manual-mode slot per run. Case 1 needs 1/(1-p) attempts on average and each
# failed one sends about half the topic (10 attempts at p = 0.9), so its slots
# are longer at large p.
slot_for() {
    local impl=$1 p=$2
    case "$impl" in
        case1)
            if p_le "$p" 0.5; then echo 60; else echo 120; fi ;;
        case2) echo 60 ;;
        *) echo 45 ;;
    esac
}

# FUNC RUN_ID SEED IMPL P
add_part2_run() {
    local rid=$1 seed=$2 impl=$3 p=$4 part cargs tmo=300 k io
    case "$impl" in
        case1) part=Part2_Case1; cargs="$CASE1_RETRIES"; tmo="$CASE1_CAP" ;;
        case2) part=Part2_Case2; cargs="$CASE2_RETRIES" ;;
        enh_K*)
            part=Part2_Case2_Enhanced
            k="${impl#enh_K}"
            k="${k%_poll}"
            io=threads
            [[ "$impl" == *_poll ]] && io=poll
            cargs="$k $CASE2_RETRIES $io"
            ;;
        *) die "unknown implementation $impl" ;;
    esac
    plan_add "$rid" "$part" "$CLIENTS" "$TOPIC" "$p $seed" "$cargs" - "$tmo" "$(slot_for "$impl" "$p")"
}

# Part I multi-threaded server with the Part II topic and client count.
add_p1cmp_run() {
    local rid=$1 _seed=$2
    plan_add "$rid" Part1 "$CLIENTS" "$TOPIC" multi - - 120 45
}

for p in $PS_LIST; do
    awk -v p="$p" 'BEGIN { exit !(p ~ /^[0-9.eE+-]+$/ && p + 0 >= 0 && p + 0 <= 1) }' ||
        die "bad probability '$p' (must be in [0, 1])"
done

if [[ "${OPT[part1-compare]:-0}" == 1 ]]; then
    exp_add_reps "p1cmp_multi_N${CLIENTS}" add_p1cmp_run
fi
for impl in $ALL_IMPLS; do
    impl_selected "$impl" || continue
    # run-id prefix: Enhanced names carry the io mode explicitly
    case "$impl" in
        enh_*_poll) prefix="$impl" ;;
        enh_*) prefix="${impl}_threads" ;;
        *) prefix="$impl" ;;
    esac
    for p in $PS_LIST; do
        exp_add_reps "${prefix}_p${p}" add_part2_run "$impl" "$p"
    done
done

exp_main
