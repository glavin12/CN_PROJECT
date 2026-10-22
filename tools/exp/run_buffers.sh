#!/usr/bin/env bash
#
# run_buffers.sh - socket-buffer stretch experiment (design 6.3): does a
# larger SO_SNDBUF / SO_RCVBUF improve throughput on the lab network?
#
# Case 2 and Enhanced K = 4 (io=threads) at p = 0, 4 clients, topic gatto.
# FDS_SNDBUF and FDS_RCVBUF are set to the same value for the server AND the
# clients: default (variables unset = kernel autotuning), 65536, 262144,
# 1048576, 4194304 bytes. 1 warm-up + 3 reps.
# Run id: buf_<case2|enh_K4_threads>_b<bytes|default>_r<r>
#
# IMPORTANT: Linux caps setsockopt(SO_SNDBUF/SO_RCVBUF) at net.core.wmem_max /
# rmem_max (often 212992). For the 1 MB and 4 MB points to mean anything, raise
# the caps on ALL THREE PCs first (needs sudo, reverts at reboot):
#     sudo sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608
# The values in force are recorded by collect_sysinfo.sh.
#
set -euo pipefail
# shellcheck source=tools/exp/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SWEEP=buffers
TOPIC_DEFAULT=gatto

usage() {
    cat <<'EOF'
usage: tools/exp/run_buffers.sh --mode emu|local|ssh|manual [options]

Buffer experiment options:
  --bufs <list>      buffer sizes in bytes, "default" = leave unset
                     (default: default,65536,262144,1048576,4194304)
  --impls <list>     case2 and/or enh_K4 (default case2,enh_K4)
  --clients <n>      clients per run (default 4)
  --topic <name>     topic (default gatto)
  (--reps defaults to 3 here)

EOF
    exp_common_help
}

while (($#)); do
    case "$1" in
        --bufs | --impls | --clients)
            (($# >= 2)) || die "option $1 needs a value"
            OPT[${1#--}]="$2"
            shift 2
            ;;
        *) exp_parse_opt "$@"; shift "$EXP_SHIFT" ;;
    esac
done

if [[ "${OPT[quick]:-0}" == 1 ]]; then
    : "${OPT[bufs]:=default,1048576}" "${OPT[reps]:=1}"
fi
: "${OPT[reps]:=3}"   # this experiment uses 3 repetitions by default
exp_finalize

BUFS_LIST="$(split_list "${OPT[bufs]:-default,65536,262144,1048576,4194304}")"
IMPLS_LIST="$(split_list "${OPT[impls]:-case2,enh_K4}")"
CLIENTS="${OPT[clients]:-4}"

# FUNC RUN_ID SEED IMPL BUF
add_buf_run() {
    local rid=$1 seed=$2 impl=$3 buf=$4 part cargs envs=-
    case "$impl" in
        case2) part=Part2_Case2; cargs=20 ;;
        enh_K4) part=Part2_Case2_Enhanced; cargs="4 20 threads" ;;
        *) die "unknown implementation '$impl' (case2 or enh_K4)" ;;
    esac
    [[ "$buf" == default ]] || envs="FDS_SNDBUF=$buf FDS_RCVBUF=$buf"
    plan_add "$rid" "$part" "$CLIENTS" "$TOPIC" "0 $seed" "$cargs" "$envs" 300 60
}

for impl in $IMPLS_LIST; do
    prefix="$impl"
    [[ "$impl" == enh_K4 ]] && prefix=enh_K4_threads
    for buf in $BUFS_LIST; do
        [[ "$buf" == default || "$buf" =~ ^[1-9][0-9]*$ ]] || die "bad buffer size '$buf'"
        exp_add_reps "buf_${prefix}_b${buf}" add_buf_run "$impl" "$buf"
    done
done

# Warn when the kernel caps would silently shrink the requested buffers.
max_buf=0
for buf in $BUFS_LIST; do
    [[ "$buf" != default ]] && ((buf > max_buf)) && max_buf=$buf
done
for key in net.core.rmem_max net.core.wmem_max; do
    cur="$(sysctl -n "$key" 2>/dev/null || echo 0)"
    if ((cur < max_buf)); then
        warn "$key = $cur < $max_buf: the kernel will cap the larger buffers. On every PC run:"
        warn "    sudo sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608"
    fi
done

exp_main
