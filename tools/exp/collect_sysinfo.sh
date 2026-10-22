#!/usr/bin/env bash
#
# collect_sysinfo.sh - record the machine details the report must state
# (design 6.4): CPU (lscpu), NIC speed (ethtool), addresses, kernel, gcc,
# core count, TCP buffer limits and clock synchronisation.
#
# usage: tools/exp/collect_sysinfo.sh [--out Results/raw] [--label S|A|B] [--iface eth0] [--stdout]
#   writes <out>/sysinfo_[<label>_]<hostname>.txt (or prints it with --stdout)
# The experiment runners call it automatically on every PC they use.
#
set -euo pipefail

EXP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$EXP_DIR/../.." && pwd)"
out="$REPO/Results/raw"
label=""
iface=""
to_stdout=0

while (($#)); do
    case "$1" in
        --out) out="${2:?}"; shift 2 ;;
        --label) label="${2:?}"; shift 2 ;;
        --iface) iface="${2:?}"; shift 2 ;;
        --stdout) to_stdout=1; shift ;;
        -h | --help) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 1 ;;
    esac
done

host="$(hostname -s 2>/dev/null || hostname)"
# The interface of the default route is the one the lab traffic uses.
if [[ -z "$iface" ]]; then
    iface="$(ip -o route show default 2>/dev/null | awk '{ for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit } }')"
    [[ -n "$iface" ]] || iface="$(ip -o link show 2>/dev/null | awk -F': ' '$2 != "lo" { sub(/@.*/, "", $2); print $2; exit }')"
fi

section() {
    echo
    echo "### $1"
    shift
    "$@" 2>&1 || echo "(command failed: $*)"
}

report() {
    echo "# FDS system information"
    echo "label:     ${label:--}"
    echo "hostname:  $(hostname 2>/dev/null)"
    echo "date:      $(date '+%F %T %z')"
    echo "user:      ${USER:-$(id -un)}"
    echo "repo:      $REPO ($(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo 'no git'))"
    echo "interface: ${iface:-?}"
    section "uname -a" uname -a
    section "nproc" nproc
    section "lscpu" lscpu
    section "free -h" free -h
    section "ip -br addr" ip -br addr
    section "ip route" ip route
    if command -v ethtool >/dev/null && [[ -n "$iface" ]]; then
        section "ethtool $iface" ethtool "$iface"
    else
        section "NIC speed (/sys)" cat "/sys/class/net/${iface:-none}/speed"
    fi
    section "gcc --version" gcc --version
    section "make --version" sh -c 'make --version | head -n 1'
    section "TCP / socket buffer sysctls" sysctl net.core.rmem_max net.core.wmem_max \
        net.core.rmem_default net.core.wmem_default net.ipv4.tcp_rmem net.ipv4.tcp_wmem \
        net.ipv4.tcp_congestion_control net.core.default_qdisc
    section "qdisc of $iface" tc qdisc show dev "${iface:-lo}"
    section "timedatectl" timedatectl status
    section "df -h /tmp" df -h /tmp
}

if ((to_stdout)); then
    report
else
    mkdir -p "$out"
    f="$out/sysinfo_${label:+${label}_}${host}.txt"
    report >"$f"
    echo "$f"
fi
