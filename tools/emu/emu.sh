#!/usr/bin/env bash
#
# emu.sh - emulate the 3-PC lab (server S + client PCs A and B) on ONE Linux
# machine, without sudo, using unprivileged user + network namespaces.
#
#   S = 10.10.0.1 (server)   A = 10.10.0.2 (clients)   B = 10.10.0.3 (clients)
#
# How it is built (so you can explain it in the viva):
#   1. A "switch" holder process:  unshare --user --map-root-user --net sleep
#      It owns a new USER namespace (inside it we are uid 0 and have
#      CAP_NET_ADMIN, but only over namespaces that this user namespace owns)
#      and a new NETWORK namespace that holds the bridge br0 (the "switch").
#   2. One holder process per host (S, A, B), started inside that user
#      namespace with `unshare --net --uts`: each host gets its own network
#      stack (interfaces, routes, sockets, ports) and its own host name.
#      The file system and the PID space are NOT separated, so every host sees
#      the repository, the binaries, /tmp and Results/ exactly as you do.
#   3. A veth pair per host: one end ("v<H>") is a port of br0 in the switch
#      namespace, the other end is moved into the host namespace and renamed
#      eth0 (ip link ... peer name eth0 netns <pid>).
#   4. Every host's egress (its eth0) is shaped with three stacked qdiscs:
#        netem  delay EMU_DELAY      (default 0.1 ms per direction, RTT ~0.2 ms
#                                     like a switched LAN)
#        tbf    rate  EMU_RATE       (default 1 Gbit/s, like the lab NICs)
#        fq_codel                    (the default qdisc of Fedora/Ubuntu: keeps
#                                     the queue short, so RTT under load stays
#                                     < 1 ms instead of ~30 ms with a plain FIFO)
#      All traffic of the server leaves through S's eth0, so S's uplink is the
#      shared bottleneck for all clients, as in the real lab.
#   Commands run "on" a host via nsenter into its user + net + uts namespaces.
#
# Usage:
#   tools/emu/emu.sh up                 build (idempotent: does nothing if up)
#   tools/emu/emu.sh down               kill every process inside, remove all
#   tools/emu/emu.sh status             PIDs, addresses, qdiscs
#   tools/emu/emu.sh exec S|A|B cmd...  run cmd on that host (same files, cwd, env)
#   tools/emu/emu.sh ping-test          connectivity + throughput check
#   tools/emu/emu.sh shape [rate] [delay]  change shaping on the running emulation
#   tools/emu/emu.sh ip S|A|B           print the host's IP address
#
# Environment:
#   EMU_RATE   (default 1gbit)   egress rate per host, tc syntax (100mbit, 1gbit, ...)
#   EMU_DELAY  (default 0.1ms)   one-way delay added at each host's egress
#   EMU_BURST  (default 128k)    tbf bucket size (tc syntax)
#   EMU_STATE_DIR (default ${XDG_RUNTIME_DIR:-/tmp}/fds-emu)  PID files
#
# Requirements: util-linux (unshare, nsenter), iproute2 (ip, tc, ss), python3
# (ping-test only), and a kernel that allows unprivileged user namespaces.
#
set -euo pipefail

STATE_DIR="${EMU_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/fds-emu}"
EMU_RATE="${EMU_RATE:-1gbit}"
EMU_DELAY="${EMU_DELAY:-0.1ms}"
EMU_BURST="${EMU_BURST:-128k}"
SUBNET="10.10.0"
HOSTS=(S A B)

prog="$(basename "$0")"

die() { echo "$prog: $*" >&2; exit 1; }
info() { echo "$prog: $*" >&2; }

# host -> last octet of its IP address
host_octet() {
    case "$1" in
        S) echo 1 ;;
        A) echo 2 ;;
        B) echo 3 ;;
        *) die "unknown host '$1' (use S, A or B)" ;;
    esac
}
host_ip() { echo "$SUBNET.$(host_octet "$1")"; }

pidfile() { echo "$STATE_DIR/$1.pid"; }

# Print the PID stored for holder $1 (sw, S, A, B) if that process is alive.
holder_pid() {
    local f pid
    f="$(pidfile "$1")"
    [[ -r "$f" ]] || return 1
    pid="$(cat "$f")"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

# Run a command inside the switch namespace (user + net).
sw_exec() {
    local pid
    pid="$(holder_pid sw)" || die "emulation is not up (run: $prog up)"
    nsenter --target "$pid" --user --net --preserve-credentials -- "$@"
}

# Run a command inside host $1's namespaces (user + net + uts).
host_exec() {
    local h="$1" pid
    shift
    pid="$(holder_pid "$h")" || die "host $h is not up (run: $prog up)"
    nsenter --target "$pid" --user --net --uts --preserve-credentials -- "$@"
}

# Print why unprivileged namespaces failed and what to do instead.
explain_userns_failure() {
    cat >&2 <<EOF
$prog: cannot create an unprivileged user + network namespace on this machine.

Possible causes and fixes:
  * Ubuntu 23.10+ (AppArmor) blocks it. Allow it temporarily:
        sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
  * Debian / older kernels:
        sudo sysctl -w kernel.unprivileged_userns_clone=1
  * user.max_user_namespaces is 0:
        sudo sysctl -w user.max_user_namespaces=15000
  * Or simply run the emulation as root:
        sudo $0 up
    (then run the experiment scripts with sudo too and afterwards
     'sudo chown -R \$USER Results' so the CSV files belong to you).
  * Or skip the emulation and use the loopback mode of the runners:
        tools/exp/run_part1.sh --mode local ...
EOF
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------

# All holders alive and S has its address -> already up.
is_up() {
    local h
    holder_pid sw >/dev/null || return 1
    for h in "${HOSTS[@]}"; do
        holder_pid "$h" >/dev/null || return 1
    done
    host_exec S ip -4 -o addr show dev eth0 2>/dev/null | grep -q "$(host_ip S)/"
}

# Start one holder process in the background and record its PID.
# nohup: survive the terminal closing.  The started command exec()s
# `sleep infinity`, so $! is the PID of the process that owns the namespaces.
start_holder() {
    local name="$1"
    shift
    nohup "$@" sleep infinity </dev/null >/dev/null 2>&1 &
    echo "$!" >"$(pidfile "$name")"
}

# Wait until process $1 is in a network namespace different from ours.
wait_new_netns() {
    local pid="$1" mine i
    mine="$(readlink /proc/self/ns/net)"
    for i in $(seq 1 50); do
        if [[ -e "/proc/$pid/ns/net" ]] && [[ "$(readlink "/proc/$pid/ns/net" 2>/dev/null)" != "$mine" ]]; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 0.05
    done
    return 1
}

# Apply (or replace) the egress shaping of host $1:
#   root netem (delay only) -> child tbf (rate) -> child fq_codel (queue).
# netem's own "rate" option is not used: it queues ~64 KB GSO packets in one
# FIFO and the RTT under load grows to ~30 ms, which would penalise the
# per-file FETCH turnaround of Case 2 Enhanced far more than a real LAN does.
shape_host() {
    local h="$1"
    host_exec "$h" tc qdisc del dev eth0 root 2>/dev/null || true
    host_exec "$h" tc qdisc add dev eth0 root handle 1: netem delay "$EMU_DELAY" limit 100000
    host_exec "$h" tc qdisc add dev eth0 parent 1:1 handle 2: tbf rate "$EMU_RATE" burst "$EMU_BURST" latency 100ms
    host_exec "$h" tc qdisc add dev eth0 parent 2:1 handle 3: fq_codel
}

# Remember the shaping in force (status / ping-test print it).
save_shape() {
    printf 'EMU_RATE=%s\nEMU_DELAY=%s\nEMU_BURST=%s\n' "$EMU_RATE" "$EMU_DELAY" "$EMU_BURST" >"$STATE_DIR/shape.conf"
}

cmd_up() {
    local h pid
    if is_up; then
        info "already up (state in $STATE_DIR). Use '$prog down' first to rebuild."
        return 0
    fi
    # Leftovers of a half-built or crashed emulation: clean them first.
    if compgen -G "$STATE_DIR/*.pid" >/dev/null 2>&1; then
        info "removing a stale/partial emulation first"
        cmd_down >/dev/null 2>&1 || true
    fi
    mkdir -p "$STATE_DIR"

    if ! unshare --user --map-root-user --net true 2>/dev/null; then
        explain_userns_failure
        exit 1
    fi

    # 1. switch holder: new user ns (we are root inside) + new net ns
    start_holder sw unshare --user --map-root-user --net
    pid="$(cat "$(pidfile sw)")"
    wait_new_netns "$pid" || { cmd_down >/dev/null 2>&1 || true; explain_userns_failure; exit 1; }

    # 2. one holder per host, inside the switch's user namespace
    for h in "${HOSTS[@]}"; do
        start_holder "$h" nsenter --target "$pid" --user --preserve-credentials -- unshare --net --uts
        wait_new_netns "$(cat "$(pidfile "$h")")" || { cmd_down >/dev/null 2>&1 || true; die "could not create the namespace of host $h"; }
    done

    # 3. bridge + one veth pair per host
    sw_exec ip link set lo up
    sw_exec ip link add br0 type bridge
    sw_exec ip link set br0 up
    for h in "${HOSTS[@]}"; do
        sw_exec ip link add "v$h" type veth peer name eth0 netns "$(holder_pid "$h")"
        sw_exec ip link set "v$h" master br0
        sw_exec ip link set "v$h" up
        host_exec "$h" hostname "fds-$h"
        host_exec "$h" ip link set lo up
        host_exec "$h" ip addr add "$(host_ip "$h")/24" dev eth0
        host_exec "$h" ip link set eth0 up
        # 4. egress shaping (delay + rate)
        shape_host "$h"
    done
    save_shape

    # The bridge needs a moment before it forwards (no STP, but link-up events).
    for _ in $(seq 1 40); do
        if host_exec A python3 -c 'import sys' 2>/dev/null &&
           host_exec S ping -c 1 -W 1 "$(host_ip A)" >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    info "up: S=$(host_ip S) A=$(host_ip A) B=$(host_ip B), egress $EMU_RATE, delay $EMU_DELAY per direction"
    info "run a command on a host with: $prog exec S|A|B <cmd...>"
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------

# PIDs of every process (ours) whose network namespace is $1 (e.g. "net:[4026...]").
procs_in_netns() {
    local target="$1" d ns
    for d in /proc/[0-9]*; do
        ns="$(readlink "$d/ns/net" 2>/dev/null)" || continue
        [[ "$ns" == "$target" ]] && echo "${d#/proc/}"
    done
    return 0
}

cmd_down() {
    local name pid ns victims=()
    [[ -d "$STATE_DIR" ]] || { info "not up"; return 0; }
    # Kill everything still running inside the host namespaces (servers,
    # clients started with 'exec'), then the holders themselves. A namespace
    # disappears when its last process exits, and with it veths and the bridge.
    for name in "${HOSTS[@]}" sw; do
        pid="$(holder_pid "$name")" || continue
        ns="$(readlink "/proc/$pid/ns/net" 2>/dev/null)" || continue
        mapfile -t -O "${#victims[@]}" victims < <(procs_in_netns "$ns")
    done
    if ((${#victims[@]})); then
        kill -TERM "${victims[@]}" 2>/dev/null || true
        for _ in $(seq 1 20); do
            local alive=0
            for pid in "${victims[@]}"; do
                kill -0 "$pid" 2>/dev/null && alive=1
            done
            ((alive)) || break
            sleep 0.1
        done
        kill -KILL "${victims[@]}" 2>/dev/null || true
    fi
    rm -f "$STATE_DIR"/*.pid "$STATE_DIR/shape.conf"
    rmdir "$STATE_DIR" 2>/dev/null || true
    info "down (${#victims[@]} processes stopped)"
}

# ---------------------------------------------------------------------------
# status / exec / ip / shape
# ---------------------------------------------------------------------------

cmd_status() {
    local h pid ok=1
    echo "state dir: $STATE_DIR"
    if pid="$(holder_pid sw)"; then
        echo "switch   pid $pid  $(readlink "/proc/$pid/ns/net")"
    else
        echo "switch   DOWN"
        ok=0
    fi
    for h in "${HOSTS[@]}"; do
        if pid="$(holder_pid "$h")"; then
            echo "host $h   pid $pid  $(readlink "/proc/$pid/ns/net")  ip $(host_ip "$h")"
        else
            echo "host $h   DOWN"
            ok=0
        fi
    done
    if ((ok == 0)); then
        echo "emulation is DOWN (run: $prog up)"
        return 1
    fi
    [[ -r "$STATE_DIR/shape.conf" ]] && sed 's/^/shape: /' "$STATE_DIR/shape.conf"
    for h in "${HOSTS[@]}"; do
        echo "--- $h ($(host_exec "$h" hostname))"
        host_exec "$h" ip -br addr show dev eth0
        host_exec "$h" tc qdisc show dev eth0
    done
    echo "--- switch"
    sw_exec bridge link show 2>/dev/null || sw_exec ip -br link
}

cmd_exec() {
    (($# >= 2)) || die "usage: $prog exec S|A|B <command> [args...]"
    local h="$1" pid
    shift
    host_octet "$h" >/dev/null
    pid="$(holder_pid "$h")" || die "host $h is not up (run: $prog up)"
    # exec: the command keeps this script's PID, so a caller that started
    # '$prog exec S ./server ... &' can signal the server with kill \$!.
    exec nsenter --target "$pid" --user --net --uts --preserve-credentials -- "$@"
}

cmd_shape() {
    local h
    EMU_RATE="${1:-$EMU_RATE}"
    EMU_DELAY="${2:-$EMU_DELAY}"
    is_up || die "emulation is not up"
    for h in "${HOSTS[@]}"; do
        shape_host "$h"
    done
    save_shape
    info "shaping set to rate $EMU_RATE, delay $EMU_DELAY on every host"
}

# ---------------------------------------------------------------------------
# ping-test: connectivity and a TCP throughput check
# ---------------------------------------------------------------------------

# Small TCP sink/source in python (no iperf needed). The receiver prints the
# rate it saw; the sender just pushes MB megabytes in 64 KB writes.
PY_RECV='
import socket, sys, time
port = int(sys.argv[1]); n = int(sys.argv[2])
ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("0.0.0.0", port)); ls.listen(8); ls.settimeout(20)
done = []
for _ in range(n):
    c, addr = ls.accept(); total = 0; t0 = None
    while True:
        b = c.recv(1 << 20)
        if not b: break
        if t0 is None: t0 = time.monotonic()
        total += len(b)
    dt = time.monotonic() - (t0 or time.monotonic())
    done.append((addr[0], total, dt))
for ip, total, dt in done:
    print("%s -> me: %.1f MB in %.2f s = %.0f Mbit/s" % (ip, total / 1e6, dt, total * 8 / dt / 1e6 if dt > 0 else 0))
'
PY_SEND='
import socket, sys, time
ip, port, mb = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(100):
    try:
        s = socket.create_connection((ip, port), timeout=5); break
    except OSError:
        time.sleep(0.05)
else:
    sys.exit("cannot connect to %s:%d" % (ip, port))
buf = b"x" * 65536
for _ in range(mb * 16):
    s.sendall(buf)
s.close()
'

cmd_ping_test() {
    local h a b out mb="${PING_TEST_MB:-200}" fail=0 port=5299 pids=()
    is_up || die "emulation is not up (run: $prog up)"
    echo "== connectivity (ping, 3 packets each)"
    for a in "${HOSTS[@]}"; do
        for b in "${HOSTS[@]}"; do
            [[ "$a" < "$b" ]] || continue
            if out="$(host_exec "$a" ping -c 3 -i 0.2 -W 1 "$(host_ip "$b")" 2>&1)"; then
                echo "$a -> $b: $(echo "$out" | grep -E 'rtt|round-trip' || echo ok)"
            else
                echo "$a -> $b: FAILED"
                fail=1
            fi
        done
    done
    echo "== throughput S -> A alone (${mb} MB), with a ping S -> A during the transfer"
    host_exec A python3 -c "$PY_RECV" "$port" 1 &
    pids=($!)
    sleep 0.3
    ( sleep 0.5; out="$(host_exec S ping -c 5 -i 0.1 -W 1 "$(host_ip A)" 2>&1 | grep -E 'rtt|round-trip' || true)"
      echo "RTT under load: ${out:-no reply}" ) &
    local pinger=$!
    host_exec S python3 -c "$PY_SEND" "$(host_ip A)" "$port" "$mb" || fail=1
    wait "${pids[0]}" || fail=1
    wait "$pinger" || true
    echo "== throughput S -> A and S -> B at the same time (shared S uplink, ${mb} MB each)"
    host_exec A python3 -c "$PY_RECV" "$port" 1 &
    pids=($!)
    host_exec B python3 -c "$PY_RECV" "$port" 1 &
    pids+=($!)
    sleep 0.3
    host_exec S python3 -c "$PY_SEND" "$(host_ip A)" "$port" "$mb" &
    local s1=$!
    host_exec S python3 -c "$PY_SEND" "$(host_ip B)" "$port" "$mb" || fail=1
    wait "$s1" || fail=1
    for h in "${pids[@]}"; do
        wait "$h" || fail=1
    done
    echo "== throughput A -> S (${mb} MB)"
    host_exec S python3 -c "$PY_RECV" "$port" 1 &
    pids=($!)
    sleep 0.3
    host_exec A python3 -c "$PY_SEND" "$(host_ip S)" "$port" "$mb" || fail=1
    wait "${pids[0]}" || fail=1
    if ((fail)); then
        echo "ping-test: FAILED"
        return 1
    fi
    echo "ping-test: OK (expected: close to EMU_RATE=$EMU_RATE alone, about half of it each when shared)"
}

# ---------------------------------------------------------------------------

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed -e '/^set -euo/d' -e 's/^# \{0,1\}//'
}

main() {
    local cmd="${1:-}"
    (($#)) && shift
    # The shaping currently applied (for status / ping-test messages).
    if [[ -r "$STATE_DIR/shape.conf" && "$cmd" != "up" && "$cmd" != "shape" ]]; then
        # shellcheck disable=SC1091
        source "$STATE_DIR/shape.conf"
    fi
    case "$cmd" in
        up) cmd_up "$@" ;;
        down) cmd_down "$@" ;;
        status) cmd_status "$@" ;;
        exec) cmd_exec "$@" ;;
        ping-test) cmd_ping_test "$@" ;;
        shape) cmd_shape "$@" ;;
        ip) host_ip "${1:?usage: $prog ip S|A|B}" ;;
        pid) holder_pid "${1:?usage: $prog pid S|A|B}" ;;
        -h | --help | help | "") usage ;;
        *) die "unknown command '$cmd' (try: $prog --help)" ;;
    esac
}

main "$@"
