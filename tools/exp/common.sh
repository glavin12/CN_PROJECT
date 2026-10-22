#!/usr/bin/env bash
#
# common.sh - shared code of the experiment runners run_part1.sh,
# run_part2.sh and run_buffers.sh (design sections 6.4-6.7).
#
# The runners `source` this file, build a PLAN (one line per run) and call
# exp_main. This file is ALSO executed directly on the client PCs, where it
# starts the clients of one run:
#
#     bash tools/exp/common.sh launch-clients --bin ... --count 2 ... -- [client args]
#
# so the exact same launch code is used in every mode.
#
# Modes (--mode):
#   emu     3 emulated hosts on this PC (tools/emu/emu.sh): server in S,
#           clients split over A and B.
#   local   everything on 127.0.0.1 (smoke tests only, not for the report).
#   ssh     this PC is the server S. Clients are started on PC A and PC B over
#           passwordless ssh (settings in tools/exp/lab.conf). Client CSVs are
#           written on A/B and copied back with scp after every run.
#   manual  no ssh: every PC runs the same command with --role server|A|B and
#           the same --start-at. All three follow an identical timetable
#           (one fixed time slot per run), so they need no communication.
#
# Files written for a sweep (SWEEP = part1 | part2 | buffers), under --out
# (default Results/raw):
#   <sweep>/server/<run_id>.csv          server FDS_LOG (one row per connection)
#   <sweep>/events/<run_id>.csv          server FDS_EVENTS (FAULT, RESUME_OK, ...)
#   <sweep>/clients/<run_id>.<A|B>.csv   FDS_LOG of all clients of that PC
#   <sweep>/runs.csv                     one row per run written by this runner
#                                        (runs_server.csv / runs_A.csv / runs_B.csv
#                                        in manual mode)
#   <sweep>/run_<role>.log               progress log
#   <sweep>/logs/                        server stderr and client launcher output
#   <sweep>/clock_offsets.csv            ssh mode: clock offset of A and B
#   sysinfo_<label>_<host>.txt           collect_sysinfo.sh output
#
# Run ids (parsed by analyze.py):
#   part1_<single|multi>_N<n>_r<r>          Part I
#   case1_p<p>_r<r>, case2_p<p>_r<r>        Part II Case 1 / Case 2
#   enh_K<k>_<threads|poll>_p<p>_r<r>       Part II Case 2 Enhanced
#   p1cmp_multi_N<n>_r<r>                   Part I multi server on the Part II topic
#   buf_<case2|enh_K4_threads>_b<bytes|default>_r<r>   socket-buffer experiment
#   warm-ups end in _w<i>_warmup instead of _r<r> and are ignored by analyze.py.
# Repetition r uses the server seed 1000 + r (warm-ups use 1000), the same
# for every case (design 6.4), so the cases see comparable fault sequences.
#

EXP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$EXP_DIR/../.." && pwd)"
EMU="$REPO/tools/emu/emu.sh"
SEED_BASE=1000

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

die() { echo "error: $*" >&2; exit 1; }
warn() { echo "warning: $*" >&2; }

now_ms() { date +%s%3N; }

# milliseconds -> "s.mmm" (for sleep and printing)
ms_to_s() {
    local ms=$1 sign=""
    ((ms < 0)) && { sign="-"; ms=$((-ms)); }
    printf '%s%d.%03d' "$sign" $((ms / 1000)) $((ms % 1000))
}

sleep_until_ms() {
    local d=$(($1 - $(now_ms)))
    ((d > 0)) && sleep "$(ms_to_s "$d")"
    return 0
}

hms() { printf '%dh%02dm%02ds' $(($1 / 3600)) $(($1 % 3600 / 60)) $(($1 % 60)); }

# "1,2 4" -> "1 2 4"
split_list() { echo "${1//,/ }"; }

# Print a command exactly as it would be run (shell-quoted), prefixed by "+ ".
show_cmd() {
    local s
    printf -v s '%q ' "$@"
    echo "+ ${s% }"
}

# Same for "ssh ... <target> <remote command string>": the remote command is
# shown in single quotes instead of with every space escaped.
show_ssh_cmd() {
    local s n=$#
    local -a args=("$@")
    printf -v s '%q ' "${args[@]:0:n-1}"
    echo "+ ${s}'${args[n - 1]% }'"
}

# Progress line on stdout and in the run log (not in --dry-run).
EXP_LOG=""
say() {
    local line
    line="[$(date +%H:%M:%S)] $*"
    echo "$line"
    if [[ -n "$EXP_LOG" && "${DRY_RUN:-0}" == 0 && -d "$(dirname "$EXP_LOG")" ]]; then
        echo "$line" >>"$EXP_LOG"
    fi
}

# Count files and bytes of a topic directory exactly like the server does:
# regular files directly inside it, hidden files skipped. Prints "files bytes".
dir_stats() {
    find "$1" -maxdepth 1 -mindepth 1 -type f ! -name '.*' -printf '%s\n' 2>/dev/null |
        awk '{ n++; s += $1 } END { printf "%d %d\n", n, s }'
}

# ---------------------------------------------------------------------------
# launch-clients: runs on a client PC (or emulated host) for one run
# ---------------------------------------------------------------------------
#
# Starts --count clients at the same moment (--start-at, epoch ms, this PC's
# clock), each with its own output dir <tmp>/<run_id>/<label><i>, its own
# FDS_CLIENT_ID and the shared CSV --csv. Waits for all of them (each one is
# killed after --timeout s, which counts as DNF), prints one line per client
#     RC <client_id> <exit code>
# checks the output of the first successful client against the reference
#     CHECK <client_id> OK|MISMATCH|NOREF|NODIR files=<n> bytes=<n> ...
# and deletes the outputs unless --keep-output.
cmd_launch_clients() {
    local bin="" sip="" port="" topic="" count=1 label=A rid="" csv="" tmp=/tmp/fds_out
    local start_at=0 tmo=600 ef="" eb="" keep=0 dataset=""
    local -a envs=() extra=()
    while (($#)); do
        case "$1" in
            --bin) bin=$2; shift 2 ;;
            --server-ip) sip=$2; shift 2 ;;
            --port) port=$2; shift 2 ;;
            --topic) topic=$2; shift 2 ;;
            --count) count=$2; shift 2 ;;
            --label) label=$2; shift 2 ;;
            --run-id) rid=$2; shift 2 ;;
            --csv) csv=$2; shift 2 ;;
            --tmp) tmp=$2; shift 2 ;;
            --start-at) start_at=$2; shift 2 ;;
            --timeout) tmo=$2; shift 2 ;;
            --expect-files) ef=$2; shift 2 ;;
            --expect-bytes) eb=$2; shift 2 ;;
            --dataset) dataset=$2; shift 2 ;;
            --env) envs+=("$2"); shift 2 ;;
            --keep-output) keep=1; shift ;;
            --) shift; extra=("$@"); break ;;
            *) echo "FATAL launch-clients: unknown option $1"; exit 1 ;;
        esac
    done
    [[ -n "$rid" && -n "$csv" && -n "$sip" && -n "$port" && -n "$topic" ]] ||
        { echo "FATAL launch-clients: --run-id --csv --server-ip --port --topic are required"; exit 1; }
    [[ -x "$bin" ]] || { echo "FATAL client binary not found or not executable: $bin (run make?)"; exit 1; }
    command -v timeout >/dev/null || { echo "FATAL 'timeout' (coreutils) is missing on $(hostname)"; exit 1; }
    # Reference for the output check: from --expect-* or from a local dataset copy.
    if [[ -z "$ef" && -n "$dataset" && -d "$dataset/$topic" ]]; then
        read -r ef eb < <(dir_stats "$dataset/$topic")
    fi

    mkdir -p "$(dirname "$csv")"
    rm -f "$csv"
    # Only this launcher's client dirs are touched: in emu/local mode the
    # launchers of A and B share one file system and run at the same time.
    local base="$tmp/$rid" i id
    local -a ids=()
    for ((i = 1; i <= count; i++)); do
        ids+=("$label$i")
    done
    for id in "${ids[@]}"; do
        rm -rf "${base:?}/$id" "$base/$id.log"
        mkdir -p "$base/$id"
    done

    sleep_until_ms "$start_at"
    echo "START $(now_ms) host=$(hostname)"
    local -a pids=()
    for id in "${ids[@]}"; do
        # timeout -k 5: SIGTERM after tmo seconds, SIGKILL 5 s later. Exit 124 = timed out.
        env FDS_RUN_ID="$rid" FDS_CLIENT_ID="$id" FDS_LOG="$csv" FDS_QUIET=1 "${envs[@]}" \
            timeout -k 5 "$tmo" "$bin" "$sip" "$port" "$topic" "$base/$id" "${extra[@]}" \
            >"$base/$id.log" 2>&1 </dev/null &
        pids+=("$!")
    done
    local rc first_ok=""
    for i in "${!pids[@]}"; do
        rc=0
        wait "${pids[$i]}" || rc=$?
        echo "RC ${ids[$i]} $rc"
        if ((rc != 0)); then
            tail -n 3 "$base/${ids[$i]}.log" 2>/dev/null | sed "s/^/ERR ${ids[$i]} /" || true
        elif [[ -z "$first_ok" ]]; then
            first_ok="${ids[$i]}"
        fi
    done
    echo "END $(now_ms)"

    # Output check of one successful client: same file count and total bytes
    # as the topic directory, and no .part/.session/hidden leftovers.
    if [[ -z "$first_ok" ]]; then
        echo "CHECK - SKIP no client succeeded"
    else
        local d="$base/$first_ok/$topic" nf nb left verdict
        if [[ ! -d "$d" ]]; then
            echo "CHECK $first_ok NODIR $d does not exist"
        else
            read -r nf nb < <(find "$d" -maxdepth 1 -mindepth 1 -type f -printf '%s\n' |
                awk '{ n++; s += $1 } END { printf "%d %d\n", n, s }')
            left=$(find "$d" -mindepth 1 \( -name '*.part' -o -name '*.session' -o -name '.*' -o -type d \) | wc -l)
            if [[ -z "$ef" ]]; then
                verdict=NOREF
            elif [[ "$nf" == "$ef" && "$nb" == "$eb" && "$left" == 0 ]]; then
                verdict=OK
            else
                verdict=MISMATCH
            fi
            echo "CHECK $first_ok $verdict files=$nf/${ef:-?} bytes=$nb/${eb:-?} leftovers=$left"
        fi
    fi
    if ((!keep)); then
        for id in "${ids[@]}"; do
            rm -rf "${base:?}/$id" "$base/$id.log"
        done
        rmdir "$base" 2>/dev/null || true   # the last launcher removes the run folder
    fi
    echo "DONE"
}

# ---------------------------------------------------------------------------
# option handling (shared by the three runners)
# ---------------------------------------------------------------------------

declare -A OPT=()
EXP_SHIFT=1

exp_common_help() {
    cat <<'EOF'
Common options (all three runners):
  --mode emu|local|ssh|manual   where to run (required, see tools/exp/README.md)
  --role server|A|B             manual mode only: which PC this is
  --start-at <time>             manual mode: shared start, epoch seconds or any
                                'date -d' text such as "14:30" (same on all PCs)
  --plan <file>                 manual mode: save the timetable to <file>, or load
                                it if <file> exists (optional; the plan hash that
                                is printed must be the same on all PCs anyway)
  --dataset <dir>               topics_root_dir on the server (default
                                $DATASET from lab.conf, else dataset/raw-img)
  --port <n>                    server port (default 5050)
  --server-ip <ip>              server address for ssh/manual (lab.conf SERVER_IP)
  --topic <name>                topic every client requests
  --reps <n>                    measured repetitions per configuration (default 5)
  --warmup <n>                  discarded warm-up runs per configuration (default 1)
  --out <dir>                   raw output root (default Results/raw)
  --timeout <s>                 per-run client time-out for every run (default: per case)
  --slot <s>                    manual mode: slot length for every run (default: per case)
  --slot-scale <x>              manual mode: multiply the default slots by x
  --gap <s>                     pause between runs (default 1)
  --conf <file>                 lab config (default tools/exp/lab.conf if it exists)
  --bin-root <dir>              folder holding Part1/ Part2_*/ binaries (default: repo)
  --client-tmp <dir>            client output dirs (default /tmp/fds_out, local disk!)
  --keep-output                 keep the clients' received files
  --resume                      skip runs that already have results (emu/local/ssh)
  --collect run|end             ssh mode: copy client CSVs after every run or at the end
  --quick                       reduced sweep for testing the pipeline
  --dry-run                     print every command, run nothing
  --yes                         do not ask for confirmation
  -h, --help                    this help
EOF
}

# Parse one common option. Sets EXP_SHIFT to the number of words consumed.
exp_parse_opt() {
    EXP_SHIFT=1
    case "$1" in
        --mode | --role | --plan | --start-at | --dataset | --port | --reps | --warmup | --out | \
            --conf | --server-ip | --topic | --timeout | --gap | --slot | --slot-scale | \
            --bin-root | --client-tmp | --collect)
            (($# >= 2)) || die "option $1 needs a value"
            OPT[${1#--}]="$2"
            EXP_SHIFT=2
            ;;
        --dry-run | --quick | --yes | --keep-output | --resume)
            OPT[${1#--}]=1
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) die "unknown option '$1' (try --help)" ;;
    esac
}

# Resolve the final settings: command line > lab.conf > defaults.
# Call after the runner has parsed its arguments (and applied --quick).
exp_finalize() {
    # Settings that may come from lab.conf; clear inherited environment values.
    unset SERVER_IP CLIENT_A CLIENT_B REMOTE_REPO REMOTE_RAW REMOTE_BIN_ROOT DATASET PORT \
        CLIENT_TMP SSH_OPTS SERVER_SSH SERVER_REPO
    local conf="${OPT[conf]:-$EXP_DIR/lab.conf}"
    if [[ -r "$conf" ]]; then
        # shellcheck disable=SC1090
        source "$conf"
        CONF_USED="$conf"
    elif [[ -n "${OPT[conf]:-}" ]]; then
        die "cannot read config file $conf"
    else
        CONF_USED="(none)"
    fi

    MODE="${OPT[mode]:-}"
    [[ -n "$MODE" ]] || die "--mode emu|local|ssh|manual is required (try --help)"
    case "$MODE" in emu | local | ssh | manual) ;; *) die "bad --mode '$MODE'" ;; esac
    ROLE="${OPT[role]:-}"
    DRY_RUN="${OPT[dry-run]:-0}"
    QUICK="${OPT[quick]:-0}"
    YES="${OPT[yes]:-0}"
    KEEP_OUTPUT="${OPT[keep-output]:-0}"
    RESUME="${OPT[resume]:-0}"
    REPS="${OPT[reps]:-5}"
    WARMUP="${OPT[warmup]:-1}"
    PORT="${OPT[port]:-${PORT:-5050}}"
    GAP="${OPT[gap]:-1}"
    COLLECT="${OPT[collect]:-run}"
    TIMEOUT_OVERRIDE="${OPT[timeout]:-}"
    SLOT_OVERRIDE="${OPT[slot]:-}"
    SLOT_SCALE="${OPT[slot-scale]:-1}"
    TOPIC="${OPT[topic]:-$TOPIC_DEFAULT}"
    CLIENT_TMP="${OPT[client-tmp]:-${CLIENT_TMP:-/tmp/fds_out}}"
    BIN_ROOT="$(realpath -m "${OPT[bin-root]:-$REPO}")"
    OUT="$(realpath -m "${OPT[out]:-$REPO/Results/raw}")"
    RAW="$OUT/$SWEEP"
    local ds_default="$REPO/dataset/raw-img"
    DATASET="$(realpath -m "${OPT[dataset]:-${DATASET:-$ds_default}}")"
    REMOTE_REPO="${REMOTE_REPO:-$REPO}"
    REMOTE_BIN_ROOT="${REMOTE_BIN_ROOT:-${OPT[bin-root]:+$BIN_ROOT}}"
    REMOTE_BIN_ROOT="${REMOTE_BIN_ROOT:-$REMOTE_REPO}"
    REMOTE_RAW="${REMOTE_RAW:-/tmp/fds_raw}"
    CLIENT_DELAY=2   # manual mode: clients start 2 s after the server
    STOP_MARGIN=3    # manual mode: server stops 3 s before the slot ends
    LEAD_MS=1000     # emu/local: clients start 1 s after the launch command
    SSH_LEAD_MS=2000 # ssh: clients start 2 s after the launch command (ssh latency)

    [[ "$REPS" =~ ^[0-9]+$ && "$WARMUP" =~ ^[0-9]+$ ]] || die "--reps/--warmup must be integers"
    [[ "$PORT" =~ ^[0-9]+$ ]] || die "--port must be a number"
    [[ "$COLLECT" == run || "$COLLECT" == end ]] || die "--collect must be run or end"

    # ssh options: BatchMode (never prompt for a password), connection sharing
    # (one TCP/ssh handshake per PC for the whole sweep, so the synchronised
    # start is not delayed by ssh set-up).
    SSH_OPTS_ARR=(-o BatchMode=yes -o ConnectTimeout=5 -o ControlMaster=auto
        -o "ControlPath=/tmp/fds-ssh-%C" -o ControlPersist=600)
    if [[ -n "${SSH_OPTS:-}" ]]; then
        local -a extra_ssh
        read -ra extra_ssh <<<"$SSH_OPTS"
        SSH_OPTS_ARR+=("${extra_ssh[@]}")
    fi

    case "$MODE" in
        emu)
            SERVER_IP="$("$EMU" ip S)"
            ;;
        local)
            SERVER_IP=127.0.0.1
            ;;
        ssh)
            if [[ -z "${SERVER_IP:-}${OPT[server-ip]:-}" || -z "${CLIENT_A:-}" || -z "${CLIENT_B:-}" ]]; then
                ((DRY_RUN)) || die "ssh mode needs SERVER_IP, CLIENT_A and CLIENT_B (copy tools/exp/lab.conf.example to tools/exp/lab.conf)"
                warn "no lab.conf: using placeholders for the dry run"
                SERVER_IP="${SERVER_IP:-192.168.1.10}"
                CLIENT_A="${CLIENT_A:-student@pcA}"
                CLIENT_B="${CLIENT_B:-student@pcB}"
            fi
            SERVER_IP="${OPT[server-ip]:-$SERVER_IP}"
            ;;
        manual)
            case "$ROLE" in server | A | B) ;; *) die "manual mode needs --role server|A|B" ;; esac
            SERVER_IP="${OPT[server-ip]:-${SERVER_IP:-}}"
            if [[ "$ROLE" != server && -z "$SERVER_IP" ]]; then
                ((DRY_RUN)) || die "manual mode, role $ROLE: give --server-ip (or SERVER_IP in lab.conf)"
                SERVER_IP=192.168.1.10
            fi
            local sa="${OPT[start-at]:-}"
            if [[ -z "$sa" ]]; then
                ((DRY_RUN)) || die "manual mode needs --start-at (the same value on all three PCs)"
                START_MS=$(($(now_ms) + 120000))
            elif [[ "$sa" =~ ^[0-9]+$ ]]; then
                START_MS=$((sa * 1000))
            else
                local e
                e="$(date -d "$sa" +%s 2>/dev/null)" || die "cannot parse --start-at '$sa'"
                START_MS=$((e * 1000))
            fi
            ;;
    esac
    if [[ "$MODE" != manual && -n "$ROLE" ]]; then
        die "--role is only for --mode manual"
    fi
    PLAN=()
}

# ---------------------------------------------------------------------------
# the plan
# ---------------------------------------------------------------------------
# One line per run:  run_id|part_dir|n_clients|topic|server_args|client_args|env|timeout_s|slot_s
#   server_args  extra server arguments after <port> <root>: "single", "0.001 1003"
#   client_args  extra client arguments after <output_dir>: "100", "4 20 threads", "-"
#   env          extra environment for server AND clients: "FDS_SNDBUF=65536 ...", "-"

PLAN=()

# plan_add RUN_ID PART_DIR N TOPIC SERVER_ARGS CLIENT_ARGS ENV TIMEOUT SLOT
plan_add() {
    local f tmo slot
    for f in "$@"; do
        [[ "$f" != *"|"* ]] || die "internal: '|' in plan field"
    done
    tmo="${TIMEOUT_OVERRIDE:-$8}"
    if [[ -n "$SLOT_OVERRIDE" ]]; then
        slot="$SLOT_OVERRIDE"
    else
        slot="$(awk -v s="$9" -v k="$SLOT_SCALE" 'BEGIN { v = s * k; printf "%d", (v == int(v)) ? v : int(v) + 1 }')"
    fi
    PLAN+=("$1|$2|$3|$4|${5:--}|${6:--}|${7:--}|$tmo|$slot")
}

# exp_add_reps CONFIG_ID FUNC [ARGS...]
# Calls "FUNC RUN_ID SEED ARGS..." for each warm-up run, then each repetition.
exp_add_reps() {
    local cfg="$1" fn="$2" w r
    shift 2
    for ((w = 1; w <= WARMUP; w++)); do
        "$fn" "${cfg}_w${w}_warmup" "$SEED_BASE" "$@"
    done
    for ((r = 1; r <= REPS; r++)); do
        "$fn" "${cfg}_r${r}" $((SEED_BASE + r)) "$@"
    done
}

# Text that identifies the plan (also the content of a --plan file).
plan_text() {
    echo "# FDS experiment plan (tools/exp). Generated; do not edit."
    echo "# sweep=$SWEEP port=$PORT client_delay=$CLIENT_DELAY stop_margin=$STOP_MARGIN"
    printf '%s\n' "${PLAN[@]}"
}

plan_hash() { plan_text | sha1sum | cut -c1-10; }

plan_load() {
    local f="$1" line
    PLAN=()
    while IFS= read -r line; do
        if [[ "$line" =~ ^#\ sweep=([a-z0-9]+)\ port=([0-9]+)\ client_delay=([0-9]+)\ stop_margin=([0-9]+) ]]; then
            [[ "${BASH_REMATCH[1]}" == "$SWEEP" ]] || die "$f is a plan for sweep '${BASH_REMATCH[1]}', not '$SWEEP'"
            PORT="${BASH_REMATCH[2]}"
            CLIENT_DELAY="${BASH_REMATCH[3]}"
            STOP_MARGIN="${BASH_REMATCH[4]}"
            continue
        fi
        [[ -z "$line" || "$line" == \#* ]] && continue
        PLAN+=("$line")
    done <"$f"
}

# Parts (binary folders) used by the plan.
plan_parts() {
    local line
    for line in "${PLAN[@]}"; do
        echo "${line#*|}" | cut -d'|' -f1
    done | sort -u
}

plan_topics() {
    local line
    for line in "${PLAN[@]}"; do
        echo "$line" | cut -d'|' -f4
    done | sort -u
}

# Client split: A gets ceil(N/2), B gets floor(N/2) (N = 1 -> only A).
n_for_label() {
    local n=$1 label=$2
    if [[ "$label" == A ]]; then echo $(((n + 1) / 2)); else echo $((n / 2)); fi
}

# ---------------------------------------------------------------------------
# runs.csv bookkeeping
# ---------------------------------------------------------------------------

RUNS_HDR="run_id,sweep,mode,role,n_clients,start_wall_ms,elapsed_s,server_rc,clients_ok,clients_dnf,clients_timeout,clients_fail,client_rcs,check,note"
RUNS_FILE=runs.csv

runs_append() {
    ((DRY_RUN)) && return 0
    local f="$RAW/$RUNS_FILE"
    [[ -s "$f" ]] || echo "$RUNS_HDR" >"$f"
    echo "$1" >>"$f"
}

run_is_done() {
    [[ -s "$RAW/server/$1.csv" ]] && grep -q "^$1," "$RAW/$RUNS_FILE" 2>/dev/null
}

# Parse launcher outputs (RC / CHECK / FATAL lines) into the globals
#   R_OK R_DNF R_TO R_FAIL R_RCS R_CHECK R_NOTE
# Exit codes: 0 OK, 2 gave up (DNF), 124/137/143 killed by the time-out (DNF),
# anything else (1 = fatal) FAIL. Clients without an RC line count as FAIL.
parse_launcher_outputs() {
    local expected=$1
    shift
    R_OK=0 R_DNF=0 R_TO=0 R_FAIL=0 R_RCS="" R_CHECK="" R_NOTE=""
    local f label tag a b rest seen=0
    for f in "$@"; do
        label="$(basename "$f" .out)"
        label="${label##*.}"
        [[ -r "$f" ]] || { R_NOTE+="no output from $label; "; continue; }
        while read -r tag a b rest; do
            case "$tag" in
                RC)
                    seen=$((seen + 1))
                    R_RCS+="${R_RCS:+;}$a=$b"
                    case "$b" in
                        0) R_OK=$((R_OK + 1)) ;;
                        2) R_DNF=$((R_DNF + 1)) ;;
                        124 | 137 | 143) R_TO=$((R_TO + 1)) ;;
                        *) R_FAIL=$((R_FAIL + 1)) ;;
                    esac
                    ;;
                CHECK) R_CHECK+="${R_CHECK:+ }$label:$b" ;;
                FATAL) R_NOTE+="$label: $a $b $rest; " ;;
            esac
        done <"$f"
    done
    if ((seen < expected)); then
        R_FAIL=$((R_FAIL + expected - seen))
        R_NOTE+="$((expected - seen)) client(s) without exit code; "
    fi
    R_NOTE="${R_NOTE//,/ }"
}

# ---------------------------------------------------------------------------
# server start / stop (emu, local, ssh = this PC, manual role server)
# ---------------------------------------------------------------------------

SERVER_PID=""
LAUNCHER_PIDS=()

# Run a command on the server host (inside S for emu).
on_server_host() {
    if [[ "$MODE" == emu ]]; then "$EMU" exec S "$@"; else "$@"; fi
}

# Is something listening on $PORT on the server host? Reads /proc/net/tcp
# (of the server's network namespace) instead of connecting: a test
# connection would be accepted and logged by the server and spoil the CSV.
server_port_busy() {
    on_server_host awk -v p=":$(printf '%04X' "$PORT")" \
        'FNR > 1 && substr($2, length($2) - 4) == p && $4 == "0A" { f = 1 } END { exit !f }' /proc/net/tcp
}

# start_server RUN_ID PART SERVER_ARGS ENV  -> sets SERVER_PID, returns 1 on failure
start_server() {
    local rid=$1 part=$2 sargs=$3 envs=$4 i
    local -a sargv=() envv=() cmd
    [[ "$sargs" == - ]] || read -ra sargv <<<"$sargs"
    [[ "$envs" == - ]] || read -ra envv <<<"$envs"
    cmd=(env FDS_RUN_ID="$rid" FDS_LOG="$RAW/server/$rid.csv" FDS_EVENTS="$RAW/events/$rid.csv"
        FDS_QUIET=1 "${envv[@]}" "$BIN_ROOT/$part/server" "$PORT" "$DATASET" "${sargv[@]}")
    [[ "$MODE" == emu ]] && cmd=("$EMU" exec S "${cmd[@]}")
    if ((DRY_RUN)); then
        show_cmd "${cmd[@]}"
        echo "    (background, stderr -> $RAW/logs/$rid.server.log; then wait until port $PORT listens)"
        SERVER_PID=0
        return 0
    fi
    # A server of the previous run may still be shutting down.
    for ((i = 0; i < 100; i++)); do
        server_port_busy || break
        sleep 0.1
    done
    server_port_busy && { R_NOTE="port $PORT busy (another server running?)"; return 1; }
    "${cmd[@]}" >"$RAW/logs/$rid.server.log" 2>&1 </dev/null &
    SERVER_PID=$!
    for ((i = 0; i < 100; i++)); do
        kill -0 "$SERVER_PID" 2>/dev/null || { R_NOTE="server exited at start: $(tail -n 1 "$RAW/logs/$rid.server.log")"; SERVER_PID=""; return 1; }
        server_port_busy && return 0
        sleep 0.1
    done
    R_NOTE="server not listening after 10 s"
    return 1
}

# Graceful stop (SIGINT: the server flushes its CSV and exits 0). Sets SERVER_RC.
stop_server() {
    local i
    SERVER_RC=""
    if ((DRY_RUN)); then
        echo "+ kill -INT <server pid>    (graceful stop; kill -KILL after 15 s)"
        return 0
    fi
    [[ -n "$SERVER_PID" ]] || return 0
    kill -INT "$SERVER_PID" 2>/dev/null || true
    for ((i = 0; i < 150; i++)); do
        kill -0 "$SERVER_PID" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -KILL "$SERVER_PID" 2>/dev/null || true
        R_NOTE+="server did not stop on SIGINT (killed); "
    fi
    SERVER_RC=0
    wait "$SERVER_PID" 2>/dev/null || SERVER_RC=$?
    SERVER_PID=""
}

# Ctrl-C: stop whatever this run started, then exit.
exp_on_interrupt() {
    trap - INT TERM
    echo
    say "interrupted: stopping the server and the clients of the current run"
    local p
    for p in "${LAUNCHER_PIDS[@]}"; do
        kill -TERM "$p" 2>/dev/null || true
    done
    if [[ -n "$SERVER_PID" && "$SERVER_PID" != 0 ]]; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        sleep 2
        kill -KILL "$SERVER_PID" 2>/dev/null || true
    fi
    say "stopped. Re-run the same command with --resume to continue (emu/local/ssh)."
    exit 130
}

# ---------------------------------------------------------------------------
# reference stats of the topic (for the output check)
# ---------------------------------------------------------------------------

declare -A TOPIC_FILES=() TOPIC_BYTES=()
load_topic_stats() {
    local t f b
    for t in $(plan_topics); do
        if [[ -d "$DATASET/$t" ]]; then
            read -r f b < <(dir_stats "$DATASET/$t")
            TOPIC_FILES[$t]=$f
            TOPIC_BYTES[$t]=$b
        fi
    done
}

# ---------------------------------------------------------------------------
# controller modes: emu, local, ssh
# ---------------------------------------------------------------------------

ssh_target() { if [[ "$1" == A ]]; then echo "$CLIENT_A"; else echo "$CLIENT_B"; fi; }

declare -A CLOCK_OFF_MS=([A]=0 [B]=0)

# Clock offset of a client PC: remote time - midpoint of the local
# before/after times (ms). Used to correct the shared start time.
measure_offsets() {
    local label t r0 r t1 off rtt when=$1
    for label in A B; do
        t="$(ssh_target "$label")"
        if ((DRY_RUN)); then
            show_cmd ssh "${SSH_OPTS_ARR[@]}" "$t" date +%s.%N
            continue
        fi
        r0=$(date +%s%N)
        r=$(ssh -n "${SSH_OPTS_ARR[@]}" "$t" date +%s%N) || die "ssh $t failed"
        t1=$(date +%s%N)
        off=$(((r - (r0 + t1) / 2) / 1000000))
        rtt=$(((t1 - r0) / 1000000))
        CLOCK_OFF_MS[$label]=$off
        [[ -s "$RAW/clock_offsets.csv" ]] || echo "when,label,target,offset_ms,ssh_rtt_ms" >"$RAW/clock_offsets.csv"
        echo "$when,$label,$t,$off,$rtt" >>"$RAW/clock_offsets.csv"
        say "clock offset of $label ($t): ${off} ms (ssh round trip ${rtt} ms)"
        if ((off > 500 || off < -500)); then
            warn "clock of $label is off by ${off} ms; start times are corrected, but enable NTP (timedatectl set-ntp true)"
        fi
    done
}

ssh_setup() {
    local label t part
    for label in A B; do
        t="$(ssh_target "$label")"
        if ((DRY_RUN)); then
            show_cmd ssh "${SSH_OPTS_ARR[@]}" "$t" mkdir -p "$REMOTE_RAW/$SWEEP/clients"
            continue
        fi
        if ! ssh -n "${SSH_OPTS_ARR[@]}" "$t" true 2>/dev/null; then
            die "cannot ssh to $t without a password. On this PC run:  ssh-keygen -t ed25519 (once), then  ssh-copy-id $t"
        fi
        ssh -n "${SSH_OPTS_ARR[@]}" "$t" mkdir -p "$REMOTE_RAW/$SWEEP/clients"
        for part in $(plan_parts); do
            ssh -n "${SSH_OPTS_ARR[@]}" "$t" test -x "$REMOTE_BIN_ROOT/$part/client" ||
                die "$t: $REMOTE_BIN_ROOT/$part/client missing (build it there: make -C $REMOTE_BIN_ROOT/$part)"
        done
        ssh -n "${SSH_OPTS_ARR[@]}" "$t" test -r "$REMOTE_REPO/tools/exp/common.sh" ||
            die "$t: $REMOTE_REPO/tools/exp/common.sh missing (set REMOTE_REPO in lab.conf)"
        local h
        h="$(ssh -n "${SSH_OPTS_ARR[@]}" "$t" hostname -s)"
        ssh -n "${SSH_OPTS_ARR[@]}" "$t" bash "$REMOTE_REPO/tools/exp/collect_sysinfo.sh" --stdout --label "$label" \
            >"$OUT/sysinfo_${label}_${h}.txt" 2>/dev/null || warn "sysinfo of $t failed"
    done
    measure_offsets start
}

# Copy the client CSVs of one run (or the whole sweep) from A and B.
ssh_collect() {
    local what="$1" label t src
    for label in A B; do
        t="$(ssh_target "$label")"
        if [[ "$what" == all ]]; then
            src="$REMOTE_RAW/$SWEEP/clients/*.$label.csv"
        else
            src="$REMOTE_RAW/$SWEEP/clients/$what.$label.csv"
        fi
        if ((DRY_RUN)); then
            show_cmd scp -q "${SSH_OPTS_ARR[@]}" "$t:$src" "$RAW/clients/"
            continue
        fi
        scp -q "${SSH_OPTS_ARR[@]}" "$t:$src" "$RAW/clients/" 2>/dev/null || true
    done
}

# Build the launch-clients command for host label $1 into the array LCMD.
build_launcher() {
    local label=$1 rid=$2 part=$3 n=$4 topic=$5 cargs=$6 envs=$7 tmo=$8 start=$9
    local common="$EXP_DIR/common.sh" bin="$BIN_ROOT/$part/client" csv="$RAW/clients/$rid.$label.csv" e
    local -a cargv=() envv=()
    [[ "$cargs" == - ]] || read -ra cargv <<<"$cargs"
    [[ "$envs" == - ]] || read -ra envv <<<"$envs"
    if [[ "$MODE" == ssh ]]; then
        common="$REMOTE_REPO/tools/exp/common.sh"
        bin="$REMOTE_BIN_ROOT/$part/client"
        csv="$REMOTE_RAW/$SWEEP/clients/$rid.$label.csv"
    fi
    LCMD=(bash "$common" launch-clients --bin "$bin" --server-ip "$SERVER_IP" --port "$PORT"
        --topic "$topic" --count "$(n_for_label "$n" "$label")" --label "$label" --run-id "$rid"
        --csv "$csv" --tmp "$CLIENT_TMP" --start-at "$start" --timeout "$tmo")
    if [[ -n "${TOPIC_FILES[$topic]:-}" ]]; then
        LCMD+=(--expect-files "${TOPIC_FILES[$topic]}" --expect-bytes "${TOPIC_BYTES[$topic]}")
    elif [[ "$MODE" == manual ]]; then
        LCMD+=(--dataset "$DATASET")
    fi
    ((KEEP_OUTPUT)) && LCMD+=(--keep-output)
    for e in "${envv[@]}"; do
        LCMD+=(--env "$e")
    done
    LCMD+=(-- "${cargv[@]}")
    case "$MODE" in
        emu) LCMD=("$EMU" exec "$label" "${LCMD[@]}") ;;
        ssh)
            local rcmd
            printf -v rcmd '%q ' "${LCMD[@]}"
            LCMD=(ssh -n "${SSH_OPTS_ARR[@]}" "$(ssh_target "$label")" "$rcmd")
            ;;
    esac
}

RUN_TIMES_SUM=0
RUN_TIMES_N=0

ctl_run_one() {
    local line=$1 idx=$2 total=$3 rid part n topic sargs cargs envs tmo slot label cnt
    IFS='|' read -r rid part n topic sargs cargs envs tmo slot <<<"$line"
    if ((RESUME)) && run_is_done "$rid"; then
        say "($idx/$total) $rid: already done, skipped (--resume)"
        return 0
    fi
    R_NOTE=""
    SERVER_RC=""
    local sdesc="$sargs"
    [[ "$sdesc" == - ]] && sdesc=""
    say "($idx/$total) $rid: $part server [$sdesc], $n client(s) (A=$(n_for_label "$n" A) B=$(n_for_label "$n" B)), time-out ${tmo}s"
    if ((DRY_RUN)); then
        echo "+ rm -f $RAW/{server,events}/$rid.csv $RAW/clients/$rid.*.csv"
    else
        rm -f "$RAW/server/$rid.csv" "$RAW/events/$rid.csv" "$RAW/clients/$rid".*.csv "$RAW/logs/$rid".*
    fi
    local t0
    t0=$(now_ms)
    if ! start_server "$rid" "$part" "$sargs" "$envs"; then
        say "($idx/$total) $rid: FAILED to start the server: $R_NOTE"
        stop_server
        runs_append "$rid,$SWEEP,$MODE,ctl,$n,$t0,0,${SERVER_RC:-},0,0,0,$n,,,${R_NOTE//,/ }"
        return 0
    fi

    # Shared start time (each launcher sleeps until then). ssh: corrected by
    # the measured clock offset of each PC, so A and B start together.
    local start=$(($(now_ms) + LEAD_MS))
    [[ "$MODE" == ssh ]] && start=$(($(now_ms) + SSH_LEAD_MS))
    LAUNCHER_PIDS=()
    local -a outs=()
    for label in A B; do
        cnt="$(n_for_label "$n" "$label")"
        ((cnt > 0)) || continue
        build_launcher "$label" "$rid" "$part" "$n" "$topic" "$cargs" "$envs" "$tmo" \
            $((start + ${CLOCK_OFF_MS[$label]}))
        outs+=("$RAW/logs/$rid.$label.out")
        if ((DRY_RUN)); then
            if [[ "$MODE" == ssh ]]; then show_ssh_cmd "${LCMD[@]}"; else show_cmd "${LCMD[@]}"; fi
            echo "    (background, output -> $RAW/logs/$rid.$label.out)"
        else
            "${LCMD[@]}" >"$RAW/logs/$rid.$label.out" 2>&1 </dev/null &
            LAUNCHER_PIDS+=("$!")
        fi
    done
    if ((DRY_RUN)); then
        echo "+ (wait for the launchers: each client is killed after ${tmo}s)"
    else
        # Watchdog: the launchers enforce the time-out themselves; this only
        # catches a hung ssh.
        local deadline=$((start + (tmo + 60) * 1000)) p alive
        while :; do
            alive=0
            for p in "${LAUNCHER_PIDS[@]}"; do
                kill -0 "$p" 2>/dev/null && alive=1
            done
            ((alive)) || break
            if (($(now_ms) > deadline)); then
                R_NOTE+="launcher watchdog fired; "
                kill -TERM "${LAUNCHER_PIDS[@]}" 2>/dev/null || true
                break
            fi
            sleep 0.2
        done
        for p in "${LAUNCHER_PIDS[@]}"; do
            wait "$p" 2>/dev/null || true
        done
        LAUNCHER_PIDS=()
    fi
    stop_server
    [[ "$MODE" == ssh && "$COLLECT" == run ]] && ssh_collect "$rid"
    ((DRY_RUN)) && return 0

    local note_before="$R_NOTE"
    parse_launcher_outputs "$n" "${outs[@]}"
    R_NOTE="${note_before//,/ }$R_NOTE"
    local el=$(($(now_ms) - t0)) rows=0
    [[ -s "$RAW/server/$rid.csv" ]] && rows=$(($(wc -l <"$RAW/server/$rid.csv") - 1))
    ((rows > 0)) || R_NOTE+="no server CSV rows; "
    runs_append "$rid,$SWEEP,$MODE,ctl,$n,$t0,$(ms_to_s "$el"),$SERVER_RC,$R_OK,$R_DNF,$R_TO,$R_FAIL,$R_RCS,$R_CHECK,$R_NOTE"
    RUN_TIMES_SUM=$((RUN_TIMES_SUM + el))
    RUN_TIMES_N=$((RUN_TIMES_N + 1))
    local eta=$(((total - idx) * (RUN_TIMES_SUM / RUN_TIMES_N + GAP * 1000) / 1000))
    local verdict=OK
    ((R_OK == n)) || verdict=INCOMPLETE
    say "($idx/$total) $rid: $verdict ok=$R_OK dnf=$R_DNF timeout=$R_TO fail=$R_FAIL, $(ms_to_s "$el") s, server rc=$SERVER_RC rows=$rows, check [$R_CHECK]${R_NOTE:+, note: $R_NOTE} (ETA $(hms "$eta"))"
}

exp_preflight_server() {
    local part t
    [[ -d "$DATASET" ]] || die "dataset $DATASET not found (give --dataset <topics_root_dir>)"
    for t in $(plan_topics); do
        [[ -d "$DATASET/$t" ]] || die "topic folder $DATASET/$t not found"
    done
    for part in $(plan_parts); do
        [[ -x "$BIN_ROOT/$part/server" ]] || die "$BIN_ROOT/$part/server missing (run: make -C $BIN_ROOT/$part)"
    done
}

exp_preflight_clients_local() {
    local part
    for part in $(plan_parts); do
        [[ -x "$BIN_ROOT/$part/client" ]] || die "$BIN_ROOT/$part/client missing (run: make -C $BIN_ROOT/$part)"
    done
}

exp_confirm() {
    ((DRY_RUN || YES)) && return 0
    if [[ ! -t 0 ]]; then
        die "not a terminal: add --yes to start without confirmation"
    fi
    local ans
    read -r -p "Start? [y/N] " ans
    [[ "$ans" == y || "$ans" == Y || "$ans" == yes ]] || die "aborted"
}

exp_print_header() {
    local q=""
    ((QUICK)) && q=", QUICK sweep"
    echo "== FDS experiment: sweep $SWEEP, mode $MODE${ROLE:+, role $ROLE}$q"
    echo "   runs: ${#PLAN[@]}  (warm-up $WARMUP + reps $REPS per configuration)"
    echo "   server: ${SERVER_IP:-this PC}:$PORT   dataset: $DATASET   topic(s): $(plan_topics | tr '\n' ' ')"
    echo "   binaries: $BIN_ROOT   output: $RAW   config: $CONF_USED"
    [[ "$MODE" == ssh ]] && echo "   client PCs: A=$CLIENT_A  B=$CLIENT_B  (repo there: $REMOTE_REPO)"
    ((DRY_RUN)) && echo "   DRY RUN: commands are printed, nothing is executed"
    return 0
}

exp_controller() {
    RUNS_FILE=runs.csv
    EXP_LOG="$RAW/run_ctl.log"
    exp_print_header
    if ((!DRY_RUN)); then
        if [[ "$MODE" == emu ]]; then
            "$EMU" status >/dev/null 2>&1 || die "the emulation is not up: run  tools/emu/emu.sh up"
        fi
        exp_preflight_server
        [[ "$MODE" == ssh ]] || exp_preflight_clients_local
    fi
    load_topic_stats
    exp_confirm
    if ((!DRY_RUN)); then
        mkdir -p "$RAW/server" "$RAW/events" "$RAW/clients" "$RAW/logs"
        local lbl=S
        [[ "$MODE" == emu ]] && lbl=emu
        [[ "$MODE" == local ]] && lbl=local
        bash "$EXP_DIR/collect_sysinfo.sh" --out "$OUT" --label "$lbl" >/dev/null 2>&1 || warn "collect_sysinfo failed"
    fi
    [[ "$MODE" == ssh ]] && ssh_setup
    trap exp_on_interrupt INT TERM
    say "sweep $SWEEP started: ${#PLAN[@]} runs, mode $MODE"
    local i=0 line
    for line in "${PLAN[@]}"; do
        i=$((i + 1))
        ctl_run_one "$line" "$i" "${#PLAN[@]}"
        if ((DRY_RUN)); then echo; else sleep "$GAP"; fi
    done
    if [[ "$MODE" == ssh ]]; then
        [[ "$COLLECT" == end ]] && ssh_collect all
        measure_offsets end
    fi
    trap - INT TERM
    say "sweep $SWEEP finished. Raw data in $RAW"
    echo "next:  python3 tools/exp/analyze.py --raw $OUT --out $(dirname "$OUT") --dataset $DATASET"
    echo "       python3 tools/exp/plot.py --results $(dirname "$OUT")"
}

# ---------------------------------------------------------------------------
# manual mode: identical timetable on every PC
# ---------------------------------------------------------------------------

manual_server_slot() {
    local rid=$1 part=$2 n=$3 sargs=$4 envs=$5 t0=$6 slot=$7 idx=$8 total=$9
    local t_stop=$((t0 + (slot - STOP_MARGIN) * 1000))
    if ((DRY_RUN)); then
        echo "--- slot $idx/$total $rid: $(date -d "@$((t0 / 1000))" +%T) - $(date -d "@$((t0 / 1000 + slot))" +%T)"
        echo "+ (sleep until $(date -d "@$((t0 / 1000))" +%T))"
        start_server "$rid" "$part" "$sargs" "$envs"
        echo "+ (sleep until $(date -d "@$((t_stop / 1000))" +%T))"
        stop_server
        return 0
    fi
    if (($(now_ms) > t_stop - 2000)); then
        say "($idx/$total) $rid: SKIPPED (slot already over)"
        runs_append "$rid,$SWEEP,manual,server,$n,$t0,0,,,,,,,,slot missed"
        return 0
    fi
    sleep_until_ms "$t0"
    R_NOTE=""
    rm -f "$RAW/server/$rid.csv" "$RAW/events/$rid.csv"
    if ! start_server "$rid" "$part" "$sargs" "$envs"; then
        say "($idx/$total) $rid: FAILED to start the server: $R_NOTE"
    else
        say "($idx/$total) $rid: server up, stopping at $(date -d "@$((t_stop / 1000))" +%T)"
    fi
    sleep_until_ms "$t_stop"
    stop_server
    local rows=0
    [[ -s "$RAW/server/$rid.csv" ]] && rows=$(($(wc -l <"$RAW/server/$rid.csv") - 1))
    runs_append "$rid,$SWEEP,manual,server,$n,$t0,$slot,$SERVER_RC,,,,,,,rows=$rows ${R_NOTE//,/ }"
    say "($idx/$total) $rid: server stopped rc=$SERVER_RC, $rows connection rows"
}

manual_client_slot() {
    local rid=$1 part=$2 n=$3 topic=$4 cargs=$5 envs=$6 tmo=$7 t0=$8 slot=$9 idx=${10} total=${11}
    local cnt t_launch ctmo
    cnt="$(n_for_label "$n" "$ROLE")"
    t_launch=$((t0 + CLIENT_DELAY * 1000))
    # Clients must be finished before the server stops at the end of the slot.
    ctmo=$((slot - CLIENT_DELAY - STOP_MARGIN - 2))
    ((ctmo > tmo)) && ctmo=$tmo
    ((ctmo < 5)) && ctmo=5
    if ((cnt == 0)); then
        ((DRY_RUN)) && echo "--- slot $idx/$total $rid: no clients on $ROLE"
        return 0
    fi
    build_launcher "$ROLE" "$rid" "$part" "$n" "$topic" "$cargs" "$envs" "$ctmo" "$t_launch"
    if ((DRY_RUN)); then
        echo "--- slot $idx/$total $rid: $cnt client(s) at $(date -d "@$((t_launch / 1000))" +%T), time-out ${ctmo}s"
        show_cmd "${LCMD[@]}"
        return 0
    fi
    if (($(now_ms) > t_launch + 1000)); then
        say "($idx/$total) $rid: SKIPPED (too late for this slot)"
        runs_append "$rid,$SWEEP,manual,$ROLE,$cnt,$t0,0,,0,0,0,$cnt,,,slot missed"
        return 0
    fi
    local out="$RAW/logs/$rid.$ROLE.out"
    "${LCMD[@]}" >"$out" 2>&1 </dev/null || true
    parse_launcher_outputs "$cnt" "$out"
    runs_append "$rid,$SWEEP,manual,$ROLE,$cnt,$t0,$slot,,$R_OK,$R_DNF,$R_TO,$R_FAIL,$R_RCS,$R_CHECK,$R_NOTE"
    say "($idx/$total) $rid: ok=$R_OK dnf=$R_DNF timeout=$R_TO fail=$R_FAIL check [$R_CHECK]${R_NOTE:+ note: $R_NOTE}"
}

exp_manual() {
    local pf="${OPT[plan]:-}"
    if [[ -n "$pf" && -s "$pf" ]]; then
        plan_load "$pf"
        echo "plan loaded from $pf"
    elif [[ -n "$pf" ]]; then
        # Written even with --dry-run: making the plan file is the point of --plan.
        plan_text >"$pf"
        echo "plan saved to $pf (copy it to the other PCs and pass the same --plan there)"
    fi
    if [[ "$ROLE" == server ]]; then RUNS_FILE=runs_server.csv; else RUNS_FILE="runs_$ROLE.csv"; fi
    EXP_LOG="$RAW/run_$ROLE.log"
    exp_print_header

    # The timetable: run i owns [START + off_i, START + off_i + slot_i).
    local -a offs=()
    local line total_s=0 slot
    for line in "${PLAN[@]}"; do
        offs+=("$total_s")
        slot="${line##*|}"
        total_s=$((total_s + slot))
    done
    echo "   plan hash: $(plan_hash)   <- must be IDENTICAL on all three PCs"
    echo "   start: $(date -d "@$((START_MS / 1000))" '+%F %T') (--start-at $((START_MS / 1000)))"
    echo "   end:   $(date -d "@$((START_MS / 1000 + total_s))" '+%F %T') (total $(hms "$total_s"))"
    echo "   clocks must be NTP-synchronised: check 'timedatectl' shows 'System clock synchronized: yes'"
    if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo unknown)" != yes ]]; then
        warn "this PC's clock is not reported as NTP-synchronised (timedatectl); runs may not line up"
    fi
    if ((!DRY_RUN)); then
        if [[ "$ROLE" == server ]]; then exp_preflight_server; else exp_preflight_clients_local; fi
        (($(now_ms) < START_MS)) || warn "--start-at is in the past: slots that are already over are skipped"
    fi
    [[ "$ROLE" == server ]] && load_topic_stats
    exp_confirm
    if ((!DRY_RUN)); then
        mkdir -p "$RAW/logs" "$RAW/clients"
        [[ "$ROLE" == server ]] && mkdir -p "$RAW/server" "$RAW/events"
        bash "$EXP_DIR/collect_sysinfo.sh" --out "$OUT" --label "$ROLE" >/dev/null 2>&1 || warn "collect_sysinfo failed"
        trap exp_on_interrupt INT TERM
        say "manual sweep $SWEEP as $ROLE: ${#PLAN[@]} runs, plan $(plan_hash)"
    fi
    local i rid part n topic sargs cargs envs tmo t0
    for i in "${!PLAN[@]}"; do
        IFS='|' read -r rid part n topic sargs cargs envs tmo slot <<<"${PLAN[$i]}"
        t0=$((START_MS + offs[i] * 1000))
        if [[ "$ROLE" == server ]]; then
            manual_server_slot "$rid" "$part" "$n" "$sargs" "$envs" "$t0" "$slot" $((i + 1)) "${#PLAN[@]}"
        else
            manual_client_slot "$rid" "$part" "$n" "$topic" "$cargs" "$envs" "$tmo" "$t0" "$slot" $((i + 1)) "${#PLAN[@]}"
        fi
    done
    trap - INT TERM
    ((DRY_RUN)) || say "manual sweep $SWEEP finished on $ROLE"
    if [[ "$ROLE" != server ]]; then
        local dest_user_host="${SERVER_SSH:-$USER@$SERVER_IP}" dest_out
        dest_out="${SERVER_REPO:-$REPO}/${OUT#"$REPO"/}"
        [[ "$OUT" == "$REPO"/* ]] || dest_out="$OUT"
        echo
        echo "Now copy this PC's client logs to the server PC (run on THIS PC, $ROLE):"
        echo "  scp $RAW/clients/*.$ROLE.csv $dest_user_host:$dest_out/$SWEEP/clients/"
        echo "  scp $RAW/$RUNS_FILE $RAW/run_$ROLE.log $dest_user_host:$dest_out/$SWEEP/"
        echo "  scp $OUT/sysinfo_${ROLE}_*.txt $dest_user_host:$dest_out/"
        echo "or with rsync (same result):"
        echo "  rsync -av --include='clients/' --include='clients/*.$ROLE.csv' --include='$RUNS_FILE' --include='run_$ROLE.log' --exclude='*' $RAW/ $dest_user_host:$dest_out/$SWEEP/"
        echo "(no network? copy those files with a USB stick into the same folders on the server PC)"
    else
        echo
        echo "When A and B have copied their files here, run:"
        echo "  python3 tools/exp/analyze.py --raw $OUT --out $(dirname "$OUT") --dataset $DATASET"
    fi
}

exp_main() {
    ((${#PLAN[@]})) || die "the plan is empty (check the sweep options)"
    if [[ "$MODE" == manual ]]; then exp_manual; else exp_controller; fi
}

# ---------------------------------------------------------------------------
# executed directly (on a client PC): bash common.sh launch-clients ...
# ---------------------------------------------------------------------------
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -euo pipefail
    case "${1:-}" in
        launch-clients)
            shift
            cmd_launch_clients "$@"
            ;;
        *)
            echo "usage: bash $0 launch-clients [options] -- [client args]   (used by the runners)" >&2
            exit 1
            ;;
    esac
fi
