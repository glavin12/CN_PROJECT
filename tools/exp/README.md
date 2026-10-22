# tools/exp and tools/emu: running the experiments

These scripts run the performance experiments of design §6.4–6.7 (with
deviation 15). They collect the raw CSV logs into `Results/raw/`, turn them into
summary tables (`analyze.py`) and draw the graphs (`plot.py`). `tools/emu/emu.sh`
emulates the 3-PC lab on one Linux PC, so the whole pipeline can be tested
before going to the lab.

Every command below is run from the **repository root**. Every script prints
its options with `--help`.

| File | What it does |
|---|---|
| `tools/emu/emu.sh` | 3 emulated hosts S, A and B on one PC (network namespaces, no sudo) |
| `tools/exp/run_part1.sh` | Part I sweep: single vs multi server, N = 1…32 clients |
| `tools/exp/run_part2.sh` | Part II sweep: Case 1 / Case 2 / Enhanced vs fault probability p |
| `tools/exp/run_buffers.sh` | socket-buffer experiment (FDS_SNDBUF / FDS_RCVBUF) |
| `tools/exp/common.sh` | shared code; also runs the clients on A and B (`launch-clients`) |
| `tools/exp/collect_sysinfo.sh` | CPU, NIC speed, gcc, kernel and TCP sysctls → `Results/raw/sysinfo_*.txt` |
| `tools/exp/analyze.py` | raw CSV → `Results/Part1_summary.csv`, `Part2_summary.csv`, `Buffers_summary.csv`, `all_runs.csv`, `summary.md` |
| `tools/exp/plot.py` | summary CSV → `Results/*.png` + `*.pdf` |
| `tools/exp/fake_csv.py` | synthetic raw CSV, used to test analyze/plot (never put its output in `Results/`) |
| `tools/exp/lab.conf.example` | template for `tools/exp/lab.conf` (IPs, ssh targets, dataset path) |

Requirements: bash, coreutils (`timeout`), iproute2, python3. `plot.py` also
needs matplotlib (`sudo dnf install python3-matplotlib` or
`sudo apt install python3-matplotlib`, or `pip install matplotlib`). Build
first: `make -C Part1 && make -C Part2_Case1 && make -C Part2_Case2 && make -C Part2_Case2_Enhanced`
(on **every** PC that runs a server or clients).

---------------------------------------------------------------------------

## 1. What is measured

### Sweeps (defaults)

| Script | Configurations | Topic | Clients | Runs per configuration |
|---|---|---|---|---|
| `run_part1.sh` | server ∈ {single, multi} × N ∈ {1, 2, 4, 8, 16, 32} | `elefante` | N in total: A gets ⌈N/2⌉, B gets ⌊N/2⌋ (N = 1 → A only) | 1 warm-up + 5 reps |
| `run_part2.sh` | p ∈ {0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9} (per connection, DEVIATIONS 28) × {Case 1 (max_retries 100), Case 2 (20), Enhanced K ∈ {1, 2, 4, 8} io=threads, Enhanced K = 4 io=poll} | `gatto` | 4 (2 on A, 2 on B) | 1 warm-up + 5 reps |
| `run_part2.sh --part1-compare` | adds the Part I **multi** server with the same topic and 4 clients (reference for p = 0) | `gatto` | 4 | 1 + 5 |
| `run_buffers.sh` | {Case 2, Enhanced K = 4} × buffer ∈ {default, 65536, 262144, 1048576, 4194304} at p = 0 | `gatto` | 4 | 1 + 3 |

Totals: Part I 72 runs, Part II 294 runs (300 with `--part1-compare`), buffers
40 runs. `--quick` runs a small version of each sweep to test the pipeline.

### Fixed seeds

Repetition r runs the Part II server with seed `1000 + r`; warm-ups use 1000.
The seed is the same for Case 1, Case 2 and Enhanced (design §6.4).

### Run ids

`analyze.py` reads the configuration from the run id:

| Run id | Meaning |
|---|---|
| `part1_<single\|multi>_N<n>_r<r>` | Part I |
| `case1_p<p>_r<r>`, `case2_p<p>_r<r>` | Part II Case 1 / Case 2 |
| `enh_K<k>_<threads\|poll>_p<p>_r<r>` | Part II Case 2 Enhanced |
| `p1cmp_multi_N4_r<r>` | `--part1-compare` runs |
| `buf_<case2\|enh_K4_threads>_b<bytes\|default>_r<r>` | buffer experiment |
| `..._w1_warmup` | warm-up run (ignored by `analyze.py`) |

### One run, step by step

1. The runner deletes old files of that run id, then starts the server with
   `FDS_LOG=Results/raw/<sweep>/server/<run_id>.csv`,
   `FDS_EVENTS=Results/raw/<sweep>/events/<run_id>.csv`, `FDS_RUN_ID=<run_id>`
   and `FDS_QUIET=1`. It waits until the port is listening. The check reads
   `/proc/net/tcp` and never connects, because a test connection would show
   up in the server CSV.
2. The clients start on A and B at **the same moment** (a shared start time,
   1 s ahead in emu/local and 2 s ahead in ssh). Every client gets:
   - its own output dir `/tmp/fds_out/<run_id>/<A1|A2|B1…>`,
   - `FDS_CLIENT_ID=A1`…,
   - the per-PC CSV `clients/<run_id>.<A|B>.csv`,
   - a time-out (`timeout -k 5 <s>`; exit 124 = DNF).
3. The runner waits for all clients, then stops the server with SIGINT (the
   server flushes its CSV and exits 0; SIGKILL after 15 s).
4. It checks one successful client's output: file count and total bytes
   against the topic folder, plus no `.part`/`.session`/hidden leftovers. It
   then deletes the outputs (`--keep-output` keeps them).
5. It appends one row to `Results/raw/<sweep>/runs.csv`: client exit codes
   (0 OK, 2 DNF = gave up, 124 = killed by the time-out, 1 = FAIL), the
   output check and notes. It prints a progress line with an ETA. A failed
   run is recorded and the sweep continues.

Default time-outs: Part I `60 + 10·N` s; Case 1 600 s (`--case1-cap`), which
counts as DNF; Case 2 and Enhanced 300 s; `--timeout <s>` overrides them all.

### Files

```
Results/raw/sysinfo_<label>_<host>.txt          one per PC (lscpu, ethtool, gcc, sysctls, NTP)
Results/raw/<sweep>/server/<run_id>.csv          server: one row per connection
Results/raw/<sweep>/events/<run_id>.csv          server: FAULT / RESUME_OK / ... events
Results/raw/<sweep>/clients/<run_id>.A.csv       clients that ran on A (and .B.csv)
Results/raw/<sweep>/runs.csv                     runner: exit codes, check, notes
                                                 (manual mode: runs_server.csv, runs_A.csv, runs_B.csv)
Results/raw/<sweep>/run_<ctl|server|A|B>.log     progress log
Results/raw/<sweep>/logs/                        server stderr, launcher output per run
Results/raw/<sweep>/clock_offsets.csv            ssh mode: clock offset of A and B
```

---------------------------------------------------------------------------

## 2. Test everything on one PC (emulation)

```
tools/emu/emu.sh up            # builds S=10.10.0.1, A=10.10.0.2, B=10.10.0.3 (idempotent)
tools/emu/emu.sh ping-test     # ping between hosts + TCP throughput (expect ~950 Mbit/s)
tools/exp/run_part1.sh --mode emu --quick --yes
tools/exp/run_part2.sh --mode emu --quick --part1-compare --yes
tools/exp/run_buffers.sh --mode emu --quick --yes
python3 tools/exp/analyze.py --raw Results/raw --out Results --dataset dataset/raw-img
python3 tools/exp/plot.py --results Results
tools/emu/emu.sh down          # stops everything inside, removes the namespaces
```

Emulation results are **not** lab results. Use `--out /tmp/emu_raw` (and
`analyze.py --raw /tmp/emu_raw --out /tmp/emu_results`) so that they do not
end up in `Results/`.

How the emulation works (`tools/emu/emu.sh`, no sudo needed):
- **Holder process.** `unshare --user --map-root-user --net sleep infinity`
  owns a user namespace and a "switch" network namespace with bridge `br0`.
- **Hosts.** Each host S, A, B is a sleeping process in its own net + UTS
  namespace, created inside that user namespace. The hostnames are `fds-S`,
  `fds-A` and `fds-B`.
- **Wiring.** A veth pair per host: one end is a bridge port, the other is the
  host's `eth0`.
- **Shaping.** On every host's egress: `netem delay EMU_DELAY` → `tbf rate
  EMU_RATE` → `fq_codel` (defaults 0.1 ms per direction, 1 Gbit/s). fq_codel
  keeps the RTT under load below 1 ms. Plain netem rate limiting gave ~30 ms,
  which would unfairly slow Enhanced's per-file FETCH round trips.
- **Commands on a host.** `tools/emu/emu.sh exec S|A|B <cmd>` runs the command
  inside that host's namespaces. The file system is shared: the repo, the
  binaries, `/tmp` and `Results/` are the same as yours.
- **Different link.** `tools/emu/emu.sh shape 100mbit 0.1ms` (or `EMU_RATE=100mbit tools/emu/emu.sh up`).
- **State.** PIDs are kept in `${XDG_RUNTIME_DIR:-/tmp}/fds-emu/`.
- **If `up` fails.** Unprivileged user namespaces are disabled (e.g. Ubuntu
  ≥ 23.10, AppArmor). The script prints the sysctl that enables them, or use
  `sudo tools/emu/emu.sh up` (then run the runners with sudo as well), or use
  `--mode local`.

`--mode local` runs the server and all clients on 127.0.0.1. Use it only for
smoke tests: there is no real network, and every process shares one CPU.

---------------------------------------------------------------------------

## 3. The real lab: three PCs

Choose one PC as the **server S** and two PCs as **A** and **B**. On all three:

1. Clone/copy the repo at the same path if possible (otherwise set
   `REMOTE_REPO` in lab.conf), and build all four Part folders with `make`.
2. Synchronise the clocks: `timedatectl` must show `System clock synchronized:
   yes` (`sudo timedatectl set-ntp true`). This matters in manual mode.
3. Find the IPs with `ip -br addr`. Check that A and B can reach S:
   `ping <S-ip>`.

On **S** only:

4. Put the dataset on S's local disk, e.g. `dataset/raw-img/` with one folder
   per topic (`cane`, `elefante`, `gatto`, …).
5. Open the server port (default 5050):
   - Fedora: `sudo firewall-cmd --add-port=5050/tcp`
   - Ubuntu: `sudo ufw allow 5050/tcp`
6. `cp tools/exp/lab.conf.example tools/exp/lab.conf` and edit `SERVER_IP`,
   `CLIENT_A`, `CLIENT_B`, `DATASET` and `PORT`.

For the buffer experiment, raise the kernel caps on **all three** PCs first
(they reset at reboot):
`sudo sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608`.
Without this, Linux silently caps 1 MB and 4 MB buffers at ~208 KB.

### 3a. ssh mode (recommended)

S is the controller. It starts the clients on A and B over passwordless ssh.

One-time setup on S:
```
ssh-keygen -t ed25519            # press Enter for every question
ssh-copy-id student@<A-ip>
ssh-copy-id student@<B-ip>
ssh student@<A-ip> true          # must not ask for a password
```
Then on S:
```
tools/exp/run_part1.sh --mode ssh --dry-run      # look at the commands first
tools/exp/run_part1.sh --mode ssh --yes
tools/exp/run_part2.sh --mode ssh --part1-compare --yes
tools/exp/run_buffers.sh --mode ssh --yes
```
What happens:
- Clients write their CSVs to `/tmp/fds_raw/<sweep>/clients/` on A and B
  (`REMOTE_RAW`). S copies them back with scp after every run (`--collect end`:
  once at the end).
- Every run's start time is corrected by the measured clock offset of each PC,
  so A and B start together even without NTP. The offsets are recorded in
  `clock_offsets.csv` and the run log.
- A's and B's `sysinfo_*.txt` are collected automatically.

Long sweeps:
- **Interrupted?** Ctrl-C stops the current run cleanly. Run the same command
  again with `--resume` to skip the runs that are already done.
- **Shorter sessions.** `--cases case2,enh`, `--cases case1` or `--ps 0,0.1,0.2`
  split Part II. All runs land in the same folder.

### 3b. manual mode (no ssh)

Each PC runs the same script with its own `--role` and the **same**
`--start-at`. All three follow an identical timetable: run i owns a fixed time
slot. The server starts at the slot start and stops 3 s before the slot end.
The clients start 2 s after the slot start, and their time-out ends before the
server stops.

1. On S, look at the timetable and pick a start time at least 2 minutes ahead:
   ```
   tools/exp/run_part2.sh --mode manual --role server --dry-run --part1-compare
   date -d '14:30' +%s          # e.g. 1792900200 -> use this number on all PCs
   ```
2. Start all three. The order does not matter, as long as each starts before
   `--start-at`:
   ```
   # on S
   tools/exp/run_part2.sh --mode manual --role server --start-at 1792900200 --part1-compare --yes
   # on A
   tools/exp/run_part2.sh --mode manual --role A --server-ip <S-ip> --start-at 1792900200 --part1-compare --yes
   # on B
   tools/exp/run_part2.sh --mode manual --role B --server-ip <S-ip> --start-at 1792900200 --part1-compare --yes
   ```
   Every PC prints `plan hash: xxxxxxxxxx`, which **must be identical** on all
   three. If it differs, a flag differs: stop and fix it. Alternatively, write
   the plan once with `--plan plan.txt` on S, copy `plan.txt` to A and B, and
   pass `--plan plan.txt` there too.
3. When it finishes, A and B print the exact `scp` (or `rsync`) commands that
   copy their `clients/*.A.csv` / `*.B.csv`, `runs_A.csv` and `sysinfo_A_*.txt`
   to S. Run them on A and on B (or use a USB stick: same folders). File names
   never collide.

Default slot lengths:

| Sweep | Slot |
|---|---|
| Part I | 20 + 4·N s |
| Case 1 | 60 s (p ≤ 0.5), 120 s (larger p) |
| Case 2 | 60 s |
| Enhanced and `--part1-compare` | 45 s |
| Buffers | 60 s |

That gives Part I ≈ 1 h 15 min, Part II ≈ 4 h 20 min and buffers ≈ 40 min (ssh and emu modes do not wait for slots and are faster). Use
`--slot-scale 0.5` on a fast network, `--slot-scale 2` on a slow one, or
`--slot <s>` for one fixed length. A client still running at the end of its
slot is killed and counts as DNF. Slots that are already over when a PC starts
are skipped. Prefer ssh mode whenever you can.

---------------------------------------------------------------------------

## 4. Tables and graphs

```
python3 tools/exp/analyze.py --raw Results/raw --out Results --dataset dataset/raw-img
python3 tools/exp/plot.py --results Results
```

`analyze.py` (standard library only) groups the rows by run id, ignores
warm-ups and computes the following per run (design §6.7; MB = 10⁶ bytes):

| Metric | Definition |
|---|---|
| throughput | Σ server `data_bytes` ÷ (max `end_ns` − min `start_ns`) over the run's server rows (one clock, the server's) |
| total bytes | Σ server `data_bytes` (also `wire_bytes` = data + headers + control) |
| useful bytes | Σ `useful_bytes` of the clients with status OK (topic size × clients if the client CSVs are missing) |
| redundant | total − useful |
| overhead % | redundant ÷ useful × 100 |
| wire overhead % | (wire − useful) ÷ useful × 100 |
| goodput | useful ÷ the same server interval |
| completion | per client, `end_ns − start_ns` on its own clock; mean and max over the run's clients |
| faults | FAULT rows in the events CSV |
| reconnects, attempts | from the client CSVs |

**Run status:**
- A run is **OK** when every client finished with status OK.
- A run is **DNF** if a client gave up (exit 2) or hit the time-out (exit 124).
- A run is **FAIL** otherwise.
- Means ± sample sd are taken over the OK repetitions. DNF and FAIL runs are
  counted and listed under "Data checks" in `summary.md`.

**With `--dataset`:** the topic's N (Σ ⌈size / 65536⌉) and T are computed, and
the predictions of the per-connection fault model (DEVIATIONS 28: every
connection is cut with probability p, at a uniform byte of its first request's
data) are added next to the measurements:
- Case 1 attempts 1/(1−p), faults p/(1−p) per client.
- Case 1 overhead p/(1−p) × (T−1)/2 ÷ T: each cut wastes half the topic on average.
- P(a Case 1 run is DNF): a client gives up with probability p^max_retries.
- Case 2 faults p/(1−p) per client; Enhanced about K·p/(1−p) per client (each of
  the K connections can be cut); overhead 0 for both.

**Consistency check:** where faults ≠ reconnects, the run is listed under
"Data checks".

**Outputs** (in `--out`):
- `Part1_summary.csv`, `Part2_summary.csv`, `Buffers_summary.csv`: one row per
  configuration, with `<metric>_mean` / `<metric>_sd` columns.
- `all_runs.csv`: one row per measured run.
- `summary.md`: markdown tables to paste into the report.

`plot.py` writes PNG (150 dpi) and PDF:

| Graph | Shows |
|---|---|
| `Part1_SingleVsMulti_throughput` | aggregate throughput vs N, both servers |
| `Part1_SingleVsMulti_completion` | mean and slowest per-client completion vs N |
| `Part2_Case1_throughput`, `Part2_Case2_throughput` | throughput (all bytes) and goodput (useful) vs p, Part I reference line |
| `Part2_Case1_overhead` | measured vs predicted overhead (symlog) |
| `Part2_Case2_overhead` | redundant data % and wire overhead % |
| `Part2_Case2_Enhanced_throughput`, `_overhead` | one line per K, K = 4 poll dashed |
| `Part2_Comparison_throughput`, `_overhead`, `_completion` | Case 1, Case 2, Enhanced K = 4 threads and poll vs p, with the Part I multi reference (from `--part1-compare`, else Part I N = 4) |
| `Part2_ThroughputVsOverhead` | goodput vs overhead, one point per (case, p) |
| `Part2_Enhanced_ThreadsVsPoll` | throughput and completion, threads vs poll |
| `Buffers_throughput` | throughput per buffer size |

Plot conventions:
- p is on a symlog axis, with p = 0 at the left edge.
- Error bars are the sample sd.
- A configuration whose repetitions were all DNF is drawn as an "x" with "DNF"
  at the top of the plot. Partly DNF points are annotated "k/n DNF".
- Each case keeps the same colour and marker in every graph. The colours were
  checked for colour-vision deficiencies.

Test analyze/plot without any lab data:
```
python3 tools/exp/fake_csv.py --out /tmp/fake/raw --make-dataset /tmp/fake/dataset --sweep full
python3 tools/exp/analyze.py --raw /tmp/fake/raw --out /tmp/fake/results --dataset /tmp/fake/dataset
python3 tools/exp/plot.py --results /tmp/fake/results
```

---------------------------------------------------------------------------

## 5. Notes and caveats

- **Server-side throughput and socket buffers.** `data_bytes` counts bytes
  handed to the socket, and `end_ns` is taken when the server finishes the
  connection. Up to a few MB can still be in the kernel's send buffer at that
  moment. With large topics and many clients this is negligible. For N = 1 it
  can overstate throughput by several percent. The per-client completion
  times (`compl_mean_s`, client clock) are the cross-check.
- **Client outputs on local disk.** Client outputs go to `/tmp/fds_out` on
  local disk. Do not point `--client-tmp` at an NFS home directory.
- **Keep the repo in sync.** The repo must be the same version on all PCs
  (`common.sh` runs on A and B in ssh mode).
- **Do not commit `lab.conf`.** It holds your lab's IPs and user names.
