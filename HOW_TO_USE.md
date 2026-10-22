# HOW_TO_USE: teammate runbook

For a teammate who knows Linux basics but has never seen this repo. Every command is run
from the **repository root** unless it says `cd`. Detailed reference for the experiment
scripts: `tools/exp/README.md`. Rules of the project: `CLAUDE.md`.

## 1. Prerequisites

| | Fedora (`dnf`) | Ubuntu (`apt`) |
|---|---|---|
| required | `sudo dnf install gcc make python3 git iproute` | `sudo apt install gcc make python3 git iproute2` |
| graphs | `sudo dnf install python3-matplotlib` | `sudo apt install python3-matplotlib` |
| unit tests, `SAN=` builds | `sudo dnf install libasan libubsan libtsan` | already part of gcc |
| optional | `sudo dnf install clang valgrind` | `sudo apt install clang valgrind` |

Without `libasan`, `make -C tests/unit run` fails at link time with
`cannot find /usr/lib64/libasan.so...`. Check: `gcc --version && python3 -c "import matplotlib"`.

## 2. Getting the code and the dataset

```
git clone <repo-url> comp_net && cd comp_net
git checkout impl            # the branch with the finished code
```

Dataset: Animals-10 from <https://www.kaggle.com/datasets/alessiocorrado99/animals10> (also on
Google Classroom). Unzip it so that the topic folders sit directly under `dataset/raw-img/`:

```
mkdir -p dataset && unzip ~/Downloads/<downloaded-zip> -d dataset/   # gives dataset/raw-img/...
for d in dataset/raw-img/*; do echo "$(basename "$d"): $(ls "$d" | wc -l) files"; done
```
Expected: ten folders, about `cane: 4863 files`, `gatto: 1668 files`, `elefante: 1446 files`.
If the zip has an extra level, move `raw-img` up so the path is exactly `dataset/raw-img/<topic>`.
(The topic names are the Italian folder
names.) `dataset/` is git-ignored.

No dataset yet? Generate a small one: `python3 tests/gen_dataset.py dataset/raw-img --profile animals --scale 0.05 --seed 1`
(same topic names, about 14 MB per big topic).

## 3. Build and a first smoke test

```
for d in Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced; do make -C $d; done
```
There must be no `warning:` lines. Each folder now holds `server` and `client`.
Run a program without arguments to see its exact usage line, e.g. `./client` prints
`usage: ./client <server_ip> <server_port> <topic> <output_dir>`.

Smoke test, two terminals:
```
# terminal 1                                        # terminal 2
cd Part1                                            cd Part1
./server 5050 ../dataset/raw-img                    ./client 127.0.0.1 5050 gatto ../out
```
Terminal 2 prints (numbers depend on your dataset):
```
gatto: 167 files, 3753925 bytes in 0.005 s (797.23 MB/s)
```
Check the copy, then stop the server with Ctrl-C (terminal 1 prints a summary line):
```
tests/verify_output.sh dataset/raw-img/gatto out/gatto      # OK 167 files 3753925 bytes
```

## 4. Run every case by hand (one PC)

Servers: `./server <port> <topics_root_dir> ...`. Clients: `./client <ip> <port> <topic> <output_dir> ...`.
Use a new output dir each time (or `rm -rf out`) so you see a fresh transfer. Stop each server
with Ctrl-C.

| Case | Server (terminal 1) | Client (terminal 2) |
|---|---|---|
| Part I single | `cd Part1; ./server 5050 ../dataset/raw-img single` | `./client 127.0.0.1 5050 gatto ../out` |
| Part I multi (default) | `./server 5050 ../dataset/raw-img multi` | same |
| Case 1 | `cd Part2_Case1; ./server 5050 ../dataset/raw-img 0.002 42` | `./client 127.0.0.1 5050 gatto ../out 100` |
| Case 2 | `cd Part2_Case2; ./server 5050 ../dataset/raw-img 0.01 42` | `./client 127.0.0.1 5050 gatto ../out 20` |
| Enhanced, threads | `cd Part2_Case2_Enhanced; ./server 5050 ../dataset/raw-img 0.01 42` | `./client 127.0.0.1 5050 gatto ../out 4 20 threads` |
| Enhanced, poll | same server | `./client 127.0.0.1 5050 gatto ../out 4 20 poll` |

The third server argument is the failure probability p (per 64 KB chunk; see README), the
fourth is the optional seed (same seed, same fault positions). Real outputs (small dataset):

Case 1, p = 0.002, seed 42 (the whole topic is sent again after the cut):
```
[client] attempt 1/100 failed after 3410144 data bytes: connection lost (closed by the server); retrying in 50 ms
[client] 2 attempt(s), 7164069 data bytes received in total (3410144 redundant)
gatto: 167 files, 3753925 bytes in 0.061 s (61.87 MB/s)
```
Case 2, p = 0.01, seed 42 (resumes at the byte where it was cut, nothing redundant):
```
[client] session started: 167 files, 3753925 bytes
[client] connection lost at file 147 offset 3081; resuming in 50 ms
[client] attempts=2 reconnects=1 rejects=0
gatto: 167 files, 3753925 bytes in 0.068 s (55.24 MB/s)
```
Enhanced, p = 0.01, seed 42, K = 4, threads (the failed file is re-queued at its offset):
```
[client] [conn 4] lost at file 160 offset 4803 (closed by the server); re-queued file 160 at offset 4803, retry in 50 ms
gatto: 167 files, 3753925 bytes in 0.055 s (68.05 MB/s), k=4, io=threads
```
Always verify with `tests/verify_output.sh dataset/raw-img/gatto out/gatto`. Exit code
`echo $?`: 0 ok, 2 gave up, 1 error. Try `p = 1` to see a client give up (exit 2).

## 5. Run the tests

```
make -C tests/unit run                   # C unit tests of common/
tests/integration/run_all.sh --quick     # about 1 minute; drop --quick for the full run
```
PASS output ends with a table (quick run):
```
  test                   pass   fail   skip    time
  test_build.sh            13      0      8     12s  OK
  test_cli.sh              83      0      0      1s  OK
  test_part1.sh            34      0      1      4s  OK
  test_case1.sh            34      0      1      3s  OK
  test_case2.sh            57      0      1     16s  OK
  test_enhanced.sh         87      0      2     14s  OK
  TOTAL                   308      0     13
```
`fail` must be 0. Skips are normal (e.g. clang or sanitizers not installed, slow checks in
`--quick`). Sanitizers: `SAN=address tests/integration/run_all.sh` and `SAN=thread ...`
(needs libasan/libtsan; the script rebuilds the binaries with the sanitizer, so run
`make -C <Part> clean && make -C <Part>` afterwards if you want normal binaries again).
More options: `tests/README.md`.

## 6. Dry run of the lab on one PC (emulation)

`tools/emu/emu.sh` builds three emulated hosts with network namespaces and no sudo:
S = 10.10.0.1 (server), A = 10.10.0.2, B = 10.10.0.3, with a 1 Gbit/s shaped link.
Use it to check that the whole pipeline works before going to the lab.

```
tools/emu/emu.sh up
tools/emu/emu.sh ping-test       # ends with "ping-test: OK"
tools/exp/run_part1.sh --mode emu --quick --yes --out /tmp/emu_raw
tools/exp/run_part2.sh --mode emu --quick --part1-compare --yes --out /tmp/emu_raw
tools/exp/run_buffers.sh --mode emu --quick --yes --out /tmp/emu_raw
python3 tools/exp/analyze.py --raw /tmp/emu_raw --out /tmp/emu_results --dataset dataset/raw-img
python3 tools/exp/plot.py --results /tmp/emu_results
tools/emu/emu.sh down
```
Always use `--out /tmp/emu_raw` and a separate `--out` for `analyze.py`/`plot.py`, so emulation
numbers never land in `Results/`. Each run prints a progress line, for example:
```
[13:52:56] (1/4) part1_single_N1_r1: Part1 server [single], 1 client(s) (A=1 B=0), time-out 70s
[13:52:58] (1/4) part1_single_N1_r1: OK ok=1 dnf=0 timeout=0 fail=0, 1.447 s, server rc=0 rows=1, check [A:OK] (ETA 0h00m07s)
```
`OK ... check [A:OK B:OK]` means the clients received the topic with the right file count and
size. If `emu.sh up` fails, user namespaces are disabled: the script prints the fix (Ubuntu 23.10+
needs a sysctl). Fallback: `--mode local` runs everything on 127.0.0.1 (smoke test only).

## 7. The real 3-PC experiments

This section produces the numbers for the report. Plan about 7-8 hours of lab time in total
(Part I about 1 h 15 min, Part II about 5 h 15 min, buffers about 40 min), and split Part II
over sessions if needed (see "Long sweeps").

### 7.1 Roles and preparation

- **S** = server PC (also the controller in ssh mode). **A** and **B** = client PCs. All three
  on the same LAN, wired if possible.
- On **every** PC: clone the repo (same path on all three if possible), checkout `impl`, and
  build all four Part folders (section 3). The repo must be the same version everywhere.
- On **S** only: the dataset under `dataset/raw-img/` (section 2).
- Find the IPs with `ip -br addr` on each PC. From A and B: `ping -c 3 <S-ip>` must work.
- Clock sync, on every PC: `timedatectl` must show `System clock synchronized: yes`
  (otherwise `sudo timedatectl set-ntp true`). It matters most in manual mode.
- **Open the port on S** (default 5050):

  | | open | undo afterwards |
  |---|---|---|
  | Fedora | `sudo firewall-cmd --add-port=5050/tcp` | it is temporary: a reboot or `sudo firewall-cmd --reload` removes it; or `sudo firewall-cmd --remove-port=5050/tcp` |
  | Ubuntu | `sudo ufw allow 5050/tcp` | `sudo ufw delete allow 5050/tcp` |

- **Socket-buffer experiment only:** on all three PCs run
  `sudo sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608` (resets at reboot).
  Without it Linux silently caps the 1 MB and 4 MB buffers at about 208 KB.

### 7.2 ssh mode (recommended)

S starts the clients on A and B over ssh and collects their CSV files.

One-time setup, on S:
```
ssh-keygen -t ed25519                  # press Enter at every question
ssh-copy-id <user>@<A-ip>
ssh-copy-id <user>@<B-ip>
ssh <user>@<A-ip> true                 # must NOT ask for a password; same for B
cp tools/exp/lab.conf.example tools/exp/lab.conf
```
Edit `tools/exp/lab.conf` (plain bash `KEY=value`; **do not commit it**):

| Key | Set to |
|---|---|
| `SERVER_IP` | S's IP as A and B see it |
| `CLIENT_A`, `CLIENT_B` | `user@ip` of A and B |
| `DATASET` | absolute path of `dataset/raw-img` on S |
| `PORT` | 5050 (must match the firewall rule) |
| `REMOTE_REPO` | repo path on A and B, only if it differs from S |

Look first, then run (from S, repo root):
```
tools/exp/run_part1.sh --mode ssh --dry-run          # prints every command, runs nothing
tools/exp/run_part1.sh --mode ssh --yes              # Part I: 72 runs, about 1 h 15 min
tools/exp/run_part2.sh --mode ssh --part1-compare --yes    # Part II: 342 runs, about 5 h 15 min
tools/exp/run_buffers.sh --mode ssh --yes            # buffers: 40 runs, about 40 min
```
Each script ends with `sweep ... finished. Raw data in .../Results/raw/<sweep>` and prints the
`analyze.py`/`plot.py` commands to run next. The dry run header shows the run count
(`runs: 72`, `runs: 342`) and which hosts and paths will be used: read it.

Long sweeps:
- Interrupted (Ctrl-C, ssh dropped)? Run the **same command** again with `--resume`; finished
  runs are skipped.
- Split Part II across sessions: `--cases case1`, `--cases case2,enh`, or `--ps 0,0.001`. All
  runs land in the same folder.
- Do not use A, B or S for other heavy work while a sweep runs.

### 7.3 Manual mode (no ssh)

Each PC runs the same script with its own `--role` and the **same** `--start-at`; all three
follow an identical timetable. Prefer ssh mode if at all possible.

1. On S, look at the timetable and choose a start at least 2 minutes ahead:
   ```
   tools/exp/run_part2.sh --mode manual --role server --dry-run --part1-compare
   date -d '14:30' +%s            # prints e.g. 1792900200: use this on all three PCs
   ```
2. Start the same command on the three PCs, each before the start time (order is free):
   ```
   # on S
   tools/exp/run_part2.sh --mode manual --role server --start-at 1792900200 --part1-compare --yes
   # on A
   tools/exp/run_part2.sh --mode manual --role A --server-ip <S-ip> --start-at 1792900200 --part1-compare --yes
   # on B
   tools/exp/run_part2.sh --mode manual --role B --server-ip <S-ip> --start-at 1792900200 --part1-compare --yes
   ```
   Every PC prints `plan hash: xxxxxxxxxx`. It **must be identical on all three**; if not, some
   flag differs: stop and fix. (Alternative: `--plan plan.txt` on S, copy the file to A and B,
   pass `--plan plan.txt` there.) The same pattern works for `run_part1.sh` and
   `run_buffers.sh`. If the network is slow use `--slot-scale 2`; on a fast one `--slot-scale 0.5`.
3. At the end A and B print the `scp` commands that copy their `clients/*.A.csv` / `*.B.csv`,
   `runs_A.csv` and `sysinfo_A_*.txt` to S. Run them (or use a USB stick, same folders).

### 7.4 Where the results land

On S: `Results/raw/sysinfo_*.txt` (one per PC), and per sweep `Results/raw/{part1,part2,buffers}/`
with `server/`, `events/`, `clients/` (CSV per run), `runs.csv` and progress logs. In ssh mode the
client CSVs are copied back automatically. Copy `Results/` to the PC where you write the
report: `scp -r user@<S-ip>:comp_net/Results .` (or a USB stick). Commit CSVs and PNGs
to the team repo or a shared drive: they are the evidence for the report.

## 8. Analysis: tables and graphs

```
python3 tools/exp/analyze.py --raw Results/raw --out Results --dataset dataset/raw-img
python3 tools/exp/plot.py --results Results
```
`analyze.py` (standard library only) prints which summaries it wrote and lists problems under
"Data checks" in `summary.md`. It writes `Part1_summary.csv`, `Part2_summary.csv`,
`Buffers_summary.csv` (one row per configuration, mean and sd), `all_runs.csv` (one row per
run) and `summary.md` (tables to paste into the report). `plot.py` (needs matplotlib) writes
each graph as `.png` and `.pdf` in `Results/`:

| Report section | Graphs in `Results/` |
|---|---|
| Part I | `Part1_SingleVsMulti_throughput`, `Part1_SingleVsMulti_completion` |
| Part II Case 1 | `Part2_Case1_throughput`, `Part2_Case1_overhead` (measured vs predicted) |
| Part II Case 2 | `Part2_Case2_throughput`, `Part2_Case2_overhead` |
| Part II Case 2 Enhanced | `Part2_Case2_Enhanced_throughput`, `Part2_Case2_Enhanced_overhead`, `Part2_Enhanced_ThreadsVsPoll` |
| Comparison of all cases | `Part2_Comparison_throughput`, `Part2_Comparison_overhead`, `Part2_Comparison_completion` |
| **Throughput vs overhead** | `Part2_ThroughputVsOverhead` (goodput against overhead, one point per case and p) |
| Socket buffers | `Buffers_throughput` |

Metrics, the DNF rule (a run where a client gave up or timed out) and the design 4.4
predictions are explained in `tools/exp/README.md` section 4. Runs that did not finish are drawn
as an "x" labelled DNF; mention them in the report instead of hiding them.
Test the scripts without lab data (never copy this output into `Results/`):
```
python3 tools/exp/fake_csv.py --out /tmp/fake/raw --make-dataset /tmp/fake/dataset --sweep full
python3 tools/exp/analyze.py --raw /tmp/fake/raw --out /tmp/fake/results --dataset /tmp/fake/dataset
python3 tools/exp/plot.py --results /tmp/fake/results
```

## 9. Packaging the submission

1. Put the finished report in the repo root as `FinalReport_Group_006_043.pdf`, and make sure
   `Results/` holds the CSVs and graphs of section 8.
2. Run:
   ```
   tools/package.sh                                   # uses ./FinalReport_Group_006_043.pdf
   tools/package.sh --report /path/to/report.pdf --out /tmp/submit    # other locations
   ```
   It syncs and checks `common/`, copies README.md, the report, the four Part folders (sources
   and Makefile only) and `Results/` into `FinalSubmission_Group_006_043/`, creates
   `FinalSubmission_Group_006_043.tar.gz` (default folder `dist/`), then extracts it into a temp
   dir and checks: layout, `make` with zero warnings, exactly `server` + `client` per Part folder,
   and one real Part I transfer compared byte for byte.
3. It must end with `RESULT: PASS` and **no** `WARNING: ... MISSING` banner. Without the report
   it still passes but prints a loud warning: that archive is not submittable.
4. Final look: `tar -tzf dist/FinalSubmission_Group_006_043.tar.gz | head -40`. The only
   top-level entries are the PDF, `README.md`, `Part1`, `Part2_Case1`, `Part2_Case2`,
   `Part2_Case2_Enhanced`, `Results`. Submit the single `.tar.gz`, not loose files.

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Server: `cannot listen on port 5050: Address already in use` (exit 1) | Another server (maybe yours, in another terminal) holds the port. `ss -ltnp \| grep 5050`, stop it, or choose another port. |
| Client: `cannot connect to <ip>:<port>: Connection refused` or a time-out | Server not running, wrong IP/port, or the firewall: open the port on S (7.1). Test with `ping <S-ip>` and `nc -vz <S-ip> 5050`. |
| Client: `server error 404: unknown topic` | The topic is not a folder of `topics_root_dir`; check `ls dataset/raw-img` and the path given to the server. The server prints `N topic(s) loaded`: it should say 10. |
| Server says `error: topics_root_dir '...': No such file or directory` (exit 1), or loads `0 topic(s)` | Wrong dataset path. It must be the folder that **contains** the topic folders (`dataset/raw-img`, not `dataset/raw-img/gatto`). Relative paths are relative to where you started the server (`../dataset/raw-img` from a Part folder). |
| Case 1 client exits 2 / run counted DNF at high p | Expected, not a bug. Case 1 re-sends the whole topic, and an attempt of N chunks succeeds with probability (1-p)^N. For `gatto` N is about 1 700, so at p = 0.01 that is about 5e-8 per attempt: it never finishes within `max_retries`. Design section 4.4 predicts this; report it as DNF and show the predicted curve. Case 2 and Enhanced avoid it by resuming. |
| Sweep clients time out although the network is fine | Clocks not synchronised (manual mode), or other heavy processes on A/B/S. Check `timedatectl`. |
| `ssh ... Permission denied` in a sweep | Passwordless ssh not set up: `ssh-copy-id`, then `ssh user@ip true` must not prompt. |
| `plot.py`: `matplotlib is not installed` | `sudo dnf install python3-matplotlib` or `sudo apt install python3-matplotlib` (or `pip install matplotlib`). `analyze.py` does not need it. |
| `make -C tests/unit run`: `cannot find libasan.so` | `sudo dnf install libasan libubsan` (Fedora). |
| `emu.sh up` fails | Unprivileged user namespaces disabled; follow the printed hint or use `--mode local`. |
| Leftover `.part` / `.session` in an output dir | A client was killed; run the same client command again and it resumes (Case 2, Enhanced). Case 1 and Part I leave no such files. |
| Packaging says `make printed warnings` | Fix the warning in `common/` (never in `Part*/`), then `tools/sync_common.sh`. |
