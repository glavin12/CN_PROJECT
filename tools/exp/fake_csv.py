#!/usr/bin/env python3
"""
fake_csv.py - write realistic SYNTHETIC raw CSV logs, to test analyze.py and
plot.py before the real binaries or the lab are available.

usage: python3 tools/exp/fake_csv.py --out <raw dir> [--sweep quick|full] [--seed 1]
                                     [--make-dataset <dir>] [--files 1668]

The output has exactly the layout and run ids of tools/exp/run_*.sh and the
exact CSV columns and formatting of common/log.c (quoted topic, %g for p,
32-hex session ids, CLOCK_MONOTONIC *_ns with a different base per machine).
The numbers come from a small model of the real protocols:
  * a 1 Gbit/s server uplink shared fairly by the clients of a run,
  * one fault draw per 64 KB chunk with probability p; on a fault a random
    prefix of 0..len-1 bytes of the chunk is sent first (deviation 9),
  * Case 1 restarts the topic (back-off 50 ms doubling to 1 s, gives up after
    100 attempts = DNF), Case 2 resumes from the byte offset (back-off resets
    after progress, gives up after 20 attempts without progress), Enhanced has
    K worker connections taking files from a shared queue with one FETCH
    round trip per file,
  * Part I single serves the clients one after another, multi concurrently.
With --make-dataset the synthetic topics are also written (sparse files of
the same sizes) so that `analyze.py --dataset <dir>` can compute N and T.
NEVER copy this output into Results/: it is not measured data.
"""

import argparse
import os
import random
import zlib

CH = 65536
RATE = 117.0e6        # server uplink, bytes/s (~940 Mbit/s of TCP goodput on 1 Gbit/s)
RTT = 0.25e-3         # s
CONNECT = 0.4e-3      # TCP + request set-up per connection, s
HDR = 16              # frame header
MS = 1e-3

SERVER_HDR = ("run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,start_ns,end_ns,"
              "start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status")
EVENTS_HDR = "run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail"
CLIENT_HDR = ("run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,"
              "useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status")
RUNS_HDR = ("run_id,sweep,mode,role,n_clients,start_wall_ms,elapsed_s,server_rc,clients_ok,clients_dnf,"
            "clients_timeout,clients_fail,client_rcs,check,note")

QUICK = {"ns": [1, 4], "servers": ["single", "multi"], "ps": ["0", "0.001", "0.01"],
         "impls": ["case1", "case2", "enh_K4", "enh_K4_poll"], "reps": 2, "warmup": 1,
         "bufs": ["default", "1048576"], "buf_reps": 1}
FULL = {"ns": [1, 2, 4, 8, 16, 32], "servers": ["single", "multi"],
        "ps": ["0", "0.0001", "0.0002", "0.0005", "0.001", "0.002", "0.005", "0.01"],
        "impls": ["case1", "case2", "enh_K1", "enh_K2", "enh_K4", "enh_K8", "enh_K4_poll"], "reps": 5,
        "warmup": 1, "bufs": ["default", "65536", "262144", "1048576", "4194304"], "buf_reps": 3}


def backoff(streak):
    return min(50 * (2 ** max(streak - 1, 0)), 1000) * MS


class Out:
    """Collects CSV lines per file and writes them with a header, like log.c."""

    def __init__(self, root):
        self.root = root
        self.files = {}

    def add(self, rel, header, line):
        self.files.setdefault(rel, (header, []))[1].append(line)

    def write(self):
        for rel, (header, lines) in self.files.items():
            path = os.path.join(self.root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(header + "\n")
                f.write("".join(x + "\n" for x in lines))


def q(s):
    return '"%s"' % s if s else "-"


def make_sizes(rng, nfiles):
    """Animals-10-like image sizes: lognormal, median ~18 KB, a few > 64 KB."""
    return [max(800, int(rng.lognormvariate(9.8, 0.55))) for _ in range(nfiles)]


def chunk_list(sizes):
    """(chunk length, file index, offset in file, last chunk of the file?)"""
    out = []
    for i, s in enumerate(sizes):
        off = 0
        while off < s:
            n = min(CH, s - off)
            out.append((n, i, off, off + n == s))
            off += n
    return out


class Run:
    """State of one simulated run: clocks, connection counter, output rows."""

    def __init__(self, sim, sweep, rid, case, smode, p, seed, n, topic):
        self.sim, self.sweep, self.rid, self.case = sim, sweep, rid, case
        self.smode, self.p, self.seed, self.n, self.topic = smode, p, seed, n, topic
        rng = sim.rng
        self.sbase = rng.randrange(10**12, 10**13)              # server CLOCK_MONOTONIC
        self.cbase = {"A": rng.randrange(10**12, 10**13), "B": rng.randrange(10**12, 10**13)}
        self.wall0 = sim.wall
        self.conn_id = 0
        # per-run generator, reproducible (str hash() is randomised per process)
        self.rng = random.Random(zlib.crc32(("%s/%s" % (rid, seed)).encode()))

    def sns(self, t):
        return self.sbase + int(t * 1e9)

    def server_row(self, kind, sid, k, label, t0, t1, data, wire, chunks, files, faulted, status):
        self.conn_id += 1
        cid = self.conn_id
        pstr = "-1" if self.p is None else "%g" % self.p
        seed = "-" if self.seed is None else str(self.seed)
        peer = "10.0.0.%d:%d" % (2 if label == "A" else 3, 40000 + self.rng.randrange(20000))
        line = ",".join(str(x) for x in (
            self.rid, self.case, self.smode, pstr, seed, cid, kind, sid or "-", q(self.topic), k, peer,
            self.sns(t0), self.sns(t1), self.wall0 + int(t0 * 1e9), data, wire, chunks, files, faulted, status))
        self.sim.out.add("%s/server/%s.csv" % (self.sweep, self.rid), SERVER_HDR, line)
        return cid

    def event(self, t, ev, cid, sid, fidx, off, chunk, cdata, detail=""):
        line = ",".join(str(x) for x in (
            self.rid, self.case, self.sns(t), ev, cid, sid or "-", q(self.topic), fidx, off, chunk, cdata,
            q(detail)))
        self.sim.out.add("%s/events/%s.csv" % (self.sweep, self.rid), EVENTS_HDR, line)

    def client_row(self, label, cid, k, io, t0, t1, useful, data_rx, wire_rx, files, attempts, reconnects, status):
        line = ",".join(str(x) for x in (
            self.rid, self.case, "pc" + label, cid, 20000 + self.rng.randrange(40000), q(self.topic), k, io,
            self.cbase[label] + int(t0 * 1e9), self.cbase[label] + int(t1 * 1e9),
            self.wall0 + int(t0 * 1e9), self.wall0 + int(t1 * 1e9),
            useful, data_rx, wire_rx, files, attempts, reconnects, 0, status))
        self.sim.out.add("%s/clients/%s.%s.csv" % (self.sweep, self.rid, label), CLIENT_HDR, line)


def labels(n):
    na = (n + 1) // 2
    return ["A%d" % (i + 1) for i in range(na)] + ["B%d" % (i + 1) for i in range(n - na)]


class Sim:
    def __init__(self, out, seed, nfiles):
        self.out = Out(out)
        self.rng = random.Random(seed)
        self.wall = 1_790_000_000 * 10**9
        self.topics = {"gatto": make_sizes(self.rng, nfiles), "elefante": make_sizes(self.rng, int(nfiles * 0.87))}
        self.chunks = {t: chunk_list(s) for t, s in self.topics.items()}

    def runs_row(self, run, rcs, elapsed):
        ok = sum(1 for _, rc in rcs if rc == 0)
        dnf = sum(1 for _, rc in rcs if rc == 2)
        to = sum(1 for _, rc in rcs if rc == 124)
        fail = len(rcs) - ok - dnf - to
        line = ",".join(str(x) for x in (
            run.rid, run.sweep, "emu", "ctl", run.n, run.wall0 // 10**6, "%.3f" % elapsed, 0, ok, dnf, to, fail,
            ";".join("%s=%d" % x for x in rcs), "A:OK" + (" B:OK" if run.n > 1 else ""), ""))
        self.out.add("%s/runs.csv" % run.sweep, RUNS_HDR, line)
        self.wall += int((elapsed + 3) * 1e9)

    # ---- Part I -----------------------------------------------------------
    def part1(self, sweep, rid, smode, n, topic):
        run = Run(self, sweep, rid, "part1", smode, None, None, n, topic)
        sizes = self.topics[topic]
        T, nf, nch = sum(sizes), len(sizes), len(self.chunks[topic])
        wire = T + nf * (2 * HDR + 40) + 2 * HDR + 24
        rng = run.rng
        arrivals = sorted((rng.uniform(0, 0.004), c) for c in labels(n))
        rcs, t_end = [], 0.0
        if smode == "single":
            free = 0.0
            for arr, c in arrivals:
                s = max(arr, free) + CONNECT
                e = s + T / (RATE * rng.uniform(0.95, 0.985))
                free = e
                run.server_row("get", None, 0, c[0], s, e, T, wire, nch, nf, 0, "OK")
                run.client_row(c[0], c, 1, "-", arr, e + RTT, T, T, wire, nf, 1, 0, "OK")
                rcs.append((c, 0))
                t_end = e
        else:
            eff = max(0.6, 0.975 - 0.0035 * n)  # CPU / contention loss at large N
            for arr, c in arrivals:
                s = arr + CONNECT
                e = s + n * T / (RATE * eff) * rng.uniform(0.97, 1.03)
                run.server_row("get", None, 0, c[0], s, e, T, wire, nch, nf, 0, "OK")
                run.client_row(c[0], c, 1, "-", arr, e + RTT, T, T, wire, nf, 1, 0, "OK")
                rcs.append((c, 0))
                t_end = max(t_end, e)
        self.runs_row(run, rcs, t_end + 0.5)

    # ---- Part II, Case 1 and Case 2 ----------------------------------------
    def case12(self, sweep, rid, impl, p, seed, n, topic, max_retries, cap=600.0, share_scale=1.0):
        case = impl
        run = Run(self, sweep, rid, case, "multi", p, seed, n, topic)
        rng = run.rng
        chunks = self.chunks[topic]
        sizes = self.topics[topic]
        T, nf = sum(sizes), len(sizes)
        share = RATE * 0.96 * share_scale / n
        rcs, t_last = [], 0.0
        for c in labels(n):
            label = c[0]
            sid = "%032x" % rng.getrandbits(128) if impl == "case2" else None
            t = rng.uniform(0, 0.004)
            t_start = t
            attempts = reconnects = streak = 0
            data_rx = wire_rx = 0
            j, done_in_chunk = 0, 0          # Case 2 resume point: chunk j, bytes of it already stored
            stored_files = 0
            status = None
            while status is None:
                attempts += 1
                s = t + CONNECT
                kind = "get" if (impl == "case1" or attempts == 1) else "resume"
                if impl == "case1":
                    j, done_in_chunk, stored_files = 0, 0, 0
                start_j = j
                sent = chunks_sent = files_done = 0
                faulted = False
                f_info = None
                while j < len(chunks):
                    ln, fidx, off, last = chunks[j]
                    rem = ln - done_in_chunk
                    chunks_sent += 1
                    if rng.random() < p:
                        prefix = rng.randrange(rem)
                        sent += prefix
                        done_in_chunk += prefix
                        f_info = (fidx, off + done_in_chunk, j, sent)
                        faulted = True
                        break
                    sent += rem
                    done_in_chunk = 0
                    j += 1
                    if last:
                        files_done += 1
                stored_files += files_done
                e = s + sent / share + files_done * 1e-6
                wire = sent + files_done * (2 * HDR + 40) + (2 * HDR + 24 if kind == "get" else 2 * HDR + 12)
                cid = run.server_row(kind, sid, 0, label, s, e, sent, wire, chunks_sent, files_done,
                                     int(faulted), "FAULT" if faulted else "OK")
                if kind == "resume":
                    run.event(s, "RESUME_OK", cid, sid, chunks[min(start_j, len(chunks) - 1)][1], 0, 0, 0)
                data_rx += sent
                wire_rx += wire
                if faulted:
                    run.event(e, "FAULT", cid, sid, f_info[0], f_info[1], f_info[2], f_info[3])
                    progress = j > start_j or sent > 0
                    if impl == "case1":
                        streak += 1
                        give_up = attempts >= max_retries
                    else:
                        streak = 1 if progress else streak + 1
                        give_up = streak > max_retries
                    if give_up:
                        status = "DNF"
                        t = e
                        break
                    reconnects += 1
                    t = e + backoff(streak)
                else:
                    status = "OK"
                    t = e
            t_end = t + RTT
            if t_end - t_start > cap:          # killed by the runner's time-out: no client row
                rcs.append((c, 124))
                t_last = max(t_last, t_start + cap)
                continue
            useful = T if status == "OK" else 0
            run.client_row(label, c, 1, "-", t_start, t_end, useful, data_rx, wire_rx,
                           nf if status == "OK" else stored_files, attempts, reconnects, status)
            rcs.append((c, 0 if status == "OK" else 2))
            t_last = max(t_last, t_end)
        self.runs_row(run, rcs, t_last + 0.5)

    # ---- Part II, Case 2 Enhanced -----------------------------------------
    def enhanced(self, sweep, rid, k, io, p, seed, n, topic, max_retries=20, share_scale=1.0):
        run = Run(self, sweep, rid, "enhanced", "multi", p, seed, n, topic)
        rng = run.rng
        sizes = self.topics[topic]
        T, nf = sum(sizes), len(sizes)
        share = RATE * 0.96 * share_scale / n * (0.97 if io == "poll" else 1.0)
        rcs, t_last = [], 0.0
        for c in labels(n):
            label = c[0]
            sid = "%032x" % rng.getrandbits(128)
            t0 = rng.uniform(0, 0.004)
            # manifest connection
            m_end = t0 + CONNECT + RTT + nf * 40 / share
            run.server_row("manifest", sid, k, label, t0 + CONNECT, m_end, 0, HDR + 8 + nf * 40, 0, 0, 0, "OK")
            wire_rx = HDR + 8 + nf * 40
            data_rx = 0
            workers = [m_end + i * 1e-5 for i in range(k)]
            # per worker: current connection accumulators
            conn = [dict(start=w + CONNECT, data=0, wire=0, chunks=0, files=0) for w in workers]
            workers = [w + CONNECT for w in workers]
            reconnects = 0
            queue = list(range(nf))
            rate_per = share / k
            while queue:
                wi = min(range(k), key=lambda i: workers[i])
                fidx = queue.pop(0)
                size, off = sizes[fidx], 0
                while off < size:
                    workers[wi] += RTT          # FETCH round trip
                    cc = conn[wi]
                    faulted = False
                    while off < size:
                        ln = min(CH, size - off)
                        cc["chunks"] += 1
                        if rng.random() < p:
                            prefix = rng.randrange(ln)
                            off += prefix
                            cc["data"] += prefix
                            workers[wi] += prefix / rate_per
                            faulted = True
                            break
                        off += ln
                        cc["data"] += ln
                        workers[wi] += ln / rate_per
                    cc["wire"] += HDR + 40
                    if not faulted:
                        cc["files"] += 1
                        cc["wire"] += HDR
                        break
                    cid = run.server_row("fetch", sid, k, label, cc["start"], workers[wi], cc["data"],
                                         cc["data"] + cc["wire"], cc["chunks"], cc["files"], 1, "FAULT")
                    run.event(workers[wi], "FAULT", cid, sid, fidx, off, cc["chunks"] - 1, cc["data"])
                    data_rx += cc["data"]
                    wire_rx += cc["data"] + cc["wire"]
                    reconnects += 1
                    workers[wi] += backoff(1) + CONNECT
                    conn[wi] = dict(start=workers[wi], data=0, wire=0, chunks=0, files=0)
            for wi in range(k):
                cc = conn[wi]
                run.server_row("fetch", sid, k, label, cc["start"], workers[wi], cc["data"], cc["data"] + cc["wire"],
                               cc["chunks"], cc["files"], 0, "OK")
                data_rx += cc["data"]
                wire_rx += cc["data"] + cc["wire"]
            t_end = max(workers) + RTT
            run.client_row(label, c, k, io, t0, t_end, T, data_rx, wire_rx, nf, k + 1 + reconnects, reconnects, "OK")
            rcs.append((c, 0))
            t_last = max(t_last, t_end)
        self.runs_row(run, rcs, t_last + 0.5)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True, help="raw output folder (a scratch folder, NOT Results/raw)")
    ap.add_argument("--sweep", choices=["quick", "full"], default="quick")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--files", type=int, default=1668, help="files of the synthetic gatto (elefante: 87%%)")
    ap.add_argument("--make-dataset", metavar="DIR", help="also write the synthetic topics (sparse files) here")
    a = ap.parse_args()
    cfg = QUICK if a.sweep == "quick" else FULL
    sim = Sim(a.out, a.seed, a.files)

    if a.make_dataset:
        for topic, sizes in sim.topics.items():
            d = os.path.join(a.make_dataset, topic)
            os.makedirs(d, exist_ok=True)
            for i, s in enumerate(sizes):
                with open(os.path.join(d, "img_%05d.jpg" % i), "wb") as f:
                    f.truncate(s)

    def reps(cfg_id, warmup, nreps):
        for w in range(1, warmup + 1):
            yield "%s_w%d_warmup" % (cfg_id, w), 1000
        for r in range(1, nreps + 1):
            yield "%s_r%d" % (cfg_id, r), 1000 + r

    nruns = 0
    for smode in cfg["servers"]:
        for n in cfg["ns"]:
            for rid, _seed in reps("part1_%s_N%d" % (smode, n), cfg["warmup"], cfg["reps"]):
                sim.part1("part1", rid, smode, n, "elefante")
                nruns += 1
    for rid, _seed in reps("p1cmp_multi_N4", cfg["warmup"], cfg["reps"]):
        sim.part1("part2", rid, "multi", 4, "gatto")
        nruns += 1
    for impl in cfg["impls"]:
        for ps in cfg["ps"]:
            p = float(ps)
            if impl.startswith("enh_"):
                k = int(impl.split("_")[1][1:])
                io = "poll" if impl.endswith("_poll") else "threads"
                cfg_id = "enh_K%d_%s_p%s" % (k, io, ps)
            else:
                cfg_id = "%s_p%s" % (impl, ps)
            for rid, seed in reps(cfg_id, cfg["warmup"], cfg["reps"]):
                if impl.startswith("enh_"):
                    sim.enhanced("part2", rid, k, io, p, seed, 4, "gatto")
                else:
                    sim.case12("part2", rid, impl, p, seed, 4, "gatto", 100 if impl == "case1" else 20)
                nruns += 1
    for impl, prefix in (("case2", "case2"), ("enh_K4", "enh_K4_threads")):
        for buf in cfg["bufs"]:
            scale = {"default": 1.0, "65536": 0.92, "262144": 1.0, "1048576": 1.01, "4194304": 0.995}[buf]
            for rid, seed in reps("buf_%s_b%s" % (prefix, buf), cfg["warmup"], cfg["buf_reps"]):
                if impl == "case2":
                    sim.case12("buffers", rid, "case2", 0.0, seed, 4, "gatto", 20, share_scale=scale)
                else:
                    sim.enhanced("buffers", rid, 4, "threads", 0.0, seed, 4, "gatto", share_scale=scale)
                nruns += 1
    sim.out.write()
    print("wrote %d synthetic runs (%d CSV files) to %s%s" % (
        nruns, len(sim.out.files), a.out,
        "; synthetic topics in %s" % a.make_dataset if a.make_dataset else ""))


if __name__ == "__main__":
    main()
