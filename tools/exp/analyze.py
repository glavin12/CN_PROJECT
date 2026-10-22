#!/usr/bin/env python3
"""
analyze.py - turn the raw CSV logs written by tools/exp/run_*.sh into the
summary tables of the report (metrics exactly as in design section 6.7).

usage: python3 tools/exp/analyze.py [--raw Results/raw] [--out Results] [--dataset <topics_root_dir>]

Input: every *.csv below --raw. Files are recognised by their header line, so
the folder layout does not matter (server/, events/, clients/, runs*.csv):
  server CSV  (FDS_LOG of a server)   one row per connection
  events CSV  (FDS_EVENTS)            one row per FAULT / RESUME_OK / ...
  client CSV  (FDS_LOG of clients)    one row per client run
  runs CSV    (written by the runner) exit codes, output check, notes
Rows are grouped by run_id; the configuration is parsed from the run_id.
Warm-up runs (run_id ending in _warmup) are ignored.

Per run (one server clock, so the interval is consistent):
  throughput  = sum(server data_bytes) / (max end_ns - min start_ns)
  total bytes = sum(server data_bytes)      (wire bytes reported too)
  useful      = sum(useful_bytes of the clients that finished OK)
                (topic bytes x clients when the client CSVs are missing)
  redundant   = total - useful,  overhead % = redundant / useful x 100
  goodput     = useful / the same server interval
  completion  = per client end_ns - start_ns (client's own monotonic clock);
                mean and max over the run's clients
  faults      = FAULT rows of the events CSV; reconnects/attempts from clients
A run is OK when every client finished OK. Runs with a client that gave up
(exit 2), was killed by the time-out (DNF) or failed are counted but excluded
from the means. Means are over the OK repetitions; sd is the sample standard
deviation (blank with fewer than 2 repetitions).

With --dataset, the topic's N (64 KB chunks, sum of ceil(size/65536)) and T
(bytes) give the predictions of design section 4.4 next to the measurements.

Output (in --out): Part1_summary.csv, Part2_summary.csv, Buffers_summary.csv,
all_runs.csv (one row per measured run) and summary.md (markdown tables).
Only the standard library is used.
"""

import argparse
import csv
import math
import os
import re
import statistics
import sys
from collections import defaultdict

CHUNK = 65536
MB = 1e6  # all MB in the outputs are 10^6 bytes

SERVER_COLS = ("run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,start_ns,end_ns,"
               "start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status").split(",")
EVENT_COLS = ("run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,"
              "conn_data_bytes,detail").split(",")
CLIENT_COLS = ("run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,"
               "useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status").split(",")
RUNS_COLS = ("run_id,sweep,mode,role,n_clients,start_wall_ms,elapsed_s,server_rc,clients_ok,clients_dnf,"
             "clients_timeout,clients_fail,client_rcs,check,note").split(",")

# ----------------------------------------------------------------------------
# run_id parsing
# ----------------------------------------------------------------------------

SUFFIX = r"_(?:r(?P<rep>\d+)|w(?P<w>\d+)_warmup)$"
P_RE = r"(?P<p>[0-9.]+(?:[eE][-+]?\d+)?)"
RUN_PATTERNS = [
    ("part1", re.compile(r"^part1_(?P<smode>single|multi)_N(?P<n>\d+)" + SUFFIX)),
    ("p1cmp", re.compile(r"^p1cmp_(?P<smode>single|multi)_N(?P<n>\d+)" + SUFFIX)),
    ("case", re.compile(r"^(?P<impl>case1|case2)_p" + P_RE + SUFFIX)),
    ("enh", re.compile(r"^enh_K(?P<k>\d+)_(?P<io>threads|poll)_p" + P_RE + SUFFIX)),
    ("buf", re.compile(r"^buf_(?P<impl>case2|enh_K(?P<k>\d+)_(?P<io>threads|poll))_b(?P<buf>default|\d+)" + SUFFIX)),
]

IMPL_LABEL = {"case1": "Case 1", "case2": "Case 2", "part1_multi": "Part I multi"}


def impl_label(impl):
    if impl in IMPL_LABEL:
        return IMPL_LABEL[impl]
    m = re.match(r"enh_K(\d+)(_poll)?$", impl)
    if m:
        return "Enhanced K=%s %s" % (m.group(1), "poll" if m.group(2) else "threads")
    return impl


def parse_run_id(rid):
    """Return a dict describing the run, or None if the run_id is not ours."""
    for kind, rx in RUN_PATTERNS:
        m = rx.match(rid)
        if not m:
            continue
        g = m.groupdict()
        d = {"run_id": rid, "warmup": g.get("w") is not None,
             "rep": int(g["rep"]) if g.get("rep") else 0}
        if kind == "part1":
            d.update(sweep="part1", smode=g["smode"], n=int(g["n"]), key=(g["smode"], int(g["n"])))
        elif kind == "p1cmp":
            d.update(sweep="part2", impl="part1_" + g["smode"], p=None, p_str="", k=1, io="-",
                     n=int(g["n"]), key=("part1_" + g["smode"], -1.0))
        elif kind == "case":
            d.update(sweep="part2", impl=g["impl"], p=float(g["p"]), p_str=g["p"], k=1, io="-",
                     key=(g["impl"], float(g["p"])))
        elif kind == "enh":
            impl = "enh_K%s%s" % (g["k"], "_poll" if g["io"] == "poll" else "")
            d.update(sweep="part2", impl=impl, p=float(g["p"]), p_str=g["p"], k=int(g["k"]), io=g["io"],
                     key=(impl, float(g["p"])))
        else:
            impl = "case2" if g["impl"] == "case2" else "enh_K%s%s" % (g["k"], "_poll" if g["io"] == "poll" else "")
            buf = g["buf"]
            d.update(sweep="buffers", impl=impl, buf=buf,
                     buf_val=0 if buf == "default" else int(buf), key=(impl, buf))
        return d
    return None


# ----------------------------------------------------------------------------
# loading
# ----------------------------------------------------------------------------

def classify(header):
    h = [c.strip() for c in header]
    for name, cols in (("server", SERVER_COLS), ("events", EVENT_COLS),
                       ("client", CLIENT_COLS), ("runs", RUNS_COLS)):
        if h[:len(cols)] == cols:
            return name
    return None


def load_raw(raw):
    """Read every recognised CSV below raw. Returns dict kind -> list of row dicts."""
    data = {"server": [], "events": [], "client": [], "runs": []}
    seen = {k: set() for k in data}
    nfiles = defaultdict(int)
    for dirpath, _dirs, files in os.walk(raw):
        for fn in sorted(files):
            if not fn.endswith(".csv"):
                continue
            path = os.path.join(dirpath, fn)
            try:
                with open(path, newline="") as f:
                    rd = csv.reader(f)
                    header = next(rd, None)
                    if header is None:
                        continue
                    kind = classify(header)
                    if kind is None:
                        continue
                    nfiles[kind] += 1
                    cols = [c.strip() for c in header]
                    for row in rd:
                        if not row or len(row) < len(cols):
                            continue
                        r = dict(zip(cols, row))
                        # The same file copied twice (e.g. from A by hand and by
                        # scp) must not count twice: drop exact duplicates.
                        sig = tuple(row)
                        if sig in seen[kind]:
                            continue
                        seen[kind].add(sig)
                        r["_file"] = path
                        data[kind].append(r)
            except (OSError, UnicodeDecodeError, csv.Error) as e:
                print("warning: cannot read %s: %s" % (path, e), file=sys.stderr)
    return data, nfiles


def ival(r, k, default=0):
    try:
        return int(r[k])
    except (KeyError, ValueError):
        return default


# ----------------------------------------------------------------------------
# topic statistics and the theory of design 4.4
# ----------------------------------------------------------------------------

def topic_chunks(dataset, topic):
    """Chunk sizes of the topic in the server's send order (files sorted by
    name, byte order like strcmp; hidden files skipped). None if not found."""
    if not dataset or not topic:
        return None
    d = os.path.join(dataset, topic)
    if not os.path.isdir(d):
        try:
            match = [x for x in os.listdir(dataset) if x.lower() == topic.lower()]
        except OSError:
            return None
        if not match:
            return None
        d = os.path.join(dataset, match[0])
    names = sorted(os.fsencode(x) for x in os.listdir(d))
    chunks = []
    for name in names:
        if name.startswith(b"."):
            continue
        path = os.path.join(os.fsencode(d), name)
        if not os.path.isfile(path):  # regular files only (follows symlinks, like stat())
            continue
        size = os.path.getsize(path)
        full, rest = divmod(size, CHUNK)
        chunks.extend([CHUNK] * full)
        if rest:
            chunks.append(rest)
    return chunks


def predictions(impl, p, n_clients, chunks, case1_retries=100):
    """Design 4.4 predictions for one (implementation, p). Per client values
    are multiplied by n_clients where the column is per run."""
    out = {}
    if chunks is None or p is None:
        return out
    N = len(chunks)
    T = sum(chunks)
    out["topic_chunks_N"] = N
    out["topic_MB_T"] = T / MB
    q = 1.0 - p
    P = q ** N  # probability that one attempt sees no fault
    if impl == "case1":
        if P <= 0.0:
            return out
        attempts = 1.0 / P
        out["pred_attempts_per_client"] = attempts
        out["pred_chunks_per_client"] = N if p == 0 else (1.0 - P) / (p * P)
        out["pred_faults"] = n_clients * (attempts - 1.0)
        # Exact expected data bytes per client for these chunk sizes: a failed
        # attempt sends every chunk before the faulted one plus a uniform
        # prefix of 0..c-1 bytes of it (deviation 9); the successful attempt
        # sends T. E = T + (1/P) * sum_j q^(j-1) p (S_(j-1) + (c_j - 1)/2)
        acc, s_prev, qj = 0.0, 0, 1.0
        if p > 0:
            for c in chunks:
                acc += qj * p * (s_prev + (c - 1) / 2.0)
                s_prev += c
                qj *= q
        exp_bytes = T + acc / P
        out["pred_overhead_pct"] = (exp_bytes - T) / T * 100.0 if T else 0.0
        p_dnf_client = (1.0 - P) ** case1_retries
        out["pred_dnf_run_prob"] = 1.0 - (1.0 - p_dnf_client) ** n_clients
    elif impl.startswith("case2") or impl.startswith("enh_"):
        out["pred_faults"] = n_clients * N * p / q if q > 0 else float("inf")
        out["pred_overhead_pct"] = 0.0
    return out


# ----------------------------------------------------------------------------
# per-run metrics
# ----------------------------------------------------------------------------

def merge_runs_rows(rows):
    """Combine runner rows of one run (controller row, or manual server/A/B)."""
    rows = sorted(rows, key=lambda r: ival(r, "start_wall_ms"))
    latest = {}
    for r in rows:  # a re-run appends a new row: keep the last one per role
        latest[r["role"]] = r
    info = {"n_expected": None, "ok": 0, "dnf": 0, "timeout": 0, "fail": 0, "have_clients": False,
            "checks": [], "notes": []}
    for role, r in latest.items():
        if role in ("ctl", "server") and r.get("n_clients"):
            info["n_expected"] = ival(r, "n_clients", None)
        if role in ("ctl", "A", "B"):
            if r.get("clients_ok", "") != "":
                info["have_clients"] = True
                for k, col in (("ok", "clients_ok"), ("dnf", "clients_dnf"),
                               ("timeout", "clients_timeout"), ("fail", "clients_fail")):
                    info[k] += ival(r, col)
        if r.get("check"):
            info["checks"].append(r["check"])
        if r.get("note"):
            info["notes"].append("%s: %s" % (role, r["note"].strip()))
    if info["n_expected"] is None and info["have_clients"]:
        info["n_expected"] = sum(ival(r, "n_clients") for role, r in latest.items() if role in ("A", "B"))
    return info


def run_metrics(rid, info, srows, erows, crows, rrows, chunks_by_topic, notes):
    m = {"run_id": rid, "sweep": info["sweep"]}
    rinfo = merge_runs_rows(rrows) if rrows else None

    # ---- server side
    topic = ""
    if srows:
        tc = defaultdict(int)
        for r in srows:
            if r["topic"] not in ("", "-"):
                tc[r["topic"]] += 1
        if tc:
            topic = max(tc, key=tc.get)
    if not topic and crows:
        topic = crows[0]["topic"]
    m["topic"] = topic
    real = [r for r in srows if r["kind"] != "-"] or srows  # ignore stray connections
    data = sum(ival(r, "data_bytes") for r in srows)
    wire = sum(ival(r, "wire_bytes") for r in srows)
    span = None
    if real:
        span_ns = max(ival(r, "end_ns") for r in real) - min(ival(r, "start_ns") for r in real)
        span = span_ns / 1e9 if span_ns > 0 else None
    m["conns"] = len(srows)
    m["span_s"] = span
    m["total_MB"] = data / MB if srows else None
    m["wire_MB"] = wire / MB if srows else None
    m["server_chunks"] = sum(ival(r, "chunks") for r in srows)
    m["throughput_MBps"] = data / MB / span if span else None
    m["faults"] = sum(1 for e in erows if e["event"] == "FAULT")
    m["server_faulted_conns"] = sum(ival(r, "faulted") for r in srows)
    m["resume_ok"] = sum(1 for e in erows if e["event"] == "RESUME_OK")
    m["rejects_server"] = sum(1 for e in erows if e["event"] in ("RESUME_REJECT", "FETCH_REJECT"))

    # ---- clients
    n_expected = None
    if rinfo and rinfo["n_expected"]:
        n_expected = rinfo["n_expected"]
    elif "n" in info:
        n_expected = info["n"]
    elif crows:
        n_expected = len(crows)
    m["n_clients"] = n_expected
    ok_rows = [r for r in crows if r["status"] == "OK"]
    dnf_rows = [r for r in crows if r["status"] == "DNF"]
    fail_rows = [r for r in crows if r["status"] not in ("OK", "DNF")]
    timeouts = rinfo["timeout"] if rinfo else 0
    m["clients_ok"] = len(ok_rows)
    m["dnf_clients"] = len(dnf_rows) + timeouts
    m["fail_clients"] = len(fail_rows) + (rinfo["fail"] if rinfo and not crows else 0)

    chunks = chunks_by_topic.get(topic)
    T = sum(chunks) if chunks is not None else None
    useful = None
    status = "OK"
    if crows:
        useful = sum(ival(r, "useful_bytes") for r in ok_rows)
        if n_expected and len(ok_rows) < n_expected:
            status = "DNF" if (dnf_rows or timeouts) else "FAIL"
        if not srows:
            status = "FAIL"
            notes.append("%s: no server rows" % rid)
    elif rinfo and rinfo["have_clients"]:
        # client CSVs missing (e.g. not copied from A/B): trust the exit codes
        if rinfo["ok"] < (n_expected or 0):
            status = "DNF" if (rinfo["dnf"] or rinfo["timeout"]) else "FAIL"
        if T is not None and status == "OK":
            useful = T * (n_expected or 0)
        notes.append("%s: no client CSV rows, %s" % (
            rid, "useful bytes = topic size x clients" if useful is not None else "useful bytes unknown"))
        m["clients_ok"] = rinfo["ok"]
        m["dnf_clients"] = rinfo["dnf"] + rinfo["timeout"]
        m["fail_clients"] = rinfo["fail"]
    else:
        status = "NODATA"
        notes.append("%s: no client CSV rows and no runner exit codes" % rid)
    m["status"] = status

    m["useful_MB"] = useful / MB if useful is not None else None
    if useful and srows:
        m["redundant_MB"] = (data - useful) / MB
        m["overhead_pct"] = (data - useful) / useful * 100.0
        m["wire_overhead_pct"] = (wire - useful) / useful * 100.0
        m["goodput_MBps"] = useful / MB / span if span else None
    else:
        for k in ("redundant_MB", "overhead_pct", "wire_overhead_pct", "goodput_MBps"):
            m[k] = None

    compl = [(ival(r, "end_ns") - ival(r, "start_ns")) / 1e9 for r in ok_rows]
    m["compl_mean_s"] = statistics.mean(compl) if compl else None
    m["compl_max_s"] = max(compl) if compl else None
    gp = [ival(r, "useful_bytes") / MB / c for r, c in zip(ok_rows, compl) if c > 0]
    m["client_goodput_MBps"] = statistics.mean(gp) if gp else None
    nc = len(crows) or None
    m["reconnects"] = sum(ival(r, "reconnects") for r in crows) if crows else None
    m["attempts_per_client"] = sum(ival(r, "attempts") for r in crows) / nc if nc else None
    m["rejects"] = sum(ival(r, "rejects") for r in crows) if crows else None
    m["data_rx_MB"] = sum(ival(r, "data_bytes_rx") for r in crows) / MB if crows else None
    m["chunks_per_client"] = m["server_chunks"] / n_expected if n_expected else None
    m["check"] = " ".join(rinfo["checks"]) if rinfo else ""
    if rinfo:
        for c in rinfo["checks"]:
            if "MISMATCH" in c or "NODIR" in c:
                notes.append("%s: output check %s" % (rid, c))
    # Consistency check (design 6.7): reconnects should equal injected faults,
    # apart from time-outs.
    if crows and status == "OK" and m["reconnects"] is not None and info["sweep"] != "part1":
        f, rc = m["faults"], m["reconnects"]
        if abs(f - rc) > max(2, 0.1 * max(f, rc)):
            notes.append("%s: %d faults but %d reconnects" % (rid, f, rc))
    return m


# ----------------------------------------------------------------------------
# aggregation and output
# ----------------------------------------------------------------------------

MEAN_METRICS_P1 = ["throughput_MBps", "total_MB", "wire_MB", "span_s", "compl_mean_s", "compl_max_s",
                   "client_goodput_MBps"]
MEAN_METRICS_P2 = ["throughput_MBps", "goodput_MBps", "client_goodput_MBps", "span_s", "total_MB",
                   "useful_MB", "redundant_MB", "overhead_pct", "wire_MB", "wire_overhead_pct",
                   "compl_mean_s", "compl_max_s", "faults", "reconnects", "attempts_per_client",
                   "chunks_per_client", "rejects"]
MEAN_METRICS_BUF = ["throughput_MBps", "goodput_MBps", "client_goodput_MBps", "span_s",
                    "compl_mean_s", "compl_max_s", "wire_overhead_pct"]
PRED_COLS = ["topic_chunks_N", "topic_MB_T", "pred_attempts_per_client", "pred_chunks_per_client",
             "pred_faults", "pred_overhead_pct", "pred_dnf_run_prob"]


def mean_sd(vals):
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], None
    return statistics.mean(vals), statistics.stdev(vals)


def aggregate(runs, metrics):
    """runs: list of per-run metric dicts of one configuration."""
    ok = [r for r in runs if r["status"] == "OK"]
    out = {"runs": len(runs), "ok_runs": len(ok),
           "dnf_runs": sum(1 for r in runs if r["status"] == "DNF"),
           "fail_runs": sum(1 for r in runs if r["status"] not in ("OK", "DNF")),
           "dnf_clients": sum(r["dnf_clients"] or 0 for r in runs)}
    for k in metrics:
        mu, sd = mean_sd([r.get(k) for r in ok])
        out[k + "_mean"] = mu
        out[k + "_sd"] = sd
    return out


def fmt_num(v):
    if v is None or v == "":
        return ""
    if isinstance(v, float):
        if math.isinf(v):
            return "inf"
        if v == 0:
            return "0"
        a = abs(v)
        if a >= 1e7 or a < 1e-4:
            return "%.4g" % v
        return "%.6g" % v
    return str(v)


def write_csv(path, cols, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([fmt_num(r.get(c)) for c in cols])


def pm(r, k, digits=None):
    """'mean ± sd' for markdown."""
    mu, sd = r.get(k + "_mean"), r.get(k + "_sd")
    if mu is None:
        return "–"
    if digits is None:  # same number of decimals for mean and sd, chosen by the mean
        a = abs(mu)
        digits = 0 if a >= 1000 else 1 if a >= 100 else 2 if a >= 1 else 3
        if 0 < a < 0.001:
            return "%.2g" % mu + (" ± %.2g" % sd if sd is not None else "")
    return "%.*f" % (digits, mu) + (" ± %.*f" % (digits, sd) if sd is not None else "")


def num(v, d=2):
    if v is None or v == "":
        return "–"
    if isinstance(v, float) and math.isinf(v):
        return "∞"
    if isinstance(v, float) and (abs(v) >= 1e5 or (0 < abs(v) < 1e-3)):
        return "%.2g" % v
    return "%.*f" % (d, v) if isinstance(v, float) else str(v)


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)


def impl_order(impl):
    if impl == "part1_multi":
        return (0, 0, 0)
    if impl == "case1":
        return (1, 0, 0)
    if impl == "case2":
        return (2, 0, 0)
    m = re.match(r"enh_K(\d+)(_poll)?$", impl)
    if m:
        return (3, 1 if m.group(2) else 0, int(m.group(1)))
    return (9, 0, 0)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(os.path.dirname(here))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw", default=os.path.join(repo, "Results", "raw"), help="raw CSV folder (default Results/raw)")
    ap.add_argument("--out", default=os.path.join(repo, "Results"), help="output folder (default Results)")
    ap.add_argument("--dataset", default=None,
                    help="topics_root_dir, for N, T and the design 4.4 predictions (default dataset/raw-img if present)")
    ap.add_argument("--case1-retries", type=int, default=100, help="Case 1 max_retries used (for P(DNF))")
    args = ap.parse_args()
    if args.dataset is None:
        d = os.path.join(repo, "dataset", "raw-img")
        args.dataset = d if os.path.isdir(d) else None

    if not os.path.isdir(args.raw):
        sys.exit("error: raw folder %s not found" % args.raw)
    data, nfiles = load_raw(args.raw)
    print("read %d server, %d events, %d client and %d runs CSV files from %s" % (
        nfiles["server"], nfiles["events"], nfiles["client"], nfiles["runs"], args.raw))

    by_run = defaultdict(lambda: {"server": [], "events": [], "client": [], "runs": []})
    for kind, rows in data.items():
        for r in rows:
            by_run[r["run_id"]][kind].append(r)

    notes = []
    warmups = 0
    unknown = []
    runs = []
    chunks_by_topic = {}
    for rid in sorted(by_run):
        info = parse_run_id(rid)
        if info is None:
            unknown.append(rid)
            continue
        if info["warmup"]:
            warmups += 1
            continue
        g = by_run[rid]
        for t in {r["topic"] for r in g["server"] + g["client"]}:
            if t not in chunks_by_topic and t not in ("", "-"):
                chunks_by_topic[t] = topic_chunks(args.dataset, t)
        m = run_metrics(rid, info, g["server"], g["events"], g["client"], g["runs"], chunks_by_topic, notes)
        m["_info"] = info
        runs.append(m)
    if unknown:
        notes.append("ignored %d rows/run ids not made by the runners: %s" % (
            len(unknown), ", ".join(unknown[:5]) + (" ..." if len(unknown) > 5 else "")))
    print("%d measured runs (%d warm-up runs ignored)" % (len(runs), warmups))
    os.makedirs(args.out, exist_ok=True)

    # ---- all_runs.csv
    run_cols = ["run_id", "sweep", "status", "topic", "n_clients", "clients_ok", "dnf_clients", "fail_clients",
                "conns", "span_s", "throughput_MBps", "goodput_MBps", "total_MB", "useful_MB", "redundant_MB",
                "overhead_pct", "wire_MB", "wire_overhead_pct", "compl_mean_s", "compl_max_s",
                "client_goodput_MBps", "faults", "server_faulted_conns", "resume_ok", "reconnects",
                "attempts_per_client", "chunks_per_client", "rejects", "rejects_server", "data_rx_MB", "check"]
    write_csv(os.path.join(args.out, "all_runs.csv"), run_cols, runs)

    md = ["# FDS experiment summary", "",
          "Generated by `tools/exp/analyze.py` from `%s`. MB = 10^6 bytes, MB/s = 10^6 bytes/s. "
          "Values are mean ± sample sd over the repetitions that completed (OK); DNF/failed "
          "repetitions are counted but excluded from the means. Throughput and goodput use the "
          "server's clock: bytes ÷ (last connection end − first connection start)." % os.path.relpath(args.raw, args.out),
          ""]

    # ---- Part I
    groups = defaultdict(list)
    for m in runs:
        if m["sweep"] == "part1":
            groups[m["_info"]["key"]].append(m)
    p1_rows = []
    for (smode, n) in sorted(groups, key=lambda k: (k[0] != "single", k[1])):
        g = groups[(smode, n)]
        a = aggregate(g, MEAN_METRICS_P1)
        a.update(server=smode, n_clients=n, topic=g[0]["topic"])
        for k in ("mean", "sd"):
            v = a["throughput_MBps_" + k]
            a["throughput_Mbps_" + k] = v * 8 if v is not None else None
        p1_rows.append(a)
    if p1_rows:
        cols = ["server", "n_clients", "topic", "runs", "ok_runs", "dnf_runs", "fail_runs"]
        for k in ["throughput_MBps", "throughput_Mbps"] + MEAN_METRICS_P1[1:]:
            cols += [k + "_mean", k + "_sd"]
        write_csv(os.path.join(args.out, "Part1_summary.csv"), cols, p1_rows)
        md += ["## Part I: single- vs multi-threaded server", "",
               "Topic `%s`; N clients in total, split evenly over PC A and PC B." % p1_rows[0]["topic"], ""]
        md.append(md_table(
            ["Server", "N", "OK/runs", "Aggregate throughput (MB/s)", "(Mbit/s)", "Mean completion (s)",
             "Max completion (s)", "Per-client goodput (MB/s)"],
            [[r["server"], r["n_clients"], "%d/%d" % (r["ok_runs"], r["runs"]), pm(r, "throughput_MBps"),
              pm(r, "throughput_Mbps", 0), pm(r, "compl_mean_s"), pm(r, "compl_max_s"),
              pm(r, "client_goodput_MBps")] for r in p1_rows]))
        md.append("")

    # ---- Part II
    groups = defaultdict(list)
    for m in runs:
        if m["sweep"] == "part2":
            groups[m["_info"]["key"]].append(m)
    p2_rows = []
    for key in sorted(groups, key=lambda k: (impl_order(k[0]), k[1])):
        g = groups[key]
        info = g[0]["_info"]
        a = aggregate(g, MEAN_METRICS_P2)
        n_cl = g[0]["n_clients"] or 0
        a.update(impl=info["impl"], label=impl_label(info["impl"]), k=info["k"], io=info["io"],
                 p=info["p"] if info["p"] is not None else "", p_str=info["p_str"], topic=g[0]["topic"],
                 n_clients=n_cl)
        a.update(predictions(info["impl"], info["p"], n_cl, chunks_by_topic.get(g[0]["topic"]),
                             args.case1_retries))
        p2_rows.append(a)
    if p2_rows:
        cols = ["impl", "label", "k", "io", "p", "topic", "n_clients", "runs", "ok_runs", "dnf_runs",
                "fail_runs", "dnf_clients"]
        for k in MEAN_METRICS_P2:
            cols += [k + "_mean", k + "_sd"]
        cols += PRED_COLS
        write_csv(os.path.join(args.out, "Part2_summary.csv"), cols, p2_rows)
        topic = p2_rows[0]["topic"]
        ch = chunks_by_topic.get(topic)
        md += ["## Part II: fault injection (p = per-64-KB-chunk fault probability)", "",
               "Topic `%s`, %s clients per run%s." % (
                   topic, p2_rows[0]["n_clients"],
                   ", N = %d chunks, T = %.2f MB" % (len(ch), sum(ch) / MB) if ch else
                   " (run analyze.py with --dataset for N, T and the design 4.4 predictions)"), ""]
        for impl in sorted({r["impl"] for r in p2_rows}, key=impl_order):
            rows = [r for r in p2_rows if r["impl"] == impl]
            md += ["### " + impl_label(impl), ""]
            hdr = ["p", "OK/runs (DNF)", "Throughput (MB/s)", "Goodput (MB/s)", "Total (MB)", "Useful (MB)",
                   "Redundant (MB)", "Overhead (%)", "Wire overhead (%)", "Mean completion (s)",
                   "Faults", "Pred. faults", "Reconnects"]
            if impl == "case1":
                hdr += ["Attempts/client", "Pred. attempts", "Pred. overhead (%)", "Pred. P(DNF run)"]
            body = []
            for r in rows:
                line = [r["p_str"] or "–", "%d/%d (%d)" % (r["ok_runs"], r["runs"], r["dnf_runs"]),
                        pm(r, "throughput_MBps"), pm(r, "goodput_MBps"), pm(r, "total_MB"), pm(r, "useful_MB"),
                        pm(r, "redundant_MB"), pm(r, "overhead_pct"), pm(r, "wire_overhead_pct"),
                        pm(r, "compl_mean_s"), pm(r, "faults"), num(r.get("pred_faults"), 1), pm(r, "reconnects")]
                if impl == "case1":
                    line += [pm(r, "attempts_per_client"), num(r.get("pred_attempts_per_client"), 2),
                             num(r.get("pred_overhead_pct"), 1), num(r.get("pred_dnf_run_prob"), 3)]
                body.append(line)
            md += [md_table(hdr, body), ""]
        # comparison at p = 0 with Part I multi at the same client count
        base = [r for r in p2_rows if r["impl"] == "part1_multi"]
        src = "Part I multi-threaded server, same topic and client count (`--part1-compare`)"
        if not base:
            base = [r for r in p1_rows if r["server"] == "multi" and r["n_clients"] == p2_rows[0]["n_clients"]]
            src = "Part I sweep, multi-threaded, N = %s (note: Part I topic `%s`)" % (
                p2_rows[0]["n_clients"], base[0]["topic"] if base else "?")
        zero = [r for r in p2_rows if r["p"] == 0.0]
        if zero:
            md += ["### p = 0 compared with Part I", "", "Reference: " + src + ".", ""]
            body = []
            if base:
                body.append(["Part I multi", pm(base[0], "throughput_MBps"), pm(base[0], "compl_mean_s"), "100 %"])
            bt = base[0].get("throughput_MBps_mean") if base else None
            for r in zero:
                rel = "%.0f %%" % (r["throughput_MBps_mean"] / bt * 100) if bt and r["throughput_MBps_mean"] else "–"
                body.append([r["label"], pm(r, "throughput_MBps"), pm(r, "compl_mean_s"), rel])
            md += [md_table(["Implementation", "Throughput (MB/s)", "Mean completion (s)", "vs Part I"], body), ""]

    # ---- buffers
    groups = defaultdict(list)
    for m in runs:
        if m["sweep"] == "buffers":
            groups[m["_info"]["key"]].append(m)
    b_rows = []
    for key in sorted(groups, key=lambda k: (impl_order(k[0]), groups[k][0]["_info"]["buf_val"])):
        g = groups[key]
        info = g[0]["_info"]
        a = aggregate(g, MEAN_METRICS_BUF)
        a.update(impl=info["impl"], label=impl_label(info["impl"]), buf=info["buf"], buf_bytes=info["buf_val"],
                 topic=g[0]["topic"], n_clients=g[0]["n_clients"])
        b_rows.append(a)
    if b_rows:
        cols = ["impl", "label", "buf", "buf_bytes", "topic", "n_clients", "runs", "ok_runs", "dnf_runs", "fail_runs"]
        for k in MEAN_METRICS_BUF:
            cols += [k + "_mean", k + "_sd"]
        write_csv(os.path.join(args.out, "Buffers_summary.csv"), cols, b_rows)
        md += ["## Socket buffer sizes (p = 0)", "",
               "FDS_SNDBUF = FDS_RCVBUF = the given size on the server and the clients; "
               "`default` = not set (kernel autotuning). The kernel caps the sizes at "
               "net.core.wmem_max / rmem_max (see sysinfo_*.txt).", ""]
        md.append(md_table(["Implementation", "Buffer (bytes)", "OK/runs", "Throughput (MB/s)", "Goodput (MB/s)",
                            "Mean completion (s)"],
                           [[r["label"], r["buf"], "%d/%d" % (r["ok_runs"], r["runs"]), pm(r, "throughput_MBps"),
                             pm(r, "goodput_MBps"), pm(r, "compl_mean_s")] for r in b_rows]))
        md.append("")

    # ---- checks
    md += ["## Data checks", "",
           "- %d measured runs analysed, %d warm-up runs ignored." % (len(runs), warmups)]
    bad = [m for m in runs if m["status"] != "OK"]
    if bad:
        md.append("- Runs not counted in the means: " + ", ".join(
            "%s (%s)" % (m["run_id"], m["status"]) for m in bad[:40]) + (" ..." if len(bad) > 40 else ""))
    for n_ in notes[:60]:
        md.append("- " + n_)
    if len(notes) > 60:
        md.append("- ... %d more notes" % (len(notes) - 60))
    if not notes and not bad:
        md.append("- No problems found.")
    md.append("")
    with open(os.path.join(args.out, "summary.md"), "w") as f:
        f.write("\n".join(md))

    for name, rows in (("Part1_summary.csv", p1_rows), ("Part2_summary.csv", p2_rows),
                       ("Buffers_summary.csv", b_rows)):
        print("%-20s %s" % (name, "%d configurations" % len(rows) if rows else "no data (not written)"))
    print("wrote %s and summary.md, all_runs.csv in %s" % (
        ", ".join(n for n, r in (("Part1", p1_rows), ("Part2", p2_rows), ("Buffers", b_rows)) if r) or "nothing",
        args.out))
    if bad:
        print("%d run(s) were not OK; see 'Data checks' in summary.md" % len(bad))


if __name__ == "__main__":
    main()
