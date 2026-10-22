#!/usr/bin/env python3
"""
plot.py - draw the report graphs from the summary CSVs of analyze.py.

usage: python3 tools/exp/plot.py [--results Results]

Reads Results/Part1_summary.csv, Part2_summary.csv and Buffers_summary.csv and
writes PNG (150 dpi) + PDF files into the same folder, named as the guidelines
suggest (Partname_CaseName_throughput.png / _overhead.png):

  Part1_SingleVsMulti_throughput    aggregate throughput vs N, single vs multi
  Part1_SingleVsMulti_completion    mean (and max) per-client completion vs N
  Part2_Case1_throughput / _overhead
  Part2_Case2_throughput / _overhead
  Part2_Case2_Enhanced_throughput / _overhead   one line per K
  Part2_Comparison_throughput / _overhead / _completion   all cases vs p,
                                    with the Part I multi-threaded reference
  Part2_ThroughputVsOverhead        goodput vs overhead, one point per (case, p)
  Part2_Enhanced_ThreadsVsPoll      blocking threads vs non-blocking poll client
  Buffers_throughput                socket-buffer experiment

Conventions: p on a linear axis (a per-connection probability), error bars = sample
sd over the repetitions, MB = 10^6 bytes. A configuration in which every
repetition was DNF is drawn as an "x" at the top of the plot with "DNF";
partly-DNF points carry a "k/n DNF" note. Each case keeps its colour and
marker in every chart (colours validated for colour-vision deficiencies).
"""

import argparse
import csv
import math
import os
import sys

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
except ImportError:
    print("matplotlib is not installed: pip/dnf install python3-matplotlib", file=sys.stderr)
    sys.exit(1)

# ---- style: one colour + marker per implementation, in every chart --------
INK = "#0b0b0b"
INK2 = "#52514e"      # secondary text, reference lines
GRID = "#e4e3df"
SURFACE = "#fcfcfb"
COLOR = {
    "case1": "#2a78d6",          # blue
    "case2": "#eb6834",          # orange
    "enh_K1": "#5cc29b",         # Enhanced: one teal ramp, light (K=1) -> dark (K=8)
    "enh_K2": "#26a072",
    "enh_K4": "#14805a",
    "enh_K8": "#0b5a3f",
    "enh_K4_poll": "#4a3aa7",    # deep violet
    "part1_multi": INK2,
    "single": "#e34948",         # Part I servers
    "multi": INK2,
}
MARKER = {"case1": "o", "case2": "s", "enh_K1": "v", "enh_K2": "<", "enh_K4": "^", "enh_K8": ">",
          "enh_K4_poll": "D", "single": "o", "multi": "s"}
MAIN = ["case1", "case2", "enh_K4", "enh_K4_poll"]

plt.rcParams.update({
    "font.size": 11, "axes.titlesize": 12, "axes.labelsize": 11, "legend.fontsize": 9.5,
    "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.8, "grid.linestyle": "-", "axes.axisbelow": True,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "lines.linewidth": 1.8, "lines.markersize": 6.5, "errorbar.capsize": 3,
})


def color(impl):
    if impl in COLOR:
        return COLOR[impl]
    if impl.endswith("_poll"):
        return COLOR["enh_K4_poll"]
    return COLOR["enh_K4"] if impl.startswith("enh_") else INK2


def marker(impl):
    return MARKER.get(impl, "D" if impl.endswith("_poll") else "^")


def label(impl):
    if impl == "case1":
        return "Case 1 (restart)"
    if impl == "case2":
        return "Case 2 (resume)"
    if impl == "part1_multi":
        return "Part I multi"
    if impl.startswith("enh_K"):
        k = impl[5:].split("_")[0]
        return "Enhanced K=%s%s" % (k, " poll" if impl.endswith("_poll") else "")
    return impl


# ---- data -------------------------------------------------------------------

def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k, v in list(r.items()):
            if k in ("impl", "label", "io", "topic", "server", "buf"):
                continue
            try:
                r[k] = float(v) if v not in ("", "inf") else (math.inf if v == "inf" else None)
            except (TypeError, ValueError):
                pass
    return rows


def series(rows, impl):
    return sorted((r for r in rows if r["impl"] == impl and r.get("p") is not None), key=lambda r: r["p"])


def v(r, k):
    x = r.get(k)
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else x


# ---- axes helpers -------------------------------------------------------------

def p_axis(ax, ps):
    """Linear x axis for p (a per-connection probability, deviation 28: the
    sweep spans 0 .. 0.9, so a log axis is not needed)."""
    ticks = sorted(set(ps))
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _pos: "0" if x == 0 else ("%g" % x)))
    hi = max(ps) if ps else 1.0
    ax.set_xlim(-0.03 * hi, hi * 1.06)
    ax.set_xlabel("fault probability p (per connection)")


def n_axis(ax, ns):
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_locator(FixedLocator(sorted(set(ns))))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _pos: "%d" % x))
    ax.set_xlabel("number of clients N (split over PC A and PC B)")


class DnfMarks:
    """Collects all-DNF configurations; drawn at the top once the y range is final."""

    def __init__(self):
        self.items = []

    def add(self, x, impl):
        self.items.append((x, impl))

    def draw(self, ax):
        if not self.items:
            return
        if ax.get_yscale() == "linear":  # keep a free row at the top for the DNF marks
            lo, hi = ax.get_ylim()
            ax.set_ylim(lo, lo + (hi - lo) / 0.84)
        tr = ax.get_xaxis_transform()  # x in data units, y in axes fraction
        done = set()
        for x, impl in self.items:
            ax.plot([x], [0.95], transform=tr, marker="x", ms=9, mew=2.2, color=color(impl), ls="none",
                    clip_on=False, zorder=5)
            if x not in done:
                ax.text(x, 0.985, "DNF", transform=tr, ha="center", va="bottom", fontsize=8.5, color=INK2,
                        clip_on=False)
                done.add(x)


def plot_vs_p(ax, rows, impl, key, dnf, ls="-", lab=None, hollow=False, color_=None, lw=None, note_dnf=True):
    """mean +- sd of key vs p for one implementation; DNF handling."""
    pts = series(rows, impl)
    xs, ys, es = [], [], []
    c = color_ or color(impl)
    for r in pts:
        mu = v(r, key + "_mean")
        if (r.get("ok_runs") or 0) == 0 or mu is None:
            if (r.get("dnf_runs") or 0) > 0:
                dnf.add(r["p"], impl)
            continue
        xs.append(r["p"])
        ys.append(mu)
        es.append(v(r, key + "_sd") or 0.0)
        if note_dnf and (r.get("dnf_runs") or 0) > 0:
            ax.annotate("%d/%d DNF" % (r["dnf_runs"], r["runs"]), (r["p"], mu), textcoords="offset points",
                        xytext=(6, 6), fontsize=8, color=INK2)
    if not xs:
        return None
    ax.errorbar(xs, ys, yerr=es, color=c, ls=ls, lw=lw or plt.rcParams["lines.linewidth"], marker=marker(impl),
                mfc=SURFACE if hollow else c, mec=c, mew=1.5, elinewidth=1, label=lab or label(impl), zorder=3)
    return min(y - e for y, e in zip(ys, es)), max(y + e for y, e in zip(ys, es))


def zero_based(ax, extents, min_top=0.05):
    """Linear y axis from 0 (or the lowest value if negative) to 25 % above the data."""
    ext = [e for e in extents if e]
    if not ext:
        return
    lo = min(e[0] for e in ext)
    hi = max(e[1] for e in ext)
    ax.set_ylim(min(0.0, lo * 1.25), max(hi * 1.25, min_top))


def shared_legend(fig, ax):
    """One legend for a multi-panel figure, below the panels."""
    h, lab = ax.get_legend_handles_labels()
    if len(h) >= 2:
        fig.legend(h, lab, loc="lower center", ncol=min(len(h), 5), frameon=False, bbox_to_anchor=(0.5, -0.06))


def reference_line(ax, ref, key, text):
    """Horizontal Part I multi-threaded reference with a direct label."""
    if ref is None or v(ref, key + "_mean") is None:
        return
    y = ref[key + "_mean"]
    ax.axhline(y, color=INK2, lw=1.1, zorder=2)
    ax.text(1.0, y, " " + text, transform=ax.get_yaxis_transform(), va="center", ha="left", fontsize=8.5,
            color=INK2, clip_on=False)


def save(fig, out, name, made):
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(out, "%s.%s" % (name, ext)), dpi=150, bbox_inches="tight")
    plt.close(fig)
    made.append(name)


def legend(ax, **kw):
    h, _l = ax.get_legend_handles_labels()
    if len(h) >= 2:
        ax.legend(frameon=False, **kw)


# ---- the charts -------------------------------------------------------------------

def part1_charts(rows, out, made):
    if not rows:
        return
    ns = [r["n_clients"] for r in rows]
    topic = rows[0].get("topic", "")
    for key, name, ylab, title in (
            ("throughput_MBps", "Part1_SingleVsMulti_throughput", "aggregate throughput (MB/s)",
             "Part I: server aggregate throughput vs number of clients"),
            ("compl_mean_s", "Part1_SingleVsMulti_completion", "per-client completion time (s)",
             "Part I: per-client completion time vs number of clients")):
        fig, ax = plt.subplots(figsize=(7, 4.4))
        for smode in ("single", "multi"):
            pts = sorted((r for r in rows if r["server"] == smode and v(r, key + "_mean") is not None),
                         key=lambda r: r["n_clients"])
            if not pts:
                continue
            x = [r["n_clients"] for r in pts]
            name_ = "single-threaded" if smode == "single" else "multi-threaded"
            ax.errorbar(x, [r[key + "_mean"] for r in pts], yerr=[v(r, key + "_sd") or 0 for r in pts],
                        color=COLOR[smode], marker=MARKER[smode], mec=COLOR[smode], elinewidth=1,
                        label=name_ + (" (mean)" if key.startswith("compl") else ""), zorder=3)
            if key.startswith("compl"):
                ax.plot(x, [v(r, "compl_max_s_mean") for r in pts], color=COLOR[smode], lw=1.1, ls=(0, (4, 2)),
                        label=name_ + " (slowest client)")
        n_axis(ax, ns)
        ax.set_ylabel(ylab)
        ax.set_ylim(bottom=0)
        ax.set_title(title + (" (topic %s)" % topic if topic else ""), loc="left")
        legend(ax, loc="best")
        save(fig, out, name, made)


def part2_case_charts(rows, ref, out, made):
    """Per-case throughput and overhead charts (Case 1, Case 2, Enhanced)."""
    ps = sorted({r["p"] for r in rows if r.get("p") is not None})
    ref_txt = "Part I multi"

    for impl, name in (("case1", "Part2_Case1"), ("case2", "Part2_Case2")):
        if not series(rows, impl):
            continue
        # throughput: all bytes sent (dashed, hollow) and useful bytes (solid)
        fig, ax = plt.subplots(figsize=(7, 4.4))
        dnf = DnfMarks()
        plot_vs_p(ax, rows, impl, "throughput_MBps", dnf, ls=(0, (4, 2)), hollow=True,
                  lab="throughput (all data bytes sent)")
        plot_vs_p(ax, rows, impl, "goodput_MBps", DnfMarks(), lab="goodput (useful bytes)", note_dnf=False)
        reference_line(ax, ref, "throughput_MBps", ref_txt)
        p_axis(ax, ps)
        ax.set_ylim(bottom=0)
        ax.set_ylabel("MB/s (server clock)")
        ax.set_title("%s: throughput and goodput vs p" % label(impl), loc="left")
        legend(ax, loc="lower left")
        dnf.draw(ax)
        save(fig, out, name + "_throughput", made)

        # overhead
        if impl == "case1":
            fig, ax = plt.subplots(figsize=(7, 4.4))
            dnf = DnfMarks()
            plot_vs_p(ax, rows, impl, "overhead_pct", dnf, lab="measured (redundant ÷ useful)")
            pred = [(r["p"], r["pred_overhead_pct"]) for r in series(rows, impl) if v(r, "pred_overhead_pct") is not None]
            if pred:
                ax.plot([x for x, _ in pred], [y for _, y in pred], color=INK2, lw=1.1, ls=(0, (1, 2)),
                        label="predicted (p/(1−p) × T/2)", zorder=2)
            p_axis(ax, ps)
            ax.set_yscale("symlog", linthresh=1.0)
            ax.set_ylim(bottom=0)
            ax.set_ylabel("overhead (%)")
            ax.set_title("Case 1: redundant data vs p", loc="left")
            legend(ax, loc="upper left")
            dnf.draw(ax)
        else:
            fig, axs = plt.subplots(1, 2, figsize=(10, 4.2))
            for ax, key, t in ((axs[0], "overhead_pct", "redundant data bytes"),
                               (axs[1], "wire_overhead_pct", "wire overhead (headers + control)")):
                dnf = DnfMarks()
                zero_based(ax, [plot_vs_p(ax, rows, impl, key, dnf)])
                p_axis(ax, ps)
                ax.set_ylabel("% of useful bytes")
                ax.set_title(t, loc="left")
                dnf.draw(ax)
            fig.suptitle("Case 2: overhead vs p", x=0.01, ha="left", fontsize=12)
        save(fig, out, name + "_overhead", made)

    # Enhanced: one line per K (threads), K=4 poll dashed
    enh = sorted({r["impl"] for r in rows if r["impl"].startswith("enh_")},
                 key=lambda s: (s.endswith("_poll"), int(s[5:].split("_")[0])))
    if enh:
        fig, ax = plt.subplots(figsize=(7, 4.4))
        dnf = DnfMarks()
        for impl in enh:
            plot_vs_p(ax, rows, impl, "throughput_MBps", dnf, ls=(0, (4, 2)) if impl.endswith("_poll") else "-")
        reference_line(ax, ref, "throughput_MBps", ref_txt)
        p_axis(ax, ps)
        ax.set_ylim(bottom=0)
        ax.set_ylabel("aggregate throughput (MB/s)")
        ax.set_title("Case 2 Enhanced: throughput vs p for K parallel connections", loc="left")
        legend(ax, loc="lower left", ncol=2)
        dnf.draw(ax)
        save(fig, out, "Part2_Case2_Enhanced_throughput", made)

        fig, axs = plt.subplots(1, 2, figsize=(10, 4.2))
        for ax, key, t in ((axs[0], "overhead_pct", "redundant data bytes"),
                           (axs[1], "wire_overhead_pct", "wire overhead (headers + control)")):
            dnf = DnfMarks()
            zero_based(ax, [plot_vs_p(ax, rows, impl, key, dnf, ls=(0, (4, 2)) if impl.endswith("_poll") else "-")
                            for impl in enh])
            p_axis(ax, ps)
            ax.set_ylabel("% of useful bytes")
            ax.set_title(t, loc="left")
            dnf.draw(ax)
        shared_legend(fig, axs[1])
        fig.suptitle("Case 2 Enhanced: overhead vs p", x=0.01, ha="left", fontsize=12)
        save(fig, out, "Part2_Case2_Enhanced_overhead", made)


def comparison_charts(rows, ref, ref_note, out, made):
    impls = [i for i in MAIN if series(rows, i)]
    if not impls:
        return
    ps = sorted({r["p"] for r in rows if r.get("p") is not None})

    fig, axs = plt.subplots(1, 2, figsize=(11, 4.4), sharey=True)
    for ax, key, t in ((axs[0], "throughput_MBps", "aggregate throughput (all data bytes)"),
                       (axs[1], "goodput_MBps", "goodput (useful bytes only)")):
        dnf = DnfMarks()
        for impl in impls:
            plot_vs_p(ax, rows, impl, key, dnf)
        reference_line(ax, ref, "throughput_MBps", "Part I multi")
        p_axis(ax, ps)
        ax.set_ylim(bottom=0)
        ax.set_title(t, loc="left")
        dnf.draw(ax)
    axs[0].set_ylabel("MB/s (server clock)")
    shared_legend(fig, axs[0])
    fig.suptitle("Part II: all cases vs p" + ref_note, x=0.01, ha="left", fontsize=12)
    save(fig, out, "Part2_Comparison_throughput", made)

    fig, ax = plt.subplots(figsize=(7, 4.4))
    dnf = DnfMarks()
    for impl in impls:
        plot_vs_p(ax, rows, impl, "overhead_pct", dnf)
    p_axis(ax, ps)
    ax.set_yscale("symlog", linthresh=1.0)
    ax.set_ylim(bottom=0)
    ax.set_ylabel("overhead (% redundant of useful bytes)")
    ax.set_title("Part II: redundant data vs p", loc="left")
    legend(ax, loc="upper left")
    dnf.draw(ax)
    save(fig, out, "Part2_Comparison_overhead", made)

    fig, ax = plt.subplots(figsize=(7, 4.4))
    dnf = DnfMarks()
    for impl in impls:
        plot_vs_p(ax, rows, impl, "compl_mean_s", dnf)
    reference_line(ax, ref, "compl_mean_s", "Part I multi")
    p_axis(ax, ps)
    ax.set_yscale("log")
    ax.set_ylabel("mean per-client completion time (s)")
    ax.set_title("Part II: completion time vs p" + ref_note, loc="left")
    legend(ax, loc="upper left")
    dnf.draw(ax)
    save(fig, out, "Part2_Comparison_completion", made)

    # goodput vs overhead: one point per (case, p). A thin line joins each
    # case's points in order of p. Points are labelled with p where the case
    # spreads out (Case 1); cases that stay near 0 % overhead only get their
    # largest-p point labelled, or the labels would pile up.
    fig, ax = plt.subplots(figsize=(7.5, 5))
    for impl in impls:
        pts = [r for r in series(rows, impl) if v(r, "goodput_MBps_mean") is not None and v(r, "overhead_pct_mean") is not None]
        if not pts:
            continue
        xs = [max(r["overhead_pct_mean"], 0.0) for r in pts]
        ys = [r["goodput_MBps_mean"] for r in pts]
        ax.plot(xs, ys, color=color(impl), lw=0.8, alpha=0.6, zorder=2)
        eb = ax.errorbar(xs, ys, xerr=[v(r, "overhead_pct_sd") or 0 for r in pts],
                         yerr=[v(r, "goodput_MBps_sd") or 0 for r in pts], color=color(impl), marker=marker(impl),
                         ls="none", elinewidth=0.8, label=label(impl), zorder=3, ms=7)
        for part in eb[2]:
            part.set_alpha(0.45)
        spread = max(xs) > 1.0
        dy = {"case2": -11, "enh_K4_poll": -11}.get(impl, 4)  # neighbours: one label above, one below
        for i, (x, y, r) in enumerate(zip(xs, ys, pts)):
            if spread or i == len(pts) - 1:
                ax.annotate("p=%g" % r["p"], (x, y), textcoords="offset points", xytext=(6, dy),
                            fontsize=7.5, color=INK2)
    ax.set_xscale("symlog", linthresh=0.1)
    ax.set_xlim(left=-0.02)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("overhead (% redundant of useful bytes)")
    ax.set_ylabel("goodput (MB/s)")
    ax.set_title("Part II: throughput (goodput) vs overhead", loc="left")
    legend(ax, loc="lower left")
    save(fig, out, "Part2_ThroughputVsOverhead", made)

    # threads vs poll
    if series(rows, "enh_K4") and series(rows, "enh_K4_poll"):
        fig, axs = plt.subplots(1, 2, figsize=(10, 4.2))
        for ax, key, t, yl in ((axs[0], "throughput_MBps", "aggregate throughput", "MB/s"),
                               (axs[1], "compl_mean_s", "mean per-client completion time", "s")):
            dnf = DnfMarks()
            for impl in ("enh_K4", "enh_K4_poll"):
                plot_vs_p(ax, rows, impl, key, dnf,
                          lab="K=4, blocking threads" if impl == "enh_K4" else "K=4, non-blocking poll()")
            p_axis(ax, ps)
            ax.set_ylim(bottom=0)
            ax.set_ylabel(yl)
            ax.set_title(t, loc="left")
            dnf.draw(ax)
        shared_legend(fig, axs[0])
        fig.suptitle("Case 2 Enhanced: threaded vs poll() client", x=0.01, ha="left", fontsize=12)
        save(fig, out, "Part2_Enhanced_ThreadsVsPoll", made)


def buffers_chart(rows, out, made):
    if not rows:
        return
    bufs = sorted({(r["buf_bytes"], r["buf"]) for r in rows})
    impls = sorted({r["impl"] for r in rows}, key=lambda s: (not s.startswith("case"), s))
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    w = 0.8 / len(impls)
    for j, impl in enumerate(impls):
        xs, ys, es = [], [], []
        for i, (_b, name) in enumerate(bufs):
            r = next((r for r in rows if r["impl"] == impl and r["buf"] == name), None)
            if r is None or v(r, "throughput_MBps_mean") is None:
                continue
            xs.append(i - 0.4 + w * (j + 0.5))
            ys.append(r["throughput_MBps_mean"])
            es.append(v(r, "throughput_MBps_sd") or 0)
        ax.bar(xs, ys, width=w, color=color(impl), edgecolor=SURFACE, linewidth=1.5, label=label(impl), zorder=3)
        ax.errorbar(xs, ys, yerr=es, fmt="none", ecolor=INK, elinewidth=1, capsize=3, zorder=4)

    def pretty(name):
        if name == "default":
            return "default\n(autotuning)"
        b = int(name)
        return "%d KiB" % (b // 1024) if b < 1 << 20 else "%d MiB" % (b // (1 << 20))
    ax.set_xticks(range(len(bufs)))
    ax.set_xticklabels([pretty(n) for _b, n in bufs])
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("SO_SNDBUF = SO_RCVBUF (FDS_SNDBUF / FDS_RCVBUF), p = 0")
    ax.set_ylabel("aggregate throughput (MB/s)")
    lo, hi = ax.get_ylim()
    ax.set_ylim(0, hi * 1.18)  # headroom so the legend sits above the bars
    ax.set_title("Socket buffer size vs throughput", loc="left")
    legend(ax, loc="upper right", ncol=2)
    save(fig, out, "Buffers_throughput", made)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--results", default=os.path.join(os.path.dirname(os.path.dirname(here)), "Results"),
                    help="folder with the summary CSVs; the graphs are written there too (default Results)")
    a = ap.parse_args()
    p1 = read_csv(os.path.join(a.results, "Part1_summary.csv"))
    p2 = read_csv(os.path.join(a.results, "Part2_summary.csv"))
    bf = read_csv(os.path.join(a.results, "Buffers_summary.csv"))
    if not (p1 or p2 or bf):
        sys.exit("no summary CSVs in %s: run tools/exp/analyze.py first" % a.results)
    made = []
    part1_charts(p1, a.results, made)
    if p2:
        # Part I reference for p = 0: --part1-compare runs (same topic) if present,
        # else the Part I sweep at the same client count (different topic).
        ref = next((r for r in p2 if r["impl"] == "part1_multi"), None)
        note = ""
        if ref is None and p1:
            n = next((r["n_clients"] for r in p2 if r.get("n_clients")), 4)
            ref = next((r for r in p1 if r["server"] == "multi" and r["n_clients"] == n), None)
            if ref is not None:
                note = " (Part I line: topic %s)" % ref.get("topic", "?")
        part2_case_charts(p2, ref, a.results, made)
        comparison_charts(p2, ref, note, a.results, made)
    buffers_chart(bf, a.results, made)
    print("wrote %d graphs (PNG + PDF) to %s:" % (len(made), a.results))
    for m in made:
        print("  " + m + ".png")


if __name__ == "__main__":
    main()
