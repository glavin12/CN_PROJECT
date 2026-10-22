#!/usr/bin/env python3
"""
gen_dataset.py - deterministic synthetic datasets for the FDS tests.

IT305 Socket Programming Project, Group 006 + 043.

    python3 tests/gen_dataset.py <out_root> --profile edge|animals [--scale F] [--seed N] [--force]

The same arguments always produce byte-identical files (names and contents),
because every random choice comes from one random.Random(seed) and the
topics/files are generated in a fixed order. File contents are pseudo-random
bytes (they do not need to be real images: the server only streams bytes).

Profile "edge" (correctness corner cases, about 26 MB):
    empty/      no files at all
    tiny/       files of 0, 1 and 2 bytes
    boundary/   65535, 65536, 65537, 131072, 131073 bytes (64 KiB chunk edges)
    names/      spaces, UTF-8, '=', '#', ',', '"' and a 255-byte file name
    big/        8 MiB, 3 MiB + 7 bytes and 3 random files of 100 KB - 1 MB
    many/       400 files of 1-60 KB (log-normal-ish sizes)
    Mixed/      a mixed-case topic name, 3 files
  plus noise the server must ignore:
    .hidden/    a hidden topic folder (with a file inside)
    many/.DS_Store, many/sub/nested.jpg   (hidden file, nested folder)
    README.txt  a regular file directly in the root (not a topic)

Profile "animals" (the 10 Animals-10 topics, file counts x --scale):
    cane cavallo elefante farfalla gallina gatto mucca pecora ragno scoiattolo
    sizes log-normal, median ~20 KB, ~8% over 64 KB, capped at ~500 KB;
    names like "OIP-<random>.jpeg" or "<n>.jpeg".

The output root must be empty or absent. With --force, only the entries this
profile creates are deleted first (nothing else in out_root is touched).
"""

import argparse
import math
import os
import random
import shutil
import string
import sys

# Real number of images per Animals-10 topic (the Kaggle "raw-img" folders).
ANIMALS_COUNTS = {
    "cane": 4863,
    "cavallo": 2623,
    "elefante": 1446,
    "farfalla": 2112,
    "gallina": 3098,
    "gatto": 1668,
    "mucca": 1866,
    "pecora": 1820,
    "ragno": 4821,
    "scoiattolo": 1862,
}

KB = 1000
KIB = 1024
MIB = 1024 * 1024


def write_file(path, rng, size):
    """Write `size` pseudo-random bytes drawn from rng (deterministic)."""
    with open(path, "wb") as f:
        left = size
        while left > 0:
            n = min(left, 1 << 20)          # 1 MiB at a time keeps memory small
            f.write(rng.randbytes(n))
            left -= n


def lognormal_size(rng, median, sigma, lo, hi):
    """A log-normal size with the given median, clipped to [lo, hi]."""
    v = int(rng.lognormvariate(math.log(median), sigma))
    return max(lo, min(hi, v))


# --------------------------------------------------------------------------- #
# Profile "edge"
# --------------------------------------------------------------------------- #

def edge_plan(rng):
    """Return (topics, noise).

    topics: list of (topic_name, [(file_name, size), ...]) in creation order.
    noise:  list of (relative_path, size) for things the server must ignore;
            a path ending in "/" is a directory.
    """
    topics = []

    topics.append(("empty", []))

    topics.append(("tiny", [("0_bytes.bin", 0), ("1_byte.bin", 1), ("2_bytes.bin", 2)]))

    topics.append(("boundary", [
        ("b_065535.bin", 65535),     # one byte less than one chunk
        ("b_065536.bin", 65536),     # exactly one chunk
        ("b_065537.bin", 65537),     # one chunk + 1 byte
        ("b_131072.bin", 131072),    # exactly two chunks
        ("b_131073.bin", 131073),    # two chunks + 1 byte
    ]))

    long_name = "L" * (255 - len(".jpg")) + ".jpg"   # exactly 255 bytes (NAME_MAX)
    assert len(long_name.encode()) == 255
    names = [
        "file with spaces.jpg",
        "café-ünïcode.jpg",          # UTF-8 bytes >= 0x80 are legal in names
        "key=value.jpg",             # '=' is the .session key/value separator
        "hash#tag.jpg",              # '#' often starts a comment
        "comma,separated.jpg",       # ',' is the CSV separator
        'quote"d.jpg',               # '"' is the CSV quote character
        long_name,
    ]
    topics.append(("names", [(n, rng.randint(1 * KB, 150 * KB)) for n in names]))

    big = [("big_8MiB.bin", 8 * MIB), ("big_3MiB_plus7.bin", 3 * MIB + 7)]
    for i in range(1, 4):
        big.append(("big_rand_%d.bin" % i, rng.randint(100 * KB, 1000 * KB)))
    topics.append(("big", big))

    many = []
    for i in range(1, 401):
        many.append(("img_%04d.jpg" % i, lognormal_size(rng, 12 * KB, 0.8, 1 * KB, 60 * KB)))
    topics.append(("many", many))

    topics.append(("Mixed", [("Mixed_a.jpg", rng.randint(1 * KB, 90 * KB)),
                             ("Mixed_b.jpg", rng.randint(1 * KB, 90 * KB)),
                             ("Mixed_c.jpg", rng.randint(1 * KB, 90 * KB))]))

    noise = [
        (".hidden/", None),
        (".hidden/secret.jpg", 1234),
        ("many/.DS_Store", 6148),
        ("many/sub/", None),
        ("many/sub/nested.jpg", 2048),
        ("README.txt", 300),
    ]
    return topics, noise


# --------------------------------------------------------------------------- #
# Profile "animals"
# --------------------------------------------------------------------------- #

OIP_ALPHABET = string.ascii_letters + string.digits + "-_"


def animal_name(rng, n):
    """Mimic Animals-10 names: mostly "OIP-<27 chars>.jpeg", some "<n>.jpeg"."""
    if rng.random() < 0.75:
        return "OIP-" + "".join(rng.choice(OIP_ALPHABET) for _ in range(27)) + ".jpeg"
    return "%d.jpeg" % n


def animals_plan(rng, scale):
    # Log-normal with median 20 KB. P(size > 64 KiB) = 8% gives
    # sigma = ln(65536 / 20000) / z(0.92) = 1.187 / 1.405 = 0.845.
    median, sigma = 20 * KB, 0.845
    topics = []
    for topic, real_count in ANIMALS_COUNTS.items():
        count = max(3, int(round(real_count * scale)))
        used = set()
        files = []
        n = 1
        while len(files) < count:
            name = animal_name(rng, n)
            n += 1
            if name in used:
                continue
            used.add(name)
            files.append((name, lognormal_size(rng, median, sigma, 1 * KIB, 500 * KIB)))
        topics.append((topic, files))
    return topics, []


# --------------------------------------------------------------------------- #

def prepare_root(root, topics, noise, force):
    """Make sure root is empty (or remove exactly what we are about to create)."""
    if not os.path.exists(root):
        os.makedirs(root)
        return
    if not os.path.isdir(root):
        sys.exit("gen_dataset: %s exists and is not a directory" % root)
    entries = os.listdir(root)
    if not entries:
        return
    if not force:
        sys.exit("gen_dataset: %s is not empty (use --force to regenerate it)" % root)
    ours = {t for t, _ in topics} | {p.split("/")[0] for p, _ in noise}
    for e in entries:
        if e in ours:
            p = os.path.join(root, e)
            if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p)
            else:
                os.remove(p)


def main():
    ap = argparse.ArgumentParser(description="Generate a deterministic FDS test dataset.")
    ap.add_argument("out_root", help="topics_root_dir to create")
    ap.add_argument("--profile", required=True, choices=["edge", "animals"])
    ap.add_argument("--scale", type=float, default=0.1,
                    help="animals: fraction of the real file counts (default 0.1)")
    ap.add_argument("--seed", type=int, default=1, help="random seed (default 1)")
    ap.add_argument("--force", action="store_true",
                    help="delete this profile's entries in out_root first")
    args = ap.parse_args()
    if args.scale <= 0:
        ap.error("--scale must be > 0")

    # One generator for everything, used in a fixed order -> reproducible output.
    rng = random.Random(args.seed)
    if args.profile == "edge":
        topics, noise = edge_plan(rng)
    else:
        topics, noise = animals_plan(rng, args.scale)

    prepare_root(args.out_root, topics, noise, args.force)

    total_files = total_bytes = 0
    print("%-12s %7s %14s" % ("topic", "files", "bytes"))
    for topic, files in topics:
        tdir = os.path.join(args.out_root, topic)
        os.makedirs(tdir)
        tbytes = 0
        for name, size in files:
            write_file(os.path.join(tdir, name), rng, size)
            tbytes += size
        total_files += len(files)
        total_bytes += tbytes
        print("%-12s %7d %14d" % (topic, len(files), tbytes))

    for rel, size in noise:
        path = os.path.join(args.out_root, rel)
        if rel.endswith("/"):
            os.makedirs(path, exist_ok=True)
        else:
            write_file(path, rng, size)
    print("%-12s %7d %14d" % ("TOTAL", total_files, total_bytes))
    if noise:
        print("noise (must be ignored): " + ", ".join(p for p, s in noise if s is not None))
    print("profile=%s seed=%d%s root=%s" % (
        args.profile, args.seed,
        " scale=%g" % args.scale if args.profile == "animals" else "",
        args.out_root))


if __name__ == "__main__":
    main()
