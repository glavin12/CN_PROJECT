#!/usr/bin/env bash
# package.sh - build and self-check the final submission archive.
#
#   tools/package.sh [--report <pdf>] [--out <dir>]
#
#   --report <pdf>  the consolidated report (default ./FinalReport_Group_006_043.pdf
#                   if it exists; if it is missing we only WARN, so the team can
#                   test the packaging before the report is written)
#   --out <dir>     where to create the archive (default: dist/)
#
# Result: <out>/FinalSubmission_Group_006_043/ and
#         <out>/FinalSubmission_Group_006_043.tar.gz
# whose layout is exactly the one the guidelines demand: the report, README.md,
# the four Part folders (sources + Makefile only) and Results/. Nothing else
# (no tests/, tools/, docs/) goes into the archive.
#
# After packing, the script unpacks the archive into a fresh temp dir and
# checks it the way the auto-grader will: make has zero warnings, exactly
# `server` and `client` are executable in each Part folder, and one real
# Part 1 transfer is byte-identical to the source topic.
# Exit code: 0 = all checks passed, 1 = a check failed or bad usage.
set -euo pipefail

GROUP="006_043"
NAME="FinalSubmission_Group_${GROUP}"
PARTS="Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced"

# Run from the repo root no matter where the script was started.
cd "$(dirname "$0")/.."

REPORT=""
OUT="dist"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --report) [[ $# -ge 2 ]] || { echo "package.sh: --report needs a file" >&2; exit 1; }
                  REPORT=$2; shift 2 ;;
        --out)    [[ $# -ge 2 ]] || { echo "package.sh: --out needs a directory" >&2; exit 1; }
                  OUT=$2; shift 2 ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "usage: tools/package.sh [--report <pdf>] [--out <dir>]" >&2; exit 1 ;;
    esac
done
if [[ -z "$REPORT" && -f "FinalReport_Group_${GROUP}.pdf" ]]; then
    REPORT="FinalReport_Group_${GROUP}.pdf"
fi

FAILS=0
WARNED=0
fail() { echo "FAIL: $*" >&2; FAILS=$((FAILS + 1)); }
pass() { echo "PASS: $*"; }

TMP=$(mktemp -d)
SRV_PID=""
# Always stop the test server and delete the temp dir, even on errors.
cleanup() {
    if [[ -n "$SRV_PID" ]]; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT

# ---- 1. make sure the Part folders hold the current shared modules ----------
echo "== 1. sync + check shared modules"
tools/sync_common.sh
tools/check_common.sh

# ---- 2. assemble the folder -------------------------------------------------
echo "== 2. assemble $OUT/$NAME"
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)           # absolute, needed after we cd elsewhere
STAGE="$OUT/$NAME"
rm -rf "$STAGE" "$OUT/$NAME.tar.gz"
mkdir -p "$STAGE"

cp README.md "$STAGE/README.md"

if [[ -n "$REPORT" && -f "$REPORT" ]]; then
    cp "$REPORT" "$STAGE/FinalReport_Group_${GROUP}.pdf"
else
    echo "################################################################"
    echo "# WARNING: FinalReport_Group_${GROUP}.pdf is MISSING."
    echo "# The archive is NOT submittable without it. Re-run with"
    echo "#   tools/package.sh --report <path/to/report.pdf>"
    echo "################################################################"
fi

# Sources and Makefile only: skip objects, dependency files and the binaries.
for d in $PARTS; do
    mkdir -p "$STAGE/$d"
    find "$d" -maxdepth 1 -type f ! -name '*.o' ! -name '*.d' \
         ! -name server ! -name client -exec cp {} "$STAGE/$d/" \;
done

# Results/: copy everything except raw/scratch (throw-away runs, see .gitignore).
mkdir -p "$STAGE/Results"
if [[ -d Results ]]; then
    (cd Results && find . -path ./raw/scratch -prune -o -type f -print0 |
        while IFS= read -r -d '' f; do
            mkdir -p "$STAGE/Results/$(dirname "$f")"
            cp "$f" "$STAGE/Results/$f"
        done)
fi
if [[ -z "$(find "$STAGE/Results" -type f -print -quit)" ]]; then
    echo "WARNING: Results/ is empty (run the experiments, analyze.py and plot.py first)."
fi

# ---- 3. archive -------------------------------------------------------------
echo "== 3. create $NAME.tar.gz"
tar -C "$OUT" -czf "$OUT/$NAME.tar.gz" "$NAME"

# ---- 4. verify the archive, as the grader would ------------------------------
echo "== 4. verify the archive"
mkdir "$TMP/x"
tar -C "$TMP/x" -xzf "$OUT/$NAME.tar.gz"
X="$TMP/x/$NAME"

# 4a. the top level must hold exactly the allowed entries.
expected="README.md Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced Results"
if [[ -f "$X/FinalReport_Group_${GROUP}.pdf" ]]; then
    expected="FinalReport_Group_${GROUP}.pdf $expected"
else
    WARNED=1   # already announced loudly above; not a failure (see header)
fi
want=$(printf '%s\n' $expected | sort)
have=$(ls -A "$X" | sort)
if [[ "$want" == "$have" ]]; then pass "top-level layout"; else fail "top-level layout differs"; fi

# 4b. clean build in every Part folder: zero warnings, exactly 2 executables.
for d in $PARTS; do
    if ! (cd "$X/$d" && make >"$TMP/make_$d.log" 2>&1); then
        fail "$d: make failed"; cat "$TMP/make_$d.log" >&2; continue
    fi
    if grep -qiE 'warning|error' "$TMP/make_$d.log"; then
        fail "$d: make printed warnings"; grep -iE 'warning|error' "$TMP/make_$d.log" >&2
    else
        pass "$d: make, zero warnings"
    fi
    exes=$(cd "$X/$d" && find . -maxdepth 1 -type f -perm -u+x -printf '%f\n' | sort | tr '\n' ' ')
    if [[ "$exes" == "client server " ]]; then
        pass "$d: executables are exactly client + server"
    else
        fail "$d: executables are '$exes' (want 'client server ')"
    fi
done

# 4c. one real transfer with the archived Part 1 binaries. The dataset comes
#     from the repo's generator (it is not part of the archive).
python3 tests/gen_dataset.py "$TMP/data" --profile edge >/dev/null
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
FDS_QUIET=1 "$X/Part1/server" "$PORT" "$TMP/data" >"$TMP/server.log" 2>&1 &
SRV_PID=$!
sleep 1
if FDS_QUIET=1 "$X/Part1/client" 127.0.0.1 "$PORT" tiny "$TMP/out" >"$TMP/client.log" 2>&1; then
    if tests/verify_output.sh "$TMP/data/tiny" "$TMP/out/tiny" >"$TMP/verify.log" 2>&1; then
        pass "Part1 transfer of topic 'tiny' verified ($(cat "$TMP/verify.log"))"
    else
        fail "Part1 output differs from the source"; cat "$TMP/verify.log" >&2
    fi
else
    fail "Part1 client failed"; cat "$TMP/client.log" "$TMP/server.log" >&2
fi
kill "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
SRV_PID=""

# ---- 5. summary --------------------------------------------------------------
echo "== archive: $OUT/$NAME.tar.gz ($(du -h "$OUT/$NAME.tar.gz" | cut -f1))"
tar -tzf "$OUT/$NAME.tar.gz" | awk -F/ 'NF>=2 && $2!="" {print $2}' | sort -u | sed "s|^|  $NAME/|"
if [[ $FAILS -eq 0 ]]; then
    if [[ $WARNED -eq 1 ]]; then echo "RESULT: PASS (WARNING: report PDF missing, archive not submittable yet)"; else echo "RESULT: PASS"; fi
else
    echo "RESULT: FAIL ($FAILS check(s) failed)"
    exit 1
fi
