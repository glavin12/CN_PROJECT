#!/usr/bin/env bash
# test_build.sh - the build rules the auto-grader checks (CLAUDE.md "Hard rules").
#
# IT305 Socket Programming Project, Group 006 + 043.
#
# For Part1, Part2_Case1, Part2_Case2 and Part2_Case2_Enhanced:
#   - `make clean && make` succeeds and prints no "warning"/"error" on stderr;
#   - the folder then holds exactly two executable files: server and client;
#   - `make strict` (-Werror) succeeds with gcc, and with clang if installed;
#   - (not with --quick) `make strict` also at -O0 and -O3, where gcc finds
#     different warnings.
# Plus tools/check_common.sh: the copies of common/ in each folder have not drifted.
# A folder without server.c/client.c is SKIPPED. At the end every folder is
# rebuilt with plain `make` (and SAN=$SAN if set) so the other tests use it.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

FOLDERS=(Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced)

# mk <folder> <make args...>: make in that folder, quietly about directories.
mk() {
    local d=$1
    shift
    make -C "$ROOT/$d" --no-print-directory "$@"
}

# strict_build <folder> <label> <make args...>: PASS if `make strict ...` succeeds.
strict_build() {
    local d=$1 label=$2 log rc=0
    shift 2
    log=$TEST_TMP/$d.strict.$((++_SEQ)).log
    mk "$d" strict "$@" >"$log" 2>&1 || rc=$?
    if (( rc == 0 )); then
        pass "$d: make strict $label"
    else
        fail "$d: make strict $label failed (exit $rc)"
        show_log "$log" 20
    fi
}

section "shared modules"
have_src=0
for d in "${FOLDERS[@]}"; do
    [[ -f $ROOT/$d/server.c && -f $ROOT/$d/client.c ]] && have_src=1
done
if (( ! have_src )); then
    # before any case is written, common/ is incomplete and nothing is synced yet
    skip "tools/check_common.sh (no folder has server.c/client.c yet)"
elif out=$("$ROOT/tools/check_common.sh" 2>&1); then
    pass "tools/check_common.sh: ${out:-ok}"
else
    fail "tools/check_common.sh reports drift between common/ and the Part folders"
    printf '%s\n' "$out" | sed -n '1,15s/^/       /p'
fi

for d in "${FOLDERS[@]}"; do
    section "$d"
    if [[ ! -f $ROOT/$d/server.c || ! -f $ROOT/$d/client.c ]]; then
        skip "$d: server.c / client.c not written yet"
        continue
    fi

    # 1. clean build, zero warnings
    mk "$d" clean >/dev/null 2>&1 || true
    out=$TEST_TMP/$d.make.out
    err=$TEST_TMP/$d.make.err
    rc=0
    mk "$d" >"$out" 2>"$err" || rc=$?
    if (( rc != 0 )); then
        fail "$d: make failed (exit $rc)"
        show_log "$err" 25
        continue
    fi
    if grep -qiE 'warning|error' "$err"; then
        fail "$d: make printed warnings or errors on stderr"
        show_log "$err" 25
    else
        pass "$d: make clean && make, no warning/error on stderr"
    fi

    # 2. exactly two executable files: server and client
    exes=$(find "$ROOT/$d" -maxdepth 1 -type f -perm -u+x -printf '%P\n' | sort | tr '\n' ' ')
    check_eq "$d: executable files after make" "${exes% }" "client server"

    # 3. -Werror builds
    strict_build "$d" "(gcc)"
    if command -v clang >/dev/null 2>&1; then
        strict_build "$d" "(clang)" CC=clang
    else
        skip "$d: make CC=clang strict (clang not installed)"
    fi
    if is_quick; then
        skip "$d: make strict OPT=-O0 / -O3 (--quick)"
    else
        strict_build "$d" "OPT=-O0" OPT=-O0
        strict_build "$d" "OPT=-O3" OPT=-O3
    fi

    # 4. leave a normal build behind for the other tests
    mk "$d" clean >/dev/null 2>&1 || true
    # shellcheck disable=SC2086  # ${SAN:+...} must vanish entirely when unset
    if ! mk "$d" -s ${SAN:+SAN=$SAN} >/dev/null 2>&1; then
        fail "$d: final rebuild failed"
    fi
done

summary
