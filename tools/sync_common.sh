#!/usr/bin/env bash
# sync_common.sh - copy the canonical shared modules from common/ into every
# case folder, so each folder builds on its own with `make`.
#
#   tools/sync_common.sh           copy (run after editing anything in common/)
#   tools/sync_common.sh --check   exit 1 if any folder copy differs from common/
#
# Never edit the copies inside Part*/ directly: edit common/ and re-run this.
set -euo pipefail
cd "$(dirname "$0")/.."

base_mods="util net protocol log topic_map transfer server_core"
declare -A extra=(
    [Part1]=""
    [Part2_Case1]="fault"
    [Part2_Case2]="fault session checkpoint"
    [Part2_Case2_Enhanced]="fault session checkpoint workq"
)
folders="Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced"

check=0
[[ "${1:-}" == "--check" ]] && check=1
status=0

for d in $folders; do
    files="common/fds.h"
    for m in $base_mods ${extra[$d]}; do
        files+=" common/$m.h common/$m.c"
    done
    for f in $files; do
        [[ -f "$f" ]] || { echo "missing $f" >&2; status=1; continue; }
        dst="$d/$(basename "$f")"
        if (( check )); then
            if ! cmp -s "$f" "$dst"; then
                echo "DRIFT: $dst differs from $f" >&2
                status=1
            fi
        else
            cp -p "$f" "$dst"
        fi
    done
done

if (( check )); then
    (( status == 0 )) && echo "common copies OK"
else
    echo "synced common/ into: $folders"
fi
exit $status
