#!/usr/bin/env bash
# verify_output.sh - check that a client's output directory is an exact copy
# of one server topic directory.
#
# IT305 Socket Programming Project, Group 006 + 043.
#
#   tests/verify_output.sh <source_topic_dir> <output_topic_dir>
#
# Expected files = the regular files directly inside <source_topic_dir> whose
# names do not start with '.' (exactly what the server serves; hidden files
# and sub-folders are ignored by the server).
#
# The output directory must contain EXACTLY those names:
#   - every expected file must exist and be byte-identical (sha256sum);
#   - anything else is an error: leftovers such as <name>.part, .session,
#     .session.tmp, hidden files, directories, symlinks.
#
# Prints "OK <n> files <bytes> bytes" and exits 0, or prints FAIL lines and
# exits 1. File names may contain spaces, quotes, '=' or UTF-8: every list is
# NUL-separated, so no name is ever split or re-interpreted by the shell.
set -euo pipefail

MAX_LINES=20     # print at most this many FAIL lines (a missing topic = 400 lines)

usage() {
    echo "usage: $0 <source_topic_dir> <output_topic_dir>" >&2
    exit 1
}
[[ $# -eq 2 ]] || usage
src=$1
out=$2

if [[ ! -d "$src" ]]; then
    echo "FAIL source directory does not exist: $src"
    exit 1
fi
if [[ ! -d "$out" ]]; then
    echo "FAIL output directory does not exist: $out"
    exit 1
fi

nfail=0
report() {          # report <message>: count a failure, print the first MAX_LINES
    nfail=$((nfail + 1))
    if (( nfail <= MAX_LINES )); then
        printf 'FAIL %s\n' "$1"
    fi
}

# hash_files <dir> <name>...: prints "<sha256>  <name>\0" for each name.
# sha256sum -z: NUL-terminated output and no escaping of odd file names.
hash_files() {
    local dir=$1
    shift
    (( $# > 0 )) || return 0
    (cd "$dir" && sha256sum -z -- "$@")
}

# ---- 1. expected files (name -> size and name -> hash) -----------------------
declare -A exp_size=() exp_hash=() out_type=()
exp_names=()
total_bytes=0
while IFS= read -r -d '' rec; do
    size=${rec%% *}          # "%s %P": size, one space, name
    name=${rec#* }
    exp_size[$name]=$size
    exp_names+=("$name")
    total_bytes=$((total_bytes + size))
done < <(find "$src" -mindepth 1 -maxdepth 1 -type f ! -name '.*' -printf '%s %P\0' | LC_ALL=C sort -z -k2)

if (( ${#exp_names[@]} > 0 )); then
    while IFS= read -r -d '' rec; do
        exp_hash[${rec#*  }]=${rec%%  *}
    done < <(hash_files "$src" "${exp_names[@]}")
fi

# ---- 2. everything that is in the output directory ---------------------------
# %y = type letter (f file, d directory, l symlink, ...), %P = name.
while IFS= read -r -d '' rec; do
    out_type[${rec#? }]=${rec%% *}
done < <(find "$out" -mindepth 1 -maxdepth 1 -printf '%y %P\0')

# ---- 3. extra entries (leftovers) --------------------------------------------
present=()
for name in "${!out_type[@]}"; do
    type=${out_type[$name]}
    if [[ -z "${exp_size[$name]+set}" ]]; then
        case "$name" in
            *.part)                   what="partial file left over" ;;
            .session|.session.tmp)    what="checkpoint left over" ;;
            *)                        what="unexpected entry" ;;
        esac
        [[ "$type" == d ]] && what="unexpected directory"
        report "extra: '$name' ($what)"
    elif [[ "$type" != f ]]; then
        report "not a regular file (type $type): '$name'"
    else
        present+=("$name")
    fi
done

# ---- 4. missing files ---------------------------------------------------------
for name in "${exp_names[@]}"; do
    [[ -n "${out_type[$name]+set}" ]] || report "missing: '$name'"
done

# ---- 5. contents of the files that are present ---------------------------------
if (( ${#present[@]} > 0 )); then
    declare -A out_hash=()
    while IFS= read -r -d '' rec; do
        out_hash[${rec#*  }]=${rec%%  *}
    done < <(hash_files "$out" "${present[@]}")
    for name in "${present[@]}"; do
        if [[ "${out_hash[$name]:-}" != "${exp_hash[$name]}" ]]; then
            got=$(stat -c %s -- "$out/$name")
            report "corrupt: '$name' (size $got, expected ${exp_size[$name]}; sha256 differs)"
        fi
    done
fi

if (( nfail > 0 )); then
    (( nfail > MAX_LINES )) && echo "FAIL ... and $((nfail - MAX_LINES)) more problems"
    echo "FAIL $nfail problem(s) in $out (source $src)"
    exit 1
fi
echo "OK ${#exp_names[@]} files $total_bytes bytes"
