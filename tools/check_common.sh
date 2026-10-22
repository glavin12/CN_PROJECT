#!/usr/bin/env bash
# check_common.sh - fail if a Part*/ copy of a shared module drifted from common/.
exec "$(dirname "$0")/sync_common.sh" --check
