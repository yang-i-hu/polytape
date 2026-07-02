#!/bin/bash
# polytape admin-scratch janitor.
#
# Why this exists: the admin builds per-match download/extract archives in
# POLYTAPE_SCRATCH_DIR and rmtree's its own scratch on completion/error. But the
# admin runs under MemoryMax=768M, so a large build can be OOM-KILLED mid-flight —
# a hard kill skips the cleanup and orphans a multi-GB scratch dir. Enough of those
# accumulate, fill /data, and crash-loop the recorder with ENOSPC (this is exactly
# what took the recorder down on 2026-06-27: 8 orphaned dirs = 41 GB).
#
# This sweeps scratch dirs older than AGE_MIN minutes. A real in-flight build is far
# younger than that, so the age floor means we never race an active download/extract.
# Safe by construction: it only ever touches POLYTAPE_SCRATCH_DIR's own transient
# build dirs (polytape-extract-* / polytape-dl-*) — never run-wc or any recorded data.
set -uo pipefail

ENV_FILE=${JANITOR_ENV_FILE:-/etc/polytape/autogrow.env}   # reuse POLYTAPE_SCRATCH_DIR if set there
[ -r "$ENV_FILE" ] && . "$ENV_FILE"

SCRATCH=${POLYTAPE_SCRATCH_DIR:-/data/tmp/polytape-admin}
AGE_MIN=${JANITOR_AGE_MIN:-180}

log() { logger -t polytape-scratch-janitor "$*"; echo "polytape-scratch-janitor: $*"; }

[ -d "$SCRATCH" ] || { log "scratch dir $SCRATCH absent; nothing to do"; exit 0; }

# find runs as root (this unit is root), so it can traverse the 2770 scratch dir that
# a non-root glob can't. -mmin +AGE_MIN = modified more than AGE_MIN minutes ago.
mapfile -t stale < <(
    find "$SCRATCH" -mindepth 1 -maxdepth 1 -type d \
        \( -name 'polytape-extract-*' -o -name 'polytape-dl-*' \) -mmin +"$AGE_MIN"
)

if [ "${#stale[@]}" -eq 0 ]; then
    log "ok: no orphaned scratch older than ${AGE_MIN}m in $SCRATCH"
    exit 0
fi

freed=$(du -sch "${stale[@]}" 2>/dev/null | tail -1 | cut -f1)
rm -rf "${stale[@]}"
log "pruned ${#stale[@]} orphaned scratch dir(s) (~${freed}) older than ${AGE_MIN}m from $SCRATCH"
