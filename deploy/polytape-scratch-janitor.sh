#!/bin/bash
# polytape scratch janitor.
#
# Why this exists: jobs that stage multi-GB transient dirs under POLYTAPE_SCRATCH_DIR
# — the offloader's tar staging (polytape-offload-*) and, on a VM that runs the admin
# dashboard, its download/extract builds (polytape-extract-* / polytape-dl-*) — clean
# up after themselves on completion or error, but a hard kill (OOM, `systemctl kill`,
# a reboot mid-upload) skips that cleanup and orphans the dir. Enough orphans fill
# /data and crash-loop the recorder with ENOSPC (exactly what took the WC recorder
# down on 2026-06-27: 8 orphaned dirs = 41 GB).
#
# This sweeps scratch dirs older than AGE_MIN minutes — measured on the NEWEST file
# inside the dir, not the dir itself (a dir's mtime is set when its tar/.zst entry is
# created and never advances while it is being written or read) — and never while
# polytape-offload.service is running, so a legitimately long segment compress +
# upload is never pulled out from under the offloader. Safe by construction: it only
# ever touches POLYTAPE_SCRATCH_DIR's own transient build dirs — never the run dir or
# any recorded data.
set -uo pipefail

# Config: the autogrow env (WC layout) then the offloader's env (this campaign; wins).
# Only POLYTAPE_SCRATCH_DIR is read from them. An explicit JANITOR_ENV_FILE wins over both.
if [ -n "${JANITOR_ENV_FILE:-}" ]; then
    [ -r "$JANITOR_ENV_FILE" ] && . "$JANITOR_ENV_FILE"
else
    for f in /etc/polytape/autogrow.env /etc/polytape/offload.env; do
        [ -r "$f" ] && . "$f"
    done
fi

SCRATCH=${POLYTAPE_SCRATCH_DIR:-/data/tmp/polytape-offload}
AGE_MIN=${JANITOR_AGE_MIN:-180}
OFFLOAD_UNIT=${JANITOR_OFFLOAD_UNIT:-polytape-offload.service}

log() { logger -t polytape-scratch-janitor "$*"; echo "polytape-scratch-janitor: $*"; }

[ -d "$SCRATCH" ] || { log "scratch dir $SCRATCH absent; nothing to do"; exit 0; }

# Never sweep under a running offloader: its scratch is in use however old it looks.
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$OFFLOAD_UNIT" 2>/dev/null; then
    log "skip: $OFFLOAD_UNIT is running; its scratch is in use"
    exit 0
fi

# find runs as root (this unit is root), so it can traverse a group-only scratch dir
# that a non-root glob can't. A dir is stale when NOTHING inside it (nor the dir) was
# modified in the last AGE_MIN minutes (-mmin -N = modified within N minutes).
mapfile -t stale < <(
    find "$SCRATCH" -mindepth 1 -maxdepth 1 -type d \
        \( -name 'polytape-offload-*' -o -name 'polytape-extract-*' -o -name 'polytape-dl-*' \) \
        -mmin +"$AGE_MIN" \
        | while IFS= read -r d; do
            [ -z "$(find "$d" -mmin -"$AGE_MIN" -print -quit 2>/dev/null)" ] && printf '%s\n' "$d"
        done
)

if [ "${#stale[@]}" -eq 0 ]; then
    log "ok: no orphaned scratch older than ${AGE_MIN}m in $SCRATCH"
    exit 0
fi

freed=$(du -sch "${stale[@]}" 2>/dev/null | tail -1 | cut -f1)
rm -rf "${stale[@]}"
log "pruned ${#stale[@]} orphaned scratch dir(s) (~${freed}) older than ${AGE_MIN}m from $SCRATCH"
