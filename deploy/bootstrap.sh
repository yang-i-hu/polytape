#!/bin/bash
# polytape campaign bootstrap — Debian 12, run as root, IDEMPOTENT (re-run to upgrade).
#
# What it does (every step is a no-op when already done):
#   1. apt: python3, python3-venv, zstd
#   2. system user `polytape` (no home, nologin) — the recorder AND offloader identity
#   3. data disk: mkfs.ext4 the attached disk IF it carries no filesystem, mount it at
#      /data via fstab (by UUID, `nofail`), own it to polytape. REFUSES to continue if
#      /data would land on the 20 GB root fs (override: ALLOW_ROOT_DATA=1, dev only).
#   4. /opt/polytape/{src,venv,scripts}: unpack the source tarball, pip install
#      "src[admin]" under deploy/constraints.txt (the recorder deps pinned to uv.lock;
#      google-cloud-storage for the offloader — the admin dashboard itself is NOT
#      installed as a service; no --upgrade), copy the discovery script + helper scripts
#   5. /etc/polytape: campaign.json (from deploy/, never overwritten once installed),
#      polytape.env, offload.env (ADC — no key file, no secrets)
#   6. systemd units: polytape (its --run-name follows RUN_NAME), polytape-refresh.{service,timer},
#      polytape-offload.{service,timer}, polytape-scratch-janitor.{service,timer}
#   7. one discovery pass -> /etc/polytape/campaign_events.json, ONLY when none is
#      installed yet (a re-run leaves the live set to polytape-refresh.timer, which has
#      the empty-set guard and sticky main-line picks) and only if it found events
#   8. enable everything, (re)start the recorder, THEN start the timers (an
#      already-running recorder is RESTARTED = upgrade; the refresh timer would
#      otherwise fire its own restart right behind this one)
#
# NOT installed, on purpose: polytape-admin (dashboard), polytape-control (privileged
# intent bridge), polytape-autogrow (needs compute-rw on the SA), the admin tmpfiles.
# This campaign needs no dashboard — healthcheck.sh + journalctl cover operations — and
# every unit left out is one less root-run code path / IAM scope to defend. The SA
# attached to the VM has objectAdmin on the archive bucket ONLY and the storage-rw
# scope; the offloader uses it through Application Default Credentials.
#
# Inputs (env; DATA_DISK and BUCKET have no defaults and are required on a FIRST install,
# a re-run finds /data mounted and offload.env written and needs neither; the rest optional):
#   SRC_TARBALL=/tmp/polytape-src.tar.gz            git-archive tarball of the repo (CAMPAIGN.md)
#   DATA_DISK=/dev/disk/by-id/google-<device-name>  the attached data disk (its GCE device-name)
#   BUCKET=<archive-bucket>                         Coldline archive bucket (-> offload.env)
#   RUN_NAME=maker                                  -> /data/run-<RUN_NAME>, polytape --run-name,
#                                                   GCS prefix run-<RUN_NAME>
#   SKIP_APT=0 / SKIP_DISCOVERY=0                   1 = skip that step
set -euo pipefail

SRC_TARBALL=${SRC_TARBALL:-/tmp/polytape-src.tar.gz}
DATA_DISK=${DATA_DISK:-}
DATA_MOUNT=/data
BUCKET=${BUCKET:-}
RUN_NAME=${RUN_NAME:-maker}
SKIP_APT=${SKIP_APT:-0}
SKIP_DISCOVERY=${SKIP_DISCOVERY:-0}
ALLOW_ROOT_DATA=${ALLOW_ROOT_DATA:-0}

PREFIX=/opt/polytape
SRC=$PREFIX/src
VENV=$PREFIX/venv
ETC=/etc/polytape
UNITS=(polytape.service polytape-refresh.service polytape-refresh.timer
       polytape-offload.service polytape-offload.timer
       polytape-scratch-janitor.service polytape-scratch-janitor.timer)
HELPERS=(polytape-refresh.sh polytape-scratch-janitor.sh polytape-event-set.py healthcheck.sh)

log() { echo "bootstrap: $*"; }
die() { echo "bootstrap: ERROR: $*" >&2; exit 1; }

[ "$(id -u)" = 0 ] || die "run as root (sudo bash bootstrap.sh)"
[ -f "$SRC_TARBALL" ] || die "source tarball not found at $SRC_TARBALL (scp it first; see deploy/CAMPAIGN.md)"
[ -n "$BUCKET" ] || [ -f "$ETC/offload.env" ] \
    || die "BUCKET unset: a first install needs the archive bucket (sudo BUCKET=<archive-bucket> DATA_DISK=... bash bootstrap.sh; see deploy/CAMPAIGN.md)"

# ---- 1. packages ----------------------------------------------------------- #
if [ "$SKIP_APT" != 1 ]; then
    log "apt: python3 python3-venv zstd"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq --no-install-recommends python3 python3-venv zstd >/dev/null
fi

# ---- 2. user --------------------------------------------------------------- #
if ! getent passwd polytape >/dev/null; then
    log "creating system user polytape"
    useradd --system --user-group --home-dir /nonexistent --no-create-home \
        --shell /usr/sbin/nologin polytape
fi

# ---- 3. data disk ---------------------------------------------------------- #
mkdir -p "$DATA_MOUNT"
if mountpoint -q "$DATA_MOUNT"; then
    log "$DATA_MOUNT already mounted ($(findmnt -no SOURCE "$DATA_MOUNT"))"
elif [ -b "$DATA_DISK" ]; then
    fstype=$(blkid -o value -s TYPE "$DATA_DISK" || true)
    pttype=$(blkid -o value -s PTTYPE "$DATA_DISK" || true)
    if [ -z "$fstype" ] && [ -n "$pttype" ]; then
        # No filesystem on the whole device but a partition table: somebody's disk.
        lsblk -f "$DATA_DISK" >&2 || true
        die "$DATA_DISK carries a $pttype partition table; refusing to format it"
    elif [ -z "$fstype" ]; then
        log "formatting $DATA_DISK (ext4, 0% reserved, discard); before:"
        lsblk -f "$DATA_DISK" || true
        mkfs.ext4 -q -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard "$DATA_DISK"
    elif [ "$fstype" != ext4 ]; then
        die "$DATA_DISK carries a $fstype filesystem; refusing to touch it"
    else
        log "$DATA_DISK already has ext4; reusing it"
    fi
    uuid=$(blkid -o value -s UUID "$DATA_DISK")
    [ -n "$uuid" ] || die "could not read the UUID of $DATA_DISK"
    if ! grep -q "UUID=$uuid" /etc/fstab; then
        log "adding $DATA_MOUNT to /etc/fstab (UUID=$uuid, nofail)"
        printf 'UUID=%s %s ext4 defaults,nofail,discard,noatime 0 2\n' "$uuid" "$DATA_MOUNT" >> /etc/fstab
    fi
    systemctl daemon-reload   # fstab -> data.mount, which the units bind to (RequiresMountsFor)
    mount "$DATA_MOUNT"
    log "mounted $DATA_DISK at $DATA_MOUNT"
elif [ "$ALLOW_ROOT_DATA" = 1 ]; then
    log "WARNING: no data disk; $DATA_MOUNT is on the root fs (ALLOW_ROOT_DATA=1 — dev only)"
else
    die "no block device at ${DATA_DISK:-(DATA_DISK unset)} and $DATA_MOUNT is not a mount point — the recorder must not write to the root fs. Attach the data disk and set DATA_DISK=/dev/disk/by-id/google-<device-name>; see CAMPAIGN.md"
fi
install -d -o polytape -g polytape -m 0755 "$DATA_MOUNT"
install -d -o polytape -g polytape -m 0755 "$DATA_MOUNT/tmp"
install -d -o polytape -g polytape -m 0750 "$DATA_MOUNT/tmp/polytape-offload"

# ---- 4. source + venv ------------------------------------------------------ #
log "unpacking $SRC_TARBALL -> $SRC"
rm -rf "$SRC.new" "$SRC.tmp"
mkdir -p "$SRC.new"
tar -xzf "$SRC_TARBALL" -C "$SRC.new"
if [ ! -f "$SRC.new/pyproject.toml" ]; then
    # `git archive --prefix=<dir>/` tarballs carry one top-level directory.
    top=$(find "$SRC.new" -mindepth 1 -maxdepth 1 -type d | head -1)
    [ -n "$top" ] && [ -f "$top/pyproject.toml" ] \
        || die "tarball has no pyproject.toml at its root or under a single top-level dir"
    mv "$top" "$SRC.tmp" && rm -rf "$SRC.new" && mv "$SRC.tmp" "$SRC.new"
fi
# Normalize line endings in the operational files: a tarball built on a Windows checkout
# with core.autocrlf=true carries CRLF, which breaks shebangs, `set -euo pipefail` and
# systemd directive values. Python handles CRLF, but strip it there too for tidiness.
find "$SRC.new/deploy" "$SRC.new/scripts" -type f \
    \( -name '*.sh' -o -name '*.py' -o -name '*.service' -o -name '*.timer' -o -name '*.path' \
       -o -name '*.conf' -o -name '*.example' -o -name '*.json' -o -name '*.md' \) \
    -exec sed -i 's/\r$//' {} +
for f in scripts/list_campaign_events.py deploy/campaign.json deploy/constraints.txt "${UNITS[@]/#/deploy/}" "${HELPERS[@]/#/deploy/}"; do
    [ -f "$SRC.new/$f" ] || die "tarball is missing $f (build it from a branch that has the campaign work)"
done
rm -rf "$SRC.old"
if [ -d "$SRC" ]; then mv "$SRC" "$SRC.old"; fi
mv "$SRC.new" "$SRC"
rm -rf "$SRC.old"

if [ ! -x "$VENV/bin/python" ]; then
    log "creating venv at $VENV"
    python3 -m venv "$VENV"
fi
log "pip install polytape[admin] (google-cloud-storage for the offloader)"
# -c deploy/constraints.txt pins the recorder dependencies (websockets, httpx, ...) to
# the versions uv.lock holds — the set CI tests — so two bootstraps a week apart run
# the same code; google-cloud-storage is not locked and floats within its floor. No
# --upgrade: a re-run reinstalls the project from the new tree but never silently
# moves an already-satisfied dependency.
"$VENV/bin/pip" install --quiet -c "$SRC/deploy/constraints.txt" "$SRC[admin]"
"$VENV/bin/python" -c 'import polytape, google.cloud.storage' \
    || die "venv import check failed (polytape / google-cloud-storage)"

install -d -m 0755 "$PREFIX/scripts"
install -m 0755 "$SRC/scripts/list_campaign_events.py" "$PREFIX/scripts/list_campaign_events.py"
for h in "${HELPERS[@]}"; do
    install -m 0755 "$SRC/deploy/$h" "$PREFIX/$h"
done

# ---- 5. config ------------------------------------------------------------- #
install -d -m 0755 "$ETC"
if [ ! -f "$ETC/campaign.json" ]; then
    install -m 0644 "$SRC/deploy/campaign.json" "$ETC/campaign.json"
    log "installed campaign spec -> $ETC/campaign.json"
elif ! cmp -s "$SRC/deploy/campaign.json" "$ETC/campaign.json"; then
    log "NOTE: $ETC/campaign.json differs from deploy/campaign.json — keeping the installed one" \
        "(diff and replace by hand if the change is intended)"
fi
if [ ! -f "$ETC/polytape.env" ]; then
    # Required by polytape.service (EnvironmentFile without '-'); a public-feed capture
    # needs no secrets, so it is just a placeholder with the documented mode/owner.
    install -m 0600 -o polytape -g polytape /dev/null "$ETC/polytape.env"
    printf '# polytape recorder env (0600 polytape:polytape). No secrets needed for a public-feed capture.\n' \
        > "$ETC/polytape.env"
fi
if [ ! -f "$ETC/offload.env" ]; then
    cat > "$ETC/offload.env" <<EOF
# polytape-offload.service config (written by bootstrap.sh; see deploy/offload.env.example).
# NO SECRETS: auth is Application Default Credentials from the VM's attached service
# account. Do NOT add POLYTAPE_GCS_KEY / GOOGLE_APPLICATION_CREDENTIALS.
POLYTAPE_RUN_DIR=$DATA_MOUNT/run-$RUN_NAME
POLYTAPE_GCS_BUCKET=$BUCKET
POLYTAPE_GCS_PREFIX=run-$RUN_NAME
POLYTAPE_GCS_STORAGE_CLASS=COLDLINE
POLYTAPE_SCRATCH_DIR=$DATA_MOUNT/tmp/polytape-offload
POLYTAPE_EVENTS_FILE=$ETC/campaign_events.json
EOF
    chmod 0644 "$ETC/offload.env"
    log "wrote $ETC/offload.env"
fi

# ---- 6. units -------------------------------------------------------------- #
for u in "${UNITS[@]}"; do
    if [ "$u" = polytape.service ]; then
        # The unit hard-codes the run name; keep it in lock-step with RUN_NAME (offload.env
        # and the GCS prefix above), or the offloader would watch a run dir nobody writes.
        sed "s|--run-name maker|--run-name $RUN_NAME|" "$SRC/deploy/$u" > "/etc/systemd/system/$u.tmp"
        install -m 0644 "/etc/systemd/system/$u.tmp" "/etc/systemd/system/$u"
        rm -f "/etc/systemd/system/$u.tmp"
    else
        install -m 0644 "$SRC/deploy/$u" "/etc/systemd/system/$u"
    fi
done
systemctl daemon-reload
# Deliberately NOT installed here: polytape-admin.service, polytape-admin.tmpfiles.conf,
# polytape-control.{service,path}, polytape-autogrow.{service,timer} (see the header).

# ---- 7. discovery once (first install only) -------------------------------- #
# On a re-run the live set is left to polytape-refresh.timer: it re-discovers with
# --previous (sticky main-line picks), refuses an empty result and restarts only on a
# genuine change. Installing a raw discovery here would bypass all of that.
if [ "$SKIP_DISCOVERY" != 1 ] && [ ! -s "$ETC/campaign_events.json" ]; then
    tmp=$(mktemp)
    if "$VENV/bin/python" "$PREFIX/scripts/list_campaign_events.py" \
            --spec "$ETC/campaign.json" --out "$tmp" --open-only; then
        summary=$("$VENV/bin/python" "$PREFIX/polytape-event-set.py" --open-only --summary "$tmp" || echo "events=0")
        case "$summary" in
            events=0*)
                log "WARNING: discovery found no open events ($summary); not installing it —" \
                    "check the spec; polytape-refresh.timer retries every 10 min" ;;
            *)
                install -o polytape -g polytape -m 0644 "$tmp" "$ETC/campaign_events.json"
                log "discovery: $summary" ;;
        esac
    else
        log "WARNING: discovery failed; polytape-refresh.timer retries every 10 min" \
            "(the recorder cannot start until $ETC/campaign_events.json exists)"
    fi
    rm -f "$tmp"
elif [ -s "$ETC/campaign_events.json" ]; then
    log "keeping the installed $ETC/campaign_events.json (the refresh timer maintains it)"
fi

# ---- 8. enable, (re)start the recorder, then the timers -------------------- #
systemctl enable polytape polytape-refresh.timer polytape-offload.timer polytape-scratch-janitor.timer
if [ -s "$ETC/campaign_events.json" ]; then
    if systemctl is-active --quiet polytape; then
        log "restarting polytape (upgrade; this is one capture gap)"
    else
        log "starting polytape"
    fi
    systemctl restart polytape
else
    log "polytape enabled but NOT started: no $ETC/campaign_events.json yet (refresh timer will create it)"
fi
# Timers last: on a long-booted VM their OnBootSec is already past, so `--now` would
# fire the refresh (and possibly a second restart) right behind the one above.
systemctl start polytape-refresh.timer polytape-offload.timer polytape-scratch-janitor.timer

log "done. Next: journalctl -u polytape -f   |   $PREFIX/healthcheck.sh   |   $PREFIX/polytape-refresh.sh --check"
