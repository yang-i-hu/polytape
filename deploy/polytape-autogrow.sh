#!/bin/bash
# polytape disk auto-grow guard.
#
# The recorder APPENDS forever and never deletes, so the data volume (/data) only
# grows. This watches its usage and, when it crosses a threshold, grows the backing
# GCP persistent disk and extends the filesystem online (no reboot, recorder keeps
# running). It is a SAFETY NET, not a substitute for retention/offload — see the
# hard cap below.
#
# Safety rails (all enforced here, every run):
#   - NO-OP below AUTOGROW_THRESHOLD_PCT — we only act on a genuinely full volume.
#   - HARD CAP at AUTOGROW_MAX_GB — past this we refuse to grow and exit non-zero
#     (so the timer/journal surfaces it for a human) instead of billing unbounded.
#   - Resize is online and additive; we never shrink and never touch /data's data.
#   - A transient gcloud/API failure is logged and exits non-zero — never silent.
#
# Prerequisites (verify on the VM before trusting this):
#   1. The instance SERVICE ACCOUNT can resize the disk:
#        - IAM role roles/compute.instanceAdmin.v1 (or a custom role with
#          compute.disks.resize) on the project/disk, AND
#        - the instance access scope includes compute-rw or cloud-platform.
#      Test:  gcloud compute disks describe "$AUTOGROW_DISK" --zone "$ZONE" --project "$PROJECT"
#      !! polytape-rec currently has NO service account attached, so the on-VM
#         resize WILL fail until you attach one. Attaching a SA requires the VM to
#         be STOPPED (brief recorder downtime):
#           gcloud compute instances stop polytape-rec --zone=europe-west2-a --project=polytape-prod-194347
#           gcloud compute instances set-service-account polytape-rec \
#             --service-account=<SA_EMAIL> --scopes=cloud-platform \
#             --zone=europe-west2-a --project=polytape-prod-194347
#           gcloud compute instances start polytape-rec --zone=europe-west2-a --project=polytape-prod-194347
#         then grant that SA roles/compute.instanceAdmin.v1 (or compute.disks.resize).
#      If you can't take downtime, drop the resize step and run it remotely instead
#      (your laptop's user creds CAN resize); keep only the FS-grow half on the VM.
#   2. growpart (pkg cloud-guest-utils) + resize2fs (e2fsprogs) or xfs_growfs
#      (xfsprogs) installed.  apt-get install -y cloud-guest-utils
#   3. gcloud installed and on root's PATH.
#
# Config: /etc/polytape/autogrow.env (see deploy/autogrow.env.example). Run as root
# by polytape-autogrow.timer (every ~5 min).
set -uo pipefail

ENV_FILE=${AUTOGROW_ENV_FILE:-/etc/polytape/autogrow.env}
[ -r "$ENV_FILE" ] && . "$ENV_FILE"

# ---- config (env-overridable; sane defaults) -------------------------------- #
MOUNT=${AUTOGROW_MOUNT:-/data}
THRESHOLD=${AUTOGROW_THRESHOLD_PCT:-85}   # grow when used% >= this
INCREMENT=${AUTOGROW_INCREMENT_GB:-50}    # add this many GB per grow
MAX_GB=${AUTOGROW_MAX_GB:-500}            # never grow beyond this (cost ceiling)
DISK=${AUTOGROW_DISK:-}                   # GCP disk NAME backing $MOUNT (required)

log() { logger -t polytape-autogrow "$*"; echo "polytape-autogrow: $*"; }
die() { log "$*"; exit 1; }

md() { curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/$1"; }
PROJECT=${AUTOGROW_PROJECT:-$(md project/project-id)}
ZONE=${AUTOGROW_ZONE:-$(basename "$(md instance/zone)")}

[ -n "$DISK" ]    || die "AUTOGROW_DISK unset; set the GCP disk name in $ENV_FILE (find it: gcloud compute disks list)"
[ -n "$PROJECT" ] || die "could not resolve project (set AUTOGROW_PROJECT)"
[ -n "$ZONE" ]    || die "could not resolve zone (set AUTOGROW_ZONE)"
command -v gcloud   >/dev/null || die "gcloud not found on PATH"
command -v growpart >/dev/null || die "growpart not found (apt-get install cloud-guest-utils)"

# ---- 1. only act on a genuinely full volume --------------------------------- #
USE=$(df --output=pcent "$MOUNT" 2>/dev/null | tail -1 | tr -dc '0-9')
[ -n "$USE" ] || die "could not read usage for $MOUNT (mounted?)"
if [ "$USE" -lt "$THRESHOLD" ]; then
    log "ok: $MOUNT at ${USE}% (< ${THRESHOLD}%); no action"
    exit 0
fi

# ---- 2. compute the new size under the hard cap ----------------------------- #
CUR_GB=$(gcloud compute disks describe "$DISK" --zone "$ZONE" --project "$PROJECT" \
            --format='value(sizeGb)' 2>/dev/null)
[ -n "$CUR_GB" ] || die "could not describe disk '$DISK' (name/zone/project right? SA can read it?)"

if [ "$CUR_GB" -ge "$MAX_GB" ]; then
    die "ALERT: $MOUNT at ${USE}% but disk '$DISK' is already at the ${MAX_GB}GB cap — manual intervention needed (resize cap, or add retention/offload)"
fi
NEW_GB=$(( CUR_GB + INCREMENT ))
[ "$NEW_GB" -gt "$MAX_GB" ] && NEW_GB=$MAX_GB

log "GROW: $MOUNT at ${USE}% -> resizing disk '$DISK' ${CUR_GB}GB -> ${NEW_GB}GB"

# ---- 3. resize the GCP disk (online) ---------------------------------------- #
if ! gcloud compute disks resize "$DISK" --size="${NEW_GB}GB" \
        --zone "$ZONE" --project "$PROJECT" --quiet; then
    die "gcloud disk resize FAILED (SA lacks compute.disks.resize, or wrong scope?) — disk unchanged"
fi

# ---- 4. extend the partition + filesystem online ---------------------------- #
DEV=$(findmnt -no SOURCE "$MOUNT")              # e.g. /dev/sdb1 or /dev/sdb
FSTYPE=$(findmnt -no FSTYPE "$MOUNT")
PKNAME=$(lsblk -no PKNAME "$DEV" 2>/dev/null | head -1)   # parent disk if partitioned

if [ -n "$PKNAME" ]; then
    PARTNUM=$(basename "$DEV" | grep -o '[0-9]*$')
    # growpart is a no-op-safe extend of the partition to fill the grown disk.
    growpart "/dev/$PKNAME" "$PARTNUM" || log "growpart reported no change (already full-span?)"
fi

case "$FSTYPE" in
    ext2|ext3|ext4) resize2fs "$DEV"    || die "resize2fs failed on $DEV (disk grew but FS did not)";;
    xfs)            xfs_growfs "$MOUNT" || die "xfs_growfs failed on $MOUNT (disk grew but FS did not)";;
    *)              die "unsupported fstype '$FSTYPE' on $MOUNT — grow it by hand";;
esac

NEWUSE=$(df --output=pcent "$MOUNT" 2>/dev/null | tail -1 | tr -dc '0-9')
log "done: disk '$DISK' now ${NEW_GB}GB; $MOUNT now at ${NEWUSE}%"
