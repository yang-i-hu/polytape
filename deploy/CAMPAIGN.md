# Maker campaign — deployment runbook

Open-ended, multi-family recording on a fresh GCP VM: sports game markets (main
moneyline / spread / total per game for several leagues; all lines for MLB; games
starting within 72 h) plus the hourly BTC/ETH strike ladders (current + next two
hours). One recorder process, ~1,000 markets / ~2,000 tokens / ~12 websocket shards,
feeding the maker-side market-making study and the executor-profile measurement.

This document is the operator's runbook: provision, ship, verify, daily checks, cost,
teardown. The mechanics live in `deploy/` (units, scripts) and are installed by
`deploy/bootstrap.sh`.

| | |
|---|---|
| Project / zone | `polytape-prod-194347` / `europe-west2-a` |
| VM | `polytape-maker` — e2-medium, Debian 12, 20 GB pd-balanced boot |
| Data disk | `polytape-maker-data` — 200 GB pd-balanced, mounted at `/data` |
| Service account (attached) | `polytape-offload@polytape-prod-194347.iam.gserviceaccount.com` — objectAdmin on the archive bucket ONLY; instance scope `storage-rw` |
| Archive bucket | `gs://polytape-prod-194347-archive` (exists; Coldline objects under `run-maker/`) |
| Run dir | `/data/run-maker` (`polytape --run-name maker --out /data`) |

---

## 0. What runs on the VM

```
polytape.service            recorder: --matches-file /etc/polytape/campaign_events.json
                            --open-only --run-name maker --out /data      (user polytape)
polytape-refresh.timer      every 10 min: list_campaign_events.py --spec campaign.json,
                            compare the (event, recorded-market) set, install + restart
                            the recorder ONLY on a genuine change              (root)
polytape-offload.timer      hourly: finished matches + sealed segments -> GCS Coldline,
                            delete locally after verified upload; ADC       (user polytape)
polytape-scratch-janitor.timer  hourly: prune orphaned offload tar scratch     (root)
/opt/polytape/healthcheck.sh    on demand: freshness, sizes, timers, disk
```

Files: `/opt/polytape/{src,venv,scripts/list_campaign_events.py,*.sh,polytape-event-set.py}`,
`/etc/polytape/{campaign.json,campaign_events.json,polytape.env,offload.env,heartbeat.env?}`,
`/data/run-maker/`, `/data/tmp/polytape-offload/` (scratch).

### Decisions (and why)

* **No admin dashboard, no control plane, no autogrow.** `polytape-admin.service`,
  `polytape-control.{service,path}` and `polytape-autogrow.*` from the World Cup
  deployment are deliberately NOT installed. Nobody needs a dashboard for this run —
  `healthcheck.sh` + `journalctl` cover operations — and each of those units is a
  root-run code path (the intent bridge), an extra listener, or an IAM scope
  (`compute-rw` for disk resize) that we would otherwise have to defend. Disk growth is
  bounded by the hourly offload instead of by growing the disk.
* **Auth = attached SA + Application Default Credentials.** The offloader calls
  `storage.Client()` with no key file; credentials come from the metadata server. There
  is no `offload-sa.json` on disk and `POLYTAPE_GCS_KEY` is never set. The SA can touch
  one bucket and nothing else; the instance scope is `storage-rw` (no compute, no
  logging-write, no cloud-platform).
* **Offloader runs as `polytape`, not root.** Everything it touches is under `/data`
  and owned by the recorder user. Fewer root code paths.
* **Ephemeral external IP: keep it.** The VM needs egress for apt/pip (bootstrap and
  upgrades), Gamma (discovery every 10 min) and the CLOB websocket. An external IP is the
  simplest way to get that (Cloud NAT would cost more than the IP and add a component).
  **Inbound: nothing from the internet.** SSH goes through IAP TCP forwarding
  (`--tunnel-through-iap`), which needs a firewall rule from Google's IAP range only.
  No port is ever opened to `0.0.0.0/0`; there is no dashboard to reach anyway.
* **Set changes restart the recorder** — see §7. Roughly once an hour (ladder
  roll-over), a few seconds each.

---

## 1. Provision

Prerequisites on your workstation: `gcloud` authenticated as a project owner/editor,
with `roles/iam.serviceAccountUser` on the SA (to attach it) and
`roles/iap.tunnelResourceAccessor` (to SSH via IAP). Owners have both.

```bash
PROJECT=polytape-prod-194347
ZONE=europe-west2-a
VM=polytape-maker
DISK=polytape-maker-data
SA=polytape-offload@polytape-prod-194347.iam.gserviceaccount.com
BUCKET=polytape-prod-194347-archive

# 0. sanity: the bucket and the scoped SA exist; the SA has NO project-level roles
gcloud storage buckets describe gs://$BUCKET --project=$PROJECT \
  --format='value(name,location,storageClass)'
gcloud storage buckets get-iam-policy gs://$BUCKET --project=$PROJECT \
  --flatten='bindings[].members' --filter="bindings.members:serviceAccount:$SA" \
  --format='value(bindings.role)'                 # expect: roles/storage.objectAdmin
gcloud projects get-iam-policy $PROJECT \
  --flatten='bindings[].members' --filter="bindings.members:serviceAccount:$SA" \
  --format='value(bindings.role)'                 # expect: (empty)

# 1. the data disk (200 GB pd-balanced)
gcloud compute disks create $DISK --project=$PROJECT --zone=$ZONE \
  --size=200GB --type=pd-balanced

# 2. the VM: SA attached with storage-rw, data disk attached as device "polytape-data"
#    (-> /dev/disk/by-id/google-polytape-data, which bootstrap.sh formats + mounts)
gcloud compute instances create $VM --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-medium \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=20GB --boot-disk-type=pd-balanced \
  --disk=name=$DISK,device-name=polytape-data,mode=rw,auto-delete=no \
  --service-account=$SA --scopes=storage-rw \
  --metadata=enable-oslogin=TRUE \
  --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
  --tags=polytape-maker
# (no --no-address: the ephemeral external IP is what gives us egress; see Decisions)

# 3. inbound: SSH via IAP only. One rule, from Google's IAP range, to this tag.
gcloud compute firewall-rules create allow-iap-ssh-polytape-maker --project=$PROJECT \
  --network=default --direction=INGRESS --action=ALLOW --rules=tcp:22 \
  --source-ranges=35.235.240.0/20 --target-tags=polytape-maker
# If the default network still carries default-allow-ssh (tcp:22 from 0.0.0.0/0 to EVERY
# VM), delete it unless something else depends on it:
gcloud compute firewall-rules describe default-allow-ssh --project=$PROJECT --format='value(sourceRanges)' \
  && gcloud compute firewall-rules delete default-allow-ssh --project=$PROJECT --quiet

# 4. can we get in?
gcloud compute ssh $VM --project=$PROJECT --zone=$ZONE --tunnel-through-iap -- \
  'curl -s -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email; echo; lsblk -o NAME,SIZE,TYPE,MOUNTPOINT'
# expect the SA email and an unformatted 200G disk (sdb) with no mountpoint.
```

---

## 2. Ship + bootstrap

Build the source tarball from the branch that carries the campaign work (it must
contain `scripts/list_campaign_events.py`, `deploy/campaign.json` and the `deploy/`
units — bootstrap checks and refuses otherwise), copy it and the bootstrap script up,
run bootstrap as root.

```bash
# on your workstation, in the polytape checkout
# (-c core.autocrlf=false: a Windows checkout would otherwise export CRLF shell scripts
#  and units that break on Debian; bootstrap also strips CR from what it installs)
git -c core.autocrlf=false archive --format=tar.gz --prefix=polytape/ -o /tmp/polytape-src.tar.gz main
gcloud compute scp /tmp/polytape-src.tar.gz deploy/bootstrap.sh $VM:/tmp/ \
  --project=$PROJECT --zone=$ZONE --tunnel-through-iap
gcloud compute ssh $VM --project=$PROJECT --zone=$ZONE --tunnel-through-iap -- \
  "sudo sed -i 's/\r\$//' /tmp/bootstrap.sh && sudo bash /tmp/bootstrap.sh"
```

`bootstrap.sh` is idempotent — re-run it to upgrade (new tarball in `/tmp` first). It:
installs `python3 python3-venv zstd`; creates user `polytape`; formats the data disk if
it has no filesystem, adds an fstab entry by UUID with `nofail`, mounts `/data`; unpacks
the tarball to `/opt/polytape/src`, `pip install "src[admin]"` into
`/opt/polytape/venv` (the `[admin]` extra brings `google-cloud-storage` for the
offloader; the admin *service* is not installed); installs the units, the helper
scripts and `/etc/polytape/{campaign.json,polytape.env,offload.env}` (existing files
are kept); runs discovery once to produce `/etc/polytape/campaign_events.json`; enables
`polytape` + the three timers. A running recorder is restarted (one gap) — that is the
upgrade path.

Environment knobs (`sudo BUCKET=... bash /tmp/bootstrap.sh`): `SRC_TARBALL`, `DATA_DISK`,
`BUCKET`, `RUN_NAME` (default `maker`), `SKIP_APT`, `SKIP_DISCOVERY`.

Optional dead-man's switch (recommended for an unattended run): create a healthchecks.io
check with a 3-minute period / 5-minute grace, then

```bash
sudo install -m 0640 -o root -g polytape /dev/null /etc/polytape/heartbeat.env
echo 'POLYTAPE_HEARTBEAT_URL=https://hc-ping.com/<uuid>' | sudo tee /etc/polytape/heartbeat.env
sudo systemctl restart polytape
```

The recorder pings while frames are flowing; a stalled loop, a dead process or a
crash-looping unit (disk full) all go quiet → alert.

---

## 3. Verify (right after bootstrap)

```bash
# recorder is up and resolved the set: expect "book: N token id(s) across M shard(s)"
# (M ≈ 12 with the main-line filter) and "recording M stream(s)"
sudo journalctl -u polytape -n 60 --no-pager
sudo journalctl -u polytape --no-pager | grep -E 'token id\(s\) across' | tail -1

# health: freshness from meta.json, sizes, timers, disk (exit 1 if stale > 5 min)
sudo /opt/polytape/healthcheck.sh

# meta.json counts directly
python3 -c 'import json; m=json.load(open("/data/run-maker/meta.json")); \
  print(len(m["events"]), "events", len(m["clob_token_ids"]), "tokens", m["counts"], m["last_record_at"])'

# the discovered set vs the spec; what the next refresh would do (changes nothing)
/opt/polytape/venv/bin/python /opt/polytape/polytape-event-set.py --open-only --summary /etc/polytape/campaign_events.json
sudo /opt/polytape/polytape-refresh.sh --check

# timers armed?
systemctl list-timers --all 'polytape-*'

# ADC really works for the offloader: run one pass by hand (it uploads only FINISHED
# matches / sealed segments, so right after bootstrap it usually says "offloaded 0")
sudo systemctl start polytape-offload.service && sudo journalctl -u polytape-offload -n 20 --no-pager
gcloud storage ls gs://$BUCKET/run-maker/ --project=$PROJECT   # from your workstation

# the data disk is the one being written
findmnt /data && df -h /data
```

Healthy signs within the first hour: `counts.book` climbing every few seconds, `age`
in `healthcheck.sh` under 60 s (the crypto ladders trade constantly), `gaps` empty or a
handful of sub-10-s reconnects, one `polytape-refresh` journal line every 10 min saying
`no change (events=N markets=M)` or `event set changed +a -r ~c ... restarted`.

---

## 4. Daily checks (2 minutes)

```bash
sudo /opt/polytape/healthcheck.sh                         # exit 0 = fresh; look at disk %
sudo journalctl -u polytape -p warning --since -24h --no-pager | tail -50   # errors/fatals
sudo journalctl -t polytape-refresh --since -24h --no-pager               # ~144 lines; count "changed"
sudo journalctl -u polytape-offload --since -24h --no-pager | grep -E 'offloaded|failed'
sudo journalctl -t polytape-scratch-janitor --since -24h --no-pager | tail -3
du -sh /data/run-maker /data/tmp                          # note the day-over-day delta
gcloud storage ls -l gs://$BUCKET/run-maker/** --project=$PROJECT | tail -5   # archive growing
```

What to act on:

* `healthcheck.sh` exits 1 (stale) → `systemctl status polytape`, then the journal. A
  Gamma outage at start-up makes the unit crash-loop (`could not start capture`) until
  Gamma is back — that self-heals. `ENOSPC` does not: see disk below.
* `/data` above ~70 % → the offloader is not keeping up or is failing
  (`journalctl -u polytape-offload`). Typical causes: ADC broken (SA detached / scope
  wrong → `403`/`Anonymous caller`), bucket IAM changed, or the offload unit disabled.
  Fix the cause and run `sudo systemctl start polytape-offload.service`. As a last
  resort the disk can be grown online by hand from your workstation
  (`gcloud compute disks resize $DISK --size=300GB` then `sudo resize2fs /dev/disk/by-id/google-polytape-data`
  on the VM) — the VM's own SA deliberately cannot do this.
* Refresh says `discovery failed` for more than an hour → Gamma changed shape or the
  spec is wrong: run the discovery by hand
  (`/opt/polytape/venv/bin/python /opt/polytape/scripts/list_campaign_events.py --spec /etc/polytape/campaign.json --out /tmp/x.json --open-only`).
* `restart of polytape FAILED` → the refresh leaves a `.restart-pending` marker and
  retries next run; check `systemctl status polytape` meanwhile.

---

## 5. Operations

**Change the campaign spec** (leagues, lookahead, ladder count): edit
`/etc/polytape/campaign.json`, then `sudo /opt/polytape/polytape-refresh.sh --check` to
see the resulting delta and `sudo systemctl start polytape-refresh.service` to apply
(installs the new set + one restart). Keep `deploy/campaign.json` in the repo in sync —
bootstrap never overwrites an installed spec but does warn when they differ.

**Upgrade the code**: new tarball → `sudo bash /tmp/bootstrap.sh` (one restart).

**Pause / resume recording**: `sudo systemctl stop polytape polytape-refresh.timer`
(the timer, or it will restart the recorder on the next set change) / `start` both.
Counts in `meta.json` continue cumulatively across restarts.

**Pull data for research**: from GCS (`gcloud storage cp -r gs://$BUCKET/run-maker/ ...`
— Coldline retrieval + egress are billed, see §8); the archive holds finished matches
(`run-maker/matches/event-<id>.tar.gz`, byte-exact `book.jsonl` + `meta.json`) and
the sealed monolith segments. Only the live segment and the still-open matches are on
the VM.

---

## 6. Teardown

```bash
# on the VM: stop the recorder and the timers, run a final offload sweep
sudo systemctl stop polytape polytape-refresh.timer polytape-offload.timer polytape-scratch-janitor.timer
sudo systemctl start polytape-offload.service && sudo journalctl -u polytape-offload -n 20 --no-pager
# whatever is still local (the live segment, matches that were open at stop time):
sudo /opt/polytape/healthcheck.sh || true
sudo gcloud storage cp -r /data/run-maker gs://polytape-prod-194347-archive/run-maker/final/
#   (gcloud on the VM authenticates as the attached SA; objectAdmin on the bucket suffices)
gcloud storage ls -l gs://polytape-prod-194347-archive/run-maker/final/** | wc -l   # from your workstation: verify

# from your workstation: delete the VM, then the data disk (auto-delete=no), then the rule
gcloud compute instances delete $VM --project=$PROJECT --zone=$ZONE --quiet
gcloud compute disks delete $DISK --project=$PROJECT --zone=$ZONE --quiet
gcloud compute firewall-rules delete allow-iap-ssh-polytape-maker --project=$PROJECT --quiet
```

Keep the bucket. It is the only thing that costs money afterwards (Coldline, cents per
GB-month); the SA can stay — it has no other rights.

---

## 7. Known limitations

* **Set changes restart the recorder.** The refresh installs the new events file and
  `systemctl restart polytape`; the process re-resolves every event via Gamma and
  re-opens all shards, which takes a few seconds — a capture gap for *every* market,
  not just the changed event. With the hourly crypto ladders rolling in every hour
  expect ~24 restarts/day. What is logged: the refresh writes
  `event set changed +a -r ~c (...) -> installed + restarted polytape` to the journal
  (`journalctl -t polytape-refresh`); the recorder's `meta.json` gets a new
  `started_at` while `counts` continue cumulatively. The in-process `gaps[]` array
  covers websocket reconnects inside one process; a restart shows up as the
  `started_at` change and a hole in `ts_recv` around it (a future writer change may log
  restarts into `gaps[]` explicitly — if it does, they carry a distinct `note`). The
  refresh only ever restarts on a genuine change of the (event, recorded-market) set —
  price ticks, title edits and list order never trigger it — and never on a failed or
  empty discovery. Live add/remove without reconnect (`operation: subscribe`, see
  PROTOCOL.md §1.2) would remove the gap; it is not implemented.
* **Discovery depends on Gamma.** If Gamma is down the recorder keeps recording the
  last set; new fixtures / ladders are picked up on the next successful refresh.
* **200 GB is a buffer, not the archive.** Local disk is only bounded because the
  hourly offload removes what it has archived; if the offloader fails silently for a
  couple of days, the disk fills and the recorder crash-loops (exit 2). The daily
  check and the (optional) heartbeat are the safety net; there is no autogrow on
  purpose. If the measured data rate turns out far above the WC run's 36 GB/day (this
  set has ~16× the tokens), tighten the offload cadence (`OnUnitActiveSec=30min`) or
  grow the disk from your workstation.
* **No dashboard.** Freshness and sizes come from `healthcheck.sh` over SSH.

---

## 8. Cost (approximate, europe-west2 on-demand list prices — check the calculator)

| Item | Estimate |
|---|---|
| e2-medium (2 vCPU shared, 4 GB) | ≈ $28 / month |
| 200 GB pd-balanced data disk | ≈ $24 / month ($0.12 / GB-month) |
| 20 GB pd-balanced boot disk | ≈ $2.5 / month |
| ephemeral external IPv4 on a running VM | ≈ $3.7 / month |
| egress (subscribe frames, Gamma polls; GCS upload in-region is free) | ≈ $0 |
| **VM total** | **≈ $58 / month** |
| Coldline storage | ≈ $0.007 / GB-month, 90-day minimum; grows with the archive |
| Coldline retrieval + egress when pulling to a workstation | ≈ $0.02 / GB + ≈ $0.12 / GB |

Archive growth: raw JSONL gzips roughly 8–10×. At the WC rate (36 GB/day raw) that is
~4 GB/day → ~120 GB/month → after three months ≈ 360 GB ≈ $2.5/month and rising; if this
campaign records several times that (plausible: 16× the tokens, but ladders are short-
lived and most sports books are quiet), scale accordingly. Measure it after the first
week (`du -sh /data/run-maker` day over day, and `gcloud storage du -s gs://$BUCKET/run-maker`).

---

## 9. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| bootstrap: `no block device at /dev/disk/by-id/google-polytape-data` | The disk was attached with a different `device-name`, or not at all: `lsblk`, `ls /dev/disk/by-id/`, then `DATA_DISK=/dev/disk/by-id/google-<name> sudo bash /tmp/bootstrap.sh`. Never point it at the boot disk. |
| bootstrap: `tarball is missing scripts/list_campaign_events.py` | Built from a branch without the campaign work; rebuild from `main` after the merge. |
| `polytape` crash-loops with `no matching events found` | `/etc/polytape/campaign_events.json` is empty (discovery returned nothing / wrong spec). Run discovery by hand (§4); check the spec. |
| `polytape` won't start, `Dependency failed for ... data.mount` | The data disk is not mounted (`RequiresMountsFor=/data` is doing its job): `findmnt /data`, `journalctl -u data.mount`, `mount /data`. |
| offload: `403` / `Anonymous caller` / `Could not automatically determine credentials` | ADC broken: the VM has no SA attached or the scope is not `storage-rw`. Check `curl -s -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/scopes`. Changing the SA/scopes needs the VM stopped (`gcloud compute instances set-service-account`). |
| offload: `verify failed` | Upload size mismatch — transient; the native dir is kept and retried next hour. Persistent → check the bucket's retention/lifecycle rules. |
| refresh: `discovery failed` every 10 min | Gamma outage or a spec/schema problem. Run the discovery command by hand and read its stderr. |
| `/data` filling although the offloader runs | Offloader only takes *finished* matches / *sealed* segments; a long-lived open set keeps its natives local. Check `healthcheck.sh` "files" and the offload journal; tighten the offload cadence or grow the disk (§4). |
| `journalctl -t polytape-scratch-janitor` pruned something | An offload was hard-killed mid-tar (OOM/reboot); harmless — that match is retried next hour. |
