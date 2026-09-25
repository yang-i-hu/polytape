# Campaign deployment runbook

Open-ended, multi-family recording on a GCP VM: sports game markets (main
moneyline / spread / total per game for several leagues; all lines for MLB; games
starting within 72 h) plus the hourly BTC/ETH strike ladders (current + next two
hours). One recorder process, ~800–1,000 markets / ~1,600–2,000 tokens / ~9–12
websocket shards (781 / 1,562 / 9 measured on 2026-09-03 with NFL, NBA, NHL and UCL
not yet in season), feeding market microstructure research and the
executor-profile measurement.

This document is the operator's runbook: provision, ship, verify, daily checks, cost,
teardown. The mechanics live in `deploy/` (units, scripts) and are installed by
`deploy/bootstrap.sh`.

Names in angle brackets (`<gcp-project>`, `<zone>`, `<vm-name>`, `<disk-name>`,
`<device-name>`, `<offload-sa>`, `<archive-bucket>`) are placeholders for the
deployment's own identifiers, which are kept out of this repo; substitute your values.

| | |
|---|---|
| Project / zone | `<gcp-project>` / `<zone>` |
| VM | `<vm-name>` — e2-medium, Debian 12, 20 GB pd-balanced boot (EXISTS, running, no network tags yet — see §1) |
| Data disk | `<disk-name>` — 200 GB pd-balanced, attached as device-name `<device-name>` (`/dev/disk/by-id/google-<device-name>`), mounted at `/data` |
| Service account (attached) | `<offload-sa>@<gcp-project>.iam.gserviceaccount.com` — objectAdmin on the archive bucket ONLY; instance scope `storage-rw` |
| Archive bucket | `gs://<archive-bucket>` (exists; Coldline objects under `run-maker/` — `event-<id>.tar.gz` and `segments/`; the World Cup run's `matches/` and `run-wc/` sit beside it) |
| Run dir | `/data/run-maker` (`polytape --run-name maker --no-per-match --out /data`; monolith segments only — measured ~45 MB/min in game hours, so no per-match dual write) |

---

## 0. What runs on the VM

```
polytape.service            recorder: --matches-file /etc/polytape/campaign_events.json
                            --open-only --run-name maker --no-per-match --out /data   (user polytape)
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
* **Set changes restart the recorder** — see §7. Roughly 50–100 times a day (each
  hour's new ladder, each batch of fixtures entering the 72 h window; finished games
  are rolled out lazily), ~5 s each.

---

## 1. Provision

Prerequisites on your workstation: `gcloud` authenticated as a project owner/editor,
with `roles/iam.serviceAccountUser` on the SA (to attach it) and
`roles/iap.tunnelResourceAccessor` (to SSH via IAP). Owners have both.

The VM `<vm-name>` and its data disk `<disk-name>` ALREADY EXIST (created
by hand; the disk is attached as device-name `<device-name>`, the SA is attached with
`storage-rw`). Steps 1–2 below are only for re-creating them from scratch; on the
existing VM go straight to step 2b (adopt) and step 3 (firewall — in THIS order, or
you lock yourself out).

```bash
# fill in your deployment's values (see the placeholder note at the top)
PROJECT=<gcp-project>
ZONE=<zone>
VM=<vm-name>
DISK=<disk-name>
DEVICE=<device-name>             # GCE device-name -> /dev/disk/by-id/google-$DEVICE
SA=<offload-sa>@$PROJECT.iam.gserviceaccount.com
BUCKET=<archive-bucket>

# 0. sanity: the bucket and the scoped SA exist; the SA has NO project-level roles
gcloud storage buckets describe gs://$BUCKET --project=$PROJECT \
  --format='value(name,location,storageClass)'
# (get-iam-policy for buckets does not accept --filter; filter client-side)
gcloud storage buckets get-iam-policy gs://$BUCKET --project=$PROJECT --format=json \
  | python3 -c "import json,sys; p=json.load(sys.stdin); print([b['role'] for b in p.get('bindings',[]) if 'serviceAccount:$SA' in b.get('members',[])])"
#                                                   # expect: ['roles/storage.objectAdmin']
gcloud projects get-iam-policy $PROJECT \
  --flatten='bindings[].members' --filter="bindings.members:serviceAccount:$SA" \
  --format='value(bindings.role)'                 # expect: (empty)

# 1. (only if it does not exist) the data disk (200 GB pd-balanced)
gcloud compute disks create $DISK --project=$PROJECT --zone=$ZONE \
  --size=200GB --type=pd-balanced

# 2. (only if it does not exist) the VM: SA attached with storage-rw, data disk attached
#    as device "$DEVICE" (-> /dev/disk/by-id/google-$DEVICE, which bootstrap.sh formats + mounts)
gcloud compute instances create $VM --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-medium \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=20GB --boot-disk-type=pd-balanced \
  --disk=name=$DISK,device-name=$DEVICE,mode=rw,auto-delete=no \
  --service-account=$SA --scopes=storage-rw \
  --metadata=enable-oslogin=TRUE \
  --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
  --tags=polytape-maker
# (no --no-address: the ephemeral external IP is what gives us egress; see Decisions)

# 2b. adopt the EXISTING VM: it has no network tags, so the IAP rule in step 3 would not
#     apply to it. Tag it first (and enable OS Login if wanted); verify the disk is there.
gcloud compute instances add-tags $VM --project=$PROJECT --zone=$ZONE --tags=polytape-maker
gcloud compute instances add-metadata $VM --project=$PROJECT --zone=$ZONE --metadata=enable-oslogin=TRUE
gcloud compute instances describe $VM --project=$PROJECT --zone=$ZONE \
  --format='yaml(tags.items,disks[].deviceName,serviceAccounts[].scopes)'
#   expect tags [polytape-maker], deviceNames [persistent-disk-0, <device-name>], storage-rw

# 3. inbound: SSH via IAP only. Create the IAP rule for the tag, PROVE the tunnel works,
#    and only then delete default-allow-ssh (tcp:22 from 0.0.0.0/0 to EVERY VM) — deleting
#    it first would leave an untagged VM with no rule allowing port 22 at all.
gcloud compute firewall-rules create allow-iap-ssh-polytape-maker --project=$PROJECT \
  --network=default --direction=INGRESS --action=ALLOW --rules=tcp:22 \
  --source-ranges=35.235.240.0/20 --target-tags=polytape-maker
gcloud compute ssh $VM --project=$PROJECT --zone=$ZONE --tunnel-through-iap -- 'echo iap-ok' \
  && gcloud compute firewall-rules delete default-allow-ssh --project=$PROJECT --quiet

# 4. can we get in?
gcloud compute ssh $VM --project=$PROJECT --zone=$ZONE --tunnel-through-iap -- \
  'curl -s -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email; echo; lsblk -o NAME,SIZE,TYPE,MOUNTPOINT; ls -l /dev/disk/by-id/ | grep google-'
# expect the SA email, an unformatted 200G disk (sdb) with no mountpoint, and
# /dev/disk/by-id/google-<device-name> -> ../../sdb.
```

---

## 2. Ship + bootstrap

Build the source tarball from the branch that carries the campaign work (it must
contain `scripts/list_campaign_events.py`, `deploy/campaign.json` and the `deploy/`
units — bootstrap checks and refuses otherwise), copy it and the bootstrap script up,
run bootstrap as root.

```bash
# on your workstation, in the polytape checkout, on the branch that carries the campaign
# work (campaign-v2 until it is merged; `main` only after the merge — bootstrap refuses a
# tarball without scripts/list_campaign_events.py)
# (-c core.autocrlf=false: a Windows checkout would otherwise export CRLF shell scripts
#  and units that break on Debian; bootstrap also strips CR from what it installs)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
git -c core.autocrlf=false archive --format=tar.gz --prefix=polytape/ -o /tmp/polytape-src.tar.gz "$BRANCH"
gcloud compute scp /tmp/polytape-src.tar.gz deploy/bootstrap.sh $VM:/tmp/ \
  --project=$PROJECT --zone=$ZONE --tunnel-through-iap
# DATA_DISK and BUCKET have no defaults: a first install needs both (a re-run finds
# /data mounted and offload.env written, and needs neither). bootstrap dies (before touching
# anything) if the path does not exist — it never falls back to the boot disk.
gcloud compute ssh $VM --project=$PROJECT --zone=$ZONE --tunnel-through-iap -- \
  "sudo sed -i 's/\r\$//' /tmp/bootstrap.sh && sudo BUCKET=$BUCKET DATA_DISK=/dev/disk/by-id/google-$DEVICE bash /tmp/bootstrap.sh"
```

`bootstrap.sh` is idempotent — re-run it to upgrade (new tarball in `/tmp` first). It:
installs `python3 python3-venv zstd`; creates user `polytape`; formats the data disk if
it has no filesystem AND no partition table (refuses anything else), adds an fstab
entry by UUID with `nofail`, mounts `/data`; unpacks the tarball to
`/opt/polytape/src`, `pip install -c deploy/constraints.txt "src[admin]"` (the recorder
dependencies pinned to what `uv.lock` holds — the set CI tests; no `--upgrade`, so a
re-run never silently moves a dependency) into `/opt/polytape/venv` (the `[admin]` extra brings
`google-cloud-storage` for the offloader; the admin *service* is not installed);
installs the units (the recorder unit's `--run-name` follows `RUN_NAME`), the helper
scripts and `/etc/polytape/{campaign.json,polytape.env,offload.env}` (existing files
are kept); on a FIRST install runs discovery once to produce
`/etc/polytape/campaign_events.json` (only if it found events — on a re-run the
installed set is left to the refresh timer); enables `polytape` + the three timers,
(re)starts the recorder and only then starts the timers. A running recorder is
restarted (one gap) — that is the upgrade path.

Environment knobs (`sudo BUCKET=... bash /tmp/bootstrap.sh`): `SRC_TARBALL`, `DATA_DISK`,
`BUCKET` (these two have no defaults; required on a first install), `RUN_NAME`
(default `maker`; drives the run dir, the recorder's `--run-name`
and the GCS prefix `run-<RUN_NAME>` together), `SKIP_APT`, `SKIP_DISCOVERY`.

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
# recorder is up and resolved the set: expect "resolved N/N event(s)", then
# "book: N token id(s) across M shard(s)" (M ≈ 9–12 with the main-line filter) and
# "recording M stream(s)"
sudo journalctl -u polytape -n 60 --no-pager
sudo journalctl -u polytape --no-pager | grep -E 'token id\(s\) across' | tail -1

# health: freshness from meta.json, sizes, timers, disk (exit 1 if stale > 15 min)
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
`no change (events=N markets=M) tags: mlb=343/4p nfl=528/6p ...`,
`deferring pure roll-out -r ...` or `event set changed +a -r ~c ... restarted`. The
`tags:` tail lists open events / pages per league tag — an off-season league shows a
real count, a typo'd tag `0/1p`, and a truncated listing says `TRUNCATED`.

Spot-check the main-line picks once (the reason the file carries the quote fields):

```bash
python3 - <<'EOF'
import json
for e in json.load(open("/etc/polytape/campaign_events.json")):
    for m in e["record_markets"]:
        if m["sportsMarketType"] in ("spreads", "totals") and (m["liquidityNum"] or 0) < 100:
            print(e["family"], e["title"], m["sportsMarketType"], m["line"], m["bestBid"], m["bestAsk"], m["liquidityNum"])
EOF
# expect few or no lines: a main line with <$100 resting is a dead alternate (see §7)
```

---

## 4. Daily checks (2 minutes)

```bash
sudo /opt/polytape/healthcheck.sh                         # exit 0 = fresh; look at disk % and "re-entered"
sudo journalctl -u polytape -p warning --since -24h --no-pager | tail -50   # errors/fatals
sudo journalctl -t polytape-refresh --since -24h --no-pager               # ~144 lines; count "changed" / "deferring"
sudo journalctl -u polytape-offload --since -24h --no-pager | grep -E 'offloaded|failed|re-entered|leaving it alone'
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
  Fix the cause and run `sudo systemctl start polytape-offload.service`. The offloader
  also skips anything whose staging would leave `/data` with less than 5 GiB free
  (`not enough free space to stage ...` in its journal) rather than pushing the recorder
  into ENOSPC — at that point grow the disk. As a last resort the disk can be grown
  online by hand from your workstation
  (`gcloud compute disks resize $DISK --size=300GB` then `sudo resize2fs /dev/disk/by-id/google-<device-name>`
  on the VM) — the VM's own SA deliberately cannot do this.
* Refresh says `discovery failed` for more than an hour → Gamma changed shape or the
  spec is wrong. The journal line carries the discovery's last stderr lines and the
  unit shows in `systemctl --failed`; to see everything run the discovery by hand
  (`/opt/polytape/venv/bin/python /opt/polytape/scripts/list_campaign_events.py --spec /etc/polytape/campaign.json --out /tmp/x.json --open-only --previous /etc/polytape/campaign_events.json`).
* `healthcheck.sh` reports `re-entered (native + marker, next part pending)` growing
  day over day → those matches came back after being archived and are waiting for
  their quiet time; if the number never goes down the offloader is failing on them
  (`journalctl -u polytape-offload | grep -E 're-entered|leaving it alone'`).
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
(`run-maker/event-<id>.tar.gz`, byte-exact `book.jsonl` + `meta.json`, crc32c-verified;
a match that re-entered the open set after its first archive — a postponement, or an
event a restart failed to resolve once — has its later native(s) beside it as
`run-maker/event-<id>.part2.tar.gz`, `.part3...`; the local marker's `parts` lists
them, and a consumer must concatenate the parts' `book.jsonl` in part order) and the
sealed monolith segments (`run-maker/segments/book.<day>.jsonl.zst`, the complete
backup — every record of every match is in there regardless of parts). Only the live
segment and the still-open matches are on the VM.

---

## 6. Teardown

```bash
# on the VM: stop the recorder and the timers, run a final offload sweep
sudo systemctl stop polytape polytape-refresh.timer polytape-offload.timer polytape-scratch-janitor.timer
sudo systemctl start polytape-offload.service && sudo journalctl -u polytape-offload -n 20 --no-pager
# whatever is still local (the live segment, matches that were open at stop time):
sudo /opt/polytape/healthcheck.sh || true
sudo gcloud storage cp -r /data/run-maker gs://<archive-bucket>/run-maker/final/
#   (gcloud on the VM authenticates as the attached SA; objectAdmin on the bucket suffices)
gcloud storage ls -l gs://$BUCKET/run-maker/final/** | wc -l   # from your workstation: verify

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
  re-opens all shards, which takes ~5 s (measured: 275 events resolved in ~3 s, all
  shards subscribed at 4.7 s, plus the SIGTERM stop) — a capture gap for *every*
  market, not just the changed event, and each restart re-emits a `book` snapshot per
  token. Expect **50–100 restarts/day**, not one per hour: each hour's new ladder
  appears ~20 min before its hour and is a restart (its first ≤10 min are unrecorded);
  fixtures enter the 72 h window in ~28 distinct 10-min buckets a day. Two things keep
  it from being far worse: (a) main-line picks are **sticky** (the refresh passes the
  installed file back as `--previous`, and a pick is kept while it is still an open,
  quoted line priced inside 0.30–0.70 — a line the market moved away from is
  re-picked), without which adjacent lines trading places as odds move changed the
  set on nearly every tick while CFB was in the window; (b) a change that only
  REMOVES events (finished games, expired ladders) is **deferred** — a finished market
  yields nothing, so it stays subscribed — until the next addition or 60 min since the
  last install (`POLYTAPE_DEFER_REMOVALS_MIN`). What is logged: the refresh writes
  `event set changed +a -r ~c (...) -> installed + restarted polytape` or
  `deferring pure roll-out -r ...` to the journal (`journalctl -t polytape-refresh`);
  the recorder's `meta.json` gets a new `started_at` while `counts` continue
  cumulatively. The in-process `gaps[]` array covers websocket reconnects inside one
  process; a restart shows up as the `started_at` change and a hole in `ts_recv`
  around it (a future writer change may log restarts into `gaps[]` explicitly — if it
  does, they carry a distinct `note`). The refresh only ever restarts on a genuine
  change of the (event, recorded-market) set — price ticks, title edits and list
  order never trigger it — and never on a failed or empty discovery. Live add/remove
  without reconnect (`operation: subscribe`, see PROTOCOL.md §1.2) would remove the
  gap; it is not implemented. Measure the real rate in the first week
  (`journalctl -t polytape-refresh --since -24h | grep -c restarted`).
* **"Main line" means the quoted line, not the 0.50/0.50 one.** Gamma's
  `outcomePrices` is the bid/ask midpoint, so an alternate with no book at all sits at
  exactly 0.50/0.50; the pick therefore considers only markets with a real two-sided
  quote (`spread <= 0.20`, bid > 0.05, ask < 0.95) and prefers, among those, the one
  closest to 0.50, then the most liquid. The quote fields are written into
  `campaign_events.json` (`bestBid`/`bestAsk`/`spread`/`liquidityNum`) so every pick is
  auditable (see the §3 spot-check).
* **A match can be archived in parts.** The offloader treats a match as finished only
  when it is out of the recorder's open set, NOT named as open by the installed
  `campaign_events.json` (`POLYTAPE_EVENTS_FILE` in `offload.env` — the set the
  recorder was TOLD to record, so an event one process failed to resolve stays
  protected) AND its native `book.jsonl` has been quiet for 30 min
  (`POLYTAPE_MATCH_MIN_AGE_S`); it re-reads both sets before touching each match and
  refuses to delete a native that changed under it. A match that re-enters
  the set anyway (a postponement, an event one restart failed to resolve) is archived
  again as `event-<id>.part2.tar.gz` (see §5); the monolith segments hold every record
  regardless.
* **Discovery depends on Gamma.** If Gamma is down the recorder keeps recording the
  last set; new fixtures / ladders are picked up on the next successful refresh. A
  failed discovery leaves `polytape-refresh.service` failed (with the error in the
  journal) until the next run succeeds.
* **200 GB is a buffer, not the archive.** Local disk is only bounded because the
  hourly offload removes what it has archived; if the offloader fails silently for a
  couple of days, the disk fills and the recorder crash-loops (exit 2). The daily
  check and the (optional) heartbeat are the safety net; there is no autogrow on
  purpose. If the measured data rate turns out far above the WC run's 36 GB/day (this
  set has ~16× the tokens), tighten the offload cadence (`OnUnitActiveSec=30min`) or
  grow the disk from your workstation.
* **No dashboard.** Freshness and sizes come from `healthcheck.sh` over SSH.

---

## 8. Cost (approximate on-demand list prices for the VM's region; check the calculator)

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

Archive growth: raw JSONL gzips roughly 8–10×. Measured on 2026-09-03 (a US afternoon
with MLB + CFB + ladders; NFL/NBA/NHL not yet in season): a steady 0.29 MB/s ≈ 25 GB/day
into the monolith, and the same again into the per-match natives, i.e. ~50 GB/day of
local writes and ~3 GB/day of archive. Peak local footprint ≈ today's segment +
yesterday's until it is offloaded + the open set's natives + up to ~1.5 h of finished
natives ≈ 90–110 GB, so 200 GB is ~2× headroom today. A day's segment can only be
reclaimed after the day closes, so a tighter offload cadence helps natives only — if the
rate doubles once NFL/NBA/NHL join, grow the disk rather than the cadence. Re-measure
after the first week (`du -sh /data/run-maker` day over day, and
`gcloud storage du -s gs://$BUCKET/run-maker`).

---

## 9. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| bootstrap: `BUCKET unset` | A first install (no `/etc/polytape/offload.env` yet) without `BUCKET=<archive-bucket>`; pass it as in §2. |
| bootstrap: `no block device at ...` | `DATA_DISK` was not passed (it has no default), or the disk was attached with a different `device-name`, or not at all: `lsblk`, `ls /dev/disk/by-id/`, then `DATA_DISK=/dev/disk/by-id/google-<name> sudo bash /tmp/bootstrap.sh`. Never point it at the boot disk. |
| bootstrap: `carries a ... partition table; refusing to format it` | The disk is not blank (a partitioned disk from somewhere else). Check `lsblk -f`; wipe it deliberately (`wipefs -a`) only if you are sure it holds nothing. |
| bootstrap: `tarball is missing scripts/list_campaign_events.py` | Built from a branch without the campaign work; rebuild from the branch that carries it (`campaign-v2` until merged, `main` after). |
| `polytape` crash-loops with `no matching events found` | `/etc/polytape/campaign_events.json` is empty (discovery returned nothing / wrong spec). Run discovery by hand (§4); check the spec. |
| offload: `native book.jsonl changed during the offload` / `event N re-entered the open set` | Harmless: the first leaves the match for the next hour; the second archives the returned match as its next `.partN.tar.gz` (§5). |
| offload: `native dir is still on disk although its archive ... was verified` | The delete after a verified upload failed (permissions?). The archive is good; remove the dir by hand once you have checked `ls -l`. |
| `polytape` won't start, `Dependency failed for ... data.mount` | The data disk is not mounted (`RequiresMountsFor=/data` is doing its job): `findmnt /data`, `journalctl -u data.mount`, `mount /data`. |
| offload: `403` / `Anonymous caller` / `Could not automatically determine credentials` | ADC broken: the VM has no SA attached or the scope is not `storage-rw`. Check `curl -s -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/scopes`. Changing the SA/scopes needs the VM stopped (`gcloud compute instances set-service-account`). |
| offload: `verify failed` | Upload size mismatch — transient; the native dir is kept and retried next hour. Persistent → check the bucket's retention/lifecycle rules. |
| refresh: `discovery failed` every 10 min | Gamma outage or a spec/schema problem. Run the discovery command by hand and read its stderr. |
| `/data` filling although the offloader runs | Offloader only takes *finished* matches / *sealed* segments; a long-lived open set keeps its natives local. Check `healthcheck.sh` "files" and the offload journal; tighten the offload cadence or grow the disk (§4). |
| `journalctl -t polytape-scratch-janitor` pruned something | An offload was hard-killed mid-tar (OOM/reboot); harmless — that match is retried next hour. |
