"""polytape-admin: a read-only sidecar dashboard over a polytape run.

Separate from the recorder (which stays serve-free). It only *observes* the
run's on-disk outputs (the shared run dir's JSONL files + ``meta.json``) plus
systemd/disk state, and serves a small localhost dashboard. No control surface
in this phase.
"""
