"""Tests for the campaign deployment scripts under deploy/ — fully offline.

The units and scripts cannot run on the target VM here, but their contracts can:

* the (event, market-set) keyer ``polytape-event-set.py`` is pure Python;
* ``polytape-refresh.sh`` and ``healthcheck.sh`` run under bash with the discovery
  script, ``systemctl``, ``logger`` and ``flock`` replaced by recording fakes
  (skipped where bash is unavailable);
* the unit files are checked for the flags the runbook relies on.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"


def _load_keyer():
    spec = importlib.util.spec_from_file_location(
        "polytape_event_set", DEPLOY / "polytape-event-set.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


keyer = _load_keyer()


def _find_bash() -> str | None:
    candidates = [os.environ.get("POLYTAPE_TEST_BASH")]
    if sys.platform == "win32":
        # Prefer Git's bash over WSL's (which cannot see Windows paths the same way).
        candidates += [
            r"C:\Program Files\Git\bin\bash.exe",
            r"C:\Program Files\Git\usr\bin\bash.exe",
        ]
    candidates.append(shutil.which("bash"))
    for c in candidates:
        if c and Path(c).exists():
            return c
    return None


BASH = _find_bash()
needs_bash = pytest.mark.skipif(BASH is None, reason="bash not available")


def _posix(path: Path | str) -> str:
    return str(path).replace("\\", "/")


# --------------------------------------------------------------------------- #
# polytape-event-set.py (pure)
# --------------------------------------------------------------------------- #


def _events(*, extra_market: bool = False, closed_first: bool = False) -> list[dict]:
    markets = [{"conditionId": "0xb2", "question": "BTC > 60k"}, {"conditionId": "0xb1"}]
    if extra_market:
        markets.append({"conditionId": "0xb3"})
    return [
        {
            "event_id": "960280",
            "title": "BTC ladder 4pm",
            "closed": closed_first,
            "markets": markets,
        },
        {"event_id": "1234", "closed": False, "record_markets": [{"id": "77"}, {"id": "78"}]},
    ]


def test_event_set_is_sorted_and_order_invariant():
    forward = keyer.event_set(_events())
    shuffled = list(reversed(_events()))
    shuffled[1]["markets"] = list(reversed(shuffled[1]["markets"]))
    assert keyer.event_set(shuffled) == forward
    assert forward == [("1234", ["77", "78"]), ("960280", ["0xb1", "0xb2"])]
    assert keyer.format_lines(forward) == "1234\t77,78\n960280\t0xb1,0xb2\n"


def test_event_set_changes_only_with_events_or_recorded_markets():
    base = keyer.format_lines(keyer.event_set(_events()))
    cosmetic = _events()
    cosmetic[0]["title"] = "renamed"
    cosmetic[0]["markets"][0]["outcomePrices"] = ["0.51", "0.49"]
    assert keyer.format_lines(keyer.event_set(cosmetic)) == base
    assert keyer.format_lines(keyer.event_set(_events(extra_market=True))) != base
    assert keyer.format_lines(keyer.event_set(_events()[:1])) != base


def test_event_set_open_only_skips_closed_and_tolerates_junk():
    events = _events(closed_first=True) + ["junk", {"no_id": 1}, {"event_id": ""}]
    assert [e for e, _ in keyer.event_set(events)] == ["1234", "960280"]
    assert [e for e, _ in keyer.event_set(events, open_only=True)] == ["1234"]


def test_market_ids_accepts_bare_ids_condition_id_and_key_override():
    entry = {"event_id": "1", "markets": ["m2", "m1", 3], "custom": [{"condition_id": "0xc"}]}
    assert keyer.market_ids(entry) == ["3", "m1", "m2"]
    assert keyer.market_ids(entry, "custom") == ["0xc"]
    assert keyer.market_ids({"event_id": "1"}) == []  # no market list at all


def test_event_set_cli(tmp_path, capsys, monkeypatch):
    path = tmp_path / "events.json"
    path.write_text(json.dumps(_events(closed_first=True)), encoding="utf-8")
    assert keyer.main([str(path)]) == 0
    assert capsys.readouterr().out == "1234\t77,78\n960280\t0xb1,0xb2\n"
    assert keyer.main(["--open-only", str(path)]) == 0
    assert capsys.readouterr().out == "1234\t77,78\n"
    assert keyer.main(["--summary", str(path)]) == 0
    assert capsys.readouterr().out == "events=2 markets=4\n"
    monkeypatch.setenv("POLYTAPE_MARKETS_KEY", "record_markets")
    assert keyer.main([str(path)]) == 0
    assert capsys.readouterr().out == "1234\t77,78\n960280\t\n"  # key not present -> no markets

    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    assert keyer.main([str(bad)]) == 1
    assert capsys.readouterr().out == ""  # nothing on stdout -> caller sees "no key"
    notlist = tmp_path / "obj.json"
    notlist.write_text("{}", encoding="utf-8")
    assert keyer.main([str(notlist)]) == 1
    assert keyer.main([str(tmp_path / "missing.json")]) == 1


# --------------------------------------------------------------------------- #
# polytape-refresh.sh (bash + fakes)
# --------------------------------------------------------------------------- #

_FAKE_DISCOVERY = """\
import argparse, os, shutil, sys
ap = argparse.ArgumentParser()
ap.add_argument("--spec", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--open-only", action="store_true")
ap.add_argument("--previous", default=None)
a = ap.parse_args()
if os.environ.get("FAKE_DISCOVERY_FAIL"):
    print("error fetching from Gamma: <urlopen error simulated outage>", file=sys.stderr)
    sys.exit(1)
assert os.path.exists(a.spec), a.spec
if os.environ.get("FAKE_DISCOVERY_PREVIOUS_LOG"):
    with open(os.environ["FAKE_DISCOVERY_PREVIOUS_LOG"], "a") as fh:
        fh.write(str(a.previous) + "\\n")
shutil.copyfile(os.environ["FAKE_DISCOVERY_SRC"], a.out)
print("tags: mlb=2/1p nfl=0/1p", file=sys.stderr)
"""

_SHIMS = {
    "systemctl": '#!/bin/sh\necho "systemctl $*" >> "$SHIM_LOG"\nexit "${SHIM_SYSTEMCTL_RC:-0}"\n',
    "logger": "#!/bin/sh\nexit 0\n",
    "flock": "#!/bin/sh\nexit 0\n",
}


class Refresh:
    """A sandboxed polytape-refresh.sh: fake discovery + PATH shims + env overrides."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.cur = tmp_path / "campaign_events.json"
        self.pending = tmp_path / "campaign_events.json.restart-pending"
        self.shim_log = tmp_path / "shim.log"
        self.discovered = tmp_path / "discovered.json"
        self.previous_log = tmp_path / "previous.log"  # the --previous each discovery got
        (tmp_path / "campaign.json").write_text("{}", encoding="utf-8")
        (tmp_path / "disc.py").write_text(_FAKE_DISCOVERY, encoding="utf-8")
        shims = tmp_path / "shims"
        shims.mkdir()
        for name, body in _SHIMS.items():
            p = shims / name
            p.write_text(body, encoding="utf-8", newline="\n")
            p.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": str(shims) + os.pathsep + os.environ.get("PATH", ""),
            "SHIM_LOG": _posix(self.shim_log),
            "TMPDIR": _posix(tmp_path),
            "POLYTAPE_PY": _posix(sys.executable),
            "POLYTAPE_DISCOVERY": _posix(tmp_path / "disc.py"),
            "POLYTAPE_EVENT_SET": _posix(DEPLOY / "polytape-event-set.py"),
            "POLYTAPE_CAMPAIGN_SPEC": _posix(tmp_path / "campaign.json"),
            "POLYTAPE_EVENTS_FILE": _posix(self.cur),
            "POLYTAPE_LOCK": _posix(tmp_path / "refresh.lock"),
            "FAKE_DISCOVERY_SRC": _posix(self.discovered),
            "FAKE_DISCOVERY_PREVIOUS_LOG": _posix(self.previous_log),
        }
        self.env.pop("FAKE_DISCOVERY_FAIL", None)
        self.env.pop("SHIM_SYSTEMCTL_RC", None)
        self.env.pop("POLYTAPE_DEFER_REMOVALS_MIN", None)

    def install(self, events: list) -> None:
        self.cur.write_text(json.dumps(events, indent=1), encoding="utf-8")

    def run(self, discovered: list | None, *args: str, **env: str) -> subprocess.CompletedProcess:
        if discovered is not None:
            self.discovered.write_text(json.dumps(discovered), encoding="utf-8")
        return subprocess.run(
            [BASH, _posix(DEPLOY / "polytape-refresh.sh"), *args],
            env={**self.env, **env},
            capture_output=True,
            text=True,
            timeout=120,
        )

    def restarts(self) -> list[str]:
        if not self.shim_log.exists():
            return []
        return [ln for ln in self.shim_log.read_text().splitlines() if "restart" in ln]

    def previous_args(self) -> list[str]:
        if not self.previous_log.exists():
            return []
        return self.previous_log.read_text().splitlines()

    def age_install(self, seconds: float) -> None:
        t = time.time() - seconds
        os.utime(self.cur, (t, t))


@needs_bash
def test_refresh_fails_loudly_on_discovery_failure(tmp_path):
    # The unit must show as failed (exit 1) with the discovery's own error in the
    # journal — not a generic line and exit 0 — while the live set stays untouched.
    r = Refresh(tmp_path)
    r.install(_events())
    before = r.cur.read_bytes()
    out = r.run(_events(extra_market=True), FAKE_DISCOVERY_FAIL="1")
    assert out.returncode == 1, out.stdout + out.stderr
    assert "discovery failed" in out.stdout and "simulated outage" in out.stdout
    assert r.cur.read_bytes() == before and r.restarts() == []
    assert not r.pending.exists()


@needs_bash
def test_refresh_noop_on_empty_or_garbage_discovery(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    before = r.cur.read_bytes()
    out = r.run([])
    assert out.returncode == 0 and "no open events" in out.stdout, out.stdout + out.stderr
    r.discovered.write_text("{not json", encoding="utf-8")
    out = r.run(None)
    assert out.returncode == 0 and "no open events" in out.stdout
    assert r.cur.read_bytes() == before and r.restarts() == []


@needs_bash
def test_refresh_noop_when_only_cosmetics_changed(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    before = r.cur.read_bytes()
    cosmetic = list(reversed(_events()))
    cosmetic[1]["title"] = "renamed"
    cosmetic[1]["markets"] = list(reversed(cosmetic[1]["markets"]))
    out = r.run(cosmetic)
    assert out.returncode == 0, out.stderr
    assert "no change (events=2 markets=4)" in out.stdout
    assert "tags: mlb=2/1p nfl=0/1p" in out.stdout  # the per-tag diagnostics reach the journal
    assert r.cur.read_bytes() == before and r.restarts() == []
    assert r.previous_args() == [_posix(r.cur)]  # the installed file feeds sticky picks


@needs_bash
def test_refresh_installs_and_restarts_when_a_market_set_changes(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    new = _events(extra_market=True)
    out = r.run(new)
    assert out.returncode == 0, out.stderr
    assert "event set changed +0 -0 ~1 (events=2 markets=5; was events=2 markets=4)" in out.stdout
    assert "installed + restarted polytape" in out.stdout
    assert json.loads(r.cur.read_text(encoding="utf-8")) == new
    assert r.restarts() == ["systemctl restart polytape"]
    assert not r.pending.exists()


@needs_bash
def test_refresh_counts_added_and_removed_events_and_first_install(tmp_path):
    r = Refresh(tmp_path)
    out = r.run(_events())  # nothing installed yet -> everything is "added"
    assert out.returncode == 0, out.stderr
    assert "+2 -0 ~0 (events=2 markets=4; was none installed)" in out.stdout
    assert r.restarts() == ["systemctl restart polytape"]
    r.shim_log.unlink()
    out = r.run(_events()[:1] + [{"event_id": "5", "markets": [{"conditionId": "0x5"}]}])
    assert "+1 -1 ~0 (events=2 markets=3; was events=2 markets=4)" in out.stdout
    assert r.restarts() == ["systemctl restart polytape"]
    # no --previous on the first run (nothing installed), the installed file afterwards
    assert r.previous_args() == ["None", _posix(r.cur)]


@needs_bash
def test_refresh_defers_pure_removals_until_the_last_install_is_old(tmp_path):
    # A finished market yields nothing, so leaving it subscribed costs nothing; the
    # restart costs a gap on EVERY market. Only removals -> wait for the next addition
    # or for the installed file to be DEFER_MIN old.
    r = Refresh(tmp_path)
    r.install(_events())  # mtime = now
    before = r.cur.read_bytes()
    out = r.run(_events()[:1])
    assert out.returncode == 0, out.stdout + out.stderr
    assert (
        "deferring pure roll-out +0 -1 ~0" in out.stdout and "last install <60m ago" in out.stdout
    )
    assert r.cur.read_bytes() == before and r.restarts() == [] and not r.pending.exists()
    out = r.run(_events()[:1], "--check")
    assert "would defer" in out.stdout and r.restarts() == []
    # the knob: 0 minutes = never defer
    out = r.run(_events()[:1], "--check", POLYTAPE_DEFER_REMOVALS_MIN="0")
    assert "would install" in out.stdout
    # an old install -> the roll-out goes through
    r.age_install(2 * 3600)
    out = r.run(_events()[:1])
    assert out.returncode == 0, out.stdout + out.stderr
    assert "event set changed +0 -1 ~0" in out.stdout and "restarted polytape" in out.stdout
    assert r.restarts() == ["systemctl restart polytape"]
    assert json.loads(r.cur.read_text(encoding="utf-8")) == _events()[:1]
    # an addition is never deferred, however fresh the install
    r.shim_log.unlink()
    out = r.run(_events())
    assert "event set changed +1 -0 ~0" in out.stdout and r.restarts() == [
        "systemctl restart polytape"
    ]


@needs_bash
def test_refresh_check_mode_changes_nothing(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    before = r.cur.read_bytes()
    out = r.run(_events(extra_market=True), "--check")
    assert out.returncode == 0, out.stderr
    assert "CHECK: event set changed +0 -0 ~1" in out.stdout and "would install" in out.stdout
    assert r.cur.read_bytes() == before and r.restarts() == [] and not r.pending.exists()
    out = r.run(_events(), "--check")
    assert "no change" in out.stdout
    assert r.run(_events(), "--bogus").returncode == 2


@needs_bash
def test_refresh_retries_a_failed_restart_next_run(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    new = _events(extra_market=True)
    out = r.run(new, SHIM_SYSTEMCTL_RC="1")
    assert out.returncode == 1
    assert "restart of polytape FAILED" in out.stdout
    assert json.loads(r.cur.read_text(encoding="utf-8")) == new  # installed anyway
    assert r.pending.exists()
    # Same set again: normally a no-op, but the pending marker forces the retry.
    out = r.run(new)
    assert out.returncode == 0, out.stderr
    assert "installed + restarted polytape" in out.stdout
    assert r.restarts() == ["systemctl restart polytape"] * 2
    assert not r.pending.exists()
    out = r.run(new)
    assert "no change" in out.stdout and len(r.restarts()) == 2


# --------------------------------------------------------------------------- #
# healthcheck.sh (bash)
# --------------------------------------------------------------------------- #


def _meta(last_age: timedelta | None) -> dict:
    now = datetime.now(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    return {
        "started_at": (now - timedelta(hours=3)).strftime(fmt),
        "stopped_at": None,
        "counts": {"book": 12345},
        "last_record_at": (now - last_age).strftime(fmt) if last_age is not None else None,
        "clob_token_ids": ["t1", "t2"],
        "events": [{"id": "1"}],
        "gaps": [
            {
                "stream": "book",
                "disconnected_at": "x",
                "reconnected_at": "y",
                "downtime_seconds": 5.5,
                "note": "reconnect",
            }
        ],
    }


def _healthcheck(run_dir: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [BASH, _posix(DEPLOY / "healthcheck.sh"), "--run-dir", _posix(run_dir), *args],
        env={**os.environ, "POLYTAPE_PY": _posix(sys.executable)},
        capture_output=True,
        text=True,
        timeout=120,
    )


@needs_bash
def test_healthcheck_fresh_run_is_healthy(tmp_path):
    run_dir = tmp_path / "run-maker"
    run_dir.mkdir()
    (run_dir / "meta.json").write_text(json.dumps(_meta(timedelta(seconds=20))), encoding="utf-8")
    (run_dir / "book.jsonl").write_text("x" * 100, encoding="utf-8")
    (run_dir / "matches" / "event-1").mkdir(parents=True)
    (run_dir / "matches" / "event-2.offloaded.json").write_text("{}", encoding="utf-8")
    # event 3 re-entered the open set after being archived: native dir + marker side by side
    (run_dir / "matches" / "event-3").mkdir()
    (run_dir / "matches" / "event-3.offloaded.json").write_text("{}", encoding="utf-8")
    out = _healthcheck(run_dir)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "ok: fresh" in out.stdout and "RESULT: healthy" in out.stdout
    assert "gaps (this process): 1" in out.stdout
    assert "book.jsonl" in out.stdout
    assert "process age:" in out.stdout  # a just-restarted recorder is not a stalled one
    assert "per-match: 2 native dir(s), 2 offloaded marker(s), 1 re-entered" in out.stdout


@needs_bash
def test_healthcheck_stale_or_missing_meta_fails(tmp_path):
    run_dir = tmp_path / "run-maker"
    run_dir.mkdir()
    # 10 min quiet is NOT stale by default (900 s): last_record_at only advances on a
    # written record, and a pre-game European-morning window can be quiet for minutes.
    (run_dir / "meta.json").write_text(json.dumps(_meta(timedelta(minutes=10))), encoding="utf-8")
    assert _healthcheck(run_dir).returncode == 0
    (run_dir / "meta.json").write_text(json.dumps(_meta(timedelta(minutes=20))), encoding="utf-8")
    out = _healthcheck(run_dir)
    assert out.returncode == 1, out.stdout + out.stderr
    assert "STALE: last record" in out.stdout and "RESULT: UNHEALTHY" in out.stdout
    assert _healthcheck(run_dir, "--stale-after", "1800").returncode == 0  # threshold is a knob
    assert _healthcheck(run_dir, "--stale-after", "300").returncode == 1
    (run_dir / "meta.json").write_text(json.dumps(_meta(None)), encoding="utf-8")
    assert _healthcheck(run_dir).returncode == 1  # nothing recorded yet
    assert _healthcheck(tmp_path / "nowhere").returncode == 1  # no meta.json at all


# --------------------------------------------------------------------------- #
# static checks on the unit files / scripts
# --------------------------------------------------------------------------- #


@needs_bash
@pytest.mark.parametrize("script", sorted(p.name for p in DEPLOY.glob("*.sh")))
def test_shell_scripts_parse(script):
    out = subprocess.run([BASH, "-n", _posix(DEPLOY / script)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


def _env_example() -> dict[str, str]:
    """The active ``KEY=value`` lines of deploy/offload.env.example."""
    text = (DEPLOY / "offload.env.example").read_text(encoding="utf-8")
    return dict(ln.split("=", 1) for ln in text.splitlines() if ln and not ln.startswith("#"))


def _bootstrap_offload_env() -> dict[str, str]:
    """What bootstrap.sh's heredoc writes to /etc/polytape/offload.env, with its shell
    variables resolved to their defaults."""
    text = (DEPLOY / "bootstrap.sh").read_text(encoding="utf-8")
    start = text.index('cat > "$ETC/offload.env" <<EOF')
    heredoc = text[start : text.index("\nEOF", start)]
    subs = {
        "$DATA_MOUNT": "/data",
        "$RUN_NAME": "maker",
        "$BUCKET": "<archive-bucket>",
        "$ETC": "/etc/polytape",
    }
    out: dict[str, str] = {}
    for ln in heredoc.splitlines():
        if ln.startswith("POLYTAPE_") and "=" in ln:
            key, value = ln.split("=", 1)
            for var, default in subs.items():
                value = value.replace(var, default)
            out[key] = value
    return out


def _unit(name: str) -> dict[str, list[str]]:
    """Directives of a unit file as {key: [values...]} (comments dropped)."""
    out: dict[str, list[str]] = {}
    for line in (DEPLOY / name).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "[")):
            continue
        key, _, value = line.partition("=")
        out.setdefault(key.strip(), []).append(value.strip())
    return out


def test_recorder_unit_records_the_campaign_file_into_run_maker():
    unit = _unit("polytape.service")
    (exec_start,) = unit["ExecStart"]
    for flag in (
        "--matches-file /etc/polytape/campaign_events.json",
        "--open-only",
        "--run-name maker",
        "--out /data",
    ):
        assert flag in exec_start
    assert unit["User"] == ["polytape"]
    assert unit["RequiresMountsFor"] == ["/data"]
    assert "/etc/polytape/polytape.env" in unit["EnvironmentFile"]
    assert "-/etc/polytape/heartbeat.env" in unit["EnvironmentFile"]
    for hardening in ("NoNewPrivileges", "ProtectSystem", "ProtectHome", "PrivateTmp"):
        assert hardening in unit


def test_offload_unit_uses_adc_and_idle_io():
    unit = _unit("polytape-offload.service")
    assert unit["ExecStart"] == [
        "/opt/polytape/venv/bin/python -m polytape.admin.offload --log-level INFO"
    ]
    assert unit["IOSchedulingClass"] == ["idle"]
    assert unit["User"] == ["polytape"]
    assert unit["EnvironmentFile"] == ["/etc/polytape/offload.env"]
    assert unit["RequiresMountsFor"] == ["/data"]
    example = _env_example()
    assert set(example) == {
        "POLYTAPE_RUN_DIR",
        "POLYTAPE_GCS_BUCKET",
        "POLYTAPE_GCS_PREFIX",
        "POLYTAPE_GCS_STORAGE_CLASS",
        "POLYTAPE_SCRATCH_DIR",
        "POLYTAPE_EVENTS_FILE",
    }
    assert example["POLYTAPE_RUN_DIR"] == "/data/run-maker"
    assert example["POLYTAPE_GCS_STORAGE_CLASS"] == "COLDLINE"
    assert example["POLYTAPE_SCRATCH_DIR"] == "/data/tmp/polytape-offload"
    # the offloader's "finished" guard reads the same file the recorder records from
    assert example["POLYTAPE_EVENTS_FILE"] == "/etc/polytape/campaign_events.json"
    assert (
        "--matches-file /etc/polytape/campaign_events.json"
        in _unit("polytape.service")["ExecStart"][0]
    )


def test_timers_cadence():
    assert _unit("polytape-refresh.timer")["OnUnitActiveSec"] == ["10min"]
    assert _unit("polytape-offload.timer")["OnUnitActiveSec"] == ["1h"]
    assert _unit("polytape-scratch-janitor.timer")["OnUnitActiveSec"] == ["1h"]
    for timer in ("polytape-refresh", "polytape-offload", "polytape-scratch-janitor"):
        assert _unit(f"{timer}.timer")["Persistent"] == ["true"]
        assert "ExecStart" in _unit(f"{timer}.service")


def test_bootstrap_installs_only_the_campaign_units():
    text = (DEPLOY / "bootstrap.sh").read_text(encoding="utf-8")
    start = text.index("UNITS=(")
    units = text[start : text.index(")", start)]
    for unit in (
        "polytape.service",
        "polytape-refresh.service",
        "polytape-refresh.timer",
        "polytape-offload.service",
        "polytape-offload.timer",
        "polytape-scratch-janitor.service",
        "polytape-scratch-janitor.timer",
    ):
        assert unit in units
    for excluded in ("polytape-admin", "polytape-control", "polytape-autogrow"):
        assert excluded not in units
    assert "POLYTAPE_GCS_KEY=" not in text  # ADC only: no key path is ever written
    assert "[admin]" in text  # google-cloud-storage for the offloader
    assert "nofail" in text
    assert "python3-venv zstd" in text
    assert r"sed -i 's/\r$//'" in text  # a CRLF tarball (Windows checkout) must not break the VM


def test_janitor_sweeps_offload_scratch_and_reads_offload_env():
    script = (DEPLOY / "polytape-scratch-janitor.sh").read_text(encoding="utf-8")
    assert "polytape-offload-*" in script
    assert "/etc/polytape/offload.env" in script
    assert (
        "-/etc/polytape/offload.env" in _unit("polytape-scratch-janitor.service")["EnvironmentFile"]
    )


# --------------------------------------------------------------------------- #
# polytape-scratch-janitor.sh (bash + fakes)
# --------------------------------------------------------------------------- #

_JANITOR_SHIMS = {
    "systemctl": '#!/bin/sh\n[ "$1" = is-active ] && exit "${SHIM_ACTIVE_RC:-3}"\nexit 0\n',
    "logger": "#!/bin/sh\nexit 0\n",
}


def _janitor(tmp_path: Path, scratch: Path, **env: str) -> subprocess.CompletedProcess:
    shims = tmp_path / "jshims"
    if not shims.exists():
        shims.mkdir()
        for name, body in _JANITOR_SHIMS.items():
            p = shims / name
            p.write_text(body, encoding="utf-8", newline="\n")
            p.chmod(0o755)
    env_file = tmp_path / "offload.env"
    env_file.write_text(f"POLYTAPE_SCRATCH_DIR={_posix(scratch)}\n", encoding="utf-8", newline="\n")
    return subprocess.run(
        [BASH, _posix(DEPLOY / "polytape-scratch-janitor.sh")],
        env={
            **os.environ,
            "PATH": str(shims) + os.pathsep + os.environ.get("PATH", ""),
            "JANITOR_ENV_FILE": _posix(env_file),
            "JANITOR_AGE_MIN": "180",
            **env,
        },
        capture_output=True,
        text=True,
        timeout=120,
    )


def _scratch_dir(base: Path, name: str, *, dir_age_s: float, file_age_s: float) -> Path:
    d = base / name
    d.mkdir(parents=True)
    f = d / "book.2026-09-01.jsonl.zst"
    f.write_bytes(b"z" * 10)
    now = time.time()
    os.utime(f, (now - file_age_s, now - file_age_s))
    os.utime(d, (now - dir_age_s, now - dir_age_s))  # after the file: creating it touched the dir
    return d


@needs_bash
def test_janitor_prunes_only_idle_orphans_and_never_under_a_running_offloader(tmp_path):
    scratch = tmp_path / "scratch"
    old = 4 * 3600
    orphan = _scratch_dir(scratch, "polytape-offload-orphan", dir_age_s=old, file_age_s=old)
    # a dir's mtime is set when its .zst entry is created and never advances while the
    # file is written or read: a 4-hour-old dir with a fresh file is a long upload, not an orphan
    busy = _scratch_dir(scratch, "polytape-offload-busy", dir_age_s=old, file_age_s=0)
    fresh = _scratch_dir(scratch, "polytape-offload-fresh", dir_age_s=0, file_age_s=0)
    other = _scratch_dir(scratch, "somebody-else", dir_age_s=old, file_age_s=old)
    out = _janitor(tmp_path, scratch, SHIM_ACTIVE_RC="0")  # offloader running -> hands off
    assert out.returncode == 0, out.stdout + out.stderr
    assert "skip: polytape-offload.service is running" in out.stdout
    assert orphan.exists()
    out = _janitor(tmp_path, scratch)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "pruned 1 orphaned scratch dir(s)" in out.stdout
    assert not orphan.exists()
    assert busy.exists() and fresh.exists() and other.exists()
    out = _janitor(tmp_path, scratch)
    assert "ok: no orphaned scratch" in out.stdout


# --------------------------------------------------------------------------- #
# bootstrap / runbook / units: the contracts the VM deployment relies on
# --------------------------------------------------------------------------- #


def test_bootstrap_offload_env_matches_the_example_and_the_runbook():
    boot, example = _bootstrap_offload_env(), _env_example()
    assert boot == example, f"bootstrap writes {boot}, the example says {example}"
    assert example["POLYTAPE_GCS_PREFIX"] == "run-maker"  # NOT run-maker/matches
    runbook = (DEPLOY / "CAMPAIGN.md").read_text(encoding="utf-8")
    assert "`run-maker/event-<id>.tar.gz`" in runbook and "`run-maker/segments/" in runbook
    assert "run-maker/matches" not in runbook
    assert "POLYTAPE_EVENTS_FILE" in runbook


def test_bootstrap_targets_the_existing_disk_and_installs_safely():
    text = (DEPLOY / "bootstrap.sh").read_text(encoding="utf-8")
    # no baked-in disk or bucket: a first install passes both, a re-run needs neither
    assert "DATA_DISK=${DATA_DISK:-}" in text and "BUCKET=${BUCKET:-}" in text
    assert re.search(r"by-id/google-[a-z0-9]", text) is None  # only google-<device-name>
    assert '[ -n "$BUCKET" ] || [ -f "$ETC/offload.env" ]' in text
    # a partitioned disk is somebody's: refuse to mkfs it, and show what was there
    assert 'blkid -o value -s PTTYPE "$DATA_DISK"' in text
    assert "partition table; refusing to format it" in text
    assert 'lsblk -f "$DATA_DISK"' in text
    # pinned, non-upgrading install from the tarball's own constraints file
    assert '"$VENV/bin/pip" install --quiet -c "$SRC/deploy/constraints.txt" "$SRC[admin]"' in text
    assert "pip install --quiet --upgrade" not in text
    assert (
        "deploy/constraints.txt" in text.split('die "tarball is missing')[0]
    )  # required in the tarball
    # RUN_NAME drives the unit's --run-name too (offload.env and the unit stay in lock-step)
    assert "s|--run-name maker|--run-name $RUN_NAME|" in text
    # discovery on a FIRST install only, never installing an empty result
    assert '[ ! -s "$ETC/campaign_events.json" ]' in text
    assert "events=0*)" in text
    # the recorder is (re)started BEFORE the timers start, and never `enable --now`
    assert text.index("systemctl restart polytape") < text.index(
        "systemctl start polytape-refresh.timer"
    )
    assert "enable --now" not in text


def test_constraints_pin_the_recorder_deps_to_the_lock():
    lock = (DEPLOY.parent / "uv.lock").read_text(encoding="utf-8")
    cons = (DEPLOY / "constraints.txt").read_text(encoding="utf-8")
    pins = dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^\s;]+)", cons, re.M))
    for dep in ("websockets", "httpx"):
        locked = re.search(rf'^name = "{dep}"\nversion = "([^"]+)"', lock, re.M).group(1)
        assert pins[dep] == locked, f"{dep}: constraints {pins.get(dep)} vs lock {locked}"
    assert "google-cloud-storage" not in pins  # the [admin] extra is not locked: floats


def test_recorder_unit_run_name_matches_offload_env_and_has_fd_headroom():
    unit = _unit("polytape.service")
    (exec_start,) = unit["ExecStart"]
    assert (
        "--run-name maker" in exec_start and _env_example()["POLYTAPE_RUN_DIR"] == "/data/run-maker"
    )
    assert int(unit["LimitNOFILE"][0]) >= 4096  # one per-match handle per open event


def test_refresh_unit_is_bounded_and_identified():
    unit = _unit("polytape-refresh.service")
    assert unit["TimeoutStartSec"] == ["10min"]  # a oneshot has no start timeout by default
    assert unit["SyslogIdentifier"] == ["polytape-refresh"]  # journalctl -t polytape-refresh


def test_runbook_targets_the_existing_vm_and_orders_the_firewall_change_safely():
    runbook = (DEPLOY / "CAMPAIGN.md").read_text(encoding="utf-8")
    assert "VM=<vm-name>" in runbook and "DISK=<disk-name>" in runbook
    assert "polytape-maker-data" not in runbook
    assert re.search(r"by-id/google-[a-z0-9]", runbook) is None  # $DEVICE / <device-name> only
    assert (
        "BUCKET=$BUCKET DATA_DISK=/dev/disk/by-id/google-$DEVICE bash /tmp/bootstrap.sh" in runbook
    )
    assert "resize2fs /dev/disk/by-id/google-<device-name>" in runbook
    # adopt (tag) the untagged VM, create the IAP rule, PROVE the tunnel, only then delete
    # default-allow-ssh — the other order locks the VM out of SSH
    assert (
        runbook.index("add-tags $VM")
        < runbook.index("firewall-rules create allow-iap-ssh")
        < runbook.index("echo iap-ok")
        < runbook.index("firewall-rules delete default-allow-ssh")
    )
    for line in runbook.splitlines():
        if "buckets get-iam-policy" in line:
            assert "--filter" not in line  # that subcommand rejects --filter
    assert '-o /tmp/polytape-src.tar.gz "$BRANCH"' in runbook  # not `main` until merged
    assert "M ≈ 9–12" in runbook
