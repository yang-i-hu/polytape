"""Tests for the maker-campaign deployment scripts under deploy/ — fully offline.

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
import shutil
import subprocess
import sys
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
a = ap.parse_args()
if os.environ.get("FAKE_DISCOVERY_FAIL"):
    sys.exit(1)
assert os.path.exists(a.spec), a.spec
shutil.copyfile(os.environ["FAKE_DISCOVERY_SRC"], a.out)
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
        }
        self.env.pop("FAKE_DISCOVERY_FAIL", None)
        self.env.pop("SHIM_SYSTEMCTL_RC", None)

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


@needs_bash
def test_refresh_noop_on_discovery_failure(tmp_path):
    r = Refresh(tmp_path)
    r.install(_events())
    before = r.cur.read_bytes()
    out = r.run(_events(extra_market=True), FAKE_DISCOVERY_FAIL="1")
    assert out.returncode == 0, out.stderr
    assert "discovery failed" in out.stdout
    assert r.cur.read_bytes() == before and r.restarts() == []


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
    assert r.cur.read_bytes() == before and r.restarts() == []


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
    out = _healthcheck(run_dir)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "ok: fresh" in out.stdout and "RESULT: healthy" in out.stdout
    assert "gaps (this process): 1" in out.stdout
    assert "book.jsonl" in out.stdout
    assert "per-match: 1 native dir(s), 1 offloaded marker(s)" in out.stdout


@needs_bash
def test_healthcheck_stale_or_missing_meta_fails(tmp_path):
    run_dir = tmp_path / "run-maker"
    run_dir.mkdir()
    (run_dir / "meta.json").write_text(json.dumps(_meta(timedelta(minutes=10))), encoding="utf-8")
    out = _healthcheck(run_dir)
    assert out.returncode == 1, out.stdout + out.stderr
    assert "STALE: last record" in out.stdout and "RESULT: UNHEALTHY" in out.stdout
    assert _healthcheck(run_dir, "--stale-after", "1200").returncode == 0  # threshold is a knob
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
    example = (DEPLOY / "offload.env.example").read_text(encoding="utf-8")
    active = {ln.split("=", 1)[0] for ln in example.splitlines() if ln and not ln.startswith("#")}
    assert active == {
        "POLYTAPE_RUN_DIR",
        "POLYTAPE_GCS_BUCKET",
        "POLYTAPE_GCS_PREFIX",
        "POLYTAPE_GCS_STORAGE_CLASS",
        "POLYTAPE_SCRATCH_DIR",
    }
    assert "POLYTAPE_RUN_DIR=/data/run-maker" in example
    assert "POLYTAPE_GCS_STORAGE_CLASS=COLDLINE" in example
    assert "POLYTAPE_SCRATCH_DIR=/data/tmp/polytape-offload" in example


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
