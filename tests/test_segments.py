"""Tests for daily monolith segments on the admin side.

- ``download.monolith_files``: scan order over the legacy ``book.jsonl`` + daily
  segments, and the filtered / whole-run downloads reading all of them;
- ``offload.offload_segments``: the closed-segment sweep (skip today / young / open /
  marked, compress -> upload -> verify -> marker -> delete, failure keeps the file),
  with a fake backend and a fake compressor — no network, no ``zstd`` binary;
- the CLI wiring (``--segments-only`` / ``--matches-only`` / ``--list``).
"""

from __future__ import annotations

import collections
import json
import os
import time
from pathlib import Path

import pytest

from polytape.admin import download as dl
from polytape.admin import offload as ofl

TODAY = "2026-09-03"


# --------------------------------------------------------------------------- #
# Fakes + fixtures
# --------------------------------------------------------------------------- #


class FakeBackend:
    """In-memory object store standing in for GCS."""

    def __init__(self, *_a, **_k) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_upload_for: set[str] = set()

    def upload(self, local_path, name):
        if name in self.fail_upload_for:
            raise OSError("simulated upload failure")
        data = Path(local_path).read_bytes()
        self.objects[name] = data
        return {
            "gs_uri": f"gs://fake/{name}",
            "size": len(data),
            "crc32c": "c",
            "storage_class": "X",
        }

    def stat(self, name):
        d = self.objects.get(name)
        return None if d is None else {"size": len(d), "crc32c": "c"}

    def signed_url(self, name, ttl_seconds):
        return f"https://signed.example/{name}?ttl={ttl_seconds}"


def fake_compress(src: Path, dest: Path) -> None:
    """A pure-Python stand-in for the zstd CLI: a tagged copy, so tests can check bytes."""
    dest.write_bytes(b"ZSTD" + src.read_bytes())


def _write_lines(path: Path, lines) -> None:
    # newline="" so \n is written verbatim (match the recorder; avoid Windows \r\n).
    path.write_text("".join(line + "\n" for line in lines), encoding="utf-8", newline="")


def _age(path: Path, seconds: float) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


@pytest.fixture(autouse=True)
def _no_headroom(monkeypatch):
    """tmp_path sits on whatever disk CI has; the headroom rule is tested explicitly."""
    monkeypatch.setattr(ofl, "DEFAULT_HEADROOM_BYTES", 0)


def _run_meta(open_segment: str | None = "book.2026-09-03.jsonl") -> dict:
    segments = {"book": {"current": open_segment, "seen": [open_segment]}} if open_segment else {}
    return {"events": [], "counts": {"book": 5}, "segments": segments}


def _make_run(tmp_path: Path, *, open_segment: str | None = "book.2026-09-03.jsonl") -> Path:
    """A run dir with a legacy monolith, two closed segments, today's segment, and a native."""
    run = tmp_path / "run-camp"
    run.mkdir()
    (run / "meta.json").write_text(json.dumps(_run_meta(open_segment)), encoding="utf-8")
    _write_lines(run / "book.jsonl", ["legacy1"])
    _write_lines(run / "book.2026-09-01.jsonl", ["s1a", "s1b"])
    _write_lines(run / "book.2026-09-02.jsonl", ["s2a"])
    _write_lines(run / "book.2026-09-03.jsonl", ["s3a"])  # today: open, never touched
    for name in ("book.2026-09-01.jsonl", "book.2026-09-02.jsonl"):
        _age(run / name, 2 * 3600)  # long quiet -> closed
    native = run / "matches" / "event-1"
    native.mkdir(parents=True)
    _write_lines(native / "book.jsonl", ["n1"])
    return run


def _seg_lines(run: Path, name: str) -> list[str]:
    return (run / name).read_text(encoding="utf-8").splitlines()


# --------------------------------------------------------------------------- #
# download: scan order + downloads over legacy + segments
# --------------------------------------------------------------------------- #


def test_monolith_files_orders_legacy_then_segments_by_day(tmp_path):
    run = _make_run(tmp_path)
    # distractors that must be ignored
    _write_lines(run / "comments.jsonl", ["c"])
    _write_lines(run / "book.notaday.jsonl", ["x"])
    (run / "book.2026-09-01.jsonl.zst").write_bytes(b"zst")
    (run / "segments").mkdir()
    (run / "segments" / "book.2026-08-31.offloaded.json").write_text("{}", encoding="utf-8")
    assert [p.name for p in dl.monolith_files(run)] == [
        "book.jsonl",
        "book.2026-09-01.jsonl",
        "book.2026-09-02.jsonl",
        "book.2026-09-03.jsonl",
    ]
    (run / "book.jsonl").unlink()  # a run born rotating has no legacy file
    assert [p.name for p in dl.monolith_files(run)] == [
        "book.2026-09-01.jsonl",
        "book.2026-09-02.jsonl",
        "book.2026-09-03.jsonl",
    ]
    assert dl.monolith_files(tmp_path / "nowhere") == []


def _rec(market: str, rid: str) -> str:
    return json.dumps(
        {
            "stream": "book",
            "id": rid,
            "ts_recv": "2026-09-02T00:00:00.000000Z",
            "ts_server": None,
            "raw": {"event_type": "book", "market": market},
        }
    )


def _events_meta() -> dict:
    return {
        "run_name": "camp",
        "streams": ["book"],
        "counts_by_event": {"1001": {"book": 3}},
        "events": [
            {"id": "1001", "title": "A vs. B", "markets": [{"conditionId": "0xA"}]},
            {"id": "1002", "title": "C vs. D", "markets": [{"conditionId": "0xB"}]},
        ],
    }


def test_filter_run_scans_legacy_and_every_segment_in_order(tmp_path):
    run = tmp_path / "run-camp"
    run.mkdir()
    (run / "meta.json").write_text(json.dumps(_events_meta()), encoding="utf-8")
    _write_lines(run / "book.jsonl", [_rec("0xA", "legacy-a"), _rec("0xB", "legacy-b")])
    _write_lines(run / "book.2026-09-01.jsonl", [_rec("0xA", "d1-a")])
    _write_lines(run / "book.2026-09-02.jsonl", [_rec("0xB", "d2-b"), _rec("0xA", "d2-a")])
    dest = tmp_path / "out"
    entries = dl.filter_run(run, ["1001"], dest, exported_at="2026-09-03T00:00:00Z")
    assert {arc for arc, _ in entries} == {"event-1001/book.jsonl", "event-1001/meta.json"}
    ids = [json.loads(x)["id"] for x in _seg_lines(dest, "event-1001/book.jsonl")]
    assert ids == ["legacy-a", "d1-a", "d2-a"]  # chronological: legacy, then day by day
    # byte-exact: the slice is the original line bytes
    original = (run / "book.2026-09-02.jsonl").read_bytes().splitlines()[1]
    assert original in (dest / "event-1001" / "book.jsonl").read_bytes().splitlines()


def test_whole_run_entries_ships_legacy_and_segments(tmp_path):
    run = _make_run(tmp_path)
    entries = dl.whole_run_entries(run)
    assert [arc for arc, _ in entries] == [
        "run-camp/meta.json",
        "run-camp/book.jsonl",
        "run-camp/book.2026-09-01.jsonl",
        "run-camp/book.2026-09-02.jsonl",
        "run-camp/book.2026-09-03.jsonl",
    ]
    assert all(Path(p).parent == run for _, p in entries)  # verbatim, no copies


# --------------------------------------------------------------------------- #
# offload: selection
# --------------------------------------------------------------------------- #


def test_offloadable_segments_selection_rules(tmp_path):
    run = _make_run(tmp_path)
    meta = _run_meta()
    # closed (day < today, quiet, not open, no marker) -> both old days
    assert ofl.offloadable_segments(run, meta, today=TODAY) == [
        "book.2026-09-01.jsonl",
        "book.2026-09-02.jsonl",
    ]
    # a young file (written < min_age_s ago) is not closed yet
    _age(run / "book.2026-09-02.jsonl", 60)
    assert ofl.offloadable_segments(run, meta, today=TODAY) == ["book.2026-09-01.jsonl"]
    assert ofl.offloadable_segments(run, meta, today=TODAY, min_age_s=0) == [
        "book.2026-09-01.jsonl",
        "book.2026-09-02.jsonl",
    ]
    # the recorder's meta naming a segment as OPEN protects it even if old and quiet
    protecting = _run_meta(open_segment="book.2026-09-01.jsonl")
    assert ofl.offloadable_segments(run, protecting, today=TODAY, min_age_s=0) == [
        "book.2026-09-02.jsonl"
    ]
    # an earlier "today" (clock skew) never exposes a same-or-later day
    assert ofl.offloadable_segments(run, meta, today="2026-09-01", min_age_s=0) == []
    # a marked segment still on disk is left alone (never deleted unverified)
    ofl.segment_marker_path(run, "book.2026-09-01.jsonl").parent.mkdir()
    ofl.segment_marker_path(run, "book.2026-09-01.jsonl").write_text(
        json.dumps({"gs_uri": "gs://x/y"}), encoding="utf-8"
    )
    assert ofl.offloadable_segments(run, meta, today=TODAY, min_age_s=0) == [
        "book.2026-09-02.jsonl"
    ]
    assert ofl.offloadable_segments(tmp_path / "missing", meta, today=TODAY) == []


def test_segment_paths_and_names(tmp_path):
    run = tmp_path / "run-camp"
    assert ofl.segment_marker_path(run, "book.2026-09-02.jsonl") == (
        run / "segments" / "book.2026-09-02.offloaded.json"
    )
    assert (
        ofl.segment_object_name("run-camp", "book.2026-09-02.jsonl")
        == "run-camp/segments/book.2026-09-02.jsonl.zst"
    )
    assert ofl.open_segments({"segments": {"book": {"current": "book.2026-09-03.jsonl"}}}) == {
        "book.2026-09-03.jsonl"
    }
    assert ofl.open_segments({}) == set()
    assert ofl.open_segments({"segments": "junk"}) == set()


# --------------------------------------------------------------------------- #
# offload: the sweep (compress -> upload -> verify -> marker -> delete)
# --------------------------------------------------------------------------- #


def test_offload_segments_archives_closed_segments_only(tmp_path):
    run = _make_run(tmp_path)
    be = FakeBackend()
    scratch = tmp_path / "scratch"
    done = ofl.offload_segments(
        run,
        be,
        today=TODAY,
        compressor=fake_compress,
        scratch_dir=scratch,
        now_iso_fn=lambda: "2026-09-03T00:20:00Z",
    )
    assert done == ["book.2026-09-01.jsonl", "book.2026-09-02.jsonl"]

    # objects: <prefix>/segments/<segment>.zst holding the compressor's bytes
    obj = "matches/segments/book.2026-09-01.jsonl.zst"
    assert be.objects[obj] == b"ZSTD" + b"s1a\ns1b\n"
    assert "matches/segments/book.2026-09-02.jsonl.zst" in be.objects

    # local segments gone; today's segment, the legacy monolith and the native untouched
    assert not (run / "book.2026-09-01.jsonl").exists()
    assert not (run / "book.2026-09-02.jsonl").exists()
    assert _seg_lines(run, "book.2026-09-03.jsonl") == ["s3a"]
    assert _seg_lines(run, "book.jsonl") == ["legacy1"]
    assert _seg_lines(run, "matches/event-1/book.jsonl") == ["n1"]

    # marker: segments/book.<day>.offloaded.json with the documented fields
    marker = json.loads(
        (run / "segments" / "book.2026-09-01.offloaded.json").read_text(encoding="utf-8")
    )
    assert marker["gs_uri"] == f"gs://fake/{obj}"
    assert marker["object"] == obj
    assert marker["bytes"] == len(be.objects[obj])  # verified byte-exact against GCS
    assert marker["raw_bytes"] == len(b"s1a\ns1b\n")
    assert marker["lines"] == 2
    assert marker["crc32c"] == "c"
    assert marker["day"] == "2026-09-01" and marker["stream"] == "book"
    assert marker["segment"] == "book.2026-09-01.jsonl" and marker["kind"] == "segment"
    assert marker["compression"] == "zstd" and marker["offloaded_at"] == "2026-09-03T00:20:00Z"
    assert marker["schema"] == ofl.SEGMENT_MARKER_SCHEMA
    assert ofl.is_segment_offloaded(run, "book.2026-09-01.jsonl")
    assert not (run / "segments" / "book.2026-09-01.offloaded.json.partial").exists()

    # scratch .zst files are cleaned up; idempotent on a second pass
    assert list(scratch.iterdir()) == []
    assert ofl.offload_segments(run, be, today=TODAY, compressor=fake_compress) == []
    # the admin's scan now sees only what is local
    assert [p.name for p in dl.monolith_files(run)] == ["book.jsonl", "book.2026-09-03.jsonl"]


def test_offload_segments_can_skip_line_count_and_honours_limit(tmp_path):
    run = _make_run(tmp_path)
    be = FakeBackend()
    done = ofl.offload_segments(
        run, be, today=TODAY, compressor=fake_compress, limit=1, count_lines=False
    )
    assert done == ["book.2026-09-01.jsonl"]  # oldest first
    marker = ofl.read_segment_marker(run, "book.2026-09-01.jsonl")
    assert marker["lines"] is None
    assert (run / "book.2026-09-02.jsonl").exists()


def test_offload_segment_keeps_local_on_verify_mismatch(tmp_path, monkeypatch):
    run = _make_run(tmp_path)
    be = FakeBackend()
    monkeypatch.setattr(be, "stat", lambda name: {"size": 999999})  # remote reports wrong size
    scratch = tmp_path / "scratch"
    with pytest.raises(OSError, match="verify failed"):
        ofl.offload_segment(
            run,
            "book.2026-09-01.jsonl",
            be,
            now_iso="t",
            compressor=fake_compress,
            scratch_dir=scratch,
        )
    assert _seg_lines(run, "book.2026-09-01.jsonl") == ["s1a", "s1b"]  # intact
    assert not ofl.is_segment_offloaded(run, "book.2026-09-01.jsonl")  # no marker
    assert list(scratch.iterdir()) == []  # scratch .zst removed


def test_offload_segment_keeps_local_on_compress_or_upload_failure(tmp_path):
    run = _make_run(tmp_path)
    be = FakeBackend()

    def broken(src, dest):
        raise OSError("zstd exited 1")

    with pytest.raises(OSError, match="zstd exited"):
        ofl.offload_segment(run, "book.2026-09-01.jsonl", be, now_iso="t", compressor=broken)
    assert (run / "book.2026-09-01.jsonl").exists() and be.objects == {}

    be.fail_upload_for.add("matches/segments/book.2026-09-01.jsonl.zst")
    with pytest.raises(OSError, match="upload failure"):
        ofl.offload_segment(run, "book.2026-09-01.jsonl", be, now_iso="t", compressor=fake_compress)
    assert (run / "book.2026-09-01.jsonl").exists()
    assert not ofl.is_segment_offloaded(run, "book.2026-09-01.jsonl")


def test_offload_segment_refuses_non_segment_names(tmp_path):
    run = _make_run(tmp_path)
    with pytest.raises(ValueError, match="not a daily segment"):
        ofl.offload_segment(run, "book.jsonl", FakeBackend(), now_iso="t", compressor=fake_compress)
    with pytest.raises(FileNotFoundError):
        ofl.offload_segment(
            run, "book.2020-01-01.jsonl", FakeBackend(), now_iso="t", compressor=fake_compress
        )
    assert (run / "book.jsonl").exists()


def test_offload_segments_batch_survives_one_failure(tmp_path):
    run = _make_run(tmp_path)
    be = FakeBackend()
    be.fail_upload_for.add("matches/segments/book.2026-09-01.jsonl.zst")
    done = ofl.offload_segments(run, be, today=TODAY, compressor=fake_compress)
    assert done == ["book.2026-09-02.jsonl"]
    assert (run / "book.2026-09-01.jsonl").exists()  # kept for the next run
    assert not (run / "book.2026-09-02.jsonl").exists()


def test_offload_segments_without_meta_still_guards_by_day_and_age(tmp_path):
    run = _make_run(tmp_path)
    (run / "meta.json").unlink()
    be = FakeBackend()
    done = ofl.offload_segments(run, be, today=TODAY, compressor=fake_compress)
    assert done == ["book.2026-09-01.jsonl", "book.2026-09-02.jsonl"]
    assert (run / "book.2026-09-03.jsonl").exists()


def test_offload_segment_refuses_to_stage_without_headroom(tmp_path, monkeypatch):
    # The .zst is staged on the run volume: never push the recorder into ENOSPC for it.
    run = _make_run(tmp_path)
    be = FakeBackend()
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(ofl.shutil, "disk_usage", lambda p: usage(100, 99, 1))
    with pytest.raises(OSError, match="not enough free space"):
        ofl.offload_segment(
            run,
            "book.2026-09-01.jsonl",
            be,
            now_iso="t",
            compressor=fake_compress,
            headroom_bytes=1000,
        )
    assert be.objects == {} and (run / "book.2026-09-01.jsonl").exists()
    monkeypatch.setattr(ofl.shutil, "disk_usage", lambda p: usage(10**12, 0, 10**12))
    assert ofl.offload_segments(run, be, today=TODAY, compressor=fake_compress) == [
        "book.2026-09-01.jsonl",
        "book.2026-09-02.jsonl",
    ]  # the module default is 0 here (fixture); the CLI's default is 5 GiB
    assert ofl.DEFAULT_HEADROOM_BYTES == 0 and ofl._headroom(None) == 0
    monkeypatch.setattr(ofl, "DEFAULT_HEADROOM_BYTES", 7)
    assert ofl._headroom(None) == 7 and ofl._headroom(3) == 3


# --------------------------------------------------------------------------- #
# zstd CLI wrapper (subprocess is faked; never spawns the real binary)
# --------------------------------------------------------------------------- #


class _Proc:
    def __init__(self, rc, stderr=""):
        self.returncode, self.stderr = rc, stderr


def test_zstd_compress_invokes_cli_and_maps_failures(tmp_path, monkeypatch):
    src = tmp_path / "book.2026-09-01.jsonl"
    src.write_text("x\n", encoding="utf-8")
    dest = tmp_path / "out.zst"
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        Path(argv[-2]).write_bytes(b"z")  # the CLI writes -o <dest>
        return _Proc(0)

    monkeypatch.setattr(ofl.subprocess, "run", fake_run)
    ofl.zstd_compress(src, dest)
    assert calls == [["zstd", "-T0", "-6", "-q", "-o", str(dest), str(src)]]
    assert dest.read_bytes() == b"z" and src.exists()

    ofl.zstd_compress(src, dest, zstd_bin="/opt/bin/zstd")
    assert calls[-1][0] == "/opt/bin/zstd"

    monkeypatch.setattr(ofl.subprocess, "run", lambda argv, **kw: _Proc(1, "boom"))
    with pytest.raises(OSError, match="exited 1"):
        ofl.zstd_compress(src, dest)

    def missing(argv, **kw):
        raise FileNotFoundError("no zstd")

    monkeypatch.setattr(ofl.subprocess, "run", missing)
    with pytest.raises(OSError, match="is zstd installed"):
        ofl.zstd_compress(src, dest)

    monkeypatch.setattr(ofl.subprocess, "run", lambda argv, **kw: _Proc(0))  # rc 0, no file
    with pytest.raises(OSError, match="no output"):
        ofl.zstd_compress(src, tmp_path / "never-written.zst")


# --------------------------------------------------------------------------- #
# CLI: matches then segments; --segments-only / --matches-only / --list
# --------------------------------------------------------------------------- #


def _cli_run(tmp_path: Path) -> Path:
    """A run with a finished native match AND closed segments dated safely in the past
    (the CLI uses the real UTC day as ``today``)."""
    run = tmp_path / "run-cli"
    run.mkdir()
    (run / "meta.json").write_text(
        json.dumps({"events": [], "segments": {"book": {"current": "book.2020-01-03.jsonl"}}}),
        encoding="utf-8",
    )
    for name, lines in (("book.2020-01-01.jsonl", ["a"]), ("book.2020-01-02.jsonl", ["b"])):
        _write_lines(run / name, lines)
        _age(run / name, 3600)
    _write_lines(run / "book.2020-01-03.jsonl", ["c"])  # named OPEN by meta -> protected
    _age(run / "book.2020-01-03.jsonl", 3600)
    native = run / "matches" / "event-0900"
    native.mkdir(parents=True)
    _write_lines(native / "book.jsonl", ["n"])
    _age(native / "book.jsonl", 3600)  # quiet for an hour: finished
    (native / "meta.json").write_text("{}", encoding="utf-8")
    return run


@pytest.fixture
def cli_env(monkeypatch):
    be = FakeBackend()
    monkeypatch.setattr(ofl, "GcsBackend", lambda *a, **k: be)
    monkeypatch.setattr(ofl, "zstd_compress", lambda src, dest, **kw: fake_compress(src, dest))
    return be


def test_cli_default_runs_matches_then_segments(tmp_path, cli_env, capsys):
    run = _cli_run(tmp_path)
    assert ofl.main(["--run-dir", str(run), "--bucket", "b"]) == 0
    out = capsys.readouterr().out
    assert "offloaded 1 match(es): 0900" in out
    assert "offloaded 2 segment(s): book.2020-01-01.jsonl book.2020-01-02.jsonl" in out
    assert not (run / "matches" / "event-0900").exists()
    assert not (run / "book.2020-01-01.jsonl").exists()
    assert (run / "book.2020-01-03.jsonl").exists()  # open per meta -> untouched
    assert sorted(cli_env.objects) == [
        "matches/event-0900.tar.gz",
        "matches/segments/book.2020-01-01.jsonl.zst",
        "matches/segments/book.2020-01-02.jsonl.zst",
    ]


def test_cli_segments_only_and_matches_only(tmp_path, cli_env, capsys):
    run = _cli_run(tmp_path)
    assert ofl.main(["--run-dir", str(run), "--bucket", "b", "--segments-only"]) == 0
    out = capsys.readouterr().out
    assert "match(es)" not in out and "offloaded 2 segment(s)" in out
    assert (run / "matches" / "event-0900").exists()  # matches sweep skipped

    assert ofl.main(["--run-dir", str(run), "--bucket", "b", "--matches-only"]) == 0
    out = capsys.readouterr().out
    assert "offloaded 1 match(es): 0900" in out and "segment(s)" not in out

    with pytest.raises(SystemExit):  # mutually exclusive
        ofl.main(["--run-dir", str(run), "--bucket", "b", "--matches-only", "--segments-only"])


def test_cli_list_shows_both_sweeps_and_min_age(tmp_path, cli_env, capsys):
    run = _cli_run(tmp_path)
    assert ofl.main(["--run-dir", str(run), "--bucket", "b", "--list"]) == 0
    out = capsys.readouterr().out
    assert "offloadable matches (1): 0900" in out
    assert "offloadable segments (2): book.2020-01-01.jsonl book.2020-01-02.jsonl" in out
    assert cli_env.objects == {}  # --list does nothing
    # a huge --min-age makes every segment "too young"
    assert ofl.main(["--run-dir", str(run), "--bucket", "b", "--list", "--min-age", "1e9"]) == 0
    assert "offloadable segments (0): (none)" in capsys.readouterr().out
    # likewise --match-min-age for matches; and an --events-file naming the match protects it
    base = ["--run-dir", str(run), "--bucket", "b", "--list"]
    assert ofl.main([*base, "--match-min-age", "1e9"]) == 0
    assert "offloadable matches (0): (none)" in capsys.readouterr().out
    events = tmp_path / "campaign_events.json"
    events.write_text(json.dumps([{"event_id": "0900", "closed": False}]), encoding="utf-8")
    assert ofl.main([*base, "--events-file", str(events)]) == 0
    assert "offloadable matches (0): (none)" in capsys.readouterr().out
    assert ofl.main(["--run-dir", str(run), "--bucket", "b", "--events-file", str(events)]) == 0
    assert "offloaded 0 match(es)" in capsys.readouterr().out
    assert (run / "matches" / "event-0900").exists()
