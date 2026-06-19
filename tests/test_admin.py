"""Tests for the read-only admin RunReader (offline; no fastapi required)."""

from __future__ import annotations

import json

from polytape.admin.reader import RunReader
from polytape.envelope import utc_now_iso


def _meta() -> dict:
    def mk(c):
        return {"id": "m", "conditionId": c, "clobTokenIds": ["x", "y"]}

    return {
        "started_at": "2026-06-19T16:20:00.000000Z",
        "events": [
            {
                "id": "1001",
                "title": "A vs. B",
                "slug": "fifwc-a-b-2026-06-19",
                "markets": [mk("0xA1"), mk("0xA2"), mk("0xA3")],
            },
            {
                "id": "1002",
                "title": "C vs. D",
                "slug": "fifwc-c-d-2026-06-20",
                "markets": [mk("0xB1"), mk("0xB2"), mk("0xB3")],
            },
        ],
    }


def _book(market: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "book",
        "id": f"{market}-{ts}",
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"event_type": "book", "market": market},
    }


def _comment(parent: int, cid: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "comments",
        "id": cid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"type": "comment_created", "payload": {"id": cid, "parentEntityID": parent}},
    }


def _write_jsonl(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def _reader(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    return RunReader(tmp_path, env_file=tmp_path / "missing.env")


def test_status_counts_and_coverage(tmp_path):
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xA1"), _book("0xB1")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = _reader(tmp_path)
    r.update()
    st = r.status()
    assert st["records"] == {"comments": 1, "book": 3}
    assert st["open_matches"] == 2
    assert st["coverage"] == {"seen": 2, "total": 6}  # 0xA1, 0xB1 of 6 condition ids
    assert st["last_record_age_s"] is not None and st["last_record_age_s"] < 60


def test_matches_per_event_recency_and_status(tmp_path):
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xA2")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = _reader(tmp_path)
    r.update()
    by_id = {m["event_id"]: m for m in r.matches()}
    assert by_id["1001"]["counts"] == {"book": 2, "comments": 1}
    assert by_id["1001"]["title"] == "A vs. B" and by_id["1001"]["date"] == "2026-06-19"
    assert by_id["1001"]["status"] == "live"
    assert by_id["1002"]["status"] == "pending" and by_id["1002"]["counts"] == {}
    # live (recent) sorts ahead of pending (never seen)
    assert r.matches()[0]["event_id"] == "1001"


def test_incremental_reads_only_new_bytes(tmp_path):
    book = tmp_path / "book.jsonl"
    _write_jsonl(book, [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = _reader(tmp_path)
    r.update()
    assert r.status()["records"]["book"] == 1
    off1 = r._offsets["book"]
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xB1")) + "\n")
    r.update()
    assert r.status()["records"]["book"] == 2
    assert r._offsets["book"] > off1


def test_partial_trailing_line_not_consumed(tmp_path):
    book = tmp_path / "book.jsonl"
    rec1, rec2 = json.dumps(_book("0xA1")), json.dumps(_book("0xB1"))
    book.write_text(rec1 + "\n" + rec2, encoding="utf-8")  # rec2 has no trailing newline
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = _reader(tmp_path)
    r.update()
    assert r.status()["records"]["book"] == 1  # half-written line is not consumed
    with open(book, "a", encoding="utf-8") as fh:
        fh.write("\n")
    r.update()
    assert r.status()["records"]["book"] == 2


def test_heartbeat_armed_from_env_file(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    env = tmp_path / "p.env"
    env.write_text(
        "POLYTAPE_SALT=s\nPOLYTAPE_HEARTBEAT_URL=https://hc-ping.com/x\n", encoding="utf-8"
    )
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is True
    env.write_text("POLYTAPE_SALT=s\n", encoding="utf-8")
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is False
