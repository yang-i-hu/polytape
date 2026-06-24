"""Tests for CaptureReader: meta reload, incremental tail, partial lines, rotation."""

from __future__ import annotations

from polytape.viewer.reader import CaptureReader

_LINE = (
    '{"stream":"book","id":"z","ts_recv":"2026-01-01T01:00:00.000000Z","ts_server":null,'
    '"raw":{"event_type":"book","asset_id":"100","hash":"hz","timestamp":"1","bids":[],"asks":[]}}'
)


def test_read_meta_and_cache(make_book_capture):
    reader = CaptureReader(make_book_capture())
    meta = reader.read_meta()
    assert meta["event_id"] == "20200"
    assert meta["counts"]["book"] >= 4
    assert reader.read_meta() is meta  # cached until mtime/size change


def test_tail_reads_all_then_incremental(make_book_capture):
    event_dir = make_book_capture()
    reader = CaptureReader(event_dir)
    envs, offset, reset = reader.tail(0)
    assert len(envs) >= 4 and reset is False
    assert all(e["stream"] == "book" for e in envs)

    # appending a complete line is picked up incrementally.
    book = event_dir / "book.jsonl"
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(_LINE + "\n")
    envs2, offset2, reset2 = reader.tail(offset)
    assert len(envs2) == 1 and envs2[0]["id"] == "z" and offset2 > offset and reset2 is False


def test_tail_buffers_partial_line(make_book_capture):
    event_dir = make_book_capture()
    reader = CaptureReader(event_dir)
    _, offset, _ = reader.tail(0)
    book = event_dir / "book.jsonl"
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(_LINE)  # NO trailing newline
    envs, offset_after, _ = reader.tail(offset)
    assert envs == [] and offset_after == offset  # partial line withheld
    with open(book, "a", encoding="utf-8") as fh:
        fh.write("\n")  # complete it
    envs2, _, _ = reader.tail(offset_after)
    assert len(envs2) == 1 and envs2[0]["id"] == "z"


def test_tail_detects_shrink_and_reseeds(make_book_capture):
    event_dir = make_book_capture()
    reader = CaptureReader(event_dir)
    envs, offset, _ = reader.tail(0)
    # rewrite (rotate) the file smaller than the old offset.
    (event_dir / "book.jsonl").write_text(_LINE + "\n", encoding="utf-8")
    envs2, _, reset = reader.tail(offset)
    assert reset is True and len(envs2) == 1 and envs2[0]["id"] == "z"


def test_list_events(make_book_capture, tmp_path):
    make_book_capture("11111")
    make_book_capture("22222")
    events = {e["event_id"] for e in CaptureReader.list_events(tmp_path)}
    assert {"11111", "22222"} <= events


def test_list_events_empty_root(tmp_path):
    assert CaptureReader.list_events(tmp_path / "nope") == []
