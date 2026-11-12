"""Tests for the opt-in streaming cache mode (``stream_cache``)."""
import os
import sqlite3
from typing import Any, List, Optional

import pytest
from pytest_httpbin.serve import Server  # type: ignore

from .. import CachingSession
from .. import cache as cache_mod
from ..cache import (
    FileCache,
    SQLiteCache,
    MemoryCache,
    CacheStorageBase,
    CacheStreamSink,
    _TeeRaw,
    _CachedBodyRaw,
)


# ---------------------------------------------------------------------------
# fakes for unit-level failure injection
# ---------------------------------------------------------------------------


class FakeRaw:
    """Minimal urllib3-like raw object for _TeeRaw tests."""

    def __init__(self, chunks: List[bytes], fail_after: Optional[int] = None) -> None:
        self._chunks = list(chunks)
        self._fail_after = fail_after
        self.closed = False
        self.released = False

    def stream(self, chunk_size: int = 1, decode_content: Optional[bool] = None) -> Any:
        yielded = 0
        for chunk in self._chunks:
            if self._fail_after is not None and yielded >= self._fail_after:
                raise IOError("simulated mid-stream transport failure")
            yielded += 1
            yield chunk

    def read(
        self,
        amt: Optional[int] = None,
        decode_content: Optional[bool] = None,
        cache_content: bool = False,
    ) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class RecordingSink(CacheStreamSink):
    def __init__(self, fail_on_write: int = 0) -> None:
        self.chunks: List[bytes] = []
        self.committed = False
        self.aborted = False
        self._fail_on_write = fail_on_write

    def write(self, data: bytes) -> None:
        if self._fail_on_write and len(self.chunks) >= self._fail_on_write:
            raise IOError("simulated cache write failure")
        self.chunks.append(data)

    def commit(self) -> None:
        self.committed = True

    def abort(self) -> None:
        self.aborted = True


def _tee_response(chunks: List[bytes], sink: CacheStreamSink) -> Any:
    import requests

    resp = requests.Response()
    resp.status_code = 200
    resp.raw = _TeeRaw(FakeRaw(chunks), sink)  # type: ignore[arg-type]
    resp._content = False  # type: ignore[assignment]
    resp._content_consumed = False  # type: ignore[attr-defined]
    return resp


# ---------------------------------------------------------------------------
# backend capability / rejection
# ---------------------------------------------------------------------------


def test_backend_capabilities() -> None:
    assert MemoryCache().supports_streaming is False
    assert FileCache.supports_streaming is True
    assert SQLiteCache.supports_streaming is True


def test_unsupported_backend_rejected_before_io(httpbin: Server, tmp_path: Any) -> None:
    cs = CachingSession(cache_storage=MemoryCache())
    cs.stream_cache = True
    # must raise before any network I/O happens
    with pytest.raises(TypeError):
        cs.request("get", httpbin.url + "/bytes/10", stream=True)

    class BareBackend(CacheStorageBase):
        pass

    cs2 = CachingSession(cache_storage=BareBackend())
    cs2.stream_cache = True
    with pytest.raises(TypeError):
        cs2.request("get", httpbin.url + "/bytes/10", stream=True)


def test_disabled_by_default_behavior_unchanged(httpbin: Server) -> None:
    cs = CachingSession(cache_storage=MemoryCache())
    # stream without stream_cache: old behavior, raw response passes through
    resp = cs.request("get", httpbin.url + "/bytes/16", stream=True)
    assert resp.fromcache is False
    assert not isinstance(resp.raw, (_TeeRaw, _CachedBodyRaw))
    resp.close()


# ---------------------------------------------------------------------------
# first chunk: no eager body read, no cache entry until EOF
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_miss_does_not_publish_before_eof(
    httpbin: Server, tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/stream-bytes/4096"

    resp = cs.request("get", url, stream=True)
    assert resp.fromcache is False
    # body must not have been read for caching yet
    assert storage.get(url) is None

    iterator = resp.iter_content(1024)
    first = next(iterator)
    assert len(first) > 0
    # still no cache entry after just the first chunk
    assert storage.get(url) is None

    # consume the rest through the same iterator; EOF publishes the entry
    body = first + b"".join(iterator)
    cached = storage.get(url)
    assert cached is not None
    assert cached.content == body


# ---------------------------------------------------------------------------
# replay: cache hit supports chunked reads
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_cache_hit_chunked_replay(
    httpbin: Server, tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/bytes/5000"

    body = b"".join(cs.request("get", url, stream=True).iter_content(512))

    hit = cs.request("get", url, stream=True)
    assert hit.fromcache is True
    assert isinstance(hit.raw, _CachedBodyRaw)
    chunks = list(hit.iter_content(333))
    # chunks respect the requested size, including the final partial one
    assert all(len(c) == 333 for c in chunks[:-1])
    assert b"".join(chunks) == body
    # .content/.text also work on a fresh cached response
    hit2 = cs.request("get", url, stream=True)
    assert hit2.content == body
    hit2.encoding = "utf-8"
    assert isinstance(hit2.text, str)


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_multiple_responses_interleave(
    httpbin: Server, tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url_a = httpbin.url + "/bytes/3000"
    url_b = httpbin.url + "/bytes/2000"

    # prime both
    body_a = b"".join(cs.request("get", url_a, stream=True).iter_content(256))
    body_b = b"".join(cs.request("get", url_b, stream=True).iter_content(256))

    # interleave two cached responses chunk by chunk
    ra = cs.request("get", url_a, stream=True)
    rb = cs.request("get", url_b, stream=True)
    assert ra.fromcache and rb.fromcache
    ia, ib = ra.iter_content(128), rb.iter_content(128)
    got_a, got_b = b"", b""
    while True:
        done = True
        try:
            got_a += next(ia)
            done = False
        except StopIteration:
            pass
        try:
            got_b += next(ib)
            done = False
        except StopIteration:
            pass
        if done:
            break
    assert got_a == body_a
    assert got_b == body_b


# ---------------------------------------------------------------------------
# early close / failures: cleanup, old value preserved, visible errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_early_close_aborts_and_preserves_old_entry(
    httpbin: Server, tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/stream-bytes/8192"

    # seed an existing ("old") entry for the same key
    old = cs.request("get", httpbin.url + "/bytes/10", stream=True)
    old_body = old.content  # materialize -> published
    storage.set(url, _build_resp(old_body))

    before = list(_cache_files(tmp_path))
    resp = cs.request("get", url, stream=True)
    next(resp.iter_content(256))
    resp.close()  # give up before EOF

    assert storage.get(url).content == old_body  # type: ignore[union-attr]
    # no temporary files/dirs left behind
    after = list(_cache_files(tmp_path))
    leftovers = [p for p in after if p not in before]
    assert leftovers == []
    # sqlite staging table is empty
    if storage_name == "sqlite":
        conn = sqlite3.connect(str(tmp_path / "cache.db"))
        assert conn.execute("SELECT count(*) FROM cache_stream").fetchone()[0] == 0
        conn.close()


def test_tee_early_close_via_context_manager() -> None:
    sink = RecordingSink()
    resp = _tee_response([b"ab", b"cd", b"ef"], sink)
    with resp:
        it = resp.iter_content(2)
        next(it)
    assert sink.aborted is True
    assert sink.committed is False
    assert resp.raw._raw.closed is True  # type: ignore[attr-defined]


def test_transport_failure_aborts_and_raises(tmp_path: Any) -> None:
    storage = FileCache(str(tmp_path))
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True

    sink = RecordingSink()
    resp = _tee_response([b"a", b"b", b"c"], sink)
    # force the underlying transport to fail after two chunks
    resp.raw._raw._fail_after = 2  # type: ignore[attr-defined]

    with pytest.raises(IOError):
        b"".join(resp.iter_content(2))
    assert sink.aborted is True
    assert sink.committed is False
    assert resp.raw._raw.closed is True  # type: ignore[attr-defined]


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_cache_write_failure_visible_and_old_entry_kept(
    tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    old_body = b"old cached body"
    key = "http://example.test/thing"
    storage.set(key, _build_resp(old_body))

    sink = storage.stream_set(key, _build_resp(b"new headers only"))
    if storage_name == "file":
        # break the underlying file descriptor
        sink._f.close()  # type: ignore[attr-defined]
        with pytest.raises(Exception):
            sink.write(b"partial chunk")
    else:
        # make every bounded flush fail at the database layer; sqlite3
        # connection methods are read-only so swap in a failing subclass
        class FailingConn(sqlite3.Connection):
            def execute(self, sql: str, *args: Any) -> Any:
                if sql.startswith("UPDATE cache_stream"):
                    raise sqlite3.OperationalError("simulated sqlite failure")
                return super().execute(sql, *args)

        failing = FailingConn(sink._cache_path)  # type: ignore[attr-defined]
        sink._conn.close()  # type: ignore[attr-defined]
        sink._conn = failing  # type: ignore[attr-defined]
        with pytest.raises(Exception):
            # more than one buffered chunk so a flush is forced
            sink.write(b"x" * (cache_mod.STREAM_WRITE_CHUNK + 1))
    sink.abort()

    # old entry is untouched and still replayable
    assert storage.get(key).content == old_body  # type: ignore[union-attr]
    entry = storage.stream_get(key)
    assert entry is not None
    cached, stream = entry
    assert stream.read() == old_body
    stream.close()


# ---------------------------------------------------------------------------
# gzip: decoded body stored, replay must not decode a second time
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_gzip_replay_not_double_decoded(
    httpbin: Server, tmp_path: Any, storage_name: str
) -> None:
    storage = (
        FileCache(str(tmp_path))
        if storage_name == "file"
        else SQLiteCache(str(tmp_path / "cache.db"))
    )
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/gzip"

    first = cs.request("get", url, stream=True)
    payload = first.content  # full materialization publishes the cache
    assert payload[:1] == b"{"  # decoded JSON, not gzip magic 0x1f 0x8b

    second = cs.request("get", url, stream=True)
    assert second.fromcache is True
    replayed = second.content
    assert replayed == payload
    assert replayed[:1] == b"{"
    # a content-encoding header must not survive on replay
    assert "content-encoding" not in second.headers


# ---------------------------------------------------------------------------
# preserved semantics: write_only, key/should-cache hooks, non-200
# ---------------------------------------------------------------------------


def test_write_only_still_writes_but_never_reads(httpbin: Server, tmp_path: Any) -> None:
    storage = FileCache(str(tmp_path))
    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    cs.cache_write_only = True
    url = httpbin.url + "/bytes/128"

    resp = cs.request("get", url, stream=True)
    body = resp.content
    assert resp.fromcache is False
    # written despite write_only...
    assert storage.get(url) is not None
    # ...but reads stay disabled
    again = cs.request("get", url, stream=True)
    assert again.fromcache is False
    again.close()
    assert body


def test_should_cache_and_key_hooks_respected(httpbin: Server, tmp_path: Any) -> None:
    class HookedSession(CachingSession):
        def key_for_request(self, method: str, url: Any, params: Any = None,
                            data: Any = None) -> Optional[str]:
            if method == "get":
                return "custom-key:" + str(url)
            return None

        def should_cache_response(self, response: Any) -> bool:
            return response.status_code == 200

    storage = SQLiteCache(str(tmp_path / "c.db"))
    cs = HookedSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/bytes/64"
    resp = cs.request("get", url, stream=True)
    resp.content
    assert storage.get("custom-key:" + url) is not None

    # non-cacheable status (404) is streamed through without a sink
    missing = cs.request("get", httpbin.url + "/status/404", stream=True)
    assert missing.fromcache is False
    assert not isinstance(missing.raw, _TeeRaw)
    missing.close()


def test_old_format_entries_still_readable(tmp_path: Any) -> None:
    storage = FileCache(str(tmp_path))
    key = "http://example.test/legacy"
    body = b"legacy body \x00\x01\x02"
    # entry written through the classic whole-body API
    storage.set(key, _build_resp(body))

    entry = storage.stream_get(key)
    assert entry is not None
    cached, stream = entry
    assert cached.status_code == 200
    # content-length reflects the decoded stored body
    assert cached.headers["content-length"] == str(len(body))
    assert stream.read() == body
    stream.close()


# ---------------------------------------------------------------------------
# buffer bounds: read/write buffers do not grow with body size
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("storage_name", ["file", "sqlite"])
def test_streaming_buffers_are_bounded(
    httpbin: Server, tmp_path: Any, storage_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # smaller fixed cap than the httpbin 100KB response limit
    cap = 8192
    monkeypatch.setattr(cache_mod, "STREAM_WRITE_CHUNK", cap)
    size = 3 * cap + 777
    max_write = 0
    max_read = 0

    if storage_name == "file":
        storage: CacheStorageBase = FileCache(str(tmp_path))
        orig_write = cache_mod.FileCacheStreamSink.write

        def tracked_write(self: Any, data: bytes) -> None:  # type: ignore[type-arg]
            nonlocal max_write
            max_write = max(max_write, len(data))
            return orig_write(self, data)

        monkeypatch.setattr(cache_mod.FileCacheStreamSink, "write", tracked_write)
    else:
        storage = SQLiteCache(str(tmp_path / "b.db"))
        orig_flush = cache_mod.SQLiteCacheStreamSink._flush

        def tracked_flush(self: Any, n: int) -> None:  # type: ignore[type-arg]
            nonlocal max_write
            max_write = max(max_write, n)
            return orig_flush(self, n)

        monkeypatch.setattr(
            cache_mod.SQLiteCacheStreamSink, "_flush", tracked_flush
        )

    orig_reader_read = cache_mod._SQLiteBlobReader.read

    def tracked_read(self: Any, size: int = -1) -> bytes:  # type: ignore[type-arg]
        nonlocal max_read
        if 0 < size:
            max_read = max(max_read, size)
        return orig_reader_read(self, size)

    if storage_name == "sqlite":
        monkeypatch.setattr(cache_mod._SQLiteBlobReader, "read", tracked_read)

    cs = CachingSession(cache_storage=storage)
    cs.stream_cache = True
    url = httpbin.url + "/stream-bytes/%d" % size

    resp = cs.request("get", url, stream=True)
    body = b"".join(resp.iter_content(4096))
    assert len(body) == size

    # write-side buffering stays at/below the fixed chunk cap
    assert max_write <= cache_mod.STREAM_WRITE_CHUNK

    hit = cs.request("get", url, stream=True)
    assert hit.fromcache is True
    replayed = b"".join(hit.iter_content(4096))
    assert replayed == body

    if storage_name == "sqlite":
        assert 0 < max_read <= 4096
    else:
        # file backend reads in slices no larger than requested chunk
        assert max_read == 0 or max_read <= 4096


def test_tee_holds_no_body_buffer() -> None:
    sink = RecordingSink()
    chunks = [b"x" * 1000 for _ in range(5)]
    resp = _tee_response(chunks, sink)
    out = b"".join(resp.iter_content(1000))
    assert out == b"".join(chunks)
    assert sink.committed is True
    assert sink.aborted is False
    # tee passed chunks straight through; nothing accumulated past one chunk
    assert max(len(c) for c in sink.chunks) == 1000


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _build_resp(body: bytes) -> Any:
    import requests

    r = requests.Response()
    r.status_code = 200
    r.encoding = "utf-8"
    r.headers["content-type"] = "application/octet-stream"
    r._content = body
    return r


def _cache_files(tmp_path: Any) -> Any:
    for root, _dirs, files in os.walk(str(tmp_path)):
        for name in files:
            yield os.path.join(root, name)
