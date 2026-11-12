"""
    module providing caching support for requests

    use CachingSession in place of requests.Session to take advantage
"""
import re
import os
import glob
import hashlib
import requests
import sqlite3
import json
import tempfile
from typing import Any, IO, Optional, Dict, Tuple, Protocol, cast
from ._types import Response


# maximum number of decoded body bytes buffered inside a streaming cache write
STREAM_WRITE_CHUNK = 1 << 20


class CacheResponse(Response):
    fromcache: bool


class _BinaryReadStream(Protocol):
    def read(self, size: int = -1) -> bytes:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()


class CacheStreamSink:
    """Incremental sink for a streamed response body.

    ``write`` is called for every chunk consumed by the caller, ``commit``
    publishes the cache entry once the full body has been read, and
    ``abort`` discards the temporary state (leaving any previous entry in
    place) when the body is not read to EOF or something fails.
    """

    def write(self, data: bytes) -> None:
        raise NotImplementedError()

    def commit(self) -> None:
        raise NotImplementedError()

    def abort(self) -> None:
        raise NotImplementedError()


class CacheStorageBase:
    #: True for backends that can stream bodies without buffering them whole
    supports_streaming: bool = False

    def get(self, key: str) -> Optional[Response]:
        raise NotImplementedError()

    def set(self, key: str, response: Response) -> None:
        raise NotImplementedError()

    def stream_get(
        self, key: str
    ) -> Optional[Tuple[Response, _BinaryReadStream]]:
        raise NotImplementedError()

    def stream_set(self, key: str, response: Response) -> CacheStreamSink:
        raise NotImplementedError()


class MemoryCache(CacheStorageBase):
    """In memory cache for request responses."""

    def __init__(self) -> None:
        self.cache: Dict[str, Response] = {}

    def get(self, key: str) -> Optional[Response]:
        """Get cache entry for key, or return None."""
        return self.cache.get(key, None)

    def set(self, key: str, response: Response) -> None:
        """Set cache entry for key with contents of response."""
        self.cache[key] = response


def _strip_content_encoding(headers: requests.structures.CaseInsensitiveDict) -> None:
    # cached bodies are stored after content-decoding (e.g. gunzip), so a
    # replay must not advertise an encoding that would be applied again
    headers.pop("content-encoding", None)
    headers.pop("transfer-encoding", None)


def _decode_text(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return value.decode("utf8")


class _TeeRaw:
    """Proxy for a response's ``raw`` stream that mirrors every byte into
    a :class:`CacheStreamSink`.

    The cache entry is only published when the body is read to EOF.  An
    early close or any failure while reading or writing the cache aborts
    the sink instead of publishing a partial entry.
    """

    def __init__(self, raw: Any, sink: CacheStreamSink) -> None:
        self._raw = raw
        self._sink = sink
        self._finished = False
        self._aborted = False

    def _commit(self) -> None:
        if self._finished or self._aborted:
            return
        # mark only after a successful publish so a commit failure can
        # still be cleaned up by _abort
        self._sink.commit()
        self._finished = True

    def _abort(self) -> None:
        if self._finished or self._aborted:
            return
        self._aborted = True
        try:
            self._sink.abort()
        finally:
            try:
                self._raw.close()
            except Exception:
                pass

    def stream(self, chunk_size: int = 1, decode_content: Optional[bool] = None) -> Any:
        if self._aborted or self._finished:
            raise RuntimeError("response stream was already closed")
        try:
            for chunk in self._raw.stream(
                chunk_size, decode_content=decode_content
            ):
                if chunk:
                    try:
                        self._sink.write(chunk)
                    except BaseException:
                        self._abort()
                        raise
                yield chunk
        except GeneratorExit:
            self._abort()
            raise
        except BaseException:
            self._abort()
            raise
        else:
            try:
                self._commit()
            except BaseException:
                self._abort()
                raise

    def read(
        self,
        amt: Optional[int] = None,
        decode_content: Optional[bool] = None,
        cache_content: bool = False,
    ) -> bytes:
        if self._finished:
            return b""
        if self._aborted:
            raise RuntimeError("response stream was already closed")
        try:
            chunk = self._raw.read(
                amt, decode_content=decode_content, cache_content=cache_content
            )
        except BaseException:
            self._abort()
            raise
        if chunk:
            try:
                self._sink.write(chunk)
            except BaseException:
                self._abort()
                raise
        if not chunk:
            try:
                self._commit()
            except BaseException:
                self._abort()
                raise
        return chunk

    def close(self) -> None:
        self._abort()

    def release_conn(self) -> None:
        release = getattr(self._raw, "release_conn", None)
        if release is not None:
            release()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


class _CachedBodyRaw:
    """Raw-like stream over a cached body (an open file or a blob reader).

    Only a single chunk worth of bytes is held back at any time, so reads
    stay bounded regardless of the entry size.
    """

    def __init__(self, stream: _BinaryReadStream) -> None:
        self._stream = stream
        self._pending = b""
        self._eof = False
        self._closed = False

    def _fill(self, size: Optional[int]) -> bytes:
        if size is None:
            # drain everything remaining, tolerating short reads
            parts = [self._pending]
            self._pending = b""
            while not self._eof:
                chunk = self._stream.read(-1)
                if not chunk:
                    self._eof = True
                    break
                parts.append(chunk)
            return b"".join(parts)

        # keep reading until we have size bytes or the source signals EOF
        while not self._eof and len(self._pending) < size:
            chunk = self._stream.read(size - len(self._pending))
            if not chunk:
                self._eof = True
                break
            self._pending += chunk
        data = self._pending[:size]
        self._pending = self._pending[size:]
        return bytes(data)

    def read(self, amt: Optional[int] = None, *args: Any, **kwargs: Any) -> bytes:
        if self._closed:
            raise ValueError("read of closed file")
        if self._eof and not self._pending:
            return b""
        if amt is None or amt < 0:
            return self._fill(None)
        return self._fill(amt)

    def stream(self, chunk_size: int = 1, decode_content: Optional[bool] = None) -> Any:
        if self._closed:
            raise RuntimeError("response stream was already closed")
        while True:
            # chunk_size=None means "whatever size is available"; read()
            # with a negative size returns everything remaining in one go
            chunk = self.read(None if chunk_size is None else chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stream.close()

    def release_conn(self) -> None:
        pass


class FileCache(CacheStorageBase):
    """
    File-based cache for request responses.

    :param cache_dir: directory for storing responses
    :param check_last_modified:  set to True to compare last-modified
        timestamp in cached response with value from HEAD request
    """

    supports_streaming = True

    # file name escaping inspired by httplib2
    _prefix = re.compile(r"^\w+://")
    _illegal = re.compile(r"[?/:|]+")
    _header_re = re.compile(r"([-\w]+): (.*)")
    _maxlen = 200

    def _clean_key(self, key: str) -> str:
        # strip scheme
        md5 = hashlib.md5(key.encode("utf8")).hexdigest()
        key = self._prefix.sub("", key)
        key = self._illegal.sub(",", key)
        return ",".join((key[: self._maxlen], md5))

    def __init__(self, cache_dir: str, check_last_modified: bool = False):
        # normalize path
        self.cache_dir = os.path.join(os.getcwd(), cache_dir)
        self.check_last_modified = check_last_modified
        # create directory
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _read_headers(
        self, f: IO[bytes], orig_key: str
    ) -> Optional[Response]:
        """Read the header section of a cache file, leaving f positioned at
        the start of the body.  Returns None when last-modified checking
        says the entry must be re-downloaded."""
        resp = requests.Response()
        while True:
            line = f.readline().decode("utf8").strip("\r\n")

            if self.check_last_modified and re.search(
                "last-modified", line, flags=re.I
            ):
                # line contains last modified header
                head_resp = requests.head(orig_key)

                try:
                    new_lm = head_resp.headers["last-modified"]
                    old_lm = line[line.find(":") + 1 :].strip()
                    if old_lm != new_lm:
                        # last modified timestamps don't match, need to download again
                        return None
                except KeyError:
                    # no last modified header present, so redownload
                    return None

            header = self._header_re.match(line)
            if header:
                resp.headers[header.group(1)] = header.group(2)
            else:
                break

        # status & encoding will be in headers, but are faked
        # need to split spaces out of status to get code (e.g. '200 OK')
        resp.status_code = int(resp.headers.pop("status").split(" ")[0])
        resp.encoding = resp.headers.pop("encoding")
        resp.url = resp.headers.get("content-location", orig_key)
        # TODO: resp.request = request
        return resp

    def _write_headers(self, f: IO[bytes], response: Response) -> None:
        status_str = "status: {0}\n".format(response.status_code)
        f.write(status_str.encode("utf8"))
        encoding_str = "encoding: {0}\n".format(response.encoding)
        f.write(encoding_str.encode("utf8"))
        for h, v in response.headers.items():
            # header: value\n
            f.write(h.encode("utf8"))
            f.write(b": ")
            f.write(v.encode("utf8"))
            f.write(b"\n")
        # one blank line
        f.write(b"\n")

    def get(self, orig_key: str) -> Optional[Response]:
        """Get cache entry for key, or return None."""
        path = os.path.join(self.cache_dir, self._clean_key(orig_key))

        try:
            with open(path, "rb") as f:
                resp = self._read_headers(f, orig_key)
                if resp is None:
                    return None
                # everything left is the real content
                resp._content = f.read()
            return resp
        except IOError:
            return None

    def stream_get(
        self, orig_key: str
    ) -> Optional[Tuple[Response, _BinaryReadStream]]:
        """Open a cache entry for chunked reading, or return None."""
        path = os.path.join(self.cache_dir, self._clean_key(orig_key))

        try:
            f = open(path, "rb")
        except IOError:
            return None
        try:
            resp = self._read_headers(f, orig_key)
            if resp is None:
                f.close()
                return None
            body_length = os.fstat(f.fileno()).st_size - f.tell()
            _strip_content_encoding(resp.headers)
            resp.headers["content-length"] = str(body_length)
            return resp, cast(_BinaryReadStream, f)
        except BaseException:
            f.close()
            raise

    def set(self, key: str, response: Response) -> None:
        """Set cache entry for key with contents of response."""
        path = os.path.join(self.cache_dir, self._clean_key(key))

        with open(path, "wb") as f:
            self._write_headers(f, response)
            f.write(response.content)

    def stream_set(self, key: str, response: Response) -> "FileCacheStreamSink":
        """Start writing a cache entry; the body follows via the sink."""
        clean = self._clean_key(key)
        path = os.path.join(self.cache_dir, clean)
        fd, tmp_path = tempfile.mkstemp(
            prefix=clean + ".", suffix=".tmp", dir=self.cache_dir
        )
        os.close(fd)
        return FileCacheStreamSink(path, tmp_path, self, response)

    def clear(self) -> None:
        # only delete things that end w/ a md5, less dangerous this way
        cache_glob = "*," + ("[0-9a-f]" * 32)
        for fname in glob.glob(os.path.join(self.cache_dir, cache_glob)):
            os.remove(fname)


class FileCacheStreamSink(CacheStreamSink):
    """Spools a streamed body into a temporary file and atomically renames
    it into place on commit, so an aborted/failed write never disturbs an
    existing cache entry."""

    def __init__(
        self,
        path: str,
        tmp_path: str,
        storage: FileCache,
        response: Response,
    ) -> None:
        self._path = path
        self._tmp_path = tmp_path
        self._done = False
        self._f: Optional[IO[bytes]] = open(tmp_path, "wb")
        storage._write_headers(self._f, response)

    def write(self, data: bytes) -> None:
        if self._done or self._f is None:
            raise RuntimeError("cache stream sink was already finalized")
        self._f.write(data)
        self._f.flush()

    def commit(self) -> None:
        if self._done:
            return
        self._done = True
        assert self._f is not None
        self._f.close()
        self._f = None
        # atomic publish; the previous entry (if any) is only replaced now
        os.replace(self._tmp_path, self._path)

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        try:
            if self._f is not None:
                self._f.close()
                self._f = None
        finally:
            try:
                os.unlink(self._tmp_path)
            except OSError:
                pass

    def __del__(self) -> None:
        if not self._done:
            try:
                self.abort()
            except Exception:
                pass


class _SQLiteBlobReader:
    """Reads a cached blob in bounded slices so the read buffer never grows
    with the size of the cached body."""

    def __init__(self, conn: sqlite3.Connection, key: str, length: int) -> None:
        self._conn = conn
        self._key = key
        self._length = length
        self._pos = 0
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("read of closed file")
        if size is None or size < 0:
            size = self._length - self._pos
        if size <= 0 or self._pos >= self._length:
            return b""
        parts = []
        remaining = size
        # tolerate short reads: keep slicing until the request is met or
        # the blob is exhausted
        while remaining > 0 and self._pos < self._length:
            row = self._conn.execute(
                # CAST forces a BLOB result; without it a TEXT-affinity
                # column would try to decode binary slices as UTF-8
                "SELECT CAST(substr(data, ?, ?) AS BLOB) "
                "FROM cache WHERE key=?",
                (self._pos + 1, remaining, self._key),
            ).fetchone()
            chunk = row[0] if row and row[0] is not None else b""
            if not chunk:
                break
            parts.append(chunk)
            self._pos += len(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    def close(self) -> None:
        self._closed = True


class SQLiteCache(CacheStorageBase):
    """SQLite cache for request responses.

    :param cache_path: path for SQLite database file
    :param check_last_modified: set to True to compare last-modified
        timestamp in cached response with value from HEAD request

    """

    supports_streaming = True

    _columns = ["key", "status", "modified", "encoding", "data", "headers"]

    def __init__(self, cache_path: str, check_last_modified: bool = False):
        self.cache_path = cache_path
        self.check_last_modified = check_last_modified
        self._conn = sqlite3.connect(cache_path)
        # blobs (the cached bodies) must come back as bytes instead of being
        # decoded as text; text columns are decoded explicitly below
        self._conn.text_factory = bytes
        # autocommit: without this a first read opens a transaction whose
        # snapshot never sees entries committed by streaming sink connections
        self._conn.isolation_level = None
        self._build_table()

    def _build_table(self) -> None:
        """Create table for storing request information and response."""
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS cache
                (key text UNIQUE, status integer, modified text,
                 encoding text, data blob, headers blob)"""
        )
        # staging area for bodies being streamed in; a row only reaches
        # the cache table inside the commit transaction
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS cache_stream
                (key text UNIQUE, status integer, modified text,
                 encoding text, headers text, data blob)"""
        )

    def set(self, key: str, response: Response) -> None:
        """Set cache entry for key with contents of response."""
        mod = response.headers.pop("last-modified", None)
        status = int(response.status_code)
        rec = (
            key,
            status,
            mod,
            response.encoding,
            response.content,
            json.dumps(dict(response.headers)),
        )
        with self._conn:
            self._conn.execute("DELETE FROM cache WHERE key=?", (key,))
            self._conn.execute("INSERT INTO cache VALUES (?,?,?,?,?,?)", rec)

    def get(self, key: str) -> Optional[Response]:
        """Get cache entry for key, or return None."""
        query = self._conn.execute("SELECT * FROM cache WHERE key=?", (key,))
        rec = query.fetchone()
        if rec is None:
            return None
        rec = dict(zip(self._columns, rec))

        if self.check_last_modified:
            if rec["modified"] is None:
                return None  # no last modified header present, so redownload

            head_resp = requests.head(key)
            new_lm = head_resp.headers.get("last-modified", None)
            if _decode_text(rec["modified"]) != new_lm:
                return None

        resp = Response()
        resp._content = rec["data"]
        resp.status_code = rec["status"]
        resp.encoding = _decode_text(rec["encoding"])
        resp.headers = json.loads(rec["headers"])
        resp.url = key
        return resp

    def stream_get(
        self, key: str
    ) -> Optional[Tuple[Response, _BinaryReadStream]]:
        """Open a cache entry for chunked reading, or return None."""
        query = self._conn.execute(
            "SELECT status, modified, encoding, headers, length(data) "
            "FROM cache WHERE key=?",
            (key,),
        )
        rec = query.fetchone()
        if rec is None:
            return None
        status, modified, encoding, headers_blob, length = rec

        if self.check_last_modified:
            if modified is None:
                return None  # no last modified header present, so redownload

            head_resp = requests.head(key)
            new_lm = head_resp.headers.get("last-modified", None)
            if _decode_text(modified) != new_lm:
                return None

        resp = Response()
        resp.status_code = status
        resp.encoding = _decode_text(encoding)
        resp.headers = json.loads(headers_blob)
        resp.url = key
        _strip_content_encoding(resp.headers)
        resp.headers["content-length"] = str(length)
        reader = _SQLiteBlobReader(self._conn, key, length)
        return resp, cast(_BinaryReadStream, reader)

    def stream_set(self, key: str, response: Response) -> "SQLiteCacheStreamSink":
        """Start writing a cache entry; the body follows via the sink."""
        return SQLiteCacheStreamSink(self.cache_path, key, response)

    def clear(self) -> None:
        """Remove all records from cache."""
        with self._conn:
            self._conn.execute("DELETE FROM cache")

    def __del__(self) -> None:
        self._conn.close()


class SQLiteCacheStreamSink(CacheStreamSink):
    """Streams a body into a staging row, appending in bounded chunks so
    the write buffer does not grow with the body size.  The staged row is
    moved into the cache table in a single transaction at commit, so a
    failure leaves any previous entry untouched."""

    def __init__(
        self, cache_path: str, key: str, response: Response
    ) -> None:
        self._key = key
        self._cache_path = cache_path
        # a dedicated connection keeps interleaved responses independent
        self._conn = sqlite3.connect(cache_path, timeout=30)
        self._conn.isolation_level = None  # autocommit; manage txns explicitly
        self._buf = bytearray()
        self._done = False

        mod = response.headers.get("last-modified")
        self._conn.execute("DELETE FROM cache_stream WHERE key=?", (key,))
        self._conn.execute(
            "INSERT INTO cache_stream "
            "(key, status, modified, encoding, headers, data) "
            "VALUES (?, ?, ?, ?, ?, NULL)",
            (
                key,
                int(response.status_code),
                mod,
                response.encoding,
                json.dumps(dict(response.headers)),
            ),
        )

    def write(self, data: bytes) -> None:
        if self._done:
            raise RuntimeError("cache stream sink was already finalized")
        self._buf.extend(data)
        while len(self._buf) >= STREAM_WRITE_CHUNK:
            self._flush(STREAM_WRITE_CHUNK)

    def _flush(self, n: int) -> None:
        chunk = bytes(self._buf[:n])
        del self._buf[:n]
        # NULL is treated as a zero-length blob for the first chunk; the
        # outer CAST is required because || otherwise gives its result text
        # affinity and corrupts binary (non-UTF-8) bodies
        self._conn.execute(
            "UPDATE cache_stream SET data = CAST("
            "COALESCE(data, zeroblob(0)) || CAST(? AS BLOB) AS BLOB) "
            "WHERE key=?",
            (chunk, self._key),
        )

    def commit(self) -> None:
        if self._done:
            return
        self._done = True
        try:
            if self._buf:
                self._flush(len(self._buf))
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "DELETE FROM cache WHERE key=?", (self._key,)
                )
                self._conn.execute(
                    "INSERT INTO cache "
                    "(key, status, modified, encoding, data, headers) "
                    "SELECT key, status, modified, encoding, data, headers "
                    "FROM cache_stream WHERE key=?",
                    (self._key,),
                )
                self._conn.execute(
                    "DELETE FROM cache_stream WHERE key=?", (self._key,)
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        finally:
            self._conn.close()

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        try:
            self._conn.execute(
                "DELETE FROM cache_stream WHERE key=?", (self._key,)
            )
        except sqlite3.Error:
            pass
        finally:
            self._conn.close()

    def __del__(self) -> None:
        if not self._done:
            try:
                self.abort()
            except Exception:
                pass
