# API Reference

### Scraper

::: scrapelib.Scraper
    rendering:
      heading_level: 4

## Caching

Assign a `MemoryCache`, `FileCache`, or `SQLiteCache` to
the `cache_storage` property of a `scrapelib.Scraper` to cache responses:

``` python

from scrapelib import Scraper
from scrapelib.cache import FileCache
cache = FileCache('cache-directory')
scraper = Scraper()
scraper.cache_storage = cache
scraper.cache_write_only = False
```

### Streaming cache

By default a cacheable response is read into memory in full before it is
stored.  Set `stream_cache = True` to instead cache streamed requests
(`stream=True`) without reading the body up-front.  Chunks consumed via
`iter_content`, `content`, or `text` are spooled into the cache as they
arrive, and the entry is only published once the body has been read to
EOF.  Closing a response early, a transport failure, or a cache write
failure discards the temporary state and leaves any previous entry for
that key untouched; a cache write failure is re-raised to the consumer.

Only `FileCache` and `SQLiteCache` support this mode.  Enabling it with
another backend raises a `TypeError` before any request is sent.  Cached
bodies are stored after content decoding, so gzipped responses are not
decoded a second time on replay, and both write and read buffers stay
bounded regardless of body size.  Cache hits support chunked reads as
well, so several cached responses can be consumed interleaved.

``` python

from scrapelib import Scraper
from scrapelib.cache import SQLiteCache
scraper = Scraper()
scraper.cache_storage = SQLiteCache('responses.db')
scraper.cache_write_only = False
scraper.stream_cache = True

# the body is cached while you read it; nothing is read up-front
response = scraper.get('https://example.com/large', stream=True)
for chunk in response.iter_content(65536):
    handle(chunk)

# a later request replays the cached body, also chunk by chunk
replayed = scraper.get('https://example.com/large', stream=True)
assert replayed.fromcache is True
```

### MemoryCache

::: scrapelib.MemoryCache
    rendering:
      heading_level: 4
    selection:
      members: False

### FileCache

::: scrapelib.FileCache
    rendering:
      heading_level: 4
    selection:
      members: False

### SQLiteCache

::: scrapelib.SQLiteCache
    rendering:
      heading_level: 4
    selection:
      members: False

## Exceptions

### HTTPError

::: scrapelib.HTTPError
    rendering:
      heading_level: 4

### HTTPMethodUnavailableError

::: scrapelib.HTTPMethodUnavailableError
    rendering:
      heading_level: 4

### FTPError

::: scrapelib.FTPError
    rendering:
      heading_level: 4
