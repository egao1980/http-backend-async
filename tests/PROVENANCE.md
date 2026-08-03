# Test provenance

## requests / httpbin

Live + fixture Content-Encoding cases track [psf/requests](https://github.com/psf/requests) httpbin usage:

| Lisp test | Inspiration |
|-----------|-------------|
| `test-decompress-gzip` / `live-decompress-gzip` | `TestRequests.test_decompress_gzip` — `GET {httpbin}/gzip`, body decodes with `gzipped: true` |
| `test-decompress-deflate` / `live-decompress-deflate` | urllib3 `test_decode_deflate` + httpbin `/deflate` (`deflated: true`) |
| `test-accept-encoding-identity` / `live-accept-encoding-identity` | Session `Accept-Encoding: identity` |
| `test-content-encoding-case-insensitive` | urllib3 case-insensitive `Content-Encoding` |
| soft-load `br` | requests FAQ: decode br when a brotli decoder is available |
| `test-session-cookie-jar` | requests `Session` cookie jar — Set-Cookie then Cookie on next GET |
| `test-redirect-*` | requests redirect follow / history / TooManyRedirects / cookies on hop |

- requests: Apache-2.0 — ideas only; no copied source
- Original Rove tests in this repo: MIT
