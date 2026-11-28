# http-backend-async

Async [`http-protocol`](https://github.com/egao1980/http-protocol) backend on
[`event-protocol`](https://github.com/egao1980/event-protocol).

One HTTP backend × N event loops (`event-backend-libuv`, `event-backend-libev`).
**Not** Carrier/cl-async — Carrier hard-wires libuv; we only use `register-io`.

## Status (wave-1)

- HTTP/1.1 over portable **usocket** TCP (nonblocking connect on SBCL →
  FD → `event-protocol:register-io` `:read-write`)
- HTTPS via `cl+ssl` / `cl-stack-ssl` socket-BIO: `SSL_connect` /
  `SSL_read` / `SSL_write` pumped with `WANT_READ`/`WANT_WRITE` on the
  event loop (no blocking `ensure-ssl-funcall`). TLS inside TLS (an https
  origin through an `https://` proxy) runs the inner session on two memory
  BIOs carried by the outer one
- Proxies: `http://` (absolute-form, or CONNECT for https), `https://` and
  `socks5://` / `socks5h://`. An `https://` proxy gets TLS first, verified
  against the proxy's host name with the client's `:verify`; CONNECT or the
  absolute-form request and `Proxy-Authorization` go inside it, as curl and
  urllib3 do (#53). Nothing reaches such a proxy in cleartext
- `send-async` + `cancel-request`; facade promises via `http:*-async`
- Cookie-jar (cl-cookie) + redirect follow (`response-history`; 301/302/303→GET).
  Each hop is built for its own URL (#82): the proxy is resolved again,
  `http://` goes to it in absolute-form with `Proxy-Authorization`, `https://`
  through CONNECT, and the request inside the tunnel never carries
  `Proxy-Authorization`. A hop to another origin (scheme, host or port) drops
  `Authorization`; `Cookie` comes from the jar for the new URL
- Content-Encoding decode (chipz; soft-load br/zstd)
- HTTP/1 framing (#91): interim responses (`100 Continue`, `103 Early Hints`,
  any 1xx but `101`) are skipped, so the final response is the one returned
  (RFC 9110 §15.2). A transfer coding other than `chunked`
  (`Transfer-Encoding: gzip, chunked`, or `gzip` alone, which is then
  close-delimited) is decoded with the content codings: it is appended to the
  response's `Content-Encoding` (so with `:decompress nil` it stays applied and
  is listed there). `chunked` that is not the final coding is an
  `http-protocol-error`. A close-delimited body ends at the peer's FIN; one cut
  by an RST fails as a connection error, as in urllib3. An error of a body
  sink is signalled as it is, not as "response parse failed"
- Bodies via `http-protocol` ≥ **0.2.0**: `:form-data` / typed `:data` / `:content` / streams
- Live CE tests: `HTTP_ASYNC_LIVE=1` (default in CI) → httpbingo `/gzip` `/brotli`
- HTTP/2 over ALPN `h2`, multiplexed: one pooled `h2-mux` per origin and event
  loop carries concurrent requests as streams, up to the peer's
  `SETTINGS_MAX_CONCURRENT_STREAMS` (RFC 9113 §5.1.2) capped by
  `*h2-max-streams-per-connection*` (100). A full connection leaves the pool;
  the next request queues (FIFO) for its next free stream, and dials another
  connection only after `*h2-stream-wait-timeout*` (2 s; NIL dials at once) or
  when `*h2-max-stream-waiters*` (32) already wait. Needs http2
  ≥ 2.1.2 to read the peer's limit — older http2 reuses one stream at a time.
  Live h2 tests: `HTTP_ASYNC_H2_LIVE=1`
- Timeouts follow `http-protocol:http-timeout`: `:connect` bounds TCP/proxy/TLS
  setup, `:read` the silence between octets (a stream that keeps arriving is
  never cut off), `:total` the whole attempt. An unset phase has no limit. A
  bare number is `:total`; no timeout is the protocol default (30 s total);
  `(:connect 30 :read 300)` has no total cap. Errors are `http-timeout-error`
  ("connect timed out …", "read timed out: no data for N s …").
- Blocking `send` with `:want-stream t` returns when the headers arrive; the
  body is the live stream (SSE events reach the reader as they arrive). A
  pump thread runs the request's own loop until the body ends; read it to EOF
  or `close` it (closing early ends the request). Timeouts while the body
  streams signal from the read, and are not retried. On a loop the caller
  bound (`event-protocol:*event-loop*`), a streamed request runs on a private
  loop of the same event backend.
- HTTP/1.1 keep-alive pool (`make-lru-connection-pool`): an idle connection
  is reused until just before the server's `Keep-Alive: timeout=N`. Without
  that header it is reused for `*pool-idle-limit*` seconds (30; per pool
  `:idle-limit`, NIL for no limit), then closed: servers that drop idle
  sockets without saying when (api.anthropic.com) otherwise answer the next
  request with EOF. An idempotent request (GET, HEAD, OPTIONS, TRACE, PUT,
  DELETE) that reads EOF before any response octet on a reused socket is
  sent again on a new one; a POST is not.
- A response cut off by EOF is `http-protocol:http-incomplete-response`, a
  connection error: `:retry` resends an idempotent request.

## Use

```lisp
(asdf:load-system "event-backend-libuv")
(asdf:load-system "http-backend-async")

(let* ((eb (event-backend-libuv:make-libuv-backend))
       (el (event-protocol:make-event-loop eb))
       (hb (http-backend-async:make-async-backend)))
  (event-protocol:with-event-backend (eb)
    (event-protocol:with-event-loop-var (el)
      (let ((http-protocol:*http-backend* hb))
        (blackbird:attach
         (http:get-async "http://127.0.0.1:8080/ok")
         (lambda (res)
           (print (http:response-status res))
           (event-protocol:stop eb el)))
        (event-protocol:run eb el)))))
```

## Test matrix

```bash
HTTP_ASYNC_EVENT_BACKEND=libuv qlot exec ros -e '(asdf:test-system "http-backend-async")'
HTTP_ASYNC_EVENT_BACKEND=libev qlot exec ros -e '(asdf:test-system "http-backend-async")'
```

Tracks [egao1980/cl-stack#31](https://github.com/egao1980/cl-stack/issues/31).

## Publish

Source-only OCI publish is centralized in [`cl-stack-systems`](https://github.com/egao1980/cl-stack-systems)
(`imports/http-backend-async/qlfile` pin + shared `publish.yml`). Packaging metadata lives in the `.asd`
(`auto-package-spec`):

```bash
gh workflow run publish.yml -R egao1980/cl-stack-systems -f import=http-backend-async
```

