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
  event loop (no blocking `ensure-ssl-funcall` / MemoryBIO)
- `send-async` + `cancel-request`; facade promises via `http:*-async`
- Cookie-jar (cl-cookie) + redirect follow (`response-history`; 301/302/303→GET)
- Content-Encoding decode (chipz; soft-load br/zstd)
- Live CE tests: `HTTP_ASYNC_LIVE=1` (default in CI) → httpbingo `/gzip` `/brotli`

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
