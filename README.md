# http-backend-async

Async [`http-protocol`](https://github.com/egao1980/http-protocol) backend on
[`event-protocol`](https://github.com/egao1980/event-protocol).

One HTTP backend × N event loops (`event-backend-libuv`, `event-backend-libev`).
**Not** Carrier/cl-async — Carrier hard-wires libuv; we only use `register-io`.

## Status (wave-1)

- HTTP/1.1 cleartext over nonblocking TCP (`sb-bsd-sockets`)
- `send-async` + `cancel-request`; facade promises via `http:*-async`
- Content-Encoding decode (chipz; soft-load br/zstd)
- HTTPS/TLS: deferred (next slice on `cl-stack-ssl`)

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
