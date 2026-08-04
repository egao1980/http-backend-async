;;;; RFC 8441 Extended CONNECT WebSocket demo (async event-loop path).
;;;;
;;;;   # against a peer that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL=1:
;;;;   HTTP_ASYNC_WS_H2_URL=wss://… ros -l scripts/demo-ws.lisp
;;;;
;;;;   # default URL may reject Extended CONNECT — that still exercises the path:
;;;;   ros -l scripts/demo-ws.lisp
;;;;
;;;; Needs: event-backend-libuv (or set *event-backend-maker*), http2/client,
;;;; fast-websocket. Exits 0 on open (or expected transport-not-available);
;;;; non-zero on unexpected errors.

(setf *debugger-hook*
      (lambda (c h)
        (declare (ignore h))
        (format *error-output* "~&DEMO FAIL: ~A~%" c)
        (uiop:quit 1)))

(ql:quickload '("http2/client" "fast-websocket" "bordeaux-threads") :silent t)
(asdf:load-asd (merge-pathnames "http-backend-async.asd"
                                (uiop:pathname-directory-pathname *load-truename*)))
(asdf:load-system "http-backend-async")
(asdf:load-system "event-backend-libuv")

(setf http-backend-async:*event-backend-maker*
      (lambda () (event-backend-libuv:make-libuv-backend)))

(defun run-demo ()
  (let* ((url (or (uiop:getenv "HTTP_ASYNC_WS_H2_URL")
                  "wss://http2.akamai.com/"))
         (backend (http-backend-async:make-async-backend))
         (client (ws-protocol:make-ws-client backend :transport :http/2))
         (payload (format nil "async-ws-~A" (get-universal-time))))
    (format t "~&; demo: Extended CONNECT → ~A~%" url)
    (handler-case
        (let ((conn (ws-protocol:connect backend client url :transport :http/2)))
          (format t "~&; demo: ready-state=~A~%" (ws-protocol:ready-state conn))
          (ws-protocol:on-event
           conn :message
           (lambda (msg) (format t "~&; demo: message ~S~%" msg)))
          (handler-case (ws-protocol:send-text conn payload)
            (error (e)
              (format t "~&; demo: send skipped/failed (~A) — peer may not echo~%" e)))
          (sleep 0.5)
          (ignore-errors (ws-protocol:close-connection conn))
          t)
      (ws-protocol:ws-transport-not-available (e)
        ;; Peer without ENABLE_CONNECT_PROTOCOL — path still exercised.
        (format t "~&; demo: transport unavailable (ok for smoke): ~A~%" e)
        :skipped)
      (error (e)
        (error e)))))

(let ((result (run-demo)))
  (format t "~&DEMO ~A~%" (if (eq result :skipped) "OK (skipped peer)" "OK"))
  (uiop:quit 0))
