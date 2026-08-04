(in-package #:http-backend-async/tests)

(deftest async-backend-is-ws-backend
  (let ((b (make-instance 'async-backend)))
    (ok (typep b 'ws-protocol:ws-backend))
    (if (and (ensure-http2) (http-backend-async::ensure-fast-websocket))
        (progn
          (ok (equal '(:http/2) (backend-ws-transports b)))
          (ok (backend-supports-ws-transport-p b :http/2))
          (ok (backend-supports-ws-transport-p b :auto)))
        (progn
          (ok (null (backend-ws-transports b)))
          (ok (not (backend-supports-ws-transport-p b :auto)))))))

(deftest async-ws-rejects-http11-transport
  (let* ((b (make-instance 'async-backend))
         (c (make-ws-client b :transport :http/1.1)))
    (if (member :http/2 (backend-ws-transports b) :test #'eq)
        (ok (signals (ws-protocol:connect b c "wss://example.com/ws" :transport :http/1.1)
                     'ws-transport-not-available))
        (ok t))))

(deftest extended-connect-header-helper
  (let ((hdrs (make-extended-connect-ws-headers
               "wss://example.com/chat"
               :protocols '("chat"))))
    (ok (equal "CONNECT" (cdr (assoc :method hdrs))))
    (ok (equal "websocket" (cdr (assoc :protocol hdrs))))
    (ok (equal "/chat" (cdr (assoc :path hdrs))))))

(deftest h2-enable-connect-protocol-setting
  "Peer SETTINGS_ENABLE_CONNECT_PROTOCOL is recorded on the async H2 connection."
  (if (not (ensure-http2))
      (ok t)
      (let* ((pump (make-instance 'async-h2-pump-stream))
             (session (make-async-h2-session pump))
             (conn (http-backend-async::async-h2-session-connection session))
             (set-peer (find-symbol "SET-PEER-SETTING" :http2/core)))
        (ok (typep conn 'http-backend-async::async-h2-client-connection))
        (ok (not (h2-session-enable-connect-protocol-p session)))
        (funcall set-peer conn :enable-connect-protocol 1)
        (ok (h2-session-enable-connect-protocol-p session))
        (funcall set-peer conn :enable-connect-protocol 0)
        (ok (not (h2-session-enable-connect-protocol-p session))))))

(defun %async-ws-h2-live-p ()
  "True when live gate set. Uses feature-or-env when ws-protocol ≥0.2.1."
  (let ((fn (find-symbol "FEATURE-OR-ENV-ENABLED-P" :ws-protocol)))
    (if (and fn (fboundp fn))
        (funcall fn :http-async-ws-h2-live "HTTP_ASYNC_WS_H2_LIVE")
        (let ((v (uiop:getenv "HTTP_ASYNC_WS_H2_LIVE")))
          (and v (not (member (string-downcase v)
                              '("" "0" "false" "no" "off")
                              :test #'string=)))))))

(deftest extended-connect-live-optional
  "Live RFC 8441 WS — gate with HTTP_ASYNC_WS_H2_LIVE=1 or :http-async-ws-h2-live."
  (cond
    ((not (%async-ws-h2-live-p))
     (skip "set HTTP_ASYNC_WS_H2_LIVE=1 for Extended CONNECT live"))
    ((not (and (ensure-http2) (http-backend-async::ensure-fast-websocket)))
     (skip "http2/fast-websocket unavailable"))
    (t
     (let* ((b (make-instance 'async-backend))
            (c (make-ws-client b :transport :http/2))
            (url (or (uiop:getenv "HTTP_ASYNC_WS_H2_URL")
                     "wss://http2.akamai.com/"))) ; often ENABLE_CONNECT; may not echo
       (handler-case
           (let ((conn (ws-protocol:connect b c url :transport :http/2)))
             (ok (eq :open (ws-protocol:ready-state conn)))
             (ignore-errors (ws-protocol:close-connection conn)))
         (ws-transport-not-available (e)
           (declare (ignore e))
           (ok t)) ; peer without ENABLE_CONNECT is acceptable for this smoke
         (error (e)
           (fail (format nil "unexpected: ~A" e))))))))
