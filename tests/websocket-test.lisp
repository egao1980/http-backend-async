(in-package #:http-backend-async/tests)

(deftest async-backend-is-ws-backend
  (let ((b (make-instance 'async-backend)))
    (ok (typep b 'ws-protocol:ws-backend))
    (ok (null (backend-ws-transports b)))
    (ok (not (backend-supports-ws-transport-p b :auto)))
    (ok (not (backend-supports-ws-transport-p b :http/2)))))

(deftest async-ws-connect-rejected
  (let* ((b (make-instance 'async-backend))
         (c (make-ws-client b)))
    (ok (signals (ws-protocol:connect b c "wss://example.com/ws")
                 'ws-transport-not-available))))

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
      (ok t) ; soft-dep http2 missing in this environment
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
