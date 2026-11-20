(in-package #:http-backend-async/tests)

;;; #55: LM Studio (Express) answers GET /v1/models with Content-Length framing,
;;; Connection: keep-alive and Keep-Alive: timeout=5, then closes the idle
;;; socket after 5 s. The next request on the pooled socket read EOF before any
;;; response octet: "incomplete HTTP response".

(defparameter +lmstudio-models-body+
  (babel:string-to-octets
   "{
  \"data\": [
    {
      \"id\": \"qwen3.5-2b\",
      \"object\": \"model\",
      \"owned_by\": \"organization_owner\"
    }
  ],
  \"object\": \"list\"
}"))

(defun %lmstudio-models-handler (keep-alive-timeout)
  "Response head as LM Studio sends it (captured with curl -sv)."
  (lambda (method path headers body)
    (declare (ignore method path headers body))
    (values 200
            `(("X-Powered-By" . "Express")
              ("Content-Type" . "application/json; charset=utf-8")
              ("Content-Length" . ,(princ-to-string
                                    (length +lmstudio-models-body+)))
              ("ETag" . "W/\"26a-3gTbltnD7QTQdmN+U+MOBsnxyg8\"")
              ("Date" . "Fri, 20 Nov 2026 10:25:34 GMT")
              ("Keep-Alive" . ,(format nil "timeout=~D" keep-alive-timeout)))
            +lmstudio-models-body+)))

(deftest keep-alive-timeout-header
  (let ((ht (make-hash-table :test #'equal)))
    (ok (null (http-backend-async::keep-alive-timeout ht)))
    (setf (gethash "keep-alive" ht) "timeout=5")
    (ok (eql 5 (http-backend-async::keep-alive-timeout ht)))
    (setf (gethash "keep-alive" ht) "timeout=60, max=100")
    (ok (eql 60 (http-backend-async::keep-alive-timeout ht)))
    (setf (gethash "keep-alive" ht) "max=100")
    (ok (null (http-backend-async::keep-alive-timeout ht))))
  (ok (= 4 (http-backend-async::keep-alive-reuse-limit 5)))
  (ok (= 1/2 (http-backend-async::keep-alive-reuse-limit 1))))

(deftest pooled-connection-expires-after-keep-alive-timeout
  (let ((c (http-backend-async::make-async-pooled-connection nil :idle-timeout 5)))
    (ok (not (http-backend-async::async-conn-idle-expired-p c)))
    (decf (http-backend-async::async-conn-idle-since c)
          (* 5 internal-time-units-per-second))
    (ok (http-backend-async::async-conn-idle-expired-p c)))
  (let ((c (http-backend-async::make-async-pooled-connection nil)))
    (decf (http-backend-async::async-conn-idle-since c)
          (* 3600 internal-time-units-per-second))
    (ok (not (http-backend-async::async-conn-idle-expired-p c)))))

(deftest stale-pooled-connection-is-redialed
  "The server says keep-alive, then hangs up (its idle timeout fired): the
   second GET reads EOF on the pooled socket and is sent again on a new one."
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture ((%lmstudio-models-handler 5)
                        :keep-alive :close-after-response)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (dotimes (i 2)
            (let ((res (%await-promise
                        (get-async (fixture-url "/v1/models") :client client)
                        eb el)))
              (ok (= 200 (response-status res)))
              (ok (equalp +lmstudio-models-body+ (response-body res)))))
          (ok (= 2 *fixture-accept-count*))
          (ok (= 2 *fixture-request-count*))
          (pool-clear pool))))))

(deftest stale-pooled-socket-is-dropped-before-post
  "Before reuse, an idle cleartext socket is probed: the server's close shows
   as EOF, so the POST goes out once, on a new connection."
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture ((%lmstudio-models-handler 5)
                        :keep-alive :close-after-response)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (ok (= 200 (response-status
                      (%await-promise
                       (get-async (fixture-url "/v1/models") :client client)
                       eb el))))
          (ok (= 200 (response-status
                      (%await-promise
                       (post-async (fixture-url "/v1/chat") :client client
                                                             :content "{}")
                       eb el))))
          (ok (= 2 *fixture-accept-count*))
          (ok (= 2 *fixture-request-count*))
          (pool-clear pool))))))

(deftest keep-alive-timeout-retires-idle-connection
  "Keep-Alive: timeout=1 → after 0.6 s idle the pooled socket is not reused."
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture ((%lmstudio-models-handler 1) :keep-alive t)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (ok (= 200 (response-status
                      (%await-promise
                       (get-async (fixture-url "/v1/models") :client client)
                       eb el))))
          (sleep 0.6)
          (ok (= 200 (response-status
                      (%await-promise
                       (get-async (fixture-url "/v1/models") :client client)
                       eb el))))
          (ok (= 2 *fixture-accept-count*))
          (pool-clear pool))))))

;;; Without the probe (it can miss a close that races the request, and it
;;; skips TLS sockets) the request itself reads EOF on the stale socket.

(defmacro %without-probe (&body body)
  ;; On Windows too since #57: a reset socket is a connection error there,
  ;; not a would-block read.
  `(let ((http-backend-async::*probe-idle-sockets* nil)) ,@body))

(deftest stale-pooled-get-is-replayed
  (%without-probe
    (let ((http-protocol:*default-connection-pool* nil))
      (with-http-fixture ((%lmstudio-models-handler 5)
                          :keep-alive :close-after-response)
        (with-async-test (eb el hb)
          (let* ((pool (make-lru-connection-pool :max-size 4))
                 (client (make-http-client hb :pool pool)))
            (dotimes (i 2)
              (let ((res (%await-promise
                          (get-async (fixture-url "/v1/models") :client client)
                          eb el)))
                (ok (= 200 (response-status res)))))
            (ok (= 2 *fixture-accept-count*))
            (ok (= 2 *fixture-request-count*))
            (pool-clear pool)))))))

(deftest stale-pooled-post-is-not-replayed
  "POST is not idempotent: on a stale socket it fails; it is not sent twice."
  (%without-probe
    (let ((http-protocol:*default-connection-pool* nil))
      (with-http-fixture ((%lmstudio-models-handler 5)
                          :keep-alive :close-after-response)
        (with-async-test (eb el hb)
          (let* ((pool (make-lru-connection-pool :max-size 4))
                 (client (make-http-client hb :pool pool)))
            (ok (= 200 (response-status
                        (%await-promise
                         (get-async (fixture-url "/v1/models") :client client)
                         eb el))))
            (ok (signals (%await-promise
                          (post-async (fixture-url "/v1/chat") :client client
                                                                :content "{}")
                          eb el)
                         'http-error))
            (ok (= 1 *fixture-request-count*))
            (pool-clear pool)))))))

;;; #57: a socket the peer reset is a connection error, not a would-block read
;;; (Windows SBCL used to read recv's -1 as 4294967295 and wait).

(deftest reset-socket-recv-is-a-connection-error
  (let ((server (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                     :element-type '(unsigned-byte 8))))
    (unwind-protect
         (let* ((port (usocket:get-local-port server))
                (client (http-backend-async::tcp-connect "127.0.0.1" port))
                (peer (usocket:socket-accept server :element-type '(unsigned-byte 8))))
           (unwind-protect
                (progn
                  (http-backend-async::set-socket-nonblocking client t)
                  (ok (null (http-backend-async::socket-recv-octets
                             client (make-array 16 :element-type '(unsigned-byte 8))))
                      "nothing sent yet: would-block")
                  ;; Unread data at close makes the peer send RST, not FIN.
                  (ok (eql 4 (http-backend-async::socket-send-octets
                              client (make-array 4 :element-type '(unsigned-byte 8)
                                                   :initial-element 65)
                              0 4)))
                  (sleep 0.2)
                  (usocket:socket-close peer)
                  (setf peer nil)
                  (let ((outcome
                          (loop repeat 100
                                for r = (handler-case
                                            (http-backend-async::socket-recv-octets
                                             client (make-array 16 :element-type '(unsigned-byte 8)))
                                          (http-connection-error () :reset))
                                when r return r
                                do (sleep 0.02))))
                    (ok (member outcome '(:reset 0))
                        "reset (or EOF): never would-block for ever")
                    (ok (not (http-backend-async::socket-idle-open-p client))
                        "the idle probe drops it")))
             (when peer (usocket:socket-close peer))
             (http-backend-async::close-socket client)))
      (usocket:socket-close server))))
