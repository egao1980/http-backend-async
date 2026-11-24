(in-package #:http-backend-async/tests)

;;;; #53: an https:// proxy is reached over TLS, never in cleartext.
;;;;
;;;; The fixture is a loopback TLS proxy (127.0.0.1 only). It records every
;;;; raw octet it reads off the socket, takes the server side of TLS with a
;;;; test certificate, records the plaintext it reads inside, and answers
;;;; CONNECT. Through the tunnel it is the origin too: TLS again, with the
;;;; origin.test certificate, then the request. A client that does not start
;;;; with TLS gets its cleartext head recorded and a 502.
;;;;
;;;; tests/certs: a CA made for these tests (its key was not kept), proxy.crt
;;;; (CN 127.0.0.1, SAN IP 127.0.0.1 + localhost) and origin.crt (origin.test),
;;;; both signed by it. The proxy credentials are made up: user / secret.

(defparameter *proxy-test-certs*
  (asdf:system-relative-pathname "http-backend-async" "tests/certs/"))

(defun %test-cert (name)
  (namestring (merge-pathnames name *proxy-test-certs*)))

(defparameter +test-proxy-credentials+ "dXNlcjpzZWNyZXQ="
  "user:secret in base64, as Proxy-Authorization: Basic carries it.")

(defun %octet-buffer ()
  (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))

(defun %latin1 (octets)
  (map 'string #'code-char octets))

(defun %wire (&rest lines)
  "LINES joined with CRLF, plus the blank line that ends a head, as octets."
  (babel:string-to-octets
   (format nil "~{~A~C~C~}~C~C"
           (loop for line in lines
                 append (list line #\Return #\Newline))
           #\Return #\Newline)
   :encoding :latin-1))

(defun %starts-with-p (prefix string)
  (and (>= (length string) (length prefix))
       (string= prefix string :end2 (length prefix))))

(defclass %recording-stream (trivial-gray-streams:fundamental-binary-input-stream
                             trivial-gray-streams:fundamental-binary-output-stream)
  ((inner :initarg :inner :reader %recording-inner)
   (replay :initarg :replay :initform nil :accessor %recording-replay
           :documentation "Octets already read (and logged) that READ-BYTE returns first.")
   (log :initarg :log :reader %recording-log
        :documentation "Every octet read from INNER, in order."))
  (:documentation "A binary stream over INNER that logs what it reads. cl+ssl's
   Lisp BIO reads and writes through it, so under TLS the log is ciphertext."))

(defmethod stream-element-type ((stream %recording-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte ((stream %recording-stream))
  (if (%recording-replay stream)
      (pop (%recording-replay stream))
      (let ((byte (read-byte (%recording-inner stream) nil :eof)))
        (unless (eq byte :eof)
          (vector-push-extend byte (%recording-log stream)))
        byte)))

(defmethod trivial-gray-streams:stream-listen ((stream %recording-stream))
  (or (and (%recording-replay stream) t)
      (listen (%recording-inner stream))))

(defmethod trivial-gray-streams:stream-write-byte ((stream %recording-stream) byte)
  (write-byte byte (%recording-inner stream))
  byte)

(defmethod trivial-gray-streams:stream-write-sequence
    ((stream %recording-stream) sequence start end &key)
  (write-sequence sequence (%recording-inner stream) :start start :end end)
  sequence)

(defmethod trivial-gray-streams:stream-finish-output ((stream %recording-stream))
  (finish-output (%recording-inner stream)))

(defmethod trivial-gray-streams:stream-force-output ((stream %recording-stream))
  (force-output (%recording-inner stream)))

(defun %read-head (stream &key (limit 8192))
  "Octets from STREAM through the first blank line (CRLF CRLF), as a string."
  (let ((buf (%octet-buffer)))
    (loop for byte = (read-byte stream nil nil)
          while byte
          do (vector-push-extend byte buf)
          until (or (>= (length buf) limit)
                    (let ((n (length buf)))
                      (and (>= n 4)
                           (= 13 (aref buf (- n 4))) (= 10 (aref buf (- n 3)))
                           (= 13 (aref buf (- n 2))) (= 10 (aref buf (- n 1)))))))
    (%latin1 buf)))

(defstruct (%proxy-record (:conc-name %pr-))
  (accepted nil)
  (raw (%octet-buffer))        ; octets off the socket
  (cleartext nil)              ; the head, when the client did not start with TLS
  (inside "")                  ; the head read inside the proxy's TLS
  (tunnel-raw (%octet-buffer)) ; octets inside the proxy's TLS after CONNECT
  (origin '())                 ; heads read inside the origin's TLS, in order
  (error nil)
  (done nil))

(defun %answer (stream keep-alive)
  (write-sequence (%wire "HTTP/1.1 200 OK"
                         "Content-Type: text/plain"
                         "Content-Length: 2"
                         (if keep-alive "Connection: keep-alive" "Connection: close"))
                  stream)
  (write-sequence (babel:string-to-octets "ok") stream)
  (finish-output stream))

(defun %serve-tls-proxy (listener record cert requests)
  "Serve one connection on LISTENER, filling RECORD. CERT names the proxy's
   certificate (tests/certs/CERT.crt). Through a CONNECT tunnel, answer
   REQUESTS requests (keep-alive between them)."
  (let ((conn nil))
    (unwind-protect
         (handler-case
             (when (usocket:wait-for-input listener :timeout 10 :ready-only t)
               (setf conn (usocket:socket-accept listener :element-type '(unsigned-byte 8))
                     (%pr-accepted record) t)
               (ignore-errors (setf (usocket:socket-option conn :receive-timeout) 10))
               (let* ((socket (usocket:socket-stream conn))
                      (raw (make-instance '%recording-stream
                                          :inner socket :log (%pr-raw record)))
                      (record-header (loop repeat 5 collect (read-byte raw))))
                 (setf (%recording-replay raw) (copy-list record-header))
                 (cond
                   ((/= #x16 (first record-header))
                    ;; Not a TLS record: the request came in cleartext (#53).
                    (setf (%pr-cleartext record) (%read-head raw))
                    (write-sequence (%wire "HTTP/1.1 502 Bad Gateway"
                                           "Content-Length: 0"
                                           "Connection: close")
                                    socket)
                    (finish-output socket))
                   (t
                    (let* ((tls (cl+ssl:make-ssl-server-stream
                                 raw
                                 :certificate (%test-cert (format nil "~A.crt" cert))
                                 :key (%test-cert (format nil "~A.key" cert))
                                 :unwrap-stream-p nil))
                           (head (%read-head tls)))
                      (setf (%pr-inside record) head)
                      (cond
                        ((%starts-with-p "CONNECT " head)
                         (write-sequence (%wire "HTTP/1.1 200 Connection established") tls)
                         (finish-output tls)
                         (let* ((tunnel (make-instance '%recording-stream
                                                       :inner tls
                                                       :log (%pr-tunnel-raw record)))
                                (origin (cl+ssl:make-ssl-server-stream
                                         tunnel
                                         :certificate (%test-cert "origin.crt")
                                         :key (%test-cert "origin.key")
                                         :unwrap-stream-p nil)))
                           (loop for i from 1 to requests
                                 for request = (%read-head origin)
                                 while (plusp (length request))
                                 do (setf (%pr-origin record)
                                          (append (%pr-origin record) (list request)))
                                    (%answer origin (< i requests)))))
                        (t (%answer tls nil))))))))
           (error (e)
             (setf (%pr-error record) (princ-to-string e))))
      (when conn
        (ignore-errors (usocket:socket-close conn)))
      (setf (%pr-done record) t))))

(defun %start-tls-proxy (&key (cert "proxy") (requests 1))
  "A loopback TLS proxy serving one connection. → (values port thread record listener)."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0
                                          :reuse-address t
                                          :element-type '(unsigned-byte 8)))
         (record (make-%proxy-record))
         (thread (bt:make-thread
                  (lambda () (%serve-tls-proxy listener record cert requests))
                  :name "https-proxy-fixture")))
    (values (usocket:get-local-port listener) thread record listener)))

(defun %finish-tls-proxy (thread record listener &key (seconds 10))
  "Wait for the fixture to finish its connection, then close it."
  (let ((deadline (+ (get-internal-real-time)
                     (* seconds internal-time-units-per-second))))
    (loop until (or (%pr-done record)
                    (> (get-internal-real-time) deadline))
          do (sleep 0.02))
    (ignore-errors (usocket:socket-close listener))
    (unless (%pr-done record)
      (ignore-errors (bt:destroy-thread thread)))
    record))

(defun %https-proxy-get (urls port &key verify pool (proxy-host "127.0.0.1"))
  "GET each of URLS through the https:// proxy https://user:secret@PROXY-HOST:PORT.
   → list of responses (or the condition that ended the first failing one)."
  (with-async-test (eb el hb)
    (let ((client (make-http-client
                   hb
                   :proxy (format nil "https://user:secret@~A:~D" proxy-host port)
                   :verify verify
                   :pool pool)))
      (loop for url in urls
            for res = (handler-case
                          (%await-promise (get-async url :client client) eb el
                                          :timeout 10)
                        (error (e) e))
            collect res
            until (typep res 'condition)))))

(defun %tls-client-hello-p (octets)
  "OCTETS begin with a TLS handshake record holding a ClientHello."
  (and (>= (length octets) 6)
       (= #x16 (aref octets 0))
       (= 3 (aref octets 1))
       (= 1 (aref octets 5))))

(defun %leaks (octets)
  "What of the request or its proxy credentials OCTETS carry in cleartext."
  (let ((text (string-downcase (%latin1 octets))))
    (remove nil
            (list (and (search (string-downcase +test-proxy-credentials+) text) :credentials)
                  (and (search "proxy-authorization" text) :proxy-authorization)
                  (and (search "connect origin.test" text) :connect)
                  (and (search "get " text) :request-line)))))

(defun %ok-response-p (res)
  (and (typep res 'http-response)
       (= 200 (response-status res))
       (equalp (babel:string-to-octets "ok") (response-body res))))

(deftest https-proxy-http-origin-over-tls
  "http:// origin through an https:// proxy: TLS to the proxy, then the
   absolute-form request and Proxy-Authorization inside it (#53)."
  (multiple-value-bind (port thread record listener) (%start-tls-proxy)
    (let* ((res (first (%https-proxy-get '("http://origin.test/path") port)))
           (record (%finish-tls-proxy thread record listener)))
      (ok (%ok-response-p res) (format nil "200 ok through the proxy (got ~A)" res))
      (ok (null (%pr-cleartext record))
          (format nil "nothing in cleartext (the proxy read ~S)" (%pr-cleartext record)))
      (ok (%tls-client-hello-p (%pr-raw record))
          "the first octets on the wire are a TLS ClientHello")
      (ok (null (%leaks (%pr-raw record)))
          (format nil "no request line or credentials outside TLS (~S)" (%leaks (%pr-raw record))))
      (ok (%starts-with-p "GET http://origin.test/path HTTP/1.1" (%pr-inside record))
          (format nil "absolute-form inside TLS (~S)" (%pr-inside record)))
      (ok (search +test-proxy-credentials+ (%pr-inside record))
          "Proxy-Authorization arrives inside TLS")
      (ok (null (%pr-error record)) (format nil "fixture: ~A" (%pr-error record))))))

(deftest https-proxy-https-origin-tls-in-tls
  "https:// origin through an https:// proxy: TLS to the proxy, CONNECT
   inside it, then TLS to the origin inside the tunnel (#53)."
  (multiple-value-bind (port thread record listener) (%start-tls-proxy)
    (let* ((res (first (%https-proxy-get '("https://origin.test/path") port)))
           (record (%finish-tls-proxy thread record listener)))
      (ok (%ok-response-p res) (format nil "200 ok through the tunnel (got ~A)" res))
      (ok (null (%pr-cleartext record))
          (format nil "nothing in cleartext (the proxy read ~S)" (%pr-cleartext record)))
      (ok (%tls-client-hello-p (%pr-raw record))
          "the first octets on the wire are a TLS ClientHello")
      (ok (null (%leaks (%pr-raw record)))
          (format nil "no CONNECT or credentials outside TLS (~S)" (%leaks (%pr-raw record))))
      (ok (%starts-with-p "CONNECT origin.test:443 HTTP/1.1" (%pr-inside record))
          (format nil "CONNECT inside TLS (~S)" (%pr-inside record)))
      (ok (search +test-proxy-credentials+ (%pr-inside record))
          "Proxy-Authorization arrives inside TLS")
      (ok (%tls-client-hello-p (%pr-tunnel-raw record))
          "the tunnel starts with the origin's ClientHello: TLS inside TLS")
      (ok (null (%leaks (%pr-tunnel-raw record)))
          "the proxy cannot read the request it relays")
      (ok (%starts-with-p "GET /path HTTP/1.1" (or (first (%pr-origin record)) ""))
          (format nil "the origin reads the request (~S)" (%pr-origin record)))
      (ok (not (search +test-proxy-credentials+ (or (first (%pr-origin record)) "")))
          "the proxy credentials never reach the origin")
      (ok (null (%pr-error record)) (format nil "fixture: ~A" (%pr-error record))))))

(defun %call-trusting-test-ca (thunk)
  "Call THUNK with a TLS context that trusts the test CA (and only it)."
  (let ((ctx (cl+ssl:make-context :verify-location (%test-cert "ca.crt"))))
    (cl+ssl:with-global-context (ctx :auto-free-p t)
      (funcall thunk))))

(deftest https-proxy-verifies-the-proxy-host
  "With :verify t the proxy's certificate is checked against the proxy's
   host name (CN 127.0.0.1), the origin's against origin.test (#53)."
  (dolist (url '("http://origin.test/path" "https://origin.test/path"))
    (testing url
      (multiple-value-bind (port thread record listener) (%start-tls-proxy)
        (let* ((res (first (%call-trusting-test-ca
                            (lambda () (%https-proxy-get (list url) port :verify t)))))
               (record (%finish-tls-proxy thread record listener)))
          (ok (%ok-response-p res) (format nil "verified both hops (got ~A)" res))
          (ok (null (%leaks (%pr-raw record)))))))))

(deftest https-proxy-bad-certificate-sends-nothing
  "A proxy certificate that fails verification ends the request with
   HTTP-TLS-ERROR before any octet of the request goes out (#53)."
  (dolist (url '("http://origin.test/path" "https://origin.test/path"))
    (testing (format nil "~A: certificate for another host (origin.test)" url)
      (multiple-value-bind (port thread record listener) (%start-tls-proxy :cert "origin")
        (let* ((res (first (%call-trusting-test-ca
                            (lambda () (%https-proxy-get (list url) port :verify t)))))
               (record (%finish-tls-proxy thread record listener)))
          (ok (typep res 'http-tls-error) (format nil "HTTP-TLS-ERROR (got ~A)" res))
          (ok (%tls-client-hello-p (%pr-raw record)))
          (ok (string= "" (%pr-inside record))
              (format nil "nothing sent inside TLS either (~S)" (%pr-inside record)))
          (ok (null (%leaks (%pr-raw record)))))))
    (testing (format nil "~A: certificate from an untrusted CA" url)
      (multiple-value-bind (port thread record listener) (%start-tls-proxy)
        (let* ((res (first (%https-proxy-get (list url) port :verify t)))
               (record (%finish-tls-proxy thread record listener)))
          (ok (typep res 'http-tls-error) (format nil "HTTP-TLS-ERROR (got ~A)" res))
          (ok (string= "" (%pr-inside record)))
          (ok (null (%leaks (%pr-raw record)))))))))

(deftest https-proxy-tunnel-is-pooled
  "A kept-alive connection through the tunnel (TLS inside TLS) is pooled and
   serves the next request: the fixture accepts one connection only."
  (let ((http-protocol:*default-connection-pool* nil))
    (multiple-value-bind (port thread record listener) (%start-tls-proxy :requests 2)
      (let* ((pool (make-lru-connection-pool :max-size 4))
             (results (%https-proxy-get '("https://origin.test/a" "https://origin.test/b")
                                        port :pool pool))
             (record (progn (pool-clear pool)
                            (%finish-tls-proxy thread record listener))))
        (ok (= 2 (length results)) (format nil "two responses (~A)" results))
        (ok (every #'%ok-response-p results))
        (ok (equal '("GET /a" "GET /b")
                   (mapcar (lambda (head) (subseq head 0 (min 6 (length head))))
                           (%pr-origin record)))
            (format nil "both requests on one tunnel (~S)" (%pr-origin record)))
        (ok (null (%leaks (%pr-raw record))))))))
