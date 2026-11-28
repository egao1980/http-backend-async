(in-package #:http-backend-async/tests)

;;; #89: a response with neither Content-Length nor chunked framing ends when
;;; the server closes the connection (RFC 9112 §6.3 rule 8). Its body was cut
;;; at the first pause between writes: fast-http calls the message complete
;;; at the end of every read of such a body. A lull is not the end; only the
;;; peer's FIN is (or a timeout, which must fail, never truncate).

(defparameter +close-delimited-events+
  '("data: w0" "data: w1" "data: w2" "data: w3" "data: w4" "data: [DONE]"))

(defun %close-delimited-text ()
  (format nil "~{~A~%~%~}" +close-delimited-events+))

(defun %close-delimited-serve (stream &key (version "1.0") (gap 0.2) (head-gap 0)
                                      (stall nil) (content-length nil))
  "Answer one request with a close-delimited text/event-stream body: VERSION's
   status line, no Content-Length, no chunking, no Connection header. HEAD-GAP
   seconds after the header block, then each event, GAP seconds apart. STALL:
   seconds of silence after the second event, then hang up. CONTENT-LENGTH:
   claim that many octets instead (more than are sent: a truncated body)."
  (when (%drip-read-request stream)
    (let ((crlf (format nil "~C~C" #\Return #\Newline)))
      (%gate-write stream
                   (concatenate 'string
                                (format nil "HTTP/~A 200 OK~A" version crlf)
                                (format nil "Content-Type: text/event-stream~A" crlf)
                                (if content-length
                                    (format nil "Content-Length: ~D~A" content-length crlf)
                                    "")
                                crlf)))
    (sleep head-gap)
    (loop for event in +close-delimited-events+
          for i from 0
          do (%gate-write stream (format nil "~A~%~%" event))
             (when (and stall (= i 1))
               (sleep stall)
               (return))
             (sleep gap))))

(defvar *close-delimited-accepts* 0)

(defun %join-or-destroy (thread &key (seconds 5))
  "Join THREAD within SECONDS; past that destroy it (a connection thread
   still waiting for a request the client never sends)."
  (loop repeat (* seconds 20)
        while (bt:thread-alive-p thread)
        do (sleep 0.05))
  (if (bt:thread-alive-p thread)
      (ignore-errors (bt:destroy-thread thread))
      (ignore-errors (bt:join-thread thread))))

(defun call-with-close-delimited-origin (fn &key tls (close-notify t) serve-args)
  "Serve close-delimited answers on a loopback listener; call FN with its URL.
   TLS: https with tests/certs/origin.crt. CLOSE-NOTIFY NIL: the server drops
   the TCP connection without a TLS close_notify alert. The acceptor and every
   connection thread are joined before returning (#91)."
  (setf *close-delimited-accepts* 0)
  (let* ((server (usocket:socket-listen "127.0.0.1" 0 :reuseaddress t
                                                      :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server))
         (lock (bt:make-lock "close-delimited-origin"))
         (conns '())
         (thread
           (bt:make-thread
            (lambda ()
              (loop
                (handler-case
                    (let ((c (usocket:socket-accept server
                                                    :element-type '(unsigned-byte 8))))
                      (incf *close-delimited-accepts*)
                      (bt:with-lock-held (lock)
                        (push
                         (bt:make-thread
                          (lambda ()
                            (unwind-protect
                                (ignore-errors
                                  (if tls
                                      (let ((s (cl+ssl:make-ssl-server-stream
                                                (usocket:socket-stream c)
                                                :certificate (%test-cert "origin.crt")
                                                :key (%test-cert "origin.key"))))
                                        (apply #'%close-delimited-serve s serve-args)
                                        (close s :abort (not close-notify)))
                                      (apply #'%close-delimited-serve
                                             (usocket:socket-stream c) serve-args)))
                              (ignore-errors (usocket:socket-close c))))
                          :name "close-delimited-conn")
                         conns)))
                  (error () (return)))))
            :name "close-delimited-origin")))
    (unwind-protect
        (funcall fn (format nil "~A://127.0.0.1:~D/sse" (if tls "https" "http") port))
      (ignore-errors (usocket:socket-close server))
      (when (bt:thread-alive-p thread)
        ;; Closing the listener does not wake a blocked accept() on Linux.
        (ignore-errors (bt:destroy-thread thread)))
      (dolist (c (bt:with-lock-held (lock) conns))
        (%join-or-destroy c)))))

(defmacro with-close-delimited-origin ((url-var &rest options) &body body)
  `(call-with-close-delimited-origin (lambda (,url-var) ,@body) ,@options))

(defun %close-delimited-get (url &key want-stream (timeout '(:connect 5 :read 5)))
  "Blocking GET with no event loop bound, TLS unverified (a loopback test
   certificate). → the response; a live body is read to EOF into it."
  (let* ((client (make-http-client *http-backend* :verify nil))
         (res (let ((event-protocol:*event-backend* nil)
                    (event-protocol:*event-loop* nil))
                (http:request :get url :timeout timeout :want-stream want-stream
                                       :client client)))
         (body (response-body res)))
    (if (streamp body)
        (make-instance 'http-response
                       :status (response-status res)
                       :headers (response-headers res)
                       :body (unwind-protect (http-backend-async::slurp-octets
                                              (body-stream res))
                               (close body))
                       :url (response-url res)
                       :request (response-request res))
        res)))

(deftest-parametrize close-delimited-body-survives-pauses
    ((version want-stream head-gap)
     ("1.0" nil 0)
     ("1.0" t 0)
     ("1.1" nil 0)
     ("1.1" t 0)
     ;; the header block arrives alone, the body in a later read
     ("1.0" nil 0.2)
     ("1.0" t 0.2))
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (let ((*http-backend* hb))
      (with-close-delimited-origin
          (url :serve-args (list :version version :gap 0.2 :head-gap head-gap))
        (let ((res (%close-delimited-get url :want-stream want-stream)))
          (ok (= 200 (response-status res)))
          (ok (equal (%close-delimited-text)
                     (babel:octets-to-string (response-body res) :encoding :utf-8))
              (format nil "HTTP/~A want-stream=~A: ~S" version want-stream
                      (babel:octets-to-string (response-body res)
                                              :encoding :utf-8 :errorp nil))))))))

(deftest close-delimited-events-arrive-live
  ;; Read as a streaming SSE consumer does: every event, one at a time, as
  ;; the server writes it.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-close-delimited-origin (url :serve-args (list :gap 0.2))
      (let* ((res (%sync-live url '(:connect 5 :read 5)))
             (s (body-stream res))
             (events '()))
        (unwind-protect
             (loop for line = (%read-lf-line s)
                   while line
                   when (plusp (length line)) do (push line events))
          (close (response-body res)))
        (ok (equal +close-delimited-events+ (nreverse events))
            (format nil "~S" events))))))

(deftest-parametrize close-delimited-pause-past-read-timeout-fails
    ((want-stream)
     (nil)
     (t))
  ;; Two events, then 4 s of silence under a 1 s read timeout: a timeout,
  ;; never a short body.
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (let ((*http-backend* hb))
      (with-close-delimited-origin (url :serve-args (list :gap 0.1 :stall 4))
        (multiple-value-bind (c secs)
            (%timeout-condition
             (lambda () (%close-delimited-get url :want-stream want-stream
                                                  :timeout '(:connect 5 :read 1))))
          (ok (typep c 'http-timeout-error) (format nil "~A" c))
          (ok (< 0.9 secs 3.5) (format nil "failed after ~,2F s" secs)))))))

(defclass %counting-pool (http-backend-async::lru-connection-pool)
  ((releases :initform 0 :accessor %pool-releases)))

(defmethod pool-release :before ((pool %counting-pool) key connection &key on-evict)
  (declare (ignore key connection on-evict))
  (incf (%pool-releases pool)))

(deftest-parametrize close-delimited-response-is-never-pooled
    ((version want-stream)
     ("1.1" nil)
     ("1.1" t)
     ("1.0" nil))
  ;; HTTP/1.1 without Connection: close would otherwise read as keep-alive.
  (let ((http-protocol:*default-connection-pool* nil))
    (with-async-test (eb el hb)
      (declare (ignore eb el))
      (with-close-delimited-origin (url :serve-args (list :version version :gap 0.1))
        (let* ((pool (make-instance '%counting-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (dotimes (i 2)
            (let* ((res (let ((event-protocol:*event-backend* nil)
                              (event-protocol:*event-loop* nil))
                          (http:request :get url :timeout '(:connect 5 :read 5)
                                                 :want-stream want-stream
                                                 :client client)))
                   (body (response-body res))
                   (octets (if (streamp body)
                               (unwind-protect (http-backend-async::slurp-octets
                                                (body-stream res))
                                 (close body))
                               body)))
              (ok (equal (%close-delimited-text)
                         (babel:octets-to-string octets :encoding :utf-8)))))
          (ok (%wait-until (lambda () (= 2 *close-delimited-accepts*)) 2)
              (format nil "~D connections" *close-delimited-accepts*))
          (ok (zerop (%pool-releases pool))
              (format nil "~D connections went back to the pool"
                      (%pool-releases pool)))
          (pool-clear pool))))))

(deftest-parametrize close-delimited-https-body-survives-pauses
    ((close-notify want-stream)
     (t nil)
     (t t)
     ;; Like Python requests / urllib3 (ssl suppress_ragged_eofs): a TCP close
     ;; without close_notify ends a close-delimited body. A framed body still
     ;; fails when it is cut short (http-incomplete-response).
     (nil nil)
     (nil t))
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (let ((*http-backend* hb))
      (with-close-delimited-origin (url :tls t :close-notify close-notify
                                        :serve-args (list :gap 0.2))
        (let ((res (handler-case (%close-delimited-get url :want-stream want-stream)
                     (error (e) e))))
          (ok (typep res 'http-response) (format nil "~A" res))
          (when (typep res 'http-response)
            (ok (equal (%close-delimited-text)
                       (babel:octets-to-string (response-body res) :encoding :utf-8))
                (format nil "close_notify=~A want-stream=~A" close-notify
                        want-stream))))))))

(deftest-parametrize https-truncated-framed-body-still-fails
    ((want-stream)
     (nil)
     (t))
  ;; A TCP close without close_notify is EOF, but a Content-Length body that
  ;; it cuts short is still an incomplete response: not a short success, and
  ;; not the TLS error it was before #89.
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (let ((*http-backend* hb))
      (with-close-delimited-origin (url :tls t :close-notify nil
                                        :serve-args (list :gap 0.05
                                                          :content-length 4096))
        (let ((res (handler-case (%close-delimited-get url :want-stream want-stream)
                     (error (e) e))))
          (ok (typep res 'http-incomplete-response)
              (format nil "~S: ~A" (type-of res) res)))))))
