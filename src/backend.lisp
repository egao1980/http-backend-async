(in-package #:http-backend-async)

;;; Async http-protocol backend. One HTTP backend × N event-protocol loops.
;;; Carrier disposition: rejected (hard-wires cl-async/libuv). Thin rewrite here.
;;; Nonblocking TCP connect + register-io; HTTPS via cl+ssl socket-BIO pumped
;;; with WANT_READ/WANT_WRITE (no blocking ensure-ssl-funcall on the loop).

(defvar *event-backend-maker* nil
  "Thunk → EVENT-BACKEND. Tests/CI bind this to libuv or libev maker.")

(defclass async-backend (http-backend)
  ()
  (:default-initargs :name "async"))

(defun make-async-backend ()
  "Load chipz encoding (+ soft br/zstd/TLS) and return ASYNC-BACKEND."
  (asdf:load-system "http-encoding-chipz")
  (ignore-errors (asdf:load-system "http-encoding-brotli"))
  (ignore-errors (asdf:load-system "http-encoding-zstd"))
  (ignore-errors (ensure-tls))
  (make-instance 'async-backend))

(defclass async-request-handle ()
  ((canceled-p :initform nil :accessor async-request-canceled-p)
   (io-handle :initform nil :accessor async-request-io-handle)
   (timer-handle :initform nil :accessor async-request-timer-handle)
   (socket :initform nil :accessor async-request-socket)
   (tls-stream :initform nil :accessor async-request-tls-stream) ; tls-session
   (event-backend :initarg :event-backend :reader async-request-event-backend)
   (event-loop :initarg :event-loop :reader async-request-event-loop)))

(defmethod cancel-request ((backend async-backend) (handle async-request-handle))
  (unless (async-request-canceled-p handle)
    (setf (async-request-canceled-p handle) t)
    (let ((eb (async-request-event-backend handle)))
      (when-let ((io (async-request-io-handle handle)))
        (ignore-errors (cancel eb io)))
      (when-let ((tm (async-request-timer-handle handle)))
        (ignore-errors (cancel eb tm))))
    (tls-close (async-request-tls-stream handle))
    (setf (async-request-tls-stream handle) nil)
    (close-socket (async-request-socket handle)))
  handle)

(defun %ensure-event-context ()
  "Return (values event-backend event-loop). Prefer dynamics; else make from *event-backend-maker*."
  (let ((eb event-protocol:*event-backend*)
        (el event-protocol:*event-loop*))
    (cond
      ((and eb el) (values eb el))
      (t
       (unless *event-backend-maker*
         (error 'http-error
                :message "No event loop: bind event-protocol:*event-backend*/*event-loop* or *event-backend-maker*"))
       (let* ((backend (funcall *event-backend-maker*))
              (loop (make-event-loop backend)))
         (values backend loop))))))

(defun %timeout-seconds (request client)
  (timeout-total-seconds (effective-timeout request client)))

(defmethod send-async ((backend async-backend) client request
                       &key callback error-callback)
  (let* ((cb (or callback (lambda (r) (declare (ignore r)))))
         (eb-cb (or error-callback
                    (lambda (c) (error c))))
         (want-stream-p (http-request-want-stream request))
         (uri (quri:uri (http-request-url request)))
         (method (http-request-method request))
         (max-redirects (or (http-request-max-redirects request)
                            (http-client-max-redirects client)
                            5))
         (redirect-hops 0)
         (history nil)
         (proxy-cfg (effective-proxy-config request client))
         (proxy-url (resolve-proxy proxy-cfg uri))
         (pool (effective-connection-pool client)))
    (multiple-value-bind (host port scheme) (%uri-host-port uri)
      (multiple-value-bind (event-backend event-loop) (%ensure-event-context)
        (let* ((https (string-equal scheme "https"))
               (verify (http-client-verify client))
               (handle (make-instance 'async-request-handle
                                      :event-backend event-backend
                                      :event-loop event-loop))
               (sock nil)
               (fd nil)
               (tls nil)
               (io-dir nil)
               (cookie-jar (resolve-cookie-jar client request
                                               :url (http-request-url request)))
               (ae (%accept-encoding-header
                    (http-request-accept-encoding request)))
               (headers nil)
               (body-octets #())
               (body-stream-src nil)
               (chunked-p nil)
               (stream-body-p nil) ; stream upload (chunked or content-length)
               (chunk-frame nil)
               (body-read-buf (make-array *http-stream-buffer-size*
                                          :element-type '(unsigned-byte 8)))
               (phase :connect)
               (wpos 0)
               (req-octets nil)
               (recv-buf (make-array 65536 :element-type '(unsigned-byte 8)))
               (http nil)
               (hdrs nil)
               (body nil)
               (finishedp nil)
               (parse! nil)
               (set-body-fn nil)
               (body-feed nil)
               (streaming-final-p nil)
               (headers-delivered-p nil)
               (read-paused-p nil)
               (pool-key* (pool-key scheme host port :proxy proxy-url))
               (connect-host host)
               (connect-port port)
               (proxied-http-p nil)
               (keep-alive-p (not (null pool))))
          (declare (ignorable pool-key*))
          (when proxy-url
            (when (eq proxy-url :system)
              (error 'unsupported-operation
                     :operation :system-proxy
                     :message
                     "OS automatic proxy (:SYSTEM) not yet wired on async-backend"))
            (multiple-value-bind (pscheme phost pport)
                (parse-proxy-uri proxy-url)
              (declare (ignore pscheme))
              (when https
                (error 'unsupported-operation
                       :operation :https-proxy
                       :message
                       "HTTPS via proxy (CONNECT) not yet wired on async-backend"))
              (unless (member (string-downcase
                               (or (quri:uri-scheme (quri:uri proxy-url)) "http"))
                              '("http" "https") :test #'string=)
                (error 'unsupported-operation
                       :operation :socks-proxy
                       :message "SOCKS proxy not yet wired on async-backend"))
              (setf connect-host phost
                    connect-port pport
                    proxied-http-p t)))
          (labels
              ((build-headers-and-body ()
                 (setf headers (%merge-headers (http-client-headers client)
                                               (http-request-headers request)))
                 (setf headers (inject-auth-range-headers
                                headers
                                :auth (effective-auth client request)
                                :range (http-request-range request)))
                 (when ae
                   (setf headers (acons "accept-encoding" ae
                                        (remove "accept-encoding" headers
                                                :key #'car :test #'string-equal))))
                 (unless (assoc "host" headers :test #'string-equal)
                   (setf headers
                         (acons "host"
                                (if (or (and (string-equal scheme "http") (= port 80))
                                        (and (string-equal scheme "https") (= port 443)))
                                    host
                                    (format nil "~A:~A" host port))
                                headers)))
                 (setf headers (inject-cookie-header headers cookie-jar
                                                     (quri:render-uri uri)))
                 (multiple-value-bind (content extra-headers content-length)
                     (%prepare-body request)
                   (dolist (pair extra-headers)
                     (setf headers (acons (car pair) (cdr pair)
                                          (remove (car pair) headers
                                                  :key #'car :test #'string-equal))))
                   (cond
                     ((streamp content)
                      (setf body-stream-src content
                            body-octets #()
                            stream-body-p t
                            chunked-p (null content-length)
                            headers (if content-length
                                        (acons "content-length"
                                               (princ-to-string content-length)
                                               (remove "content-length" headers
                                                       :key #'car :test #'string-equal))
                                        (remove "content-length" headers
                                                :key #'car :test #'string-equal))
                            headers (if chunked-p
                                        (if (assoc "transfer-encoding" headers
                                                   :test #'string-equal)
                                            headers
                                            (acons "transfer-encoding" "chunked"
                                                   headers))
                                        headers)
                            req-octets (build-request-header-octets
                                        method uri headers
                                        :chunked-p chunked-p
                                        :content-length content-length
                                        :absolute-p proxied-http-p
                                        :keep-alive keep-alive-p)
                            wpos 0
                            chunk-frame nil))
                     (t
                      (setf body-stream-src nil
                            body-octets (or content #())
                            stream-body-p nil
                            chunked-p nil
                            req-octets (build-request-octets
                                        method uri headers body-octets
                                        :absolute-p proxied-http-p
                                        :keep-alive keep-alive-p)
                            wpos 0
                            chunk-frame nil)))))
               (%write-octets (octets from to)
                 "Write OCTETS[FROM:TO]. Returns (values new-pos done-p want)."
                 (multiple-value-bind (n want)
                     (if https
                         (tls-write-octets tls octets from to)
                         (values (socket-send-octets sock octets from to) nil))
                   (cond
                     ((null n) (values from nil want))
                     ((zerop n) (values from nil nil))
                     (t
                      (let ((pos (+ from n)))
                        (values pos (>= pos to) nil))))))
               (%load-next-chunk ()
                 "Fill CHUNK-FRAME from BODY-STREAM-SRC. Terminator when EOF (chunked)."
                 (let ((n (read-sequence body-read-buf body-stream-src)))
                   (setf chunk-frame
                         (cond
                           ((plusp n)
                            (if chunked-p
                                (make-chunk-frame body-read-buf :end n)
                                (subseq body-read-buf 0 n)))
                           (chunked-p +chunked-terminator+)
                           (t nil))
                         wpos 0)
                   n))
               (unpause-read ()
                 (when read-paused-p
                   (setf read-paused-p nil)
                   (next-tick
                    (lambda ()
                      (unless (or (async-request-canceled-p handle)
                                  (eq phase :reconnect))
                        (arm-io :read)
                        (do-read))))))
               (complete-request ()
                 "Stop IO/timer and close socket (after full response or stream EOF)."
                 (unless (async-request-canceled-p handle)
                   (setf (async-request-canceled-p handle) t)
                   (when-let ((io (async-request-io-handle handle)))
                     (ignore-errors (cancel event-backend io)))
                   (when-let ((tm (async-request-timer-handle handle)))
                     (ignore-errors (cancel event-backend tm)))
                   (close-connection)))
               (will-follow-redirect-p (status headers*)
                 (let ((location (gethash "location" headers*)))
                   (and location
                        (redirect-status-p status)
                        (plusp max-redirects)
                        (< redirect-hops max-redirects))))
               (begin-stream-body (status headers*)
                 "Switch parser body sink to ASYNC-BODY-INPUT-STREAM and deliver."
                 (let* ((final-url (quri:render-uri uri))
                        (set-cookies (merge-response-cookies
                                      cookie-jar final-url headers*)))
                   (setf body-feed
                         (make-async-body-input-stream
                          :on-space #'unpause-read)
                         streaming-final-p t)
                   (funcall set-body-fn
                            (lambda (data start end)
                              (async-body-feed body-feed data
                                               :start start :end end)
                              (when (async-body-full-p body-feed)
                                (setf read-paused-p t)
                                (when-let ((io (async-request-io-handle handle)))
                                  (ignore-errors (cancel event-backend io))
                                  (setf (async-request-io-handle handle) nil
                                        io-dir nil)))))
                   (multiple-value-bind (app-stream headers**)
                       (apply-response-content-encoding
                        body-feed headers*
                        :decompress (http-request-decompress request))
                     (deliver-stream
                      (make-hop-response
                       status headers** app-stream set-cookies final-url
                       :history-for-final (nreverse (copy-list history)))))))
               (on-response-headers (http-obj headers*)
                 (declare (ignore http-obj))
                 (when (and want-stream-p
                            (not streaming-final-p)
                            (not (will-follow-redirect-p
                                  (fast-http:http-status http) headers*)))
                   (begin-stream-body (fast-http:http-status http) headers*)))
               (reset-parser ()
                 (multiple-value-bind (h hd b f p set-body)
                     (make-response-accumulator
                      :on-headers #'on-response-headers)
                   (setf http h hdrs hd body b finishedp f parse! p
                         set-body-fn set-body
                         body-feed nil
                         streaming-final-p nil
                         read-paused-p nil)))
               (close-connection ()
                 (when-let ((io (async-request-io-handle handle)))
                   (ignore-errors (cancel event-backend io))
                   (setf (async-request-io-handle handle) nil))
                 (tls-close tls)
                 (setf tls nil
                       (async-request-tls-stream handle) nil)
                 (close-socket sock)
                 (setf (async-request-socket handle) nil
                       sock nil
                       fd nil
                       io-dir nil))
               (next-tick (fn)
                 ;; Prefer sleep* 0 over defer/idle: libev idle is starved while a
                 ;; socket remains writable under :read-write interest.
                 (sleep* event-backend event-loop 0 :callback fn))
               (arm-io (direction)
                 "Register FD interest. Re-arm via next-tick after cancel (same FD)."
                 (unless fd
                   (error 'http-connection-error :message "arm-io before connect"))
                 (when (and (async-request-io-handle handle) (eq io-dir direction))
                   (return-from arm-io nil))
                 (let ((old (async-request-io-handle handle)))
                   (setf (async-request-io-handle handle) nil
                         io-dir nil)
                   (flet ((register ()
                            (when (and fd
                                       (not (async-request-canceled-p handle))
                                       (not (eq phase :reconnect)))
                              (setf io-dir direction
                                    (async-request-io-handle handle)
                                    (register-io event-backend event-loop fd
                                                 direction #'on-io)))))
                     (if old
                         (progn
                           (ignore-errors (cancel event-backend old))
                           (next-tick #'register))
                         (register)))))
               (do-connect ()
                 (handler-case
                     (multiple-value-bind (usock status)
                         (tcp-connect-nb connect-host connect-port)
                       (setf sock usock
                             (async-request-socket handle) sock
                             fd (socket-fd sock)
                             phase :connecting)
                       (ecase status
                         (:connected (on-connected))
                         (:pending (arm-io :write))))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition 'http-connection-error
                                           :message (princ-to-string e))))))
               (fail (condition)
                 (when body-feed
                   (async-body-fail body-feed condition))
                 (unless (async-request-canceled-p handle)
                   (complete-request)
                   (unless headers-delivered-p
                     (handler-case (funcall eb-cb condition)
                       (error (e) (warn "error-callback failed: ~A" e))))))
               (succeed (res)
                 "Non-streaming completion: deliver response and tear down."
                 (unless (async-request-canceled-p handle)
                   (complete-request)
                   (handler-case (funcall cb res)
                     (error (e) (warn "callback failed: ~A" e)))))
               (deliver-stream (res)
                 "Streaming: deliver headers+body stream; keep socket feeding."
                 (unless headers-delivered-p
                   (setf headers-delivered-p t)
                   (handler-case (funcall cb res)
                     (error (e) (warn "callback failed: ~A" e)))))
               (make-hop-response (status headers* body* set-cookies final-url
                                    &key (history-for-final nil))
                 (make-instance 'http-response
                                :status status
                                :headers headers*
                                :body body*
                                :url final-url
                                :cookies set-cookies
                                :history history-for-final
                                :http-version
                                (format nil "HTTP/~A"
                                        (fast-http:http-version http))
                                :request request))
               (follow-redirect (status headers* body* set-cookies final-url)
                 (let ((location (gethash "location" headers*)))
                   (cond
                     ((or (null location) (not (redirect-status-p status)))
                      (succeed (make-hop-response
                                status headers* body* set-cookies final-url
                                :history-for-final (nreverse history))))
                     ((zerop max-redirects)
                      (succeed (make-hop-response
                                status headers* body* set-cookies final-url
                                :history-for-final (nreverse history))))
                     ((>= redirect-hops max-redirects)
                      (fail (make-condition
                             'http-redirect-error
                             :message (format nil "exceeded max-redirects (~D)"
                                              max-redirects))))
                     (t
                      (push (make-hop-response status headers* body* set-cookies
                                               final-url)
                            history)
                      (incf redirect-hops)
                      (handler-case
                          (let ((next (resolve-redirect-uri uri location)))
                            (when (and chunked-p
                                       (redirect-preserves-method-p status))
                              (error 'http-protocol-error
                                     :message
                                     "cannot replay streamed request body on redirect"))
                            (multiple-value-bind (m u h b ho po https?)
                                (prepare-redirect-hop status next method body-octets
                                                      headers cookie-jar ae)
                              (setf method m
                                    uri u
                                    headers h
                                    body-octets b
                                    body-stream-src nil
                                    stream-body-p nil
                                    chunked-p nil
                                    chunk-frame nil
                                    host ho
                                    port po
                                    https https?
                                    scheme (if https? "https" "http")
                                    req-octets (build-request-octets
                                                method uri headers body-octets
                                                :absolute-p proxied-http-p
                                                :keep-alive keep-alive-p)
                                    wpos 0
                                    phase :reconnect)
                              (reset-parser)
                              (next-tick
                               (lambda ()
                                 (unless (async-request-canceled-p handle)
                                   (close-connection)
                                   (setf io-dir nil)
                                   (do-connect))))))
                        (http-error (e) (fail e))
                        (error (e)
                          (fail (make-condition 'http-redirect-error
                                                :message (princ-to-string e)))))))))
               (finish-response ()
                 (cond
                   (streaming-final-p
                    (when body-feed
                      (async-body-eof body-feed))
                    (complete-request))
                   (t
                    (let* ((final-url (quri:render-uri uri))
                           (set-cookies (merge-response-cookies
                                         cookie-jar final-url hdrs))
                           (status (fast-http:http-status http)))
                      (multiple-value-bind (body* headers*)
                          (apply-response-content-encoding
                           (coerce body '(simple-array (unsigned-byte 8) (*)))
                           hdrs
                           :decompress (http-request-decompress request))
                        (follow-redirect status headers* body* set-cookies
                                         final-url))))))
               (on-connected ()
                 (set-socket-nonblocking sock t)
                 (if https
                     (progn
                       (setf tls (make-tls-session fd host :verify verify)
                             (async-request-tls-stream handle) tls
                             phase :tls-hs)
                       (arm-io :write)
                       (next-tick (lambda () (on-io :ok))))
                     (progn
                       (setf phase :write)
                       (arm-io :write)
                       (next-tick (lambda () (on-io :ok))))))
               (do-tls-hs ()
                 (loop
                   (ecase (tls-handshake-step tls)
                     (:done
                      (setf phase :write)
                      (arm-io :write)
                      (return (do-write)))
                     (:want-read
                      (arm-io :read)
                      (return))
                     (:want-write
                      (arm-io :write)
                      (return)))))
               (arm-tls-want (want)
                 "Map TLS WANT_* to a single register-io direction (never :read-write)."
                 (arm-io (ecase want
                           (:want-read :read)
                           (:want-write :write))))
               (do-write ()
                 "Write headers (+ optional body vector), or streamed body."
                 (loop
                   (cond
                     ;; 1) headers / full materialized request
                     ((and req-octets (< wpos (length req-octets)))
                      (multiple-value-bind (pos done want)
                          (%write-octets req-octets wpos (length req-octets))
                        (setf wpos pos)
                        (cond
                          (want (arm-tls-want want) (return))
                          ((not done) (return))
                          ((not stream-body-p)
                           (setf phase :read
                                 req-octets nil)
                           (arm-io :read)
                           (return))
                          (t
                           (setf req-octets nil)
                           (%load-next-chunk)
                           (unless chunk-frame
                             (setf phase :read)
                             (arm-io :read)
                             (return))))))
                     ;; 2) stream body (chunked frames or raw length-delimited)
                     (stream-body-p
                      (unless chunk-frame
                        (%load-next-chunk)
                        (unless chunk-frame
                          (setf phase :read)
                          (arm-io :read)
                          (return)))
                      (multiple-value-bind (pos done want)
                          (%write-octets chunk-frame wpos (length chunk-frame))
                        (setf wpos pos)
                        (cond
                          (want (arm-tls-want want) (return))
                          ((not done) (return))
                          ((eq chunk-frame +chunked-terminator+)
                           (setf phase :read
                                 chunk-frame nil)
                           (arm-io :read)
                           (return))
                          (t
                           (%load-next-chunk)
                           (unless chunk-frame
                             (setf phase :read)
                             (arm-io :read)
                             (return))))))
                     (t
                      (setf phase :read)
                      (arm-io :read)
                      (return)))))
               (do-read ()
                 (when read-paused-p
                   (return-from do-read nil))
                 (loop
                   (when read-paused-p
                     (return))
                   (multiple-value-bind (n want)
                       (if https
                           (tls-read-octets tls recv-buf)
                           (values (socket-recv-octets sock recv-buf) nil))
                     (cond
                       ((null n)
                        (when want (arm-tls-want want))
                        (return))
                       ((zerop n)
                        (funcall parse! #())
                        (if (funcall finishedp)
                            (finish-response)
                            (fail (make-condition
                                   'http-protocol-error
                                   :message "incomplete HTTP response")))
                        (return))
                       (t
                        (when (funcall parse! recv-buf :end n)
                          (finish-response)
                          (return))
                        (when read-paused-p
                          (return)))))))
               (on-io (status)
                 (when (or (async-request-canceled-p handle)
                           (eq phase :reconnect))
                   (return-from on-io nil))
                 (when (eq status :error)
                   (return-from on-io
                     (fail (make-condition 'http-connection-error
                                           :message "register-io error"))))
                 (handler-case
                     (ecase phase
                       (:connecting
                        (when (tcp-connect-finish sock)
                          (on-connected)))
                       (:tls-hs (do-tls-hs))
                       (:write (do-write))
                       (:read (do-read)))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition
                            (if https 'http-tls-error 'http-connection-error)
                            :message (princ-to-string e)))))))
            (build-headers-and-body)
            (reset-parser)
            (with-event-backend (event-backend)
              (with-event-loop-var (event-loop)
                (setf (async-request-timer-handle handle)
                      (sleep* event-backend event-loop
                              (%timeout-seconds request client)
                              :callback
                              (lambda ()
                                (fail (make-condition
                                       'http-timeout-error
                                       :message "request timed out")))))
                (defer event-backend event-loop #'do-connect)))
            handle))))))
