(in-package #:http-backend-async)

;;; Async http-protocol backend. One HTTP backend × N event-protocol loops.
;;; Carrier disposition: rejected (hard-wires cl-async/libuv). Thin rewrite here.
;;; Nonblocking TCP connect + register-io; HTTPS via cl+ssl socket-BIO pumped
;;; with WANT_READ/WANT_WRITE (no blocking ensure-ssl-funcall on the loop).

(defvar *event-backend-maker* nil
  "Thunk → EVENT-BACKEND. Tests/CI bind this to libuv or libev maker.")

(defclass async-backend (http-backend ws-backend)
  ()
  (:default-initargs :name "async"))

(defmethod backend-http-versions ((backend async-backend))
  "RFC 9112 + RFC 9113 (ALPN h2). Cleartext h2c = not yet."
  (declare (ignore backend))
  '(:http/1.1 :http/2))

(defun make-async-backend ()
  "Load chipz encoding (+ soft br/zstd/TLS) and return ASYNC-BACKEND."
  (asdf:load-system "http-encoding-chipz")
  (ignore-errors (asdf:load-system "http-encoding-brotli"))
  (ignore-errors (asdf:load-system "http-encoding-zstd"))
  (ignore-errors (ensure-tls))
  (make-instance 'async-backend))

(defclass async-pooled-connection ()
  ((socket :initarg :socket :accessor async-conn-socket)
   (tls :initarg :tls :accessor async-conn-tls :initform nil)
   (https :initarg :https :accessor async-conn-https-p :initform nil)
   (alive :initform t :accessor async-conn-alive-p)))

(defun make-async-pooled-connection (socket &key tls https)
  (make-instance 'async-pooled-connection
                 :socket socket :tls tls :https https))

(defmethod connection-alive-p ((c async-pooled-connection))
  "Alive if we still hold a usocket. Avoid probing the Lisp stream — on Windows
   stream/socket-receive mixing breaks keep-alive reuse (INVALID-VERSION)."
  (and (async-conn-alive-p c)
       (async-conn-socket c)
       (ignore-errors (usocket:socket (async-conn-socket c)))))

(defmethod pool-discard ((pool lru-connection-pool) (c async-pooled-connection))
  (declare (ignore pool))
  (setf (async-conn-alive-p c) nil)
  (tls-close (async-conn-tls c))
  (setf (async-conn-tls c) nil)
  (close-socket (async-conn-socket c))
  (setf (async-conn-socket c) nil)
  nil)

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
         ;; Usocket path (dexador-usocket): URL or NIL (direct).
         ;; SYSTEM-AUTOMATIC-P / PAC/WPAD is WinHTTP-only — ignored here.
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
               (http-connect-p nil) ; HTTPS via HTTP proxy → CONNECT then TLS
               (proxy-user nil)
               (proxy-pass nil)
               (connect-step nil)
               (connect-out nil)
               (connect-wpos 0)
               (connect-in (make-array 0 :element-type '(unsigned-byte 8)
                                         :adjustable t :fill-pointer 0))
               (socks-p nil)
               (socks-user nil)
               (socks-pass nil)
               (socks-remote-dns nil)
               (socks-step nil)
               (socks-out nil)
               (socks-wpos 0)
               (socks-in (make-array 0 :element-type '(unsigned-byte 8)
                                       :adjustable t :fill-pointer 0))
               ;; Pool present → advertise keep-alive; release only when the
               ;; response also allows reuse (fixture often sends Connection: close).
               (keep-alive-p (not (null pool)))
               (reuse-ok-p nil)
               (from-pool-p nil)
               (version-pref (effective-http-version client request))
               (negotiated-version :http/1.1)
               (h2-pump nil)
               (h2-session nil)
               (h2-out nil)
               (h2-out-pos 0)
               ;; Completed H2 stream waiting for outbound ACK/WINDOW_UPDATE flush.
               (h2-done-stream nil)
               (h2-req-stream nil)
               (h2-body-buf (make-array 0 :element-type '(unsigned-byte 8)
                                          :adjustable t :fill-pointer 0))
               (h2-callback-finished-p nil)
               (h2-req-end-p nil)
               (streamed-response nil))
          (when proxy-url
            (multiple-value-bind (pscheme phost pport puser ppass)
                (parse-proxy-uri proxy-url)
              (ecase (proxy-kind proxy-url)
                (:http
                 ;; dexador: cleartext → absolute-form; https → CONNECT then TLS
                 (setf connect-host phost
                       connect-port pport
                       proxy-user puser
                       proxy-pass ppass
                       http-connect-p https
                       proxied-http-p (not https)))
                (:socks5
                 (setf connect-host phost
                       connect-port pport
                       socks-p t
                       socks-user puser
                       socks-pass ppass
                       socks-remote-dns (socks-remote-dns-p pscheme)
                       proxied-http-p nil
                       http-connect-p nil))
                (:socks4
                 (error 'unsupported-operation
                        :operation :socks4-proxy
                        :message "SOCKS4 not implemented; use socks5:// or socks5h://")))))
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
                 ;; Cleartext HTTP via proxy: Proxy-Authorization (dexador).
                 (when (and proxied-http-p
                            (not (assoc "proxy-authorization" headers
                                        :test #'string-equal)))
                   (let ((v (proxy-authorization-value proxy-user proxy-pass)))
                     (when v
                       (setf headers (acons "proxy-authorization" v headers)))))
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
               (stop-io-and-timer ()
                 (when-let ((io (async-request-io-handle handle)))
                   (ignore-errors (cancel event-backend io))
                   (setf (async-request-io-handle handle) nil))
                 (when-let ((tm (async-request-timer-handle handle)))
                   (ignore-errors (cancel event-backend tm))
                   (setf (async-request-timer-handle handle) nil))
                 (setf io-dir nil))
               (close-connection ()
                 (stop-io-and-timer)
                 (tls-close tls)
                 (setf tls nil
                       (async-request-tls-stream handle) nil)
                 (close-socket sock)
                 (setf (async-request-socket handle) nil
                       sock nil
                       fd nil
                       from-pool-p nil
                       reuse-ok-p nil))
               (detach-connection ()
                 "Hand socket/TLS to a pool entry without closing."
                 (stop-io-and-timer)
                 (let ((conn (make-async-pooled-connection
                              sock :tls tls :https https)))
                   (setf sock nil
                         tls nil
                         fd nil
                         (async-request-socket handle) nil
                         (async-request-tls-stream handle) nil
                         from-pool-p nil)
                   conn))
               (release-or-close (&key (force-close nil))
                 "Return connection to POOL when keep-alive+reuse; else close."
                 (cond
                   ((or force-close
                        (null pool)
                        (null sock)
                        (not keep-alive-p)
                        (not reuse-ok-p))
                    (close-connection))
                   (t
                    (let ((conn (detach-connection)))
                      (pool-release pool pool-key* conn)))))
               (complete-request (&key (force-close nil))
                 "Stop IO/timer; pool or close after full response / stream EOF."
                 (unless (async-request-canceled-p handle)
                   (setf (async-request-canceled-p handle) t)
                   (release-or-close :force-close force-close)))
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
                   (setf reuse-ok-p
                         (and keep-alive-p
                              (response-keeps-alive-p
                               headers* (fast-http:http-version http)))
                         body-feed
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
                                  (ignore-errors (update-io event-backend io :none))
                                  (setf io-dir :none)))))
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
                         read-paused-p nil
                         reuse-ok-p nil)))
               (adopt-pooled (conn)
                 "Reuse CONN for the next HTTP request (skip TCP/TLS/proxy)."
                 (setf sock (async-conn-socket conn)
                       tls (async-conn-tls conn)
                       https (async-conn-https-p conn)
                       (async-request-socket handle) sock
                       (async-request-tls-stream handle) tls
                       fd (socket-fd sock)
                       from-pool-p t
                       phase :write
                       io-dir nil)
                 (arm-io :write)
                 (next-tick (lambda () (on-io :ok))))
               (next-tick (fn)
                 ;; Prefer sleep* 0 over defer/idle: libev idle is starved while a
                 ;; socket remains writable under :read-write interest.
                 (sleep* event-backend event-loop 0 :callback fn))
               (arm-io (direction)
                 "Register or update FD interest in place (no cancel+re-init)."
                 (unless fd
                   (error 'http-connection-error :message "arm-io before connect"))
                 (when (or (async-request-canceled-p handle)
                           (eq phase :reconnect))
                   (return-from arm-io nil))
                 (let ((cur (async-request-io-handle handle)))
                   (cond
                     ((and cur (eq io-dir direction)) nil)
                     (cur
                      (update-io event-backend cur direction)
                      (setf io-dir direction))
                     (t
                      (setf io-dir direction
                            (async-request-io-handle handle)
                            (register-io event-backend event-loop fd
                                         direction #'on-io))))))
               (do-connect ()
                 (let ((conn (and pool (pool-acquire pool pool-key*))))
                   (cond
                     (conn
                      (adopt-pooled conn))
                     (t
                      (handler-case
                          (multiple-value-bind (usock status)
                              (tcp-connect-nb connect-host connect-port)
                            (setf sock usock
                                  (async-request-socket handle) sock
                                  fd (socket-fd sock)
                                  from-pool-p nil
                                  phase :connecting)
                            (ecase status
                              (:connected (on-connected))
                              (:pending (arm-io :write))))
                        (http-error (e) (fail e))
                        (error (e)
                          (fail (make-condition 'http-connection-error
                                                :message (princ-to-string e)))))))))
               (fail (condition)
                 (when body-feed
                   (async-body-fail body-feed condition))
                 (unless (async-request-canceled-p handle)
                   (complete-request :force-close t)
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
                                    &key (history-for-final nil) trailers)
                 (let ((res (make-instance 'http-response
                                           :status status
                                           :headers headers*
                                           :body body*
                                           :url final-url
                                           :cookies set-cookies
                                           :history history-for-final
                                           :http-version negotiated-version
                                           :request request)))
                   (when trailers
                     (let ((writer (find-symbol (string '#:response-trailers)
                                                :http-protocol)))
                       (when (and writer (fboundp writer))
                         (funcall (fdefinition `(setf ,writer)) trailers res))))
                   res))
               (follow-redirect (status headers* body* set-cookies final-url
                                  &key trailers)
                 (let ((location (gethash "location" headers*)))
                   (cond
                     ((or (null location) (not (redirect-status-p status)))
                      (setf reuse-ok-p
                            (and keep-alive-p
                                 (or (eq negotiated-version :http/2)
                                     (response-keeps-alive-p
                                      headers* (fast-http:http-version http)))))
                      (succeed (make-hop-response
                                status headers* body* set-cookies final-url
                                :history-for-final (nreverse history)
                                :trailers trailers)))
                     ((zerop max-redirects)
                      (setf reuse-ok-p
                            (and keep-alive-p
                                 (or (eq negotiated-version :http/2)
                                     (response-keeps-alive-p
                                      headers* (fast-http:http-version http)))))
                      (succeed (make-hop-response
                                status headers* body* set-cookies final-url
                                :history-for-final (nreverse history)
                                :trailers trailers)))
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
                      (setf reuse-ok-p
                            (and keep-alive-p
                                 (or (eq negotiated-version :http/2)
                                     (and http
                                          (response-keeps-alive-p
                                           headers* (fast-http:http-version http))))))
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
                                    phase :reconnect
                                    pool-key* (pool-key scheme host port
                                                        :proxy proxy-url))
                              (reset-parser)
                              (next-tick
                               (lambda ()
                                 (unless (async-request-canceled-p handle)
                                   ;; Prefer pool release over hard close when
                                   ;; the redirect hop can reuse the socket.
                                   (release-or-close)
                                   (setf (async-request-canceled-p handle) nil)
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
               (unpause-h2 ()
                 "Resume H2 reads after the body queue drains; flush WINDOW_UPDATE."
                 (setf read-paused-p nil)
                 (when h2-req-stream
                   (h2-stream-release-window h2-req-stream))
                 (when (and h2-pump (h2-pump-pending-out-p h2-pump))
                   (setf h2-out (h2-pump-take-out h2-pump)
                         h2-out-pos 0
                         phase :h2-write)
                   (arm-io :write)
                   (return-from unpause-h2))
                 (unless (async-request-canceled-p handle)
                   (arm-io :read)))
               (begin-h2-stream-body (status headers* set-cookies)
                 "Deliver headers + Gray body; DATA frames feed the queue."
                 (setf reuse-ok-p keep-alive-p
                       body-feed (make-async-body-input-stream
                                  :on-space #'unpause-h2)
                       streaming-final-p t)
                 (multiple-value-bind (app-stream headers**)
                     (apply-response-content-encoding
                      body-feed headers*
                      :decompress (http-request-decompress request))
                   (setf streamed-response
                         (make-hop-response
                          status headers** app-stream set-cookies
                          (quri:render-uri uri)
                          :history-for-final (nreverse (copy-list history))))
                   (deliver-stream streamed-response)))
               (on-h2-headers (h2-stream)
                 (multiple-value-bind (status headers*)
                     (h2-streaming-status-headers h2-stream)
                   (let ((set-cookies
                           (merge-response-cookies cookie-jar
                                                   (quri:render-uri uri)
                                                   headers*)))
                     (when (and want-stream-p
                                (not streaming-final-p)
                                (not (will-follow-redirect-p status headers*)))
                       (begin-h2-stream-body status headers* set-cookies)))))
               (on-h2-data (data start end)
                 (cond
                   (streaming-final-p
                    (async-body-feed body-feed data :start start :end end)
                    (when (and body-feed (async-body-full-p body-feed))
                      (setf read-paused-p t)
                      (when h2-req-stream
                        (setf (h2-stream-hold-window-p h2-req-stream) t))
                      (when-let ((io (async-request-io-handle handle)))
                        (ignore-errors (update-io event-backend io :none))
                        (setf io-dir :none))))
                   (t
                    (h2-buf-append h2-body-buf data start end))))
               (on-h2-end (h2-stream)
                 (setf h2-callback-finished-p t)
                 (let ((tr (h2-streaming-trailers-table h2-stream)))
                   (cond
                     (streaming-final-p
                      (when streamed-response
                        (let ((writer (find-symbol (string '#:response-trailers)
                                                   :http-protocol)))
                          (when (and writer (fboundp writer))
                            (funcall (fdefinition `(setf ,writer))
                                     tr streamed-response))))
                      (when body-feed
                        (async-body-eof body-feed))
                      (complete-request))
                     (t
                      (multiple-value-bind (status headers*)
                          (h2-streaming-status-headers h2-stream)
                        (let ((set-cookies
                                (merge-response-cookies cookie-jar
                                                        (quri:render-uri uri)
                                                        headers*))
                              (body* (copy-seq h2-body-buf)))
                          (multiple-value-bind (body* headers*)
                              (apply-response-content-encoding
                               (coerce body* '(simple-array (unsigned-byte 8) (*)))
                               headers*
                               :decompress (http-request-decompress request))
                            (setf reuse-ok-p keep-alive-p
                                  h2-done-stream nil)
                            (follow-redirect status headers* body*
                                             set-cookies
                                             (quri:render-uri uri)
                                             :trailers tr))))))))
               (%body-pipe-p (x)
                 (let ((s (find-symbol (string '#:http-body-pipe-p) :http-protocol)))
                   (and s (fboundp s) (funcall s x))))
               (%pipe-listen (x)
                 (let ((s (find-symbol (string '#:http-body-pipe-listen) :http-protocol)))
                   (and s (fboundp s) (funcall s x))))
               (%pipe-eof (x)
                 (let ((s (find-symbol (string '#:http-body-pipe-eof-p) :http-protocol)))
                   (and s (fboundp s) (funcall s x))))
               (%pipe-read (x seq start end)
                 (let ((s (find-symbol (string '#:http-body-pipe-read-available)
                                       :http-protocol)))
                   (if (and s (fboundp s))
                       (funcall s x seq start end)
                       start)))
               (%pipe-set-on-data (x fn)
                 (let ((acc (find-symbol (string '#:body-pipe-on-data) :http-protocol)))
                   (when acc
                     (funcall (fdefinition `(setf ,acc)) fn x))))
               (queue-h2-out ()
                 (when (and h2-pump (h2-pump-pending-out-p h2-pump))
                   (setf h2-out (h2-pump-take-out h2-pump)
                         h2-out-pos 0
                         phase :h2-write)
                   t))
               (try-h2-request-body ()
                 "Send one DATA chunk from the request stream/pipe. T if bytes queued."
                 (when (or (not stream-body-p) h2-req-end-p (null h2-req-stream))
                   (return-from try-h2-request-body nil))
                 (cond
                   ((%body-pipe-p body-stream-src)
                    (unless (%pipe-listen body-stream-src)
                      (return-from try-h2-request-body nil))
                    (let* ((end (%pipe-read body-stream-src body-read-buf
                                            0 (length body-read-buf)))
                           (n (- end 0)))
                      (cond
                        ((plusp n)
                         (h2-write-data h2-session h2-req-stream
                                        (subseq body-read-buf 0 n)
                                        :end-stream nil)
                         t)
                        ((%pipe-eof body-stream-src)
                         (h2-write-data h2-session h2-req-stream #()
                                        :end-stream t)
                         (setf h2-req-end-p t
                               stream-body-p nil)
                         t)
                        (t nil))))
                   (t
                    (let ((n (read-sequence body-read-buf body-stream-src)))
                      (cond
                        ((plusp n)
                         (h2-write-data h2-session h2-req-stream
                                        (subseq body-read-buf 0 n)
                                        :end-stream nil)
                         t)
                        (t
                         (h2-write-data h2-session h2-req-stream #()
                                        :end-stream t)
                         (setf h2-req-end-p t
                               stream-body-p nil)
                         t))))))
               (install-pipe-wake ()
                 (when (%body-pipe-p body-stream-src)
                   (%pipe-set-on-data
                    body-stream-src
                    (lambda ()
                      (flet ((kick ()
                               (unless (async-request-canceled-p handle)
                                 (try-h2-request-body)
                                 (when (queue-h2-out)
                                   (arm-io :write)))))
                        (let ((wc (find-symbol "WAKE-CALL" :event-protocol)))
                          (if (and wc (fboundp wc))
                              (funcall wc event-backend event-loop #'kick)
                              (progn
                                (defer event-backend event-loop #'kick)
                                (ignore-errors
                                  (wake event-backend event-loop))))))))))
               (begin-h2 ()
                 "Start HTTP/2 session after ALPN=h2 (or prior-knowledge later)."
                 (unless (ensure-http2)
                   (return-from begin-h2
                     (fail (make-condition 'http-version-not-available
                                           :requested version-pref
                                           :negotiated nil
                                           :message "http2 system not loadable"))))
                 (setf h2-pump (make-instance 'async-h2-pump-stream)
                       h2-session (make-async-h2-session
                                   h2-pump
                                   :stream-class 'async-h2-streaming-client-stream)
                       negotiated-version :http/2
                       h2-callback-finished-p nil
                       h2-req-end-p nil
                       (fill-pointer h2-body-buf) 0)
                 (ensure-http-version-available version-pref negotiated-version
                                                :backend-name "async")
                 ;; RFC 9113 §3.4: preface+SETTINGS then HEADERS immediately.
                 ;; Stream uploads: HEADERS without END_STREAM, DATA as we read.
                 (setf h2-req-stream
                       (h2-open-request h2-session method uri headers
                                        :end-stream (not stream-body-p)
                                        :body (if stream-body-p nil body-octets)))
                 (when (typep h2-req-stream 'async-h2-stream-hooks)
                   (setf (h2-stream-on-headers h2-req-stream) #'on-h2-headers
                         (h2-stream-on-data h2-req-stream) #'on-h2-data
                         (h2-stream-on-end h2-req-stream) #'on-h2-end))
                 (install-pipe-wake)
                 (when stream-body-p
                   (try-h2-request-body))
                 (setf h2-out (h2-pump-take-out h2-pump)
                       h2-out-pos 0
                       phase :h2-write)
                 (arm-io :write)
                 (next-tick (lambda () (on-io :ok))))
               (begin-origin-io ()
                 "After TCP (and optional SOCKS), start TLS or HTTP write."
                 (cond
                   ((and (not https) (eq version-pref :http/2))
                    (fail (make-condition 'http-version-not-available
                                          :requested :http/2
                                          :negotiated nil
                                          :message "cleartext h2c not implemented")))
                   (https
                    (setf tls (make-tls-session
                               fd host
                               :verify verify
                               :alpn-protocols (alpn-protocols-for-version version-pref))
                          (async-request-tls-stream handle) tls
                          phase :tls-hs)
                    (arm-io :write)
                    (next-tick (lambda () (on-io :ok))))
                   (t
                    (setf negotiated-version :http/1.1
                          phase :write)
                    (arm-io :write)
                    (next-tick (lambda () (on-io :ok))))))
               (%socks-append (octets n)
                 (loop for i below n
                       do (vector-push-extend (aref octets i) socks-in)))
               (%socks-write-step ()
                 (let ((n (socket-send-octets sock socks-out socks-wpos
                                              (length socks-out))))
                   (cond
                     ((null n) (arm-io :write) nil)
                     ((zerop n) (arm-io :write) nil)
                     (t
                      (incf socks-wpos n)
                      (if (>= socks-wpos (length socks-out))
                          t
                          (progn (arm-io :write) nil))))))
               (do-socks ()
                 "SOCKS5 state machine on the cleartext socket."
                 (handler-case
                     (ecase socks-step
                       (:greet-write
                        (when (%socks-write-step)
                          (setf socks-step :greet-read
                                (fill-pointer socks-in) 0)
                          (arm-io :read)))
                       (:greet-read
                        (let ((n (socket-recv-octets sock recv-buf)))
                          (when (null n) (arm-io :read) (return-from do-socks))
                          (when (zerop n)
                            (return-from do-socks
                              (fail (make-condition 'http-connection-error
                                                    :message "SOCKS5 EOF on greet"))))
                          (%socks-append recv-buf n)
                          (when (>= (fill-pointer socks-in) 2)
                            (unless (= (aref socks-in 0) #x05)
                              (return-from do-socks
                                (fail (make-condition 'http-connection-error
                                                      :message "not SOCKS5"))))
                            (let ((method (aref socks-in 1)))
                              (case method
                                (#x00
                                 (setf socks-out (socks5-connect-request
                                                  host port
                                                  :remote-dns socks-remote-dns)
                                       socks-wpos 0
                                       socks-step :connect-write)
                                 (arm-io :write)
                                 (next-tick #'do-socks))
                                (#x02
                                 (unless socks-user
                                   (return-from do-socks
                                     (fail (make-condition
                                            'http-connection-error
                                            :message "SOCKS5 auth required"))))
                                 (setf socks-out (socks5-userpass-request
                                                  socks-user socks-pass)
                                       socks-wpos 0
                                       socks-step :auth-write)
                                 (arm-io :write)
                                 (next-tick #'do-socks))
                                (t
                                 (fail (make-condition
                                        'http-connection-error
                                        :message
                                        (format nil "SOCKS5 method ~D rejected"
                                                method)))))))))
                       (:auth-write
                        (when (%socks-write-step)
                          (setf socks-step :auth-read
                                (fill-pointer socks-in) 0)
                          (arm-io :read)))
                       (:auth-read
                        (let ((n (socket-recv-octets sock recv-buf)))
                          (when (null n) (arm-io :read) (return-from do-socks))
                          (when (zerop n)
                            (return-from do-socks
                              (fail (make-condition 'http-connection-error
                                                    :message "SOCKS5 EOF on auth"))))
                          (%socks-append recv-buf n)
                          (when (>= (fill-pointer socks-in) 2)
                            (unless (zerop (aref socks-in 1))
                              (return-from do-socks
                                (fail (make-condition 'http-connection-error
                                                      :message "SOCKS5 auth failed"))))
                            (setf socks-out (socks5-connect-request
                                             host port
                                             :remote-dns socks-remote-dns)
                                  socks-wpos 0
                                  socks-step :connect-write)
                            (arm-io :write)
                            (next-tick #'do-socks))))
                       (:connect-write
                        (when (%socks-write-step)
                          (setf socks-step :connect-read
                                (fill-pointer socks-in) 0)
                          (arm-io :read)))
                       (:connect-read
                        (let ((n (socket-recv-octets sock recv-buf)))
                          (when (null n) (arm-io :read) (return-from do-socks))
                          (when (zerop n)
                            (return-from do-socks
                              (fail (make-condition 'http-connection-error
                                                    :message "SOCKS5 EOF on connect"))))
                          (%socks-append recv-buf n)
                          (let ((len (socks5-reply-length socks-in)))
                            (when len
                              (unless (socks5-reply-ok-p socks-in)
                                (return-from do-socks
                                  (fail (make-condition
                                         'http-connection-error
                                         :message
                                         (format nil "SOCKS5 CONNECT failed status=~D"
                                                 (aref socks-in 1))))))
                              (begin-origin-io))))))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition 'http-connection-error
                                           :message (princ-to-string e))))))
               (%connect-append (octets n)
                 (loop for i below n
                       do (vector-push-extend (aref octets i) connect-in)))
               (%connect-write-step ()
                 (let ((n (socket-send-octets sock connect-out connect-wpos
                                              (length connect-out))))
                   (cond
                     ((null n) (arm-io :write) nil)
                     ((zerop n) (arm-io :write) nil)
                     (t
                      (incf connect-wpos n)
                      (if (>= connect-wpos (length connect-out))
                          t
                          (progn (arm-io :write) nil))))))
               (do-http-connect ()
                 "HTTP CONNECT tunnel (dexador make-connect-stream) before TLS."
                 (handler-case
                     (ecase connect-step
                       (:write
                        (when (%connect-write-step)
                          (setf connect-step :read
                                (fill-pointer connect-in) 0)
                          (arm-io :read)))
                       (:read
                        (let ((n (socket-recv-octets sock recv-buf)))
                          (when (null n) (arm-io :read) (return-from do-http-connect))
                          (when (zerop n)
                            (return-from do-http-connect
                              (fail (make-condition 'http-connection-error
                                                    :message "proxy CONNECT EOF"))))
                          (%connect-append recv-buf n)
                          (let ((hend (%header-block-end connect-in)))
                            (when hend
                              (unless (connect-response-ok-p connect-in)
                                (return-from do-http-connect
                                  (fail (make-condition
                                         'http-connection-error
                                         :message
                                         (format nil "proxy CONNECT failed: ~A"
                                                 (babel:octets-to-string
                                                  connect-in :end (min hend 120)
                                                  :encoding :utf-8 :errorp nil))))))
                              (begin-origin-io))))))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition 'http-connection-error
                                           :message (princ-to-string e))))))
               (on-connected ()
                 (set-socket-nonblocking sock t)
                 (cond
                   (socks-p
                    (setf phase :socks
                          socks-step :greet-write
                          socks-out (socks5-greeting :username socks-user)
                          socks-wpos 0
                          (fill-pointer socks-in) 0)
                    (arm-io :write)
                    (next-tick (lambda () (on-io :ok))))
                   (http-connect-p
                    (setf phase :http-connect
                          connect-step :write
                          connect-out (build-connect-request-octets
                                       host port
                                       :proxy-authorization
                                       (proxy-authorization-value
                                        proxy-user proxy-pass))
                          connect-wpos 0
                          (fill-pointer connect-in) 0)
                    (arm-io :write)
                    (next-tick (lambda () (on-io :ok))))
                   (t (begin-origin-io))))
               (do-tls-hs ()
                 (loop
                   (ecase (tls-handshake-step tls)
                     (:done
                      (let* ((alpn (tls-selected-alpn tls))
                             (got (or (http-version-from-alpn alpn) :http/1.1)))
                        (setf negotiated-version got)
                        (handler-case
                            (ensure-http-version-available
                             version-pref got :backend-name "async")
                          (http-version-not-available (e)
                            (return (fail e))))
                        (if (eq got :http/2)
                            (return (begin-h2))
                            (progn
                              (setf phase :write)
                              (arm-io :write)
                              (return (do-write))))))
                     (:want-read
                      (arm-io :read)
                      (return))
                     (:want-write
                      (arm-io :write)
                      (return)))))
               (finish-h2-stream (h2-stream)
                 "Deliver a completed HTTP/2 stream as an HTTP-RESPONSE hop."
                 (multiple-value-bind (status headers* body*)
                     (h2-stream-to-http-parts h2-stream)
                   (let ((set-cookies
                           (merge-response-cookies cookie-jar
                                                   (quri:render-uri uri)
                                                   headers*)))
                     (multiple-value-bind (body* headers*)
                         (apply-response-content-encoding
                          (coerce body* '(simple-array (unsigned-byte 8) (*)))
                          headers*
                          :decompress (http-request-decompress request))
                       (setf reuse-ok-p keep-alive-p
                             h2-done-stream nil)
                       (follow-redirect status headers* body*
                                        set-cookies
                                        (quri:render-uri uri))))))
               (do-h2-write ()
                 (loop
                   (when (or (null h2-out) (>= h2-out-pos (length h2-out)))
                     (setf h2-out nil h2-out-pos 0)
                     (when (h2-pump-pending-out-p h2-pump)
                       (setf h2-out (h2-pump-take-out h2-pump)
                             h2-out-pos 0))
                     (unless h2-out
                       (when (try-h2-request-body)
                         (when (h2-pump-pending-out-p h2-pump)
                           (setf h2-out (h2-pump-take-out h2-pump)
                                 h2-out-pos 0)
                           (return)))
                       ;; Flush done: if response already complete, deliver it
                       ;; (SETTINGS ACK often shares the read that finishes the stream).
                       (when (and h2-done-stream (not h2-callback-finished-p))
                         (return (finish-h2-stream h2-done-stream)))
                       (setf phase :h2-read)
                       (arm-io :read)
                       (return)))
                   (multiple-value-bind (pos done want)
                       (%write-octets h2-out h2-out-pos (length h2-out))
                     (setf h2-out-pos pos)
                     (cond
                       (want (arm-tls-want want) (return))
                       ((not done) (return))
                       (t
                        (setf h2-out nil h2-out-pos 0)
                        (when (h2-pump-pending-out-p h2-pump)
                          (setf h2-out (h2-pump-take-out h2-pump))))))))
               (do-h2-read ()
                 (loop
                   (multiple-value-bind (n want)
                       (if https
                           (tls-read-octets tls recv-buf)
                           (values (socket-recv-octets sock recv-buf) nil))
                     (cond
                       (want (arm-tls-want want) (return))
                       ((null n) (arm-io :read) (return))
                       ((zerop n)
                        (return (fail (make-condition 'http-connection-error
                                                      :message "HTTP/2 EOF"))))
                       (t
                        (h2-pump-feed-in h2-pump recv-buf n)
                        (multiple-value-bind (done h2-stream)
                            (handler-case (h2-process-pending h2-session)
                              (error (e)
                                (return (fail (make-condition
                                               'http-connection-error
                                               :message (princ-to-string e))))))
                          (when done
                            (setf h2-done-stream h2-stream))
                          (when (h2-pump-pending-out-p h2-pump)
                            (setf h2-out (h2-pump-take-out h2-pump)
                                  h2-out-pos 0
                                  phase :h2-write)
                            (arm-io :write)
                            (return (do-h2-write)))
                          (when (try-h2-request-body)
                            (when (h2-pump-pending-out-p h2-pump)
                              (setf h2-out (h2-pump-take-out h2-pump)
                                    h2-out-pos 0
                                    phase :h2-write)
                              (arm-io :write)
                              (return (do-h2-write))))
                          (when (and h2-done-stream (not h2-callback-finished-p))
                            (return (finish-h2-stream h2-done-stream)))
                          (arm-io :read)
                          (return)))))))
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
                        (when (handler-case
                                  (funcall parse! recv-buf :end n)
                                (error (e)
                                  ;; Keep raw bytes: reuse bugs (pooled sockets)
                                  ;; show up as mid-stream parses.
                                  (error 'http-protocol-error
                                         :message
                                         (format nil "response parse failed (pooled=~A n=~D): ~A; bytes: ~S"
                                                 from-pool-p n e
                                                 (babel:octets-to-string
                                                  recv-buf :end (min n 160)
                                                  :encoding :utf-8 :errorp nil)))))
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
                       (:socks (do-socks))
                       (:http-connect (do-http-connect))
                       (:tls-hs (do-tls-hs))
                       (:h2-write (do-h2-write))
                       (:h2-read (do-h2-read))
                       (:write (do-write))
                       (:read (do-read)))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition
                            (if https 'http-tls-error 'http-connection-error)
                            :message (princ-to-string e)))))))
            ;; RFC 9113 h2c prior-knowledge is P2 — fail before TCP.
            (when (and (not https) (eq version-pref :http/2))
              (error 'http-version-not-available
                     :requested :http/2
                     :negotiated nil
                     :message "cleartext h2c not implemented"))
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
