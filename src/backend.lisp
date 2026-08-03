(in-package #:http-backend-async)

;;; Async http-protocol backend. One HTTP backend × N event-protocol loops.
;;; Carrier disposition: rejected (hard-wires cl-async/libuv). Thin rewrite here.
;;;
;;; I/O model: nonblocking TCP connect + register-io; cleartext send/recv on the
;;; FD; HTTPS via cl+ssl socket-BIO pumped with WANT_READ/WANT_WRITE (no blocking
;;; ensure-ssl-funcall on the loop thread). One :read-write watcher per connection
;;; (avoids cancel/re-init races on the same FD).

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
  (let ((t* (or (http-request-timeout request)
                (http-client-timeout client))))
    (cond ((null t*) 30.0)
          ((numberp t*) (float t* 1.0d0))
          ((and (consp t*) (getf t* :total)) (float (getf t* :total) 1.0d0))
          ((and (consp t*) (getf t* :read)) (float (getf t* :read) 1.0d0))
          (t 30.0))))

(defmethod send ((backend async-backend) client request &key)
  (declare (ignore client request))
  (error 'unsupported-operation
         :operation 'send
         :message "async-backend is non-blocking; use SEND-ASYNC (or http:*-async)"))

(defmethod send-async ((backend async-backend) client request
                       &key callback error-callback)
  (let* ((cb (or callback (lambda (r) (declare (ignore r)))))
         (eb-cb (or error-callback
                    (lambda (c) (error c))))
         (uri (quri:uri (http-request-url request)))
         (method (http-request-method request))
         (max-redirects (or (http-request-max-redirects request)
                            (http-client-max-redirects client)
                            5))
         (redirect-hops 0)
         (history nil))
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
               (cookie-jar (resolve-cookie-jar client request
                                               :url (http-request-url request)))
               (ae (%accept-encoding-header
                    (http-request-accept-encoding request)))
               (headers nil)
               (body-octets #())
               (phase :connect)
               (wpos 0)
               (req-octets nil)
               (recv-buf (make-array 65536 :element-type '(unsigned-byte 8)))
               (http nil)
               (hdrs nil)
               (body nil)
               (finishedp nil)
               (parse! nil))
          (labels
              ((build-headers-and-body ()
                 (setf headers (%merge-headers (http-client-headers client)
                                               (http-request-headers request)))
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
                 (multiple-value-bind (content ce-header)
                     (%prepare-content (http-request-content request)
                                       (http-request-content-encoding request))
                   (when ce-header
                     (setf headers (acons "content-encoding" ce-header
                                          (remove "content-encoding" headers
                                                  :key #'car :test #'string-equal))))
                   (setf body-octets (or content #())))
                 (setf req-octets (build-request-octets method uri headers body-octets)
                       wpos 0))
               (reset-parser ()
                 (multiple-value-bind (h hd b f p)
                     (make-response-accumulator)
                   (setf http h hdrs hd body b finishedp f parse! p)))
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
                       fd nil))
               (arm-io ()
                 "One :read-write watcher per connection — never re-arm same FD."
                 (unless fd
                   (error 'http-connection-error :message "arm-io before connect"))
                 (when (async-request-io-handle handle)
                   (error 'http-connection-error
                          :message "arm-io: watcher already registered"))
                 (setf (async-request-io-handle handle)
                       (register-io event-backend event-loop fd :read-write
                                    #'on-io)))
               (do-connect ()
                 (handler-case
                     (multiple-value-bind (usock status)
                         (tcp-connect-nb host port)
                       (setf sock usock
                             (async-request-socket handle) sock
                             fd (socket-fd sock)
                             phase :connect)
                       (ecase status
                         (:connected (on-connected))
                         (:pending (arm-io))))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition 'http-connection-error
                                           :message (princ-to-string e))))))
               (fail (condition)
                 (unless (async-request-canceled-p handle)
                   (setf (async-request-canceled-p handle) t)
                   (when-let ((io (async-request-io-handle handle)))
                     (ignore-errors (cancel event-backend io)))
                   (when-let ((tm (async-request-timer-handle handle)))
                     (ignore-errors (cancel event-backend tm)))
                   (close-connection)
                   (handler-case (funcall eb-cb condition)
                     (error (e) (warn "error-callback failed: ~A" e)))))
               (succeed (res)
                 (unless (async-request-canceled-p handle)
                   (setf (async-request-canceled-p handle) t)
                   (when-let ((io (async-request-io-handle handle)))
                     (ignore-errors (cancel event-backend io)))
                   (when-let ((tm (async-request-timer-handle handle)))
                     (ignore-errors (cancel event-backend tm)))
                   (close-connection)
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
                            (multiple-value-bind (m u h b ho po https?)
                                (prepare-redirect-hop status next method body-octets
                                                      headers cookie-jar ae)
                              (setf method m
                                    uri u
                                    headers h
                                    body-octets b
                                    host ho
                                    port po
                                    https https?
                                    scheme (if https? "https" "http")
                                    req-octets (build-request-octets
                                                method uri headers body-octets)
                                    wpos 0
                                    phase :connect)
                              (reset-parser)
                              ;; Defer close+reconnect: canceling the active
                              ;; poll from inside its callback races libuv/libev.
                              (defer event-backend event-loop
                                (lambda ()
                                  (unless (async-request-canceled-p handle)
                                    (close-connection)
                                    (do-connect))))))
                        (http-error (e) (fail e))
                        (error (e)
                          (fail (make-condition 'http-redirect-error
                                                :message (princ-to-string e)))))))))
               (finish-response ()
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
                                      final-url))))
               (on-connected ()
                 (set-socket-nonblocking sock t)
                 (unless (async-request-io-handle handle)
                   (arm-io))
                 (if https
                     (progn
                       (setf tls (make-tls-session fd host :verify verify)
                             (async-request-tls-stream handle) tls
                             phase :tls-hs)
                       (defer event-backend event-loop
                         (lambda () (on-io :ok))))
                     (progn
                       (setf phase :write)
                       (defer event-backend event-loop
                         (lambda () (on-io :ok))))))
               (do-tls-hs ()
                 (loop
                   (ecase (tls-handshake-step tls)
                     (:done
                      (setf phase :write)
                      (return (do-write)))
                     (:want-read (return))
                     (:want-write (return)))))
               (do-write ()
                 (loop
                   (when (>= wpos (length req-octets))
                     (setf phase :read)
                     (return (do-read)))
                   (let ((n (if https
                                (tls-write-octets tls req-octets wpos
                                                  (length req-octets))
                                (socket-send-octets
                                 sock req-octets wpos (length req-octets)))))
                     (cond ((null n) (return)) ; WANT_* / would-block
                           ((zerop n) (return))
                           (t (incf wpos n))))))
               (do-read ()
                 (loop
                   (let ((n (if https
                                (tls-read-octets tls recv-buf)
                                (socket-recv-octets sock recv-buf))))
                     (cond
                       ((null n) (return))
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
                          (return))))))))
               (on-io (status)
                 (when (async-request-canceled-p handle)
                   (return-from on-io nil))
                 (when (eq status :error)
                   (return-from on-io
                     (fail (make-condition 'http-connection-error
                                           :message "register-io error"))))
                 (handler-case
                     (ecase phase
                       (:connect
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
