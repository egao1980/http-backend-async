(in-package #:http-backend-async)

;;; RFC 8441 Extended CONNECT WebSocket over HTTP/2 (cl-stack#84).
;;;
;;; Async-first: same TCP → TLS WANT_* → async-h2-pump-stream path as send-async.
;;; I/O runs on the event loop (register-io). No BT reader / no blocking
;;; connect-to-tls-server. Blocking CONNECT awaits open then keeps a loop-pump
;;; thread (event-protocol:run only).

(defvar *fast-websocket-loaded* nil)

(defun ensure-fast-websocket ()
  (or *fast-websocket-loaded*
      (setf *fast-websocket-loaded*
            (and (ignore-errors (asdf:load-system "fast-websocket") t) t))))

(defmethod backend-ws-transports ((backend async-backend))
  "RFC 8441 Extended CONNECT when http2/client + fast-websocket load."
  (declare (ignore backend))
  (if (and (ensure-http2) (ensure-fast-websocket))
      '(:http/2)
      '()))

(defun make-extended-connect-ws-headers (url &key headers protocols scheme)
  "RFC 8441 header alist (protocol-owned shape)."
  (make-http2-websocket-connect-headers
   url headers :scheme scheme :protocols protocols))

;;; Slots/accessors exist at compile time; http2 mixins mixed in at load.
(defclass async-h2-ws-payload-mixin ()
  ((payload :initform (make-array 0 :element-type '(unsigned-byte 8)
                                  :adjustable t :fill-pointer 0)
            :accessor h2-ws-payload)
   (got-headers-p :initform nil :accessor h2-ws-got-headers-p)
   (peer-ended-p :initform nil :accessor h2-ws-peer-ended-p)))

(defun %ensure-h2-ws-stream-class ()
  "CLIENT-STREAM that queues DATA for RFC 6455 parsing."
  (ensure-http2)
  (let ((client-stream (find-symbol "CLIENT-STREAM" :http2/client))
        (header-m (find-symbol "HEADER-COLLECTING-MIXIN" :http2/core))
        (apply-data (find-symbol "APPLY-DATA-FRAME" :http2/core))
        (peer-ends (find-symbol "PEER-ENDS-HTTP-STREAM" :http2/core))
        (process-end (find-symbol "PROCESS-END-HEADERS" :http2/core)))
    (unless (and client-stream header-m apply-data)
      (error 'ws-transport-not-available
             :requested :http/2
             :message "http2 missing CLIENT-STREAM / APPLY-DATA-FRAME"))
    (unless (find-class 'async-h2-ws-stream nil)
      (eval `(defclass async-h2-ws-stream
                 (,client-stream ,header-m async-h2-ws-payload-mixin)
               ())))
    (eval `(defmethod ,apply-data ((stream async-h2-ws-stream) data start end)
             (loop for i from start below end
                   do (vector-push-extend (aref data i) (h2-ws-payload stream)))))
    (when process-end
      (eval `(defmethod ,process-end :after ((connection t) (stream async-h2-ws-stream))
               (declare (ignore connection))
               (setf (h2-ws-got-headers-p stream) t))))
    (when peer-ends
      (eval `(defmethod ,peer-ends :after ((stream async-h2-ws-stream))
               (setf (h2-ws-peer-ended-p stream) t))))
    t))

(defclass async-ws-connection (ws-protocol:ws-connection)
  ((event-backend :initarg :event-backend :reader async-ws-event-backend)
   (event-loop :initarg :event-loop :reader async-ws-event-loop)
   (socket :initarg :socket :accessor async-ws-socket)
   (tls :initarg :tls :accessor async-ws-tls)
   (fd :initarg :fd :accessor async-ws-fd)
   (h2-pump :initarg :h2-pump :reader async-ws-h2-pump)
   (h2-session :initarg :h2-session :reader async-ws-h2-session)
   (h2-stream :initarg :h2-stream :accessor async-ws-h2-stream)
   (io-handle :initform nil :accessor async-ws-io-handle)
   (io-dir :initform nil :accessor async-ws-io-dir)
   (phase :initform :ws-open :accessor async-ws-phase)
   (out :initform nil :accessor async-ws-out)
   (out-pos :initform 0 :accessor async-ws-out-pos)
   (recv-buf :initform (make-array 65536 :element-type '(unsigned-byte 8))
             :reader async-ws-recv-buf)
   (handlers :initform (make-hash-table :test #'eq) :accessor async-ws-handlers)
   (parser :initform nil :accessor async-ws-parser)
   (closed-p :initform nil :accessor async-ws-closed-p)
   (loop-thread :initform nil :accessor async-ws-loop-thread)
   (owns-loop-p :initform nil :accessor async-ws-owns-loop-p)))

(defun %fw-sym (name)
  (or (find-symbol name :fast-websocket)
      (error "fast-websocket symbol ~a missing" name)))

(defun %h2-sym (name &rest packages)
  (or (loop for p in packages
            for s = (and (find-package p) (find-symbol name p))
            when s return s)
      (error "http2 symbol ~a not found in ~a" name packages)))

(defun %async-ws-fire (conn event &rest args)
  (let ((fn (gethash event (async-ws-handlers conn))))
    (when fn (ignore-errors (apply fn args)))))

(defun %take-ws-payload (h2-stream)
  (let* ((buf (h2-ws-payload h2-stream))
         (n (fill-pointer buf)))
    (when (plusp n)
      (let ((out (make-array n :element-type '(unsigned-byte 8))))
        (replace out buf :end2 n)
        (setf (fill-pointer buf) 0)
        out))))

(defun %async-ws-install-parser (conn)
  (ensure-fast-websocket)
  (let* ((make-ws (symbol-function (%fw-sym "MAKE-WS")))
         (make-parser (symbol-function (%fw-sym "MAKE-PARSER")))
         (ws (funcall make-ws)))
    (setf (async-ws-parser conn)
          (funcall make-parser ws
                   :require-masking nil
                   :message-callback
                   (lambda (msg) (%async-ws-fire conn :message msg))
                   :pong-callback
                   (lambda (payload) (%async-ws-fire conn :pong payload))
                   :close-callback
                   (lambda (payload &key code)
                     (declare (ignore payload))
                     (setf (async-ws-closed-p conn) t
                           (ws-protocol:%connection-ready-state conn) :closed)
                     (%async-ws-fire conn :close :code code :reason nil))
                   :error-callback
                   (lambda (code reason)
                     (%async-ws-fire conn :error
                                     (make-condition 'ws-protocol:ws-protocol-error
                                                     :message
                                                     (format nil "~A: ~A" code reason))))))))

(defun %ws-url-parts (url)
  (let* ((uri (quri:uri url))
         (scheme (string-downcase (or (quri:uri-scheme uri) "wss")))
         (https-p (member scheme '("wss" "https") :test #'string=))
         (host (or (quri:uri-host uri)
                   (error 'ws-protocol:ws-handshake-error
                          :message "WebSocket URL missing host")))
         (port (or (quri:uri-port uri) (if https-p 443 80))))
    (unless https-p
      (error 'ws-protocol:ws-handshake-error
             :message "Extended CONNECT requires wss:// (TLS ALPN h2)"))
    (values host port uri)))

(defun %async-ws-stop-io (conn)
  (when-let ((io (async-ws-io-handle conn)))
    (ignore-errors (cancel (async-ws-event-backend conn) io))
    (setf (async-ws-io-handle conn) nil
          (async-ws-io-dir conn) nil)))

(defun %async-ws-teardown (conn &key (stop-loop t))
  (%async-ws-stop-io conn)
  (ignore-errors (tls-close (async-ws-tls conn)))
  (setf (async-ws-tls conn) nil)
  (ignore-errors (close-socket (async-ws-socket conn)))
  (setf (async-ws-socket conn) nil
        (async-ws-closed-p conn) t
        (ws-protocol:%connection-ready-state conn) :closed)
  (when (and stop-loop (async-ws-owns-loop-p conn))
    (ignore-errors
      (stop (async-ws-event-backend conn) (async-ws-event-loop conn)))))

(defun %async-ws-arm-io (conn direction on-io)
  (let ((eb (async-ws-event-backend conn))
        (el (async-ws-event-loop conn))
        (fd (async-ws-fd conn))
        (cur (async-ws-io-handle conn)))
    (unless fd
      (error 'ws-protocol:ws-connection-error :message "arm-io before connect"))
    (cond
      ((and cur (eq (async-ws-io-dir conn) direction)) nil)
      (cur
       (update-io eb cur direction)
       (setf (async-ws-io-dir conn) direction))
      (t
       (setf (async-ws-io-dir conn) direction
             (async-ws-io-handle conn)
             (register-io eb el fd direction on-io))))))

(defun %async-ws-arm-tls-want (conn want on-io)
  (%async-ws-arm-io conn
                    (ecase want (:want-read :read) (:want-write :write))
                    on-io))

(defun %async-ws-write-octets (conn octets from to)
  (multiple-value-bind (n want)
      (tls-write-octets (async-ws-tls conn) octets from to)
    (cond
      ((null n) (values from nil want))
      ((zerop n) (values from nil nil))
      (t
       (let ((pos (+ from n)))
         (values pos (>= pos to) nil))))))

(defun %async-ws-flush-out (conn on-io)
  "Flush pump OUT via TLS. Returns T when drained."
  (loop
    (when (or (null (async-ws-out conn))
              (>= (async-ws-out-pos conn) (length (async-ws-out conn))))
      (setf (async-ws-out conn) nil
            (async-ws-out-pos conn) 0)
      (when (h2-pump-pending-out-p (async-ws-h2-pump conn))
        (setf (async-ws-out conn) (h2-pump-take-out (async-ws-h2-pump conn))
              (async-ws-out-pos conn) 0))
      (unless (async-ws-out conn)
        (%async-ws-arm-io conn :read on-io)
        (return t)))
    (multiple-value-bind (pos done want)
        (%async-ws-write-octets conn
                                (async-ws-out conn)
                                (async-ws-out-pos conn)
                                (length (async-ws-out conn)))
      (setf (async-ws-out-pos conn) pos)
      (cond
        (want (%async-ws-arm-tls-want conn want on-io) (return nil))
        ((not done) (return nil))
        (t (setf (async-ws-out conn) nil
                 (async-ws-out-pos conn) 0))))))

(defun %async-ws-drain-payload (conn)
  (let ((chunk (%take-ws-payload (async-ws-h2-stream conn))))
    (when (and chunk (async-ws-parser conn))
      (funcall (async-ws-parser conn) chunk)))
  (when (h2-ws-peer-ended-p (async-ws-h2-stream conn))
    (setf (async-ws-closed-p conn) t
          (ws-protocol:%connection-ready-state conn) :closed)
    (%async-ws-fire conn :close :code nil :reason "peer end")
    (%async-ws-teardown conn)
    t))

(defun %async-ws-schedule (conn thunk)
  "Run THUNK on the connection's event loop thread."
  (let ((eb (async-ws-event-backend conn))
        (el (async-ws-event-loop conn)))
    (defer eb el thunk)
    (ignore-errors (wake eb el))))

;;; ---------------------------------------------------------------------------
;;; connect-async — primary path
;;; ---------------------------------------------------------------------------

(defmethod connect-async ((backend async-backend) client url
                          &key transport callback error-callback)
  (let ((resolved (resolve-ws-transport backend client :transport transport)))
    (unless (eq resolved :http/2)
      (let ((e (make-condition 'ws-transport-not-available
                               :requested (or transport (ws-client-transport client))
                               :negotiated resolved
                               :message "async-backend only speaks RFC 8441 Extended CONNECT (:http/2)")))
        (if error-callback (funcall error-callback e) (error e))
        (return-from connect-async nil)))
    (unless (and (ensure-http2) (ensure-fast-websocket))
      (let ((e (make-condition 'ws-transport-not-available
                               :requested :http/2
                               :message "http2/client or fast-websocket not loadable")))
        (if error-callback (funcall error-callback e) (error e))
        (return-from connect-async nil)))
    (when (ws-client-proxy client)
      (let ((e (make-condition 'http-protocol:unsupported-operation
                               :operation :proxy
                               :message "async Extended CONNECT proxy is P2")))
        (if error-callback (funcall error-callback e) (error e))
        (return-from connect-async nil)))
    (%ensure-h2-ws-stream-class)
    (multiple-value-bind (host port uri) (%ws-url-parts url)
      (multiple-value-bind (event-backend event-loop) (%ensure-event-context)
        (let* ((cb (or callback (lambda (c) (declare (ignore c)))))
               (eb-cb (or error-callback
                          (lambda (c) (warn "ws connect-async error: ~A" c))))
               (verify (ws-client-verify client))
               (conn nil)
               (sock nil)
               (fd nil)
               (tls nil)
               (h2-pump nil)
               (h2-session nil)
               (h2-stream nil)
               (phase :connecting)
               (io-handle nil)
               (io-dir nil)
               (out nil)
               (out-pos 0)
               (recv-buf (make-array 65536 :element-type '(unsigned-byte 8)))
               (timer nil)
               (finished-p nil)
               (client-headers
                (inject-auth-headers
                 (loop for pair in (ws-client-headers client)
                       for name = (string-downcase
                                   (string (if (consp pair) (car pair) pair)))
                       for value = (if (consp pair) (cdr pair) nil)
                       when value
                         collect (cons name (if (stringp value)
                                                value
                                                (princ-to-string value))))
                 :auth (ws-client-auth client)))
               (protocols (ws-client-protocols client)))
          (labels
              ((next-tick (fn)
                 (sleep* event-backend event-loop 0 :callback fn))
               (arm-io (direction)
                 (unless fd
                   (error 'ws-protocol:ws-connection-error
                          :message "arm-io before connect"))
                 (when finished-p (return-from arm-io nil))
                 (cond
                   ((and io-handle (eq io-dir direction)) nil)
                   (io-handle
                    (update-io event-backend io-handle direction)
                    (setf io-dir direction))
                   (t
                    (setf io-dir direction
                          io-handle (register-io event-backend event-loop fd
                                                 direction #'on-io)))))
               (arm-tls-want (want)
                 (arm-io (ecase want
                           (:want-read :read)
                           (:want-write :write))))
               (stop-io-and-timer ()
                 (when io-handle
                   (ignore-errors (cancel event-backend io-handle))
                   (setf io-handle nil io-dir nil))
                 (when timer
                   (ignore-errors (cancel event-backend timer))
                   (setf timer nil)))
               (fail (condition)
                 (unless finished-p
                   (setf finished-p t)
                   (stop-io-and-timer)
                   (ignore-errors (tls-close tls))
                   (ignore-errors (close-socket sock))
                   (handler-case (funcall eb-cb condition)
                     (error (e) (warn "ws error-callback failed: ~A" e)))))
               (write-octets (octets from to)
                 (multiple-value-bind (n want)
                     (tls-write-octets tls octets from to)
                   (cond
                     ((null n) (values from nil want))
                     ((zerop n) (values from nil nil))
                     (t
                      (let ((pos (+ from n)))
                        (values pos (>= pos to) nil))))))
               (flush-out ()
                 (loop
                   (when (or (null out) (>= out-pos (length out)))
                     (setf out nil out-pos 0)
                     (when (h2-pump-pending-out-p h2-pump)
                       (setf out (h2-pump-take-out h2-pump) out-pos 0))
                     (unless out (return t)))
                   (multiple-value-bind (pos done want)
                       (write-octets out out-pos (length out))
                     (setf out-pos pos)
                     (cond
                       (want (arm-tls-want want) (return nil))
                       ((not done) (return nil))
                       (t (setf out nil out-pos 0))))))
               (deliver-open ()
                 (unless finished-p
                   (setf finished-p t)
                   (when timer
                     (ignore-errors (cancel event-backend timer))
                     (setf timer nil))
                   (setf conn
                         (make-instance 'async-ws-connection
                                        :url url
                                        :ready-state :open
                                        :event-backend event-backend
                                        :event-loop event-loop
                                        :socket sock
                                        :tls tls
                                        :fd fd
                                        :h2-pump h2-pump
                                        :h2-session h2-session
                                        :h2-stream h2-stream))
                   (setf (async-ws-io-handle conn) io-handle
                         (async-ws-io-dir conn) io-dir
                         (async-ws-phase conn) :ws-open
                         (async-ws-out conn) out
                         (async-ws-out-pos conn) out-pos)
                   (%async-ws-install-parser conn)
                   ;; Re-arm with connection steady-state callback.
                   (when io-handle
                     (ignore-errors (cancel event-backend io-handle))
                     (setf (async-ws-io-handle conn) nil
                           (async-ws-io-dir conn) nil
                           io-handle nil
                           io-dir nil))
                   (%async-ws-arm-io conn :read
                                     (lambda (st) (%async-ws-on-io conn st)))
                   (%async-ws-fire conn :open)
                   (handler-case (funcall cb conn)
                     (error (e) (warn "ws callback failed: ~A" e)))))
               (begin-h2 ()
                 (setf h2-pump (make-instance 'async-h2-pump-stream)
                       h2-session (make-async-h2-session
                                   h2-pump :stream-class 'async-h2-ws-stream)
                       out (h2-pump-take-out h2-pump)
                       out-pos 0
                       phase :h2-settings)
                 (arm-io :write)
                 (next-tick (lambda () (on-io :ok))))
               (open-connect ()
                 (setf h2-stream
                       (let* ((connection (async-h2-session-connection h2-session))
                              (open (%h2-sym "OPEN-HTTP2-STREAM" :http2/core))
                              (fields (%http2-headers-for-lib
                                       (make-extended-connect-ws-headers
                                        uri
                                        :headers client-headers
                                        :protocols protocols))))
                         (funcall open connection fields
                                  :end-stream nil :end-headers t))
                       out (or (and (h2-pump-pending-out-p h2-pump)
                                    (h2-pump-take-out h2-pump))
                               out)
                       out-pos 0
                       phase :h2-connect-write)
                 (arm-io :write)
                 (next-tick (lambda () (on-io :ok))))
               (do-tls-hs ()
                 (loop
                   (ecase (tls-handshake-step tls)
                     (:done
                      (let ((alpn (tls-selected-alpn tls)))
                        (unless (equal alpn "h2")
                          (return
                           (fail (make-condition
                                  'ws-transport-not-available
                                  :requested :http/2
                                  :message
                                  (format nil "ALPN ~S (want h2)" alpn)))))
                        (return (begin-h2))))
                     (:want-read (arm-io :read) (return))
                     (:want-write (arm-io :write) (return)))))
               (do-h2-settings ()
                 "Flush preface/SETTINGS; wait for ENABLE_CONNECT_PROTOCOL=1."
                 (unless (flush-out)
                   (return-from do-h2-settings))
                 (loop
                   (when (h2-session-enable-connect-protocol-p h2-session)
                     (return (open-connect)))
                   (multiple-value-bind (n want)
                       (tls-read-octets tls recv-buf)
                     (cond
                       (want (arm-tls-want want) (return))
                       ((null n) (arm-io :read) (return))
                       ((zerop n)
                        (return (fail (make-condition
                                       'ws-protocol:ws-connection-error
                                       :message "EOF waiting for ENABLE_CONNECT"))))
                       (t
                        (h2-pump-feed-in h2-pump recv-buf n)
                        (handler-case (h2-process-pending h2-session)
                          (error (e)
                            (return (fail (make-condition
                                           'ws-protocol:ws-connection-error
                                           :message (princ-to-string e))))))
                        (when (h2-pump-pending-out-p h2-pump)
                          (setf out (h2-pump-take-out h2-pump)
                                out-pos 0
                                phase :h2-settings)
                          (arm-io :write)
                          (return (do-h2-settings)))
                        (when (h2-session-enable-connect-protocol-p h2-session)
                          (return (open-connect)))
                        (arm-io :read)
                        (return))))))
               (do-h2-connect-write ()
                 (unless (flush-out)
                   (return-from do-h2-connect-write))
                 (setf phase :h2-connect-read)
                 (arm-io :read)
                 (do-h2-connect-read))
               (do-h2-connect-read ()
                 (loop
                   (when (and h2-stream (h2-ws-got-headers-p h2-stream))
                     (let* ((get-status (%h2-sym "GET-STATUS" :http2/client :http2/core))
                            (status-s (funcall get-status h2-stream))
                            (status (ignore-errors
                                      (parse-integer (princ-to-string status-s)))))
                       (unless (and status (<= 200 status 299))
                         (return
                          (fail (make-condition
                                 'ws-protocol:ws-handshake-error
                                 :message
                                 (format nil "Extended CONNECT status ~A (want 2xx)"
                                         status-s)))))
                       (return (deliver-open))))
                   (multiple-value-bind (n want)
                       (tls-read-octets tls recv-buf)
                     (cond
                       (want (arm-tls-want want) (return))
                       ((null n) (arm-io :read) (return))
                       ((zerop n)
                        (return (fail (make-condition
                                       'ws-protocol:ws-handshake-error
                                       :message "EOF during Extended CONNECT"))))
                       (t
                        (h2-pump-feed-in h2-pump recv-buf n)
                        (handler-case (h2-process-pending h2-session)
                          (error (e)
                            (return (fail (make-condition
                                           'ws-protocol:ws-handshake-error
                                           :message (princ-to-string e))))))
                        (when (h2-pump-pending-out-p h2-pump)
                          (setf out (h2-pump-take-out h2-pump)
                                out-pos 0
                                phase :h2-connect-write)
                          (arm-io :write)
                          (return (do-h2-connect-write)))
                        (when (and h2-stream (h2-ws-got-headers-p h2-stream))
                          (return (do-h2-connect-read)))
                        (arm-io :read)
                        (return))))))
               (on-connected ()
                 (set-socket-nonblocking sock t)
                 (setf tls (make-tls-session fd host
                                             :verify verify
                                             :alpn-protocols '("h2"))
                       phase :tls-hs)
                 (arm-io :write)
                 (next-tick (lambda () (on-io :ok))))
               (do-connect ()
                 (handler-case
                     (multiple-value-bind (usock status)
                         (tcp-connect-nb host port)
                       (setf sock usock
                             fd (socket-fd sock)
                             phase :connecting)
                       (ecase status
                         (:connected (on-connected))
                         (:pending (arm-io :write))))
                   (error (e)
                     (fail (make-condition 'ws-protocol:ws-connection-error
                                           :message (princ-to-string e))))))
               (on-io (status)
                 (when finished-p (return-from on-io nil))
                 (when (eq status :error)
                   (return-from on-io
                     (fail (make-condition 'ws-protocol:ws-connection-error
                                           :message "register-io error"))))
                 (handler-case
                     (ecase phase
                       (:connecting
                        (when (tcp-connect-finish sock)
                          (on-connected)))
                       (:tls-hs (do-tls-hs))
                       (:h2-settings (do-h2-settings))
                       (:h2-connect-write (do-h2-connect-write))
                       (:h2-connect-read (do-h2-connect-read)))
                   (ws-protocol:ws-error (e) (fail e))
                   (http-error (e) (fail e))
                   (error (e)
                     (fail (make-condition 'ws-protocol:ws-connection-error
                                           :message (princ-to-string e)))))))
            (with-event-backend (event-backend)
              (with-event-loop-var (event-loop)
                (setf timer
                      (sleep* event-backend event-loop 30.0
                              :callback
                              (lambda ()
                                (fail (make-condition
                                       'ws-protocol:ws-handshake-error
                                       :message "Extended CONNECT timed out")))))
                (defer event-backend event-loop #'do-connect)))
            t))))))

;;; Steady-state I/O after open (loop thread).

(defun %async-ws-on-io (conn status)
  (when (async-ws-closed-p conn)
    (return-from %async-ws-on-io nil))
  (when (eq status :error)
    (%async-ws-fire conn :error
                    (make-condition 'ws-protocol:ws-connection-error
                                    :message "register-io error"))
    (%async-ws-teardown conn)
    (return-from %async-ws-on-io nil))
  (handler-case
      (ecase (async-ws-phase conn)
        (:ws-write
         (when (%async-ws-flush-out conn (lambda (st) (%async-ws-on-io conn st)))
           (setf (async-ws-phase conn) :ws-open)
           (%async-ws-arm-io conn :read (lambda (st) (%async-ws-on-io conn st)))))
        (:ws-open
         (loop
           (multiple-value-bind (n want)
               (tls-read-octets (async-ws-tls conn) (async-ws-recv-buf conn))
             (cond
               (want
                (%async-ws-arm-tls-want
                 conn want (lambda (st) (%async-ws-on-io conn st)))
                (return))
               ((null n)
                (%async-ws-arm-io conn :read
                                  (lambda (st) (%async-ws-on-io conn st)))
                (return))
               ((zerop n)
                (%async-ws-fire conn :close :code nil :reason "eof")
                (%async-ws-teardown conn)
                (return))
               (t
                (h2-pump-feed-in (async-ws-h2-pump conn)
                                 (async-ws-recv-buf conn) n)
                (handler-case (h2-process-pending (async-ws-h2-session conn))
                  (error (e)
                    (%async-ws-fire conn :error e)
                    (%async-ws-teardown conn)
                    (return-from %async-ws-on-io nil)))
                (when (%async-ws-drain-payload conn)
                  (return-from %async-ws-on-io nil))
                (when (h2-pump-pending-out-p (async-ws-h2-pump conn))
                  (setf (async-ws-out conn)
                        (h2-pump-take-out (async-ws-h2-pump conn))
                        (async-ws-out-pos conn) 0
                        (async-ws-phase conn) :ws-write)
                  (%async-ws-arm-io conn :write
                                    (lambda (st) (%async-ws-on-io conn st)))
                  (return-from %async-ws-on-io
                    (%async-ws-on-io conn :ok)))
                (%async-ws-arm-io conn :read
                                  (lambda (st) (%async-ws-on-io conn st)))
                (return)))))))
    (error (e)
      (%async-ws-fire conn :error e)
      (%async-ws-teardown conn))))

(defun %async-ws-send-frame (connection octets &key (type :text))
  (when (async-ws-closed-p connection)
    (error 'ws-protocol:ws-connection-error :message "WebSocket is closed"))
  (ensure-fast-websocket)
  (let* ((compose (symbol-function (%fw-sym "COMPOSE-FRAME")))
         (write-data (%h2-sym "WRITE-DATA-FRAME" :http2/core))
         (frame (funcall compose octets :type type :masking t)))
    (labels ((do-send ()
               (funcall write-data (async-ws-h2-stream connection) frame
                        :end-stream nil)
               (when (h2-pump-pending-out-p (async-ws-h2-pump connection))
                 (setf (async-ws-out connection)
                       (h2-pump-take-out (async-ws-h2-pump connection))
                       (async-ws-out-pos connection) 0
                       (async-ws-phase connection) :ws-write)
                 (%async-ws-on-io connection :ok))))
      ;; Always hop onto the loop (safe if already there via defer).
      (%async-ws-schedule connection #'do-send))))

(defmethod send-text ((connection async-ws-connection) text &key)
  (%async-ws-send-frame connection text :type :text))

(defmethod send-binary ((connection async-ws-connection) octets &key)
  (%async-ws-send-frame connection octets :type :binary))

(defmethod ping ((connection async-ws-connection) &optional payload &key)
  (%async-ws-send-frame connection (or payload #()) :type :ping))

(defmethod close-connection ((connection async-ws-connection) &key code reason)
  (declare (ignore reason))
  (unless (async-ws-closed-p connection)
    (setf (ws-protocol:%connection-ready-state connection) :closing)
    (ignore-errors
      (%async-ws-send-frame connection #() :type :close))
    (%async-ws-schedule
     connection
     (lambda ()
       (declare (ignore code))
       (%async-ws-teardown connection))))
  t)

(defmethod on-event ((connection async-ws-connection) event handler)
  (setf (gethash event (async-ws-handlers connection)) handler))

;;; ---------------------------------------------------------------------------
;;; Blocking CONNECT — await open; loop-pump thread keeps I/O alive
;;; ---------------------------------------------------------------------------

(defmethod connect ((backend async-backend) client url &key transport)
  "Await CONNECT-ASYNC. Spawns event-protocol:run on a BT thread (loop pump,
   not a WS reader) so the connection stays live after return."
  (let ((had-context (and event-protocol:*event-backend*
                          event-protocol:*event-loop*)))
    (multiple-value-bind (event-backend event-loop) (%ensure-event-context)
      (let ((result nil)
            (err nil)
            (done nil)
            (lock (bt:make-lock "async-ws-connect"))
            (cv (bt:make-condition-variable :name "async-ws-connect"))
            (owns-loop (not had-context)))
        (with-event-backend (event-backend)
          (with-event-loop-var (event-loop)
            (connect-async
             backend client url
             :transport transport
             :callback
             (lambda (c)
               (bt:with-lock-held (lock)
                 (setf result c
                       (async-ws-owns-loop-p c) owns-loop
                       done t)
                 (bt:condition-notify cv)))
             :error-callback
             (lambda (e)
               (bt:with-lock-held (lock)
                 (setf err e done t)
                 (bt:condition-notify cv))))
            (let ((pump
                    (when owns-loop
                      (bt:make-thread
                       (lambda ()
                         (with-event-backend (event-backend)
                           (with-event-loop-var (event-loop)
                             (run event-backend event-loop
                                  :stop-when-idle nil))))
                       :name "async-ws-event-loop"))))
              ;; If OWNS-LOOP is NIL, caller must already be pumping the loop
              ;; (do not call blocking CONNECT on the loop thread — deadlock).
              (loop until done do (sleep 0.01))
              (when err
                (when owns-loop
                  (ignore-errors (stop event-backend event-loop))
                  (ignore-errors (bt:destroy-thread pump)))
                (error err))
              (when pump
                (setf (async-ws-loop-thread result) pump))
              result)))))))
