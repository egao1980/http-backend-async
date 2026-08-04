(in-package #:http-backend-async)

;;; RFC 8441 Extended CONNECT WebSocket over HTTP/2 (cl-stack#84).
;;;
;;; Blocking-style connect (BT reader thread) using http2/client TLS + framing.
;;; Event-loop-native WS is a follow-up; facade still uses connect-async → BT.

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
      (eval `(defclass async-h2-ws-stream (,client-stream ,header-m)
               ((payload :initform (make-array 0 :element-type '(unsigned-byte 8)
                                               :adjustable t :fill-pointer 0)
                         :accessor h2-ws-payload)
                (payload-lock :initform (bt:make-lock "h2-ws-payload")
                              :reader h2-ws-payload-lock)
                (got-headers-p :initform nil :accessor h2-ws-got-headers-p)
                (peer-ended-p :initform nil :accessor h2-ws-peer-ended-p)))))
    (eval `(defmethod ,apply-data ((stream async-h2-ws-stream) data start end)
             (bt:with-lock-held ((h2-ws-payload-lock stream))
               (loop for i from start below end
                     do (vector-push-extend (aref data i) (h2-ws-payload stream))))))
    (when process-end
      (eval `(defmethod ,process-end :after ((connection t) (stream async-h2-ws-stream))
               (declare (ignore connection))
               (setf (h2-ws-got-headers-p stream) t))))
    (when peer-ends
      (eval `(defmethod ,peer-ends :after ((stream async-h2-ws-stream))
               (setf (h2-ws-peer-ended-p stream) t))))
    t))

(defclass async-ws-connection (ws-protocol:ws-connection)
  ((h2-connection :initarg :h2-connection :reader async-ws-h2-connection)
   (h2-stream :initarg :h2-stream :reader async-ws-h2-stream)
   (tls-stream :initarg :tls-stream :reader async-ws-tls-stream)
   (socket :initarg :socket :reader async-ws-socket)
   (handlers :initform (make-hash-table :test #'eq) :accessor async-ws-handlers)
   (lock :initform (bt:make-lock "async-ws") :reader async-ws-lock)
   (closed-p :initform nil :accessor async-ws-closed-p)
   (reader :initform nil :accessor async-ws-reader)
   (parser :initarg :parser :accessor async-ws-parser)))

(defun %async-ws-fire (conn event &rest args)
  (let ((fn (gethash event (async-ws-handlers conn))))
    (when fn (ignore-errors (apply fn args)))))

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

(defun %h2-sym (name &rest packages)
  "Resolve NAME in PACKAGES (first hit wins)."
  (or (loop for p in packages
            for s = (and (find-package p) (find-symbol name p))
            when s return s)
      (error "http2 symbol ~a not found in ~a" name packages)))

(defun %h2-flush (connection)
  (force-output (funcall (%h2-sym "GET-NETWORK-STREAM" :http2/core) connection)))

(defun %h2-pump-once (connection)
  "Process frames currently available (non-blocking listen)."
  (funcall (%h2-sym "PROCESS-PENDING-FRAMES" :http2/stream-overlay :http2/core)
           connection t))

(defun %h2-pump-until (connection pred &key (timeout 10.0) (step 0.01))
  (let ((deadline (+ (get-internal-real-time)
                     (* timeout internal-time-units-per-second))))
    (loop
      (when (funcall pred) (return t))
      (when (> (get-internal-real-time) deadline)
        (return nil))
      (handler-case (%h2-pump-once connection)
        (end-of-file () (return nil))
        (error ()))
      (sleep step))))

(defun %take-ws-payload (h2-stream)
  (bt:with-lock-held ((h2-ws-payload-lock h2-stream))
    (let* ((buf (h2-ws-payload h2-stream))
           (n (fill-pointer buf)))
      (when (plusp n)
        (let ((out (make-array n :element-type '(unsigned-byte 8))))
          (replace out buf :end2 n)
          (setf (fill-pointer buf) 0)
          out)))))

(defun %fw-sym (name)
  (or (find-symbol name :fast-websocket)
      (error "fast-websocket symbol ~a missing" name)))

(defun %async-ws-start-reader (conn)
  (let* ((make-ws (symbol-function (%fw-sym "MAKE-WS")))
         (make-parser (symbol-function (%fw-sym "MAKE-PARSER")))
         (ws (funcall make-ws))
         (h2c (async-ws-h2-connection conn))
         (h2s (async-ws-h2-stream conn)))
    (setf (async-ws-parser conn)
          (funcall make-parser ws
                   :require-masking nil ; server→client frames are unmasked
                   :message-callback
                   (lambda (msg) (%async-ws-fire conn :message msg))
                   :pong-callback
                   (lambda (payload) (%async-ws-fire conn :pong payload))
                   :close-callback
                   (lambda (payload &key code)
                     (declare (ignore payload))
                     (bt:with-lock-held ((async-ws-lock conn))
                       (setf (async-ws-closed-p conn) t
                             (ws-protocol:%connection-ready-state conn) :closed))
                     (%async-ws-fire conn :close :code code :reason nil))
                   :error-callback
                   (lambda (code reason)
                     (%async-ws-fire conn :error
                                     (make-condition 'ws-protocol:ws-protocol-error
                                                     :message
                                                     (format nil "~A: ~A" code reason))))))
    (setf (async-ws-reader conn)
          (bt:make-thread
           (lambda ()
             (loop
               (when (async-ws-closed-p conn) (return))
               (handler-case
                   (progn
                     (%h2-pump-once h2c)
                     (let ((chunk (%take-ws-payload h2s)))
                       (when chunk
                         (funcall (async-ws-parser conn) chunk)))
                     (when (h2-ws-peer-ended-p h2s)
                       (bt:with-lock-held ((async-ws-lock conn))
                         (setf (async-ws-closed-p conn) t
                               (ws-protocol:%connection-ready-state conn) :closed))
                       (%async-ws-fire conn :close :code nil :reason "peer end")
                       (return))
                     (sleep 0.01))
                 (end-of-file ()
                   (bt:with-lock-held ((async-ws-lock conn))
                     (setf (async-ws-closed-p conn) t
                           (ws-protocol:%connection-ready-state conn) :closed))
                   (%async-ws-fire conn :close :code nil :reason "eof")
                   (return))
                 (error (e)
                   (%async-ws-fire conn :error e)
                   (bt:with-lock-held ((async-ws-lock conn))
                     (setf (async-ws-closed-p conn) t
                           (ws-protocol:%connection-ready-state conn) :closed))
                   (return)))))
           :name "async-ws-h2-reader"))))

(defmethod connect ((backend async-backend) client url &key transport)
  (let ((resolved (resolve-ws-transport backend client :transport transport)))
    (unless (eq resolved :http/2)
      (error 'ws-transport-not-available
             :requested (or transport (ws-client-transport client))
             :negotiated resolved
             :message "async-backend only speaks RFC 8441 Extended CONNECT (:http/2)"))
    (unless (and (ensure-http2) (ensure-fast-websocket))
      (error 'ws-transport-not-available
             :requested :http/2
             :message "http2/client or fast-websocket not loadable"))
    (%ensure-h2-ws-stream-class)
    (when (ws-client-proxy client)
      (error 'http-protocol:unsupported-operation :operation :proxy
             :message "async Extended CONNECT proxy is P2"))
    (multiple-value-bind (host port uri) (%ws-url-parts url)
      (let* ((connect-tls (%h2-sym "CONNECT-TO-TLS-SERVER" :http2/client))
             (open (%h2-sym "OPEN-HTTP2-STREAM" :http2/core))
             (get-status (%h2-sym "GET-STATUS" :http2/client :http2/core))
             (tls nil)
             (socket nil)
             (h2c nil)
             (h2s nil))
        (handler-case
            (progn
              (setf tls (funcall connect-tls host
                                 :port port
                                 :verify (ws-client-verify client)
                                 :alpn-protocols '("h2")))
              (setf h2c (make-instance 'async-h2-client-connection
                                       :network-stream tls
                                       :stream-class 'async-h2-ws-stream))
              (%h2-flush h2c)
              (unless (%h2-pump-until
                       h2c
                       (lambda () (h2-enable-connect-protocol-p h2c))
                       :timeout 8.0)
                (error 'ws-transport-not-available
                       :requested :http/2
                       :message
                       "peer did not advertise SETTINGS_ENABLE_CONNECT_PROTOCOL=1"))
              (let* ((headers (inject-auth-headers
                               (loop for pair in (ws-client-headers client)
                                     for name = (string-downcase
                                                 (string (if (consp pair) (car pair) pair)))
                                     for value = (if (consp pair) (cdr pair) nil)
                                     when value
                                       collect (cons name (if (stringp value)
                                                              value
                                                              (princ-to-string value))))
                               :auth (ws-client-auth client)))
                     (fields (%http2-headers-for-lib
                              (make-extended-connect-ws-headers
                               uri
                               :headers headers
                               :protocols (ws-client-protocols client)))))
                (setf h2s (funcall open h2c fields :end-stream nil :end-headers t))
                (%h2-flush h2c)
                (unless (%h2-pump-until h2c
                                        (lambda () (h2-ws-got-headers-p h2s))
                                        :timeout 10.0)
                  (error 'ws-protocol:ws-handshake-error
                         :message "Extended CONNECT timed out waiting for headers"))
                (let* ((status-s (funcall get-status h2s))
                       (status (ignore-errors
                                 (parse-integer (princ-to-string status-s)))))
                  (unless (and status (<= 200 status 299))
                    (error 'ws-protocol:ws-handshake-error
                           :message
                           (format nil "Extended CONNECT status ~A (want 2xx)"
                                   status-s))))
                (let ((conn (make-instance 'async-ws-connection
                                           :url url
                                           :ready-state :open
                                           :h2-connection h2c
                                           :h2-stream h2s
                                           :tls-stream tls
                                           :socket socket)))
                  (%async-ws-start-reader conn)
                  (%async-ws-fire conn :open)
                  conn)))
          (error (e)
            (ignore-errors (when tls (close tls :abort t)))
            (ignore-errors (when socket (usocket:socket-close socket)))
            (if (typep e 'ws-protocol:ws-error)
                (error e)
                (error 'ws-protocol:ws-connection-error
                       :message (format nil "Extended CONNECT failed: ~A" e)))))))))

(defun %async-ws-send-frame (connection octets &key (type :text))
  (ensure-fast-websocket)
  (let* ((compose (symbol-function (%fw-sym "COMPOSE-FRAME")))
         (write-data (%h2-sym "WRITE-DATA-FRAME" :http2/core))
         (frame (funcall compose octets :type type :masking t)))
    (funcall write-data (async-ws-h2-stream connection) frame :end-stream nil)
    (%h2-flush (async-ws-h2-connection connection))))

(defmethod send-text ((connection async-ws-connection) text &key)
  (%async-ws-send-frame connection text :type :text))

(defmethod send-binary ((connection async-ws-connection) octets &key)
  (%async-ws-send-frame connection octets :type :binary))

(defmethod ping ((connection async-ws-connection) &optional payload &key)
  (%async-ws-send-frame connection (or payload #()) :type :ping))

(defmethod close-connection ((connection async-ws-connection) &key code reason)
  (declare (ignore reason))
  (bt:with-lock-held ((async-ws-lock connection))
    (unless (async-ws-closed-p connection)
      (setf (async-ws-closed-p connection) t
            (ws-protocol:%connection-ready-state connection) :closing)
      (ignore-errors
        (%async-ws-send-frame connection #() :type :close))
      (ignore-errors
        (close (async-ws-tls-stream connection) :abort t))
      (setf (ws-protocol:%connection-ready-state connection) :closed)))
  t)

(defmethod on-event ((connection async-ws-connection) event handler)
  (setf (gethash event (async-ws-handlers connection)) handler))
