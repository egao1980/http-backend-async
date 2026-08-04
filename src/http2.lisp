(in-package #:http-backend-async)

;;; HTTP/2 wire (RFC 9113) — backend responsibility.
;;;
;;; Protocol (http-protocol) owns: :http-version preference, ALPN offer/map
;;; (RFC 7301), header-field policy (RFC 9113 §8.2–8.3 via
;;; MAKE-HTTP2-REQUEST-HEADERS).
;;;
;;; This file owns: TLS ALPN application, client preface/SETTINGS, HPACK/frames
;;; (via zellerin/http2), nonblocking pump over event-protocol I/O.
;;;
;;; Network I/O stays nonblocking: gray pump buffers http2 reads/writes;
;;; the event loop flushes OUT → TLS and feeds IN from TLS reads.
;;; BACKEND-HTTP-VERSIONS for ASYNC-BACKEND is specialized in backend.lisp.

(defvar *http2-loaded* nil)
(defvar *h2-classes-ready* nil)

(defun ensure-http2 ()
  "Load http2/client (soft) and define async stream classes. Returns T when available.

   Load http2/client — not the umbrella http2 system — so Windows CI does not
   pull http2/server/poll (cffi-grovel needs poll.h)."
  (or *http2-loaded*
      (setf *http2-loaded*
            (and (ignore-errors (asdf:load-system "http2/client") t) t)))
  (when (and *http2-loaded* (not *h2-classes-ready*))
    (%ensure-h2-classes)
    (setf *h2-classes-ready* t))
  *http2-loaded*)

(defun %ensure-h2-classes ()
  "CLIENT-STREAM + header/body collectors — no utf8/gzip mixins (CE is ours).
   Also define ASYNC-H2-CLIENT-CONNECTION to track RFC 8441 SETTINGS."
  (let ((client-stream (find-symbol "CLIENT-STREAM" :http2/client))
        (header-m (find-symbol "HEADER-COLLECTING-MIXIN" :http2/core))
        (body-m (find-symbol "BODY-COLLECTING-MIXIN" :http2/core))
        (peer-ends (find-symbol "PEER-ENDS-HTTP-STREAM" :http2/core))
        (client-done (find-symbol "CLIENT-DONE" :http2/client))
        (vanilla (find-symbol "VANILLA-CLIENT-CONNECTION" :http2/client))
        (set-peer (find-symbol "SET-PEER-SETTING" :http2/core)))
    (unless (and client-stream header-m body-m peer-ends client-done vanilla)
      (error 'http-version-not-available
             :requested :http/2
             :message "http2 missing CLIENT-STREAM / mixins"))
    (unless (find-class 'async-h2-client-stream nil)
      (eval `(defclass async-h2-client-stream
                 (,client-stream ,header-m ,body-m) ())))
    (eval `(defmethod ,peer-ends ((stream async-h2-client-stream))
             (signal ',client-done :result stream)))
    (unless (find-class 'async-h2-client-connection nil)
      (eval `(defclass async-h2-client-connection (,vanilla)
               ((enable-connect-protocol-p
                 :initform nil
                 :accessor h2-enable-connect-protocol-p))
               (:documentation
                "Client connection that records SETTINGS_ENABLE_CONNECT_PROTOCOL
                 (RFC 8441) for future Extended CONNECT WebSocket."))))
    (when set-peer
      (eval `(defmethod ,set-peer ((connection async-h2-client-connection)
                                   (name (eql :enable-connect-protocol))
                                   value)
               (setf (h2-enable-connect-protocol-p connection)
                     (plusp value)))))))

(defclass async-h2-pump-stream
    (trivial-gray-streams:fundamental-binary-input-stream
     trivial-gray-streams:fundamental-binary-output-stream)
  ((out :initform (make-array 4096 :element-type '(unsigned-byte 8)
                              :adjustable t :fill-pointer 0)
        :accessor h2-pump-out)
   (in :initform (make-array 4096 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0)
       :accessor h2-pump-in)
   (in-pos :initform 0 :accessor h2-pump-in-pos))
  (:documentation "Two buffers between http2 and the async TLS/socket pump."))

(defmethod stream-element-type ((s async-h2-pump-stream))
  '(unsigned-byte 8))

(defmethod close ((s async-h2-pump-stream) &key abort)
  (declare (ignore abort))
  t)

(defmethod trivial-gray-streams:stream-finish-output ((s async-h2-pump-stream))
  nil)

(defmethod trivial-gray-streams:stream-force-output ((s async-h2-pump-stream))
  nil)

(defmethod trivial-gray-streams:stream-listen ((s async-h2-pump-stream))
  (< (h2-pump-in-pos s) (fill-pointer (h2-pump-in s))))

(defmethod trivial-gray-streams:stream-read-byte ((s async-h2-pump-stream))
  (if (< (h2-pump-in-pos s) (fill-pointer (h2-pump-in s)))
      (prog1 (aref (h2-pump-in s) (h2-pump-in-pos s))
        (incf (h2-pump-in-pos s)))
      :eof))

(defmethod trivial-gray-streams:stream-read-sequence
    ((s async-h2-pump-stream) seq start end &key)
  (let* ((avail (- (fill-pointer (h2-pump-in s)) (h2-pump-in-pos s)))
         (n (min avail (- end start))))
    (when (plusp n)
      (replace seq (h2-pump-in s)
               :start1 start :end1 (+ start n)
               :start2 (h2-pump-in-pos s))
      (incf (h2-pump-in-pos s) n))
    (+ start n)))

(defmethod trivial-gray-streams:stream-write-byte ((s async-h2-pump-stream) byte)
  (vector-push-extend byte (h2-pump-out s))
  byte)

(defmethod trivial-gray-streams:stream-write-sequence
    ((s async-h2-pump-stream) seq start end &key)
  (let ((out (h2-pump-out s))
        (n (- end start)))
    (let ((old (fill-pointer out)))
      (adjust-array out (+ old n) :fill-pointer (+ old n))
      (replace out seq :start1 old :start2 start :end2 end)))
  seq)

(defun h2-pump-pending-out-p (pump)
  (plusp (fill-pointer (h2-pump-out pump))))

(defun h2-pump-take-out (pump)
  "Return OUT octets and clear the write buffer."
  (let* ((out (h2-pump-out pump))
         (octets (copy-seq out)))
    (setf (fill-pointer out) 0)
    octets))

(defun h2-pump-feed-in (pump octets &optional (n (length octets)))
  "Append N octets from OCTETS into the read buffer (compact first if needed)."
  (let ((in (h2-pump-in pump))
        (pos (h2-pump-in-pos pump)))
    (when (and (plusp pos) (>= pos (floor (length in) 2)))
      (replace in in :start2 pos)
      (decf (fill-pointer in) pos)
      (setf (h2-pump-in-pos pump) 0
            pos 0))
    (let ((old (fill-pointer in)))
      (adjust-array in (+ old n) :fill-pointer (+ old n))
      (replace in octets :start1 old :end2 n)))
  n)

(defun %h2-headers-alist (headers-ht-or-alist)
  "Normalize response header alist → lowercase string keys."
  (mapcar (lambda (pair)
            (cons (string-downcase (string (car pair)))
                  (princ-to-string (cdr pair))))
          headers-ht-or-alist))

(defun %h2-headers-table (alist)
  (let ((ht (make-hash-table :test #'equal)))
    (dolist (pair alist ht)
      (setf (gethash (car pair) ht) (cdr pair)))))

(defun %http2-headers-for-lib (header-fields)
  "Convert MAKE-HTTP2-REQUEST-HEADERS alist → http2 library ((name value)…)."
  (mapcar (lambda (pair) (list (car pair) (cdr pair))) header-fields))

(defstruct (async-h2-session (:constructor %make-async-h2-session))
  connection
  pump
  (parse-fn nil)
  (parse-need 9 :type (integer 0 *)))

(defun h2-session-enable-connect-protocol-p (session)
  "True if peer advertised SETTINGS_ENABLE_CONNECT_PROTOCOL=1 (RFC 8441)."
  (let ((conn (async-h2-session-connection session)))
    (and (typep conn 'async-h2-client-connection)
         (h2-enable-connect-protocol-p conn))))

(defun %h2-parse-frame-header-fn ()
  "Function object for http2 PARSE-FRAME-HEADER (not the symbol)."
  (symbol-function (find-symbol "PARSE-FRAME-HEADER" :http2/core)))

(defun make-async-h2-session (pump &key (stream-class 'async-h2-client-stream))
  "Create http2 client connection + incremental parse state over PUMP.

   ASYNC-H2-CLIENT-CONNECTION writes the client preface + SETTINGS (RFC 9113 §3.4)
   into PUMP immediately and tracks ENABLE_CONNECT_PROTOCOL for RFC 8441.
   STREAM-CLASS defaults to ASYNC-H2-CLIENT-STREAM; WS uses ASYNC-H2-WS-STREAM."
  (unless (ensure-http2)
    (error 'http-version-not-available
           :requested :http/2
           :negotiated nil
           :message "http2 system not loadable"))
  (let ((conn (make-instance 'async-h2-client-connection
                             :network-stream pump
                             :stream-class stream-class)))
    (%make-async-h2-session :connection conn
                            :pump pump
                            :parse-fn (%h2-parse-frame-header-fn)
                            :parse-need 9)))

(defun make-async-h2-connection (pump)
  "Deprecated alias — returns the http2 connection object only."
  (async-h2-session-connection (make-async-h2-session pump)))

(defun h2-open-request (session method uri headers &key (end-stream t) body)
  "Send HEADERS (+ optional BODY) using protocol MAKE-HTTP2-REQUEST-HEADERS.

   RFC 9113 §3.4 — may be called immediately after the client preface (no need
   to wait for peer SETTINGS)."
  (let* ((connection (async-h2-session-connection session))
         (fields (%http2-headers-for-lib
                  (make-http2-request-headers method uri headers)))
         (open (find-symbol "OPEN-HTTP2-STREAM" :http2/core))
         (no-body (or (null body) (zerop (length body))))
         (stream (funcall open connection fields
                          :end-stream (and end-stream no-body)
                          :end-headers t)))
    (when (not no-body)
      (funcall (find-symbol "WRITE-BINARY-PAYLOAD" :http2/core)
               connection stream body :end-stream t))
    stream))

(defun h2-process-pending (session)
  "Incrementally parse complete frames currently buffered on SESSION's pump.

   Unlike http2 PROCESS-PENDING-FRAMES, never reads past available octets
   (safe for nonblocking TLS fills)."
  (let ((done nil)
        (result nil)
        (client-done (find-symbol "CLIENT-DONE" :http2/client))
        (get-result (or (find-symbol "GET-RESULT" :http2/client)
                        (find-symbol "GET-RESULT" :http2/core)))
        (result-slot (find-symbol "RESULT" :http2/client))
        (conn (async-h2-session-connection session))
        (pump (async-h2-session-pump session)))
    (handler-bind ((condition
                     (lambda (c)
                       (when (and client-done (typep c client-done))
                         (setf done t
                               result (cond
                                        (get-result (funcall get-result c))
                                        (result-slot (slot-value c result-slot))
                                        (t nil)))))))
      (loop
        (let* ((need (or (async-h2-session-parse-need session) 0))
               (avail (- (fill-pointer (h2-pump-in pump))
                         (h2-pump-in-pos pump))))
          ;; Empty frames (e.g. SETTINGS length 0) must still invoke the handler.
          (unless (>= avail need)
            (return))
          (let ((buf (if (zerop need)
                         (make-array 0 :element-type '(unsigned-byte 8))
                         (let ((b (make-array need :element-type '(unsigned-byte 8))))
                           (replace b (h2-pump-in pump)
                                    :start2 (h2-pump-in-pos pump)
                                    :end2 (+ (h2-pump-in-pos pump) need))
                           (incf (h2-pump-in-pos pump) need)
                           b))))
            (multiple-value-bind (next size)
                (funcall (async-h2-session-parse-fn session) conn buf)
              (setf (async-h2-session-parse-fn session) next
                    (async-h2-session-parse-need session) (or size 0))
              (when done (return)))))))
    (values done result)))

(defun h2-stream-to-http-parts (h2-stream)
  "Return (values status headers-ht body-octets) from a finished h2 stream."
  (let* ((get-status (find-symbol "GET-STATUS" :http2/client))
         (get-headers (or (find-symbol "GET-HEADERS" :http2/core)
                          (find-symbol "GET-HEADERS" :http2/client)))
         (get-body (find-symbol "GET-BODY" :http2/core))
         (status (parse-integer (funcall get-status h2-stream)))
         (hdrs (%h2-headers-table
                (%h2-headers-alist (funcall get-headers h2-stream))))
         (body (or (funcall get-body h2-stream) #())))
    (values status hdrs
            (if (typep body '(vector (unsigned-byte 8)))
                body
                (babel:string-to-octets (princ-to-string body) :encoding :utf-8)))))
