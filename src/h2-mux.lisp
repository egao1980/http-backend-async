(in-package #:http-backend-async)

;;; Multiplexed HTTP/2 pool entry (RFC 9113 §5.1.2).
;;;
;;; One H2-MUX owns a TLS socket and its http2 session. Requests open streams on it
;;; concurrently, up to the peer's SETTINGS_MAX_CONCURRENT_STREAMS (capped by
;;; *H2-MAX-STREAMS-PER-CONNECTION*). The mux reads and writes the socket; each
;;; request only sees its stream's hooks (ASYNC-H2-STREAM-HOOKS) and its H2-MUX-USER.
;;;
;;; Pool protocol: a mux with spare streams sits in the pool under the |h2 key.
;;; ACQUIRE-POOLED-CONNECTION takes it out, the request opens its stream, and
;;; H2-MUX-REPOOL puts it back while capacity remains. A finishing stream re-pools
;;; it. Eviction or a loop mismatch drains it: open streams finish, then it closes.

(defvar *h2-max-streams-per-connection* 100
  "Upper bound on concurrent streams per HTTP/2 connection, below the peer's limit.")

(defvar *h2-unknown-peer-max-streams* 1
  "Streams allowed when the http2 library cannot report the peer's
   SETTINGS_MAX_CONCURRENT_STREAMS (http2 < 2.1.2): sequential reuse only.")

(defstruct (h2-mux-user (:constructor make-h2-mux-user (stream on-error pump-body)))
  "A request's stream on a mux. ON-ERROR (condition) fails the request;
   PUMP-BODY () queues more upload DATA and returns T when it did."
  stream on-error pump-body)

(defclass h2-mux (async-pooled-h2-connection)
  ((event-backend :initarg :event-backend :accessor h2-mux-event-backend)
   (io :initform nil :accessor h2-mux-io
       :documentation "REGISTER-IO handle on the socket, or NIL while idle.")
   (io-dir :initform nil :accessor h2-mux-io-dir)
   (io-trampoline :initform nil :accessor h2-mux-io-trampoline
                  :documentation "Set when the dialing request's IO handle was handed over:
its ON-IO then forwards here (no cancel + re-register of a live fd).")
   (out :initform nil :accessor h2-mux-out)
   (out-pos :initform 0 :accessor h2-mux-out-pos)
   (recv-buf :initform (make-array 65536 :element-type '(unsigned-byte 8))
             :reader h2-mux-recv-buf)
   (users :initform '() :accessor h2-mux-users)
   (state :initform :open :accessor h2-mux-state
          :type (member :open :draining :dead))
   (pool :initarg :pool :initform nil :accessor h2-mux-pool)
   (key :initarg :key :initform nil :accessor h2-mux-key)
   (pooled-p :initform nil :accessor h2-mux-pooled-p)
   (peak-streams :initform 0 :accessor h2-mux-peak-streams
                 :documentation "Most streams open at once (diagnostics, tests)."))
  (:documentation "Pooled HTTP/2 connection carrying several concurrent streams."))

(defvar *h2-mux-count* 0
  "Multiplexed HTTP/2 connections created (diagnostics, tests).")

(defun make-h2-mux (socket &key tls session pump event-backend event-loop pool key)
  (incf *h2-mux-count*)
  (make-instance 'h2-mux :socket socket :tls tls :https t
                         :session session :pump pump
                         :event-backend event-backend :event-loop event-loop
                         :pool pool :key key))

(defun h2-mux-connection (mux)
  (async-h2-session-connection (async-h2-conn-session mux)))

(defun h2-mux-peer-max-streams (mux)
  "The peer's SETTINGS_MAX_CONCURRENT_STREAMS, capped locally. NIL from the library
   means none announced yet: unlimited per RFC 9113 §6.5.2."
  (let ((reader (find-symbol "GET-PEER-MAX-CONCURRENT-STREAMS" :http2/core)))
    (if (and reader (fboundp reader))
        (let ((limit (funcall reader (h2-mux-connection mux))))
          (if limit
              (min limit *h2-max-streams-per-connection*)
              *h2-max-streams-per-connection*))
        (min *h2-unknown-peer-max-streams* *h2-max-streams-per-connection*))))

(defun h2-mux-active-count (mux)
  (length (h2-mux-users mux)))

(defun h2-mux-usable-p (mux)
  (and (eq (h2-mux-state mux) :open)
       (async-h2-conn-session mux)
       (not (h2-connection-saw-goaway-p (h2-mux-connection mux)))))

(defun h2-mux-has-capacity-p (mux)
  (and (h2-mux-usable-p mux)
       (< (h2-mux-active-count mux) (h2-mux-peer-max-streams mux))))

(defmethod connection-alive-p ((mux h2-mux))
  (and (h2-mux-usable-p mux)
       (async-h2-conn-socket mux)
       (ignore-errors (usocket:socket (async-h2-conn-socket mux)))))

(defmethod pool-discard ((pool lru-connection-pool) (mux h2-mux))
  "Evicted or refused: stop taking streams; close once the open ones finish."
  (declare (ignore pool))
  (setf (h2-mux-pooled-p mux) nil)
  (if (h2-mux-users mux)
      (when (eq (h2-mux-state mux) :open)
        (setf (h2-mux-state mux) :draining))
      (h2-mux-close mux))
  nil)

(defun h2-mux-repool (mux)
  "Put MUX back in its pool while it can take another stream."
  (when (and (h2-mux-pool mux)
             (not (h2-mux-pooled-p mux))
             (h2-mux-has-capacity-p mux))
    (setf (h2-mux-pooled-p mux) t)
    (pool-release (h2-mux-pool mux) (h2-mux-key mux) mux)))

;;; Transport (specialized by tests to run without sockets)

(defgeneric h2-mux-read (mux buffer)
  (:documentation "Read into BUFFER. → count, 0 on EOF, or (values NIL want).")
  (:method ((mux h2-mux) buffer)
    (if (async-h2-conn-tls mux)
        (tls-read-octets (async-h2-conn-tls mux) buffer)
        (values (socket-recv-octets (async-h2-conn-socket mux) buffer) nil))))

(defgeneric h2-mux-write (mux octets start end)
  (:documentation "Write OCTETS[START,END). → count, or (values NIL want).")
  (:method ((mux h2-mux) octets start end)
    (if (async-h2-conn-tls mux)
        (tls-write-octets (async-h2-conn-tls mux) octets start end)
        (values (socket-send-octets (async-h2-conn-socket mux) octets start end) nil))))

(defgeneric h2-mux-arm (mux direction)
  (:documentation "Register or update interest in DIRECTION (:read or :write).")
  (:method ((mux h2-mux) direction)
    (let ((io (h2-mux-io mux)))
      (cond
        ((and io (eq (h2-mux-io-dir mux) direction)) nil)
        (io
         (update-io (h2-mux-event-backend mux) io direction)
         (setf (h2-mux-io-dir mux) direction))
        (t
         (setf (h2-mux-io-dir mux) direction
               (h2-mux-io mux)
               (register-io (h2-mux-event-backend mux) (async-h2-conn-event-loop mux)
                            (socket-fd (async-h2-conn-socket mux)) direction
                            (lambda (status) (h2-mux-on-io mux status)))))))))

(defgeneric h2-mux-disarm (mux)
  (:documentation "Drop socket interest (idle mux: the loop may quiesce).")
  (:method ((mux h2-mux))
    (when-let ((io (h2-mux-io mux)))
      (ignore-errors (cancel (h2-mux-event-backend mux) io)))
    (setf (h2-mux-io mux) nil
          (h2-mux-io-dir mux) nil
          (h2-mux-io-trampoline mux) nil)))

(defgeneric h2-mux-close-transport (mux)
  (:method ((mux h2-mux))
    (tls-close (async-h2-conn-tls mux))
    (close-socket (async-h2-conn-socket mux))))

(defun h2-mux-adopt-io (mux io direction)
  "Take over the dialing request's IO handle IO (currently armed for DIRECTION).
   Returns the trampoline that request's ON-IO must call."
  (setf (h2-mux-io mux) io
        (h2-mux-io-dir mux) direction
        (h2-mux-io-trampoline mux) (lambda (status) (h2-mux-on-io mux status))))

;;; Life cycle

(defun h2-mux-close (mux)
  (unless (eq (h2-mux-state mux) :dead)
    (setf (h2-mux-state mux) :dead
          (h2-mux-pooled-p mux) nil)
    (h2-mux-disarm mux)
    (h2-mux-close-transport mux)
    (setf (async-h2-conn-alive-p mux) nil
          (async-h2-conn-tls mux) nil
          (async-h2-conn-socket mux) nil
          (async-h2-conn-session mux) nil
          (async-h2-conn-pump mux) nil)))

(defun h2-mux-die (mux condition)
  "Connection failure: close MUX and fail every request still on it."
  (let ((users (h2-mux-users mux)))
    (setf (h2-mux-users mux) '())
    (h2-mux-close mux)
    (dolist (u users)
      (ignore-errors (funcall (h2-mux-user-on-error u) condition)))))

(defun h2-mux-open-stream (mux on-error pump-body method uri headers &key (end-stream t) body)
  "Open a request stream on MUX. ON-ERROR / PUMP-BODY: see H2-MUX-USER.
   Returns the http2 stream; the caller installs its hooks, then calls H2-MUX-KICK."
  (unless (h2-mux-has-capacity-p mux)
    (error 'http-connection-error :message "HTTP/2 connection has no free stream"))
  (let ((stream (h2-open-request (async-h2-conn-session mux) method uri headers
                                 :end-stream end-stream :body body)))
    (push (make-h2-mux-user stream on-error pump-body) (h2-mux-users mux))
    (setf (h2-mux-peak-streams mux) (max (h2-mux-peak-streams mux) (h2-mux-active-count mux)))
    stream))

(defun h2-mux-reset-stream (mux stream)
  "RST_STREAM CANCEL for a stream we abandon (timeout, cancel, error)."
  (let ((conn (h2-mux-connection mux))
        (rst (find-symbol "WRITE-RST-STREAM-FRAME" :http2/core))
        (cancel-code (symbol-value (find-symbol "+CANCEL+" :http2/core)))
        (close-stream (find-symbol "CLOSE-HTTP2-STREAM" :http2/core)))
    (when (member stream (%h2-connection-streams conn))
      (ignore-errors (funcall rst stream cancel-code))
      (ignore-errors (funcall close-stream stream 'cancelled)))))

(defun h2-mux-stream-done (mux stream &key reset)
  "A request is finished with STREAM: reset it if asked (or still open), then
   re-pool, drain or idle MUX."
  (setf (h2-mux-users mux)
        (remove stream (h2-mux-users mux) :key #'h2-mux-user-stream))
  (unless (eq (h2-mux-state mux) :dead)
    (when reset
      (h2-mux-reset-stream mux stream))
    (cond
      ((and (null (h2-mux-users mux))
            (or (not (eq (h2-mux-state mux) :open))
                (null (h2-mux-pool mux))
                (not (h2-mux-usable-p mux))))
       (h2-mux-flush-then-close mux))
      (t
       (h2-mux-repool mux)
       (h2-mux-kick mux)))))

(defun h2-mux-flush-then-close (mux)
  (let ((pump (async-h2-conn-pump mux)))
    (if (or (h2-mux-out mux) (and pump (h2-pump-pending-out-p pump)))
        (progn (setf (h2-mux-state mux) :draining)
               (h2-mux-kick mux))
        (h2-mux-close mux))))

(defun h2-mux-ready-for-stream-p (mux)
  "Drain frames already buffered on an idle pooled MUX, then check it can take a stream."
  (and (h2-mux-usable-p mux)
       (handler-case (progn (h2-mux-process mux) t)
         (serious-condition (e)
           (h2-mux-die mux (make-condition 'http-connection-error
                                           :message (%h2-condition-message e)))
           nil))
       (h2-mux-has-capacity-p mux)))

;;; IO

(defun h2-mux-queue-out (mux)
  "Move pending session output to the socket write buffer. T when something is queued."
  (let ((pump (async-h2-conn-pump mux)))
    (when (and pump (h2-pump-pending-out-p pump))
      (let ((more (h2-pump-take-out pump)))
        (setf (h2-mux-out mux)
              (if (and (h2-mux-out mux) (< (h2-mux-out-pos mux) (length (h2-mux-out mux))))
                  (concatenate '(vector (unsigned-byte 8))
                               (subseq (h2-mux-out mux) (h2-mux-out-pos mux)) more)
                  more)
              (h2-mux-out-pos mux) 0)))
    (and (h2-mux-out mux) t)))

(defun h2-mux-pump-bodies (mux)
  (dolist (u (h2-mux-users mux))
    (when-let ((fn (h2-mux-user-pump-body u)))
      (loop repeat 4 while (funcall fn)))))

(defun h2-mux-kick (mux)
  "Queue uploads and pending frames, then arm the socket for the next step."
  (unless (eq (h2-mux-state mux) :dead)
    (h2-mux-pump-bodies mux)
    (cond
      ((h2-mux-queue-out mux) (h2-mux-arm mux :write))
      ((h2-mux-users mux) (h2-mux-arm mux :read))
      ((eq (h2-mux-state mux) :draining) (h2-mux-close mux))
      (t (h2-mux-disarm mux)))))

(defun h2-mux-on-io (mux status)
  (unless (eq (h2-mux-state mux) :dead)
    (handler-case
        (if (eq status :error)
            (error 'http-connection-error :message "register-io error")
            (if (h2-mux-out mux)
                (h2-mux-do-write mux)
                (h2-mux-do-read mux)))
      (http-error (e) (h2-mux-die mux e))
      (serious-condition (e)
        (h2-mux-die mux (make-condition 'http-connection-error
                                        :message (%h2-condition-message e)))))))

(defun %h2-condition-message (condition)
  "Text for CONDITION. http2 conditions are named, not printed: some of their
   report functions read unbound slots (memory fault under safety 0)."
  (let ((pkg (symbol-package (type-of condition))))
    (if (and pkg (search "HTTP2" (package-name pkg)))
        (format nil "HTTP/2 ~(~A~)" (type-of condition))
        (or (ignore-errors (princ-to-string condition))
            (format nil "~(~A~)" (type-of condition))))))

(defun %arm-want (mux want)
  (h2-mux-arm mux (ecase want (:want-read :read) (:want-write :write))))

(defun h2-mux-do-write (mux)
  (loop
    (let ((out (h2-mux-out mux)))
      (when (or (null out) (>= (h2-mux-out-pos mux) (length out)))
        (setf (h2-mux-out mux) nil (h2-mux-out-pos mux) 0)
        (return (h2-mux-kick mux)))
      (multiple-value-bind (n want)
          (h2-mux-write mux out (h2-mux-out-pos mux) (length out))
        (cond
          (want (return (%arm-want mux want)))
          ((or (null n) (zerop n)) (return (h2-mux-arm mux :write)))
          (t (incf (h2-mux-out-pos mux) n)))))))

(defun h2-mux-process (mux)
  "Parse every complete frame buffered on MUX. Stream errors fail only their request;
   a request whose stream vanished without completing (peer RST_STREAM) is failed too."
  (let ((session (async-h2-conn-session mux)))
    (loop
      (when (eq (h2-mux-state mux) :dead)
        (return-from h2-mux-process nil))
      (let ((stream-error (find-symbol "HTTP-STREAM-ERROR" :http2/core))
            (get-stream (find-symbol "GET-STREAM" :http2/core)))
        (handler-case
            (unless (h2-process-pending session)
              (return))
          (serious-condition (e)
            (unless (and stream-error (typep e stream-error))
              (error e))
            ;; The frame was consumed whole: resume at the next frame header.
            (setf (async-h2-session-parse-fn session) (%h2-parse-frame-header-fn)
                  (async-h2-session-parse-need session) 9)
            (h2-mux-fail-stream mux (ignore-errors (funcall get-stream e))
                                (%h2-condition-message e))))))
    (when (eq (h2-mux-state mux) :dead)
      (return-from h2-mux-process nil))
    (let ((live (%h2-connection-streams (h2-mux-connection mux))))
      (dolist (u (h2-mux-users mux))
        (unless (or (member (h2-mux-user-stream u) live)
                    (let ((s (h2-mux-user-stream u)))
                      (and (typep s 'async-h2-stream-hooks) (h2-stream-ended-p s))))
          (h2-mux-fail-stream mux (h2-mux-user-stream u) "HTTP/2 stream reset by peer"))))))

(defun h2-mux-fail-stream (mux stream message)
  (let ((u (find stream (h2-mux-users mux) :key #'h2-mux-user-stream)))
    (when u
      (setf (h2-mux-users mux) (remove u (h2-mux-users mux)))
      (ignore-errors
       (funcall (h2-mux-user-on-error u)
                (make-condition 'http-connection-error :message message))))))

(defun h2-mux-do-read (mux)
  (let ((buf (h2-mux-recv-buf mux)))
    (loop
      (multiple-value-bind (n want) (h2-mux-read mux buf)
        (cond
          (want (return (%arm-want mux want)))
          ((null n) (return (h2-mux-kick mux)))
          ((zerop n)
           (return (h2-mux-die mux (make-condition 'http-connection-error
                                                   :message "HTTP/2 EOF"))))
          (t
           (h2-pump-feed-in (async-h2-conn-pump mux) buf n)
           (h2-mux-process mux)
           (when (eq (h2-mux-state mux) :dead)
             (return))
           (when (h2-mux-queue-out mux)
             (return (h2-mux-do-write mux)))))))))
