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
;;;
;;; Stream wait (RFC 9113 §5.1.2): a full mux is out of the pool, so a request
;;; finds it with FIND-FULL-H2-MUX and queues (FIFO, at most *H2-MAX-STREAM-WAITERS*)
;;; for its next free stream. A freed stream goes to the oldest waiter before the
;;; pool sees it. A waiter that times out (*H2-STREAM-WAIT-TIMEOUT*) dials.

(defvar *h2-max-streams-per-connection* 100
  "Upper bound on concurrent streams per HTTP/2 connection, below the peer's limit.")

(defvar *h2-unknown-peer-max-streams* 1
  "Streams allowed when the http2 library cannot report the peer's
   SETTINGS_MAX_CONCURRENT_STREAMS (http2 < 2.1.2): sequential reuse only.")

(defvar *h2-stream-wait-timeout* 2.0
  "Seconds a request waits for a stream on a full HTTP/2 connection to its origin
   before it dials another connection. NIL or 0: dial at once.")

(defvar *h2-max-stream-waiters* 32
  "Requests that may queue on one full HTTP/2 connection; the next one dials.")

(defstruct (h2-stream-waiter (:constructor make-h2-stream-waiter (fn)))
  "A request queued for a stream. FN gets the mux (a stream is free for it) or
   NIL (timed out, or the connection died): dial."
  fn timer (done nil))

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
   (waiters :initform '() :accessor h2-mux-waiters
            :documentation "H2-STREAM-WAITERs, oldest first.")
   (reserved :initform 0 :accessor h2-mux-reserved
             :documentation "Free streams promised to waiters not yet run.")
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
       (< (+ (h2-mux-active-count mux) (h2-mux-reserved mux))
          (h2-mux-peer-max-streams mux))))

;;; Full muxes by h2 pool key, so a request can wait on one (the pool only
;;; holds muxes with a free stream). Event loops may run on several threads.

(defvar *h2-full-muxes* (make-hash-table :test #'equal))
(defvar *h2-full-muxes-lock* (bt:make-lock "h2-full-muxes"))

(defun %h2-track-full (mux)
  "Record MUX under its key while it is open, pooled and full; forget it otherwise."
  (let ((key (h2-mux-key mux))
        (full (and (h2-mux-pool mux)
                   (h2-mux-usable-p mux)
                   (not (h2-mux-has-capacity-p mux)))))
    (when key
      (bt:with-lock-held (*h2-full-muxes-lock*)
        (let ((muxes (remove mux (gethash key *h2-full-muxes*))))
          (when full (push mux muxes))
          (if muxes
              (setf (gethash key *h2-full-muxes*) muxes)
              (remhash key *h2-full-muxes*)))))))

(defun find-full-h2-mux (pool key event-loop)
  "A full, usable mux of POOL under h2 KEY on EVENT-LOOP with room in its wait
   queue (the shortest queue), or NIL."
  (let ((candidates
          (bt:with-lock-held (*h2-full-muxes-lock*)
            (copy-list (gethash key *h2-full-muxes*)))))
    (loop with best = nil
          for mux in candidates
          when (and (eq (h2-mux-pool mux) pool)
                    (eq (async-h2-conn-event-loop mux) event-loop)
                    (eq (h2-mux-state mux) :open)
                    (h2-mux-usable-p mux)
                    (< (length (h2-mux-waiters mux)) *h2-max-stream-waiters*)
                    (or (null best)
                        (< (length (h2-mux-waiters mux)) (length (h2-mux-waiters best)))))
            do (setf best mux)
          finally (return best))))

(defmethod connection-alive-p ((mux h2-mux))
  (and (h2-mux-usable-p mux)
       (async-h2-conn-socket mux)
       (ignore-errors (usocket:socket (async-h2-conn-socket mux)))))

(defmethod pool-discard ((pool lru-connection-pool) (mux h2-mux))
  "Evicted or refused: stop taking streams; close once the open ones finish."
  (declare (ignore pool))
  (setf (h2-mux-pooled-p mux) nil)
  (h2-mux-release-waiters mux)
  (if (h2-mux-users mux)
      (when (eq (h2-mux-state mux) :open)
        (setf (h2-mux-state mux) :draining))
      (h2-mux-close mux))
  nil)

(defun h2-mux-repool (mux)
  "Put MUX back in its pool while it can take another stream."
  (%h2-track-full mux)
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

(defgeneric h2-mux-read-pending-p (mux)
  (:documentation "True when the transport holds octets the socket will not
   signal (TLS records decrypted but not yet read).")
  (:method ((mux h2-mux))
    (tls-pending-p (async-h2-conn-tls mux))))

(defgeneric h2-mux-write (mux octets start end)
  (:documentation "Write OCTETS[START,END). → count, or (values NIL want).")
  (:method ((mux h2-mux) octets start end)
    (if (async-h2-conn-tls mux)
        (tls-write-octets (async-h2-conn-tls mux) octets start end)
        (values (socket-send-octets (async-h2-conn-socket mux) octets start end) nil))))

(defgeneric h2-mux-schedule (mux function)
  (:documentation "Call FUNCTION from MUX's event loop soon, not from the caller.")
  (:method ((mux h2-mux) function)
    (sleep* (h2-mux-event-backend mux) (async-h2-conn-event-loop mux) 0
            :callback function)))

(defgeneric h2-mux-start-timer (mux seconds function)
  (:documentation "Call FUNCTION after SECONDS on MUX's event loop. → a handle.")
  (:method ((mux h2-mux) seconds function)
    (sleep* (h2-mux-event-backend mux) (async-h2-conn-event-loop mux) seconds
            :callback function)))

(defgeneric h2-mux-cancel-timer (mux timer)
  (:method ((mux h2-mux) timer)
    (when timer
      (ignore-errors (cancel (h2-mux-event-backend mux) timer)))))

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
    (%h2-track-full mux)
    (setf (h2-mux-reserved mux) 0)
    (h2-mux-release-waiters mux)
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
   hand the stream to a waiter, or re-pool, drain or idle MUX."
  (setf (h2-mux-users mux)
        (remove stream (h2-mux-users mux) :key #'h2-mux-user-stream))
  (unless (eq (h2-mux-state mux) :dead)
    (when reset
      (h2-mux-reset-stream mux stream))
    (h2-mux-settle mux)))

(defun h2-mux-settle (mux)
  "After a stream left MUX: grant free streams to waiters, then re-pool, drain
   or idle it."
  (unless (eq (h2-mux-state mux) :dead)
    (h2-mux-grant-waiters mux)
    (cond
      ((and (null (h2-mux-users mux))
            (zerop (h2-mux-reserved mux))
            (or (not (eq (h2-mux-state mux) :open))
                (null (h2-mux-pool mux))
                (not (h2-mux-usable-p mux))))
       (h2-mux-flush-then-close mux))
      (t
       (h2-mux-repool mux)
       (h2-mux-kick mux)))))

(defun h2-mux-wait-for-stream (mux fn &key (timeout *h2-stream-wait-timeout*))
  "Queue FN for MUX's next free stream. T when queued; NIL (do not wait) when
   MUX is unusable, its queue is full, or TIMEOUT is NIL/0. FN runs once, from
   the event loop: with MUX when a stream is free for it, with NIL after
   TIMEOUT seconds or when MUX dies."
  (when (and timeout (plusp timeout)
             (h2-mux-usable-p mux)
             (< (length (h2-mux-waiters mux)) *h2-max-stream-waiters*))
    (let ((waiter (make-h2-stream-waiter fn)))
      (setf (h2-stream-waiter-timer waiter)
            (h2-mux-start-timer
             mux timeout
             (lambda ()
               (unless (h2-stream-waiter-done waiter)
                 (setf (h2-stream-waiter-done waiter) t
                       (h2-mux-waiters mux) (remove waiter (h2-mux-waiters mux)))
                 (funcall fn nil)))))
      (setf (h2-mux-waiters mux) (append (h2-mux-waiters mux) (list waiter)))
      t)))

(defun %h2-take-waiter (mux)
  (let ((waiter (pop (h2-mux-waiters mux))))
    (setf (h2-stream-waiter-done waiter) t)
    (h2-mux-cancel-timer mux (h2-stream-waiter-timer waiter))
    waiter))

(defun h2-mux-grant-waiters (mux)
  "Promise each free stream to the oldest waiter; it runs on the next tick. A
   waiter that no longer wants it (request canceled) passes it on."
  (loop while (and (h2-mux-waiters mux) (h2-mux-has-capacity-p mux))
        do (let ((waiter (%h2-take-waiter mux)))
             (incf (h2-mux-reserved mux))
             (h2-mux-schedule
              mux
              (lambda ()
                (if (eq (h2-mux-state mux) :dead)
                    ;; closed since: H2-MUX-CLOSE dropped the promise
                    (funcall (h2-stream-waiter-fn waiter) nil)
                    (progn
                      (decf (h2-mux-reserved mux))
                      (funcall (h2-stream-waiter-fn waiter)
                               (and (h2-mux-usable-p mux) mux))
                      (h2-mux-settle mux))))))))

(defun h2-mux-release-waiters (mux)
  "MUX takes no more streams: every waiter dials (FN with NIL), on the next tick."
  (loop while (h2-mux-waiters mux)
        do (let ((fn (h2-stream-waiter-fn (%h2-take-waiter mux))))
             (h2-mux-schedule mux (lambda () (funcall fn nil))))))

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
      ((h2-mux-users mux)
       (h2-mux-arm mux :read)
       ;; bytes already inside TLS will never make the socket readable
       (when (h2-mux-read-pending-p mux)
         (h2-mux-do-read mux)))
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
  (let ((pkg (symbol-package (type-of condition)))
        (inner (%h2-inner-error condition)))
    (cond
      ((and pkg (search "HTTP2" (package-name pkg)) inner)
       ;; DECOMPRESSION-FAILED wraps whatever broke inside the header block
       (format nil "HTTP/2 ~(~A~): ~A" (type-of condition) (%h2-condition-message inner)))
      ((and pkg (search "HTTP2" (package-name pkg)))
       (format nil "HTTP/2 ~(~A~)" (type-of condition)))
      (t
       (or (ignore-errors (princ-to-string condition))
           (format nil "~(~A~)" (type-of condition)))))))

(defun %h2-inner-error (condition)
  "The error an http2 condition wraps (DECOMPRESSION-FAILED :INTERNAL-ERROR), or NIL."
  (let ((reader (find-symbol "GET-INTERNAL-ERROR" :http2/core)))
    (and reader (fboundp reader)
         (let ((inner (ignore-errors (funcall reader condition))))
           (and (typep inner 'condition) (not (eq inner condition)) inner)))))

(defun %arm-want (mux want)
  (h2-mux-arm mux (ecase want (:want-read :read) (:want-write :write))))

(defun h2-mux-flush-out (mux)
  "Write the queued output. T when all of it went out; NIL when the socket would
   block (the mux is then armed for the direction TLS wants)."
  (loop
    (let ((out (h2-mux-out mux)))
      (when (or (null out) (>= (h2-mux-out-pos mux) (length out)))
        (setf (h2-mux-out mux) nil (h2-mux-out-pos mux) 0)
        (return t))
      (multiple-value-bind (n want)
          (h2-mux-write mux out (h2-mux-out-pos mux) (length out))
        (cond
          (want (%arm-want mux want) (return nil))
          ((or (null n) (zerop n)) (h2-mux-arm mux :write) (return nil))
          (t (incf (h2-mux-out-pos mux) n)))))))

(defun h2-mux-do-write (mux)
  (when (h2-mux-flush-out mux)
    (h2-mux-kick mux)))

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
           ;; Send WINDOW_UPDATE / SETTINGS ACK now, then keep reading: TLS may
           ;; hold more records that will not make the socket readable again.
           (when (and (h2-mux-queue-out mux)
                      (not (h2-mux-flush-out mux)))
             (return))))))))
