(in-package #:http-backend-async)

;;; Socket-fed Gray binary input stream for :want-stream (cl-stack#71).
;;; Event-loop thread produces via ASYNC-BODY-FEED; consumer thread/task reads.
;;; Bounded queue → backpressure (ASYNC-BODY-FULL-P) so peak memory stays O(limit).

(defvar *async-body-queue-limit* nil
  "Max buffered octets awaiting read (NIL → 4 × *HTTP-STREAM-BUFFER-SIZE*).")

(defun %async-body-limit ()
  (or *async-body-queue-limit*
      (* 4 *http-stream-buffer-size*)))

(defclass async-body-input-stream
    (trivial-gray-streams:fundamental-binary-input-stream)
  ((lock :initform (bt:make-lock "async-body") :reader async-body-lock)
   (cv :initform (bt:make-condition-variable :name "async-body")
       :reader async-body-cv)
   (chunks :initform (list) :accessor async-body-chunks
           :documentation "List of octet vectors; car is head.")
   (chunk-pos :initform 0 :accessor async-body-chunk-pos)
   (buffered :initform 0 :accessor async-body-buffered)
   (limit :initarg :limit :reader async-body-limit)
   (eof :initform nil :accessor async-body-eof-p)
   (error :initform nil :accessor async-body-error)
   (open :initform t :accessor async-body-open-p)
   (on-space :initform nil :initarg :on-space
             :accessor async-body-on-space
             :documentation "Thunk when buffer drains below limit (unpause IO).
Runs on the reader's thread, once per fill: see WANTS-SPACE.")
   (wants-space :initform nil :accessor async-body-wants-space-p
                :documentation "T once a feed filled the queue; the next read
that drains it below LIMIT clears it and runs ON-SPACE.")
   (finished :initform nil :accessor async-body-finished-p
             :documentation "T once EOF, an error or CLOSE ended the body.")
   (on-finish :initform nil :accessor async-body-on-finish
              :documentation "Thunk run once when the body is FINISHED, on the
thread that ended it. See ASYNC-BODY-WATCH-FINISH.")))

;; SBCL has no STREAM-ELEMENT-TYPE or STREAM-CLEAR-INPUT method for binary
;; Gray streams: a consumer asking the element type before it reads an error
;; body (llm-protocol-openai) got NO-APPLICABLE-METHOD (#74).
(defmethod stream-element-type ((s async-body-input-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-clear-input ((s async-body-input-stream))
  "No-op: a body is not interactive, and dropping queued octets would only
   lose body data."
  nil)

(defun make-async-body-input-stream (&key (limit (%async-body-limit)) on-space)
  (make-instance 'async-body-input-stream :limit limit :on-space on-space))

(defun async-body-full-p (s)
  "True when producer should pause socket reads."
  (>= (async-body-buffered s) (async-body-limit s)))

(defun async-body-feed (s octets &key (start 0) (end (length octets)))
  "Append OCTETS[START:END] for consumers. Returns NIL if stream closed."
  (when (>= start end)
    (return-from async-body-feed t))
  (let ((piece (subseq octets start end)))
    (bt:with-lock-held ((async-body-lock s))
      (unless (async-body-open-p s)
        (return-from async-body-feed nil))
      (when (async-body-error s)
        (return-from async-body-feed nil))
      (setf (async-body-chunks s) (nconc (async-body-chunks s) (list piece)))
      (incf (async-body-buffered s) (length piece))
      (when (>= (async-body-buffered s) (async-body-limit s))
        (setf (async-body-wants-space-p s) t))
      (bt:condition-notify (async-body-cv s)))
    t))

(defun %async-body-take-finish (s)
  "Mark S finished. Lock must be held. Returns the ON-FINISH thunk to run
   (outside the lock), or NIL when S had already finished."
  (unless (async-body-finished-p s)
    (setf (async-body-finished-p s) t)
    (shiftf (async-body-on-finish s) nil)))

(defun %async-body-run-finish (fn)
  (when fn
    (handler-case (funcall fn)
      (error (e) (warn "async body on-finish failed: ~A" e)))))

(defun async-body-watch-finish (s thunk)
  "Run THUNK once when S finishes (EOF, error or CLOSE). Returns T when it is
   registered, NIL when S has already finished (THUNK is then not run)."
  (bt:with-lock-held ((async-body-lock s))
    (unless (async-body-finished-p s)
      (setf (async-body-on-finish s) thunk)
      t)))

(defun async-body-eof (s)
  (%async-body-run-finish
   (bt:with-lock-held ((async-body-lock s))
     (setf (async-body-eof-p s) t)
     (bt:condition-notify (async-body-cv s))
     (%async-body-take-finish s))))

(defun async-body-fail (s condition)
  (%async-body-run-finish
   (bt:with-lock-held ((async-body-lock s))
     (setf (async-body-error s) condition
           (async-body-eof-p s) t)
     (bt:condition-notify (async-body-cv s))
     (%async-body-take-finish s))))

(defun async-body-abort (s condition)
  "Fail S with CONDITION unless it already ended: a body that reached EOF keeps
   its queued octets for the reader."
  (unless (bt:with-lock-held ((async-body-lock s))
            (async-body-eof-p s))
    (async-body-fail s condition)))

(defun %async-body-wait (s)
  "Wait until data, EOF, error, or closed. Lock must be held."
  (loop until (or (async-body-chunks s)
                  (async-body-eof-p s)
                  (async-body-error s)
                  (not (async-body-open-p s)))
        do (bt:condition-wait (async-body-cv s) (async-body-lock s))))

(defun %async-body-take-space (s)
  "Lock held. When a feed filled the queue and it is now below LIMIT, clear
   the mark and return ON-SPACE for the caller to run outside the lock."
  (when (and (async-body-wants-space-p s)
             (< (async-body-buffered s) (async-body-limit s)))
    (setf (async-body-wants-space-p s) nil)
    (async-body-on-space s)))

(defun %async-body-signal-space (fn)
  (when fn
    (ignore-errors (funcall fn))))

(defmethod trivial-gray-streams:stream-read-byte ((s async-body-input-stream))
  (let ((space nil)
        (result :eof))
    (bt:with-lock-held ((async-body-lock s))
      (%async-body-wait s)
      (when (async-body-error s)
        (error (async-body-error s)))
      (let ((chunks (async-body-chunks s)))
        (when (and chunks (async-body-open-p s))
          (let* ((chunk (car chunks))
                 (pos (async-body-chunk-pos s)))
            (setf result (aref chunk pos))
            (incf pos)
            (decf (async-body-buffered s))
            (if (>= pos (length chunk))
                (setf (async-body-chunks s) (cdr chunks)
                      (async-body-chunk-pos s) 0)
                (setf (async-body-chunk-pos s) pos))
            (setf space (%async-body-take-space s))))))
    (%async-body-signal-space space)
    result))

(defun %async-body-take (s seq pos end)
  "Lock held. Copy queued octets into SEQ[POS:END]; return the new POS."
  (loop while (and (< pos end) (async-body-open-p s))
        for chunks = (async-body-chunks s)
        do (unless chunks (return))
           (let* ((chunk (car chunks))
                  (cpos (async-body-chunk-pos s))
                  (n (min (- (length chunk) cpos) (- end pos))))
             (replace seq chunk :start1 pos :end1 (+ pos n)
                      :start2 cpos :end2 (+ cpos n))
             (incf pos n)
             (decf (async-body-buffered s) n)
             (let ((cpos* (+ cpos n)))
               (if (>= cpos* (length chunk))
                   (setf (async-body-chunks s) (cdr chunks)
                         (async-body-chunk-pos s) 0)
                   (setf (async-body-chunk-pos s) cpos*)))))
  pos)

(defmethod trivial-gray-streams:stream-read-sequence
    ((s async-body-input-stream) seq start end &key)
  (let ((pos start)
        (end (or end (length seq))))
    (loop while (< pos end)
          do (let ((space nil)
                   (before pos))
               (bt:with-lock-held ((async-body-lock s))
                 (%async-body-wait s)
                 (when (async-body-error s)
                   (error (async-body-error s)))
                 (setf pos (%async-body-take s seq pos end)
                       space (%async-body-take-space s)))
               (%async-body-signal-space space)
               (when (= pos before)
                 (return))))
    pos))

(defmethod trivial-gray-streams:stream-listen ((s async-body-input-stream))
  (bt:with-lock-held ((async-body-lock s))
    (and (async-body-open-p s)
         (not (null (async-body-chunks s))))))

(defmethod http-protocol:stream-read-available ((s async-body-input-stream) buffer)
  "Buffered wrapper refill: wait for the first chunk only, then drain what the
   loop thread has already queued. Waiting for a full buffer would deadlock a
   duplex peer (gRPC bidi) that is itself waiting for our next frame."
  (let ((pos 0)
        (space nil))
    (bt:with-lock-held ((async-body-lock s))
      (%async-body-wait s)
      (when (async-body-error s)
        (error (async-body-error s)))
      (setf pos (%async-body-take s buffer 0 (length buffer))
            space (%async-body-take-space s)))
    (%async-body-signal-space space)
    pos))

(defmethod close ((s async-body-input-stream) &key abort)
  (declare (ignore abort))
  (%async-body-run-finish
   (bt:with-lock-held ((async-body-lock s))
     (setf (async-body-open-p s) nil
           (async-body-chunks s) nil
           (async-body-buffered s) 0
           (async-body-eof-p s) t)
     (bt:condition-notify (async-body-cv s))
     (%async-body-take-finish s)))
  (call-next-method))
