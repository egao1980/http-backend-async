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
             :documentation "Thunk when buffer drains below limit (unpause IO).")))

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
      (bt:condition-notify (async-body-cv s)))
    t))

(defun async-body-eof (s)
  (bt:with-lock-held ((async-body-lock s))
    (setf (async-body-eof-p s) t)
    (bt:condition-notify (async-body-cv s))))

(defun async-body-fail (s condition)
  (bt:with-lock-held ((async-body-lock s))
    (setf (async-body-error s) condition
          (async-body-eof-p s) t)
    (bt:condition-notify (async-body-cv s))))

(defun %async-body-wait (s)
  "Wait until data, EOF, error, or closed. Lock must be held."
  (loop until (or (async-body-chunks s)
                  (async-body-eof-p s)
                  (async-body-error s)
                  (not (async-body-open-p s)))
        do (bt:condition-wait (async-body-cv s) (async-body-lock s))))

(defun %async-body-signal-space (s)
  (let ((fn (async-body-on-space s)))
    (when fn
      (ignore-errors (funcall fn)))))

(defmethod trivial-gray-streams:stream-read-byte ((s async-body-input-stream))
  (bt:with-lock-held ((async-body-lock s))
    (%async-body-wait s)
    (when (async-body-error s)
      (error (async-body-error s)))
    (unless (async-body-open-p s)
      (return-from trivial-gray-streams:stream-read-byte :eof))
    (let ((chunks (async-body-chunks s)))
      (unless chunks
        (return-from trivial-gray-streams:stream-read-byte :eof))
      (let* ((chunk (car chunks))
             (pos (async-body-chunk-pos s))
             (b (aref chunk pos)))
        (incf pos)
        (decf (async-body-buffered s))
        (cond
          ((>= pos (length chunk))
           (setf (async-body-chunks s) (cdr chunks)
                 (async-body-chunk-pos s) 0))
          (t (setf (async-body-chunk-pos s) pos)))
        (%async-body-signal-space s)
        b))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((s async-body-input-stream) seq start end &key)
  (let ((pos start))
    (loop while (< pos end)
          do (bt:with-lock-held ((async-body-lock s))
               (%async-body-wait s)
               (when (async-body-error s)
                 (error (async-body-error s)))
               (unless (async-body-open-p s)
                 (return))
               (let ((chunks (async-body-chunks s)))
                 (unless chunks
                   (return))
                 (let* ((chunk (car chunks))
                        (cpos (async-body-chunk-pos s))
                        (avail (- (length chunk) cpos))
                        (need (- end pos))
                        (n (min avail need)))
                   (replace seq chunk :start1 pos :end1 (+ pos n)
                            :start2 cpos :end2 (+ cpos n))
                   (incf pos n)
                   (decf (async-body-buffered s) n)
                   (let ((cpos* (+ cpos n)))
                     (if (>= cpos* (length chunk))
                         (setf (async-body-chunks s) (cdr chunks)
                               (async-body-chunk-pos s) 0)
                         (setf (async-body-chunk-pos s) cpos*)))
                   (%async-body-signal-space s)))))
    pos))

(defmethod close ((s async-body-input-stream) &key abort)
  (declare (ignore abort))
  (bt:with-lock-held ((async-body-lock s))
    (setf (async-body-open-p s) nil
          (async-body-chunks s) nil
          (async-body-buffered s) 0
          (async-body-eof-p s) t)
    (bt:condition-notify (async-body-cv s)))
  (call-next-method))
