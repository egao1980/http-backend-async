(in-package #:http-backend-async/tests)

;;; H2-MUX against an in-process http2/core server: no sockets, no event loop.
;;; MEM-MUX swaps the transport generics for byte queues; DRIVE-MUX shuttles
;;; frames until both sides are quiet.

(defclass mem-mux (h2-mux)
  ((to-server :initform (make-array 0 :element-type '(unsigned-byte 8)
                                      :adjustable t :fill-pointer 0)
              :reader mem-mux-to-server)
   (to-client :initform (make-array 0 :element-type '(unsigned-byte 8)
                                      :adjustable t :fill-pointer 0)
              :accessor mem-mux-to-client)
   (armed :initform nil :accessor mem-mux-armed)
   (closed :initform nil :accessor mem-mux-closed)))

(defmethod connection-alive-p ((mux mem-mux))
  (http-backend-async::h2-mux-usable-p mux))

(defmethod http-backend-async::h2-mux-read ((mux mem-mux) buffer)
  (let* ((in (mem-mux-to-client mux))
         (n (min (length buffer) (length in))))
    (if (zerop n)
        (values nil :want-read)
        (progn
          (replace buffer in :end2 n)
          (setf (mem-mux-to-client mux)
                (let ((rest (make-array (- (length in) n) :element-type '(unsigned-byte 8)
                                                          :adjustable t
                                                          :fill-pointer (- (length in) n))))
                  (replace rest in :start2 n)
                  rest))
          n))))

(defmethod http-backend-async::h2-mux-write ((mux mem-mux) octets start end)
  (loop for i from start below end
        do (vector-push-extend (aref octets i) (mem-mux-to-server mux)))
  (- end start))

(defmethod http-backend-async::h2-mux-arm ((mux mem-mux) direction)
  (setf (mem-mux-armed mux) direction))

(defmethod http-backend-async::h2-mux-disarm ((mux mem-mux))
  (setf (mem-mux-armed mux) nil))

(defmethod http-backend-async::h2-mux-close-transport ((mux mem-mux))
  (setf (mem-mux-closed mux) t))

(defvar *mem-ended* nil
  "Server streams whose request ended, newest first.")

(defun %h2c (name)
  (find-symbol name :http2/core))

(defun %ensure-mem-server-classes ()
  (ensure-http2)
  (unless (find-class 'mem-server-stream nil)
    (eval `(defclass mem-server-stream (,(%h2c "HTTP2-STREAM")) ()))
    (eval `(defmethod ,(%h2c "PEER-ENDS-HTTP-STREAM") ((s mem-server-stream))
             (push s *mem-ended*)))
    (eval `(defclass mem-server (,(%h2c "SERVER-HTTP2-CONNECTION")
                                 ,(%h2c "WRITE-BUFFER-CONNECTION-MIXIN"))
             ()
             (:default-initargs :stream-class 'mem-server-stream)))))

(defstruct (mem-pair (:constructor %make-mem-pair))
  mux server (preface-p nil) (paths '())
  ;; Server frame parser state: the next parse function and its octet count.
  (parse-fn nil) (parse-need 0)
  (pending (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)))

(defun %server-feed (pair octets)
  "Parse OCTETS on the server one frame part at a time (as H2-PROCESS-PENDING
   does on the client): PROCESS-FRAMES in http2 < 2.1.2 misreads SETTINGS."
  (let ((pending (mem-pair-pending pair))
        (server (mem-pair-server pair)))
    (loop for b across octets do (vector-push-extend b pending))
    (loop with pos = 0
          for need = (mem-pair-parse-need pair)
          while (<= (+ pos need) (length pending))
          do (multiple-value-bind (next size)
                 (funcall (mem-pair-parse-fn pair) server
                          (coerce (subseq pending pos (+ pos need))
                                  '(simple-array (unsigned-byte 8) (*))))
               (incf pos need)
               (setf (mem-pair-parse-fn pair) next
                     (mem-pair-parse-need pair) (or size 0)))
          finally (let ((rest (subseq pending pos)))
                    (setf (fill-pointer pending) 0)
                    (loop for b across rest do (vector-push-extend b pending))))))

(defun make-mem-pair (&key max-streams pool (event-loop :mem-loop))
  (%ensure-mem-server-classes)
  (let* ((pump (make-instance 'async-h2-pump-stream))
         (session (make-async-h2-session
                   pump :stream-class 'http-backend-async::async-h2-streaming-client-stream))
         (mux (make-instance 'mem-mux :socket nil :https t :session session :pump pump
                                      :event-backend nil :event-loop event-loop
                                      :pool pool :key "mem|h2"))
         (server (if max-streams
                     (make-instance 'mem-server :max-concurrent-streams max-streams)
                     (make-instance 'mem-server))))
    (http-backend-async::h2-mux-kick mux)
    (%make-mem-pair :mux mux :server server)))

(defun %server-take (server)
  (let* ((buffer (funcall (%h2c "GET-TO-WRITE") server))
         (octets (coerce buffer '(simple-array (unsigned-byte 8) (*)))))
    (setf (fill-pointer buffer) 0)
    octets))

(defun drive-mux (pair &key (rounds 50))
  "Run client IO and server frame processing until neither side has work."
  (let ((mux (mem-pair-mux pair))
        (server (mem-pair-server pair)))
    (loop repeat rounds
          for progress = nil
          do (when (eq (mem-mux-armed mux) :write)
               (http-backend-async::h2-mux-on-io mux :ok)
               (setf progress t))
             (let ((sent (mem-mux-to-server mux)))
               (when (plusp (length sent))
                 (let ((octets (coerce sent '(simple-array (unsigned-byte 8) (*)))))
                   (setf (fill-pointer sent) 0)
                   (unless (mem-pair-preface-p pair)
                     (let ((n (length (symbol-value (%h2c "+CLIENT-PREFACE-START+")))))
                       (multiple-value-bind (fn need)
                           (funcall (%h2c "PARSE-CLIENT-PREFACE") server (subseq octets 0 n))
                         (setf (mem-pair-parse-fn pair) fn
                               (mem-pair-parse-need pair) need))
                       (setf octets (subseq octets n)
                             (mem-pair-preface-p pair) t)))
                   (%server-feed pair octets)
                   (setf progress t))))
             (let ((reply (%server-take server)))
               (when (plusp (length reply))
                 (loop for b across reply
                       do (vector-push-extend b (mem-mux-to-client mux)))
                 (setf progress t)))
             (when (and (plusp (length (mem-mux-to-client mux)))
                        (member (mem-mux-armed mux) '(:read :write)))
               (http-backend-async::h2-mux-on-io mux :ok)
               (setf progress t))
          while progress)))

(defun mem-respond (stream status body &key (end-stream t))
  (let ((octets (babel:string-to-octets body)))
    (funcall (%h2c "SEND-HEADERS") stream `((:status ,(princ-to-string status)))
             :end-stream nil)
    (funcall (%h2c "WRITE-DATA-FRAME") stream octets :end-stream end-stream)))

(defun mem-open (pair path results &key errors)
  "Open GET PATH on the pair's mux. On end, push (PATH . body-string) to RESULTS
   (a cons cell used as a box); on error push (PATH . condition) to ERRORS."
  (let* ((mux (mem-pair-mux pair))
         (body (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))
         (stream (http-backend-async::h2-mux-open-stream
                  mux
                  (lambda (c) (when errors (push (cons path c) (car errors))))
                  nil :get (quri:uri (format nil "https://mem.test~A" path)) nil)))
    (push (cons (funcall (%h2c "GET-STREAM-ID") stream) path) (mem-pair-paths pair))
    (setf (h2-stream-on-data stream)
          (lambda (data start end)
            (loop for i from start below end do (vector-push-extend (aref data i) body)))
          (h2-stream-on-end stream)
          (lambda (s)
            (declare (ignore s))
            (push (cons path (babel:octets-to-string body)) (car results))))
    (http-backend-async::h2-mux-kick mux)
    stream))

(defun %server-path (pair stream)
  "The request path of server STREAM, by stream id."
  (cdr (assoc (funcall (%h2c "GET-STREAM-ID") stream) (mem-pair-paths pair))))

(defun warm-up (pair)
  "One finished request, so the peer's SETTINGS have been read (an idle mux
   does not read the socket)."
  (let ((*mem-ended* nil)
        (mux (mem-pair-mux pair)))
    (drive-mux pair)
    (mem-open pair "/warmup" (list nil))
    (drive-mux pair)
    (mem-respond (first *mem-ended*) 200 "")
    (drive-mux pair)
    ;; Not H2-MUX-STREAM-DONE: a mux without a pool would close.
    (setf (http-backend-async::h2-mux-users mux) '()
          (http-backend-async::h2-mux-peak-streams mux) 0)
    (http-backend-async::h2-mux-kick mux)
    (drive-mux pair)))

(deftest h2-mux-learns-peer-max-concurrent-streams
  (if (null (find-symbol "GET-PEER-MAX-CONCURRENT-STREAMS" :http2/core))
      (skip "http2 < 2.1.2 does not report SETTINGS_MAX_CONCURRENT_STREAMS")
      (let ((pair (make-mem-pair :max-streams 3)))
        (warm-up pair)
        (ok (= 3 (h2-mux-peer-max-streams (mem-pair-mux pair))))
        (let ((*h2-max-streams-per-connection* 2))
          (ok (= 2 (h2-mux-peer-max-streams (mem-pair-mux pair))))))))

(deftest h2-mux-carries-concurrent-streams
  (if (null (find-symbol "GET-PEER-MAX-CONCURRENT-STREAMS" :http2/core))
      (skip "http2 < 2.1.2: sequential reuse only")
      (let* ((*mem-ended* nil)
             (pair (make-mem-pair :max-streams 3
                                  :pool (make-lru-connection-pool :max-size 4)))
             (mux (mem-pair-mux pair))
             (results (list nil))
             (errors (list nil)))
        (warm-up pair)
        (let ((streams (mapcar (lambda (p) (mem-open pair p results :errors errors))
                               '("/a" "/b" "/c"))))
          (ok (= 3 (h2-mux-active-count mux)))
          (ok (not (http-backend-async::h2-mux-has-capacity-p mux)) "full at the peer's limit")
          (ok (signals (mem-open pair "/d" results) 'http-connection-error)
              "a fourth stream is refused locally, not sent")
          (drive-mux pair)
          (ok (= 3 (length *mem-ended*)) "all three requests reached the server together")
          ;; Answer out of order: the streams are independent.
          (dolist (s *mem-ended*)
            (mem-respond s 200 (format nil "body~A" (%server-path pair s))))
          (drive-mux pair)
          (ok (equal '(("/a" . "body/a") ("/b" . "body/b") ("/c" . "body/c"))
                     (sort (copy-list (car results)) #'string< :key #'car)))
          (ok (null (car errors)))
          (ok (= 3 (h2-mux-peak-streams mux)))
          (dolist (s streams)
            (http-backend-async::h2-mux-stream-done mux s))
          (ok (zerop (h2-mux-active-count mux)))
          (ok (eq :open (http-backend-async::h2-mux-state mux)))))))

(deftest h2-mux-repools-while-it-has-capacity
  (if (null (find-symbol "GET-PEER-MAX-CONCURRENT-STREAMS" :http2/core))
      (skip "http2 < 2.1.2")
      (let* ((*mem-ended* nil)
             (pool (make-lru-connection-pool :max-size 4))
             (pair (make-mem-pair :max-streams 2 :pool pool))
             (mux (mem-pair-mux pair))
             (results (list nil)))
        (warm-up pair)
        (http-backend-async::h2-mux-repool mux)
        (multiple-value-bind (conn kind)
            (http-backend-async::acquire-pooled-connection pool "mem" :mem-loop :https t)
          (ok (eq mux conn))
          (ok (eq :h2 kind)))
        (let ((s1 (mem-open pair "/1" results)))
          (http-backend-async::h2-mux-repool mux)
          (ok (http-backend-async::h2-mux-pooled-p mux) "one stream open, one free: pooled")
          (multiple-value-bind (conn kind)
              (http-backend-async::acquire-pooled-connection pool "mem" :mem-loop :https t)
            (ok (eq mux conn) "a second request shares the connection")
            (ok (eq :h2 kind)))
          (let ((s2 (mem-open pair "/2" results)))
            (http-backend-async::h2-mux-repool mux)
            (ok (not (http-backend-async::h2-mux-pooled-p mux)) "full: kept out of the pool")
            (ok (null (http-backend-async::acquire-pooled-connection
                       pool "mem" :mem-loop :https t)))
            (drive-mux pair)
            (dolist (s *mem-ended*) (mem-respond s 200 "x"))
            (drive-mux pair)
            (ok (= 2 (length (car results))))
            (http-backend-async::h2-mux-stream-done mux s1)
            (ok (http-backend-async::h2-mux-pooled-p mux) "a finished stream re-pools it")
            (http-backend-async::h2-mux-stream-done mux s2)
            (ok (eq mux (http-backend-async::acquire-pooled-connection
                         pool "mem" :mem-loop :https t)))))
        (ok (null (http-backend-async::acquire-pooled-connection
                   pool "mem" :other-loop :https t))
            "another event loop never gets it"))))

(deftest h2-mux-full-entry-is-not-discarded
  (if (null (find-symbol "GET-PEER-MAX-CONCURRENT-STREAMS" :http2/core))
      (skip "http2 < 2.1.2")
      (let* ((pool (make-lru-connection-pool :max-size 4))
             (pair (make-mem-pair :max-streams 1 :pool pool))
             (mux (mem-pair-mux pair)))
        (warm-up pair)
        (pool-release pool "mem|h2" mux)
        (mem-open pair "/only" (list nil))
        (ok (null (http-backend-async::acquire-pooled-connection pool "mem" :mem-loop :https t)))
        (ok (eq :open (http-backend-async::h2-mux-state mux)) "busy, not closed"))))

(deftest h2-mux-peer-reset-fails-one-stream
  (let* ((*mem-ended* nil)
         (*h2-max-streams-per-connection* 10)
         (http-backend-async::*h2-unknown-peer-max-streams* 10)
         (pair (make-mem-pair))
         (mux (mem-pair-mux pair))
         (results (list nil))
         (errors (list nil)))
    (warm-up pair)
    (mem-open pair "/ok" results :errors errors)
    (mem-open pair "/reset" results :errors errors)
    (drive-mux pair)
    (ok (= 2 (length *mem-ended*)))
    (dolist (s *mem-ended*)
      (if (equal "/reset" (%server-path pair s))
          (funcall (%h2c "WRITE-RST-STREAM-FRAME") s
                   (symbol-value (%h2c "+INTERNAL-ERROR+")))
          (mem-respond s 200 "fine")))
    (drive-mux pair)
    (ok (equal '(("/ok" . "fine")) (car results)))
    (ok (equal '("/reset") (mapcar #'car (car errors))))
    (ok (typep (cdr (first (car errors))) 'http-connection-error))
    (ok (= 1 (h2-mux-active-count mux)) "the reset stream left the mux")
    (ok (eq :open (http-backend-async::h2-mux-state mux)))))

(deftest h2-mux-cancel-resets-stream-and-keeps-connection
  (let* ((*mem-ended* nil)
         (http-backend-async::*h2-unknown-peer-max-streams* 10)
         (pool (make-lru-connection-pool :max-size 4))
         (pair (make-mem-pair :pool pool))
         (mux (mem-pair-mux pair))
         (results (list nil)))
    (warm-up pair)
    (let ((gone (mem-open pair "/slow" results)))
      (mem-open pair "/fast" results)
      (drive-mux pair)
      (http-backend-async::h2-mux-stream-done mux gone :reset t)
      (drive-mux pair)
      (ok (eq :open (http-backend-async::h2-mux-state mux)))
      (dolist (s *mem-ended*)
        (when (equal "/fast" (%server-path pair s))
          (mem-respond s 200 "done")))
      (drive-mux pair)
      (ok (equal '(("/fast" . "done")) (car results))))))

(deftest h2-mux-unpooled-closes-after-last-stream
  (let* ((*mem-ended* nil)
         (pair (make-mem-pair))
         (mux (mem-pair-mux pair))
         (results (list nil)))
    (warm-up pair)
    (let ((s (mem-open pair "/x" results)))
      (drive-mux pair)
      (mem-respond (first *mem-ended*) 200 "y")
      (drive-mux pair)
      (http-backend-async::h2-mux-stream-done mux s)
      (drive-mux pair)
      (ok (eq :dead (http-backend-async::h2-mux-state mux)))
      (ok (mem-mux-closed mux)))))

(deftest h2-mux-discard-drains-busy-connection
  (let* ((*mem-ended* nil)
         (pool (make-lru-connection-pool :max-size 4))
         (pair (make-mem-pair :pool pool))
         (mux (mem-pair-mux pair))
         (results (list nil)))
    (warm-up pair)
    (let ((s (mem-open pair "/x" results)))
      (pool-discard pool mux)
      (ok (eq :draining (http-backend-async::h2-mux-state mux)))
      (ok (not (http-backend-async::h2-mux-has-capacity-p mux)))
      (drive-mux pair)
      (mem-respond (first *mem-ended*) 200 "still delivered")
      (drive-mux pair)
      (ok (equal '(("/x" . "still delivered")) (car results)))
      (http-backend-async::h2-mux-stream-done mux s)
      (drive-mux pair)
      (ok (eq :dead (http-backend-async::h2-mux-state mux))))))

(deftest h2-mux-connection-error-fails-every-stream
  (let* ((*mem-ended* nil)
         (http-backend-async::*h2-unknown-peer-max-streams* 10)
         (pair (make-mem-pair))
         (mux (mem-pair-mux pair))
         (errors (list nil)))
    (warm-up pair)
    (mem-open pair "/1" (list nil) :errors errors)
    (mem-open pair "/2" (list nil) :errors errors)
    (drive-mux pair)
    (loop for b across #(0 0 0 0 0 0 0 0 0) ; zero-length DATA on stream 0: PROTOCOL_ERROR
          do (vector-push-extend b (mem-mux-to-client mux)))
    (http-backend-async::h2-mux-on-io mux :ok)
    (ok (eq :dead (http-backend-async::h2-mux-state mux)))
    (ok (equal '("/1" "/2") (sort (mapcar #'car (car errors)) #'string<)))))
