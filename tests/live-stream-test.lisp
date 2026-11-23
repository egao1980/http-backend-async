(in-package #:http-backend-async/tests)

;;; #70: the blocking SEND with :WANT-STREAM T returns at the headers with the
;;; live body; each SSE event reaches the reader as it arrives, not in one
;;; burst at the end. A gate origin proves it: after each event it waits for
;;; the reader to acknowledge that event before it sends the next one, so a
;;; buffered body would stall every gate.

(defun %gate-write (stream string)
  (write-sequence (babel:string-to-octets string :encoding :utf-8) stream)
  (force-output stream))

(defun %gate-chunk (stream text)
  (%gate-write stream (format nil "~X~C~C~A~C~C" (length (babel:string-to-octets text))
                              #\Return #\Newline text #\Return #\Newline)))

(defstruct gate-origin
  url
  (gate (bt:make-semaphore :name "sse-gate"))
  (stalls 0)                  ; gates that timed out: the reader never saw it
  (sent '())                  ; (index . seconds since start), newest first
  (write-failed-at nil)       ; seconds since start, once the peer is gone
  (start (get-internal-real-time)))

(defun %gate-seconds (origin)
  (float (/ (- (get-internal-real-time) (gate-origin-start origin))
            internal-time-units-per-second)))

(defun %gate-serve (origin stream &key (events 5) (first-delay 0) (gap 0)
                                    (gate-timeout 3) (gated t))
  "One chunked text/event-stream answer: EVENTS events `data: eN`. Before the
   first, FIRST-DELAY seconds; after each, wait for the reader's ack (GATED)
   up to GATE-TIMEOUT, then GAP seconds."
  (when (%drip-read-request stream)
    (%gate-write stream (format nil "HTTP/1.1 200 OK~C~CContent-Type: text/event-stream~C~CTransfer-Encoding: chunked~C~CConnection: close~C~C~C~C"
                                #\Return #\Newline #\Return #\Newline
                                #\Return #\Newline #\Return #\Newline
                                #\Return #\Newline))
    (sleep first-delay)
    (handler-case
        (progn
          (dotimes (i events)
            (%gate-chunk stream (format nil "data: e~D~%~%" i))
            (push (cons i (%gate-seconds origin)) (gate-origin-sent origin))
            (when gated
              (unless (bt:wait-on-semaphore (gate-origin-gate origin)
                                            :timeout gate-timeout)
                (incf (gate-origin-stalls origin))))
            (sleep gap))
          (%gate-write stream (format nil "0~C~C~C~C"
                                      #\Return #\Newline #\Return #\Newline)))
      (error ()
        (setf (gate-origin-write-failed-at origin) (%gate-seconds origin))))))

(defun call-with-gate-origin (fn &rest serve-args)
  (let* ((origin (make-gate-origin))
         (server (usocket:socket-listen "127.0.0.1" 0 :reuseaddress t
                                                      :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server))
         (thread
           (bt:make-thread
            (lambda ()
              (handler-case
                  (let ((c (usocket:socket-accept server
                                                  :element-type '(unsigned-byte 8))))
                    (unwind-protect
                         (apply #'%gate-serve origin (usocket:socket-stream c)
                                serve-args)
                      (ignore-errors (usocket:socket-close c))))
                (error () nil)))
            :name "sse-gate-origin")))
    (setf (gate-origin-url origin) (format nil "http://127.0.0.1:~D/sse" port))
    (unwind-protect (funcall fn origin)
      (%wait-until (lambda () (not (bt:thread-alive-p thread))) 10)
      (ignore-errors (usocket:socket-close server))
      (when (bt:thread-alive-p thread)
        (ignore-errors (bt:destroy-thread thread))))))

(defmacro with-gate-origin ((origin-var &rest serve-args) &body body)
  `(call-with-gate-origin (lambda (,origin-var) ,@body) ,@serve-args))

(defun %sync-live (url timeout)
  "Blocking SEND, no loop bound, :WANT-STREAM T: the response as returned."
  (let ((event-protocol:*event-backend* nil)
        (event-protocol:*event-loop* nil))
    (http:request :get url :timeout timeout :want-stream t)))

(defun %read-lf-line (stream)
  "One LF-terminated line as a string (without the LF), NIL at EOF."
  (let ((out (make-array 0 :element-type '(unsigned-byte 8)
                           :adjustable t :fill-pointer 0)))
    (loop for b = (read-byte stream nil nil)
          do (cond ((null b)
                    (return (and (plusp (length out))
                                 (babel:octets-to-string out :encoding :utf-8))))
                   ((= b 10)
                    (return (babel:octets-to-string out :encoding :utf-8)))
                   (t (vector-push-extend b out))))))

(defun %pump-threads ()
  (remove-if-not (lambda (th)
                   (and (bt:thread-alive-p th)
                        (search "stream pump" (bt:thread-name th))))
                 (bt:all-threads)))

(defun %wait-until (predicate seconds)
  (loop with end = (+ (get-internal-real-time)
                      (* seconds internal-time-units-per-second))
        until (or (funcall predicate) (> (get-internal-real-time) end))
        do (sleep 0.05))
  (funcall predicate))

(deftest sync-want-stream-delivers-each-event-as-it-arrives
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-gate-origin (origin :events 5 :first-delay 1 :gap 0.3)
      (let* ((res (%sync-live (gate-origin-url origin) '(:connect 5 :read 5)))
             (returned-at (%gate-seconds origin))
             (seen '()))
        (ok (= 200 (response-status res)))
        (ok (streamp (response-body res)) "the body is the live stream")
        (ok (< returned-at 0.9)
            (format nil "SEND returned at the headers (~,2F s), before the first event"
                    returned-at))
        (let ((s (body-stream res)))
          (unwind-protect
               (loop for line = (%read-lf-line s)
                     while line
                     when (and (> (length line) 6) (string= "data: " line :end2 6))
                       do (push (cons (subseq line 6) (%gate-seconds origin)) seen)
                          (bt:signal-semaphore (gate-origin-gate origin)))
            (close (response-body res))))
        (setf seen (nreverse seen))
        (ok (equal '("e0" "e1" "e2" "e3" "e4") (mapcar #'car seen)))
        (ok (zerop (gate-origin-stalls origin))
            (format nil "each event reached the reader before the next was sent (~D stalls)"
                    (gate-origin-stalls origin)))
        (let ((first-seen (cdr (first seen)))
              (last-sent (cdr (first (gate-origin-sent origin)))))
          (ok (and first-seen last-sent (< (+ first-seen 0.8) last-sent))
              (format nil "first event read at ~,2F s, last sent at ~,2F s"
                      first-seen last-sent)))
        (ok (%wait-until (lambda () (null (%pump-threads))) 3)
            "the pump ends with the body")))))

(deftest sync-want-stream-close-ends-the-request
  ;; The reader stops after one event: CLOSE ends the request, so the origin
  ;; sees the connection go and the pump thread exits.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-gate-origin (origin :events 100 :gap 0.1 :gated nil)
      (let* ((res (%sync-live (gate-origin-url origin) '(:connect 5 :read 5)))
             (s (body-stream res))
             (line (loop for l = (%read-lf-line s)
                         while l
                         when (plusp (length l)) return l))
             (closed-at nil))
        (ok (equal "data: e0" line))
        (close (response-body res))
        (setf closed-at (%gate-seconds origin))
        (ok (%wait-until (lambda () (gate-origin-write-failed-at origin)) 5)
            "the origin saw the connection close")
        (let ((failed (gate-origin-write-failed-at origin)))
          (ok (and failed (< failed (+ closed-at 3)))
              (format nil "closed at ~,2F s, origin noticed at ~A" closed-at failed)))
        (ok (%wait-until (lambda () (null (%pump-threads))) 3)
            "the pump thread ended")))))

(deftest sync-want-stream-read-timeout-ends-the-live-body
  ;; Headers, one event, then silence: the :READ (idle) limit fails the read,
  ;; not the SEND, which returned at the headers.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-gate-origin (origin :events 1 :gap 8 :gated nil)
      (let* ((res (%sync-live (gate-origin-url origin) '(:connect 5 :read 1)))
             (s (body-stream res))
             (c (handler-case (loop while (%read-lf-line s)) (http-timeout-error (e) e)))
             (at (%gate-seconds origin)))
        (ignore-errors (close (response-body res)))
        (ok (typep c 'http-timeout-error) (format nil "~A" c))
        (ok (< at 4) (format nil "failed at ~,2F s" at))
        (ok (%wait-until (lambda () (null (%pump-threads))) 3)
            "the pump thread ended")))))

(deftest sync-want-stream-h2-live
  "Live H2 :want-stream via the blocking SEND: chunks arrive over time.
   Gate with HTTP_ASYNC_H2_LIVE=1 (nghttp2.org httpbin /drip)."
  (if (not (equal (uiop:getenv "HTTP_ASYNC_H2_LIVE") "1"))
      (skip "set HTTP_ASYNC_H2_LIVE=1")
      (with-async-test (eb el hb)
        (declare (ignore eb el hb))
        (let* ((start (get-internal-real-time))
               (res (let ((event-protocol:*event-backend* nil)
                          (event-protocol:*event-loop* nil))
                      (http:request :get "https://nghttp2.org/httpbin/drip?duration=3&numbytes=3&delay=0"
                                    :timeout '(:connect 10 :read 10)
                                    :want-stream t)))
               (returned (%elapsed-since start))
               (s (body-stream res))
               (times '()))
          (unwind-protect
               (loop for b = (read-byte s nil nil)
                     while b do (push (%elapsed-since start) times))
            (close (response-body res)))
          (setf times (nreverse times))
          (ok (eq :http/2 (response-http-version res)))
          (ok (= 3 (length times)))
          (ok (and times (< returned 2.0) (< (first times) (- (car (last times)) 1.0)))
              (format nil "returned ~,2F s; bytes at ~{~,2F~^ ~}" returned times))))))
