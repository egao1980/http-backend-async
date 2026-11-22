(in-package #:http-backend-async/tests)

;;; Connect / read (idle) / total timeouts (http-protocol HTTP-TIMEOUT).
;;; A drip origin sends a chunked body slowly but steadily (an LLM token
;;; stream from a slow local model): a :READ timeout must let it finish
;;; however long it takes, and fail once the origin goes silent.

(defvar *drip-accepts* 0)

(defun %drip-read-request (stream)
  "Read one request's head and Content-Length body. NIL at EOF."
  (let ((line (%read-line-octets stream))
        (length 0))
    (when line
      (loop for h = (%read-line-octets stream)
            while (and h (> (length h) 2))
            do (let* ((s (string-right-trim '(#\Return #\Newline) h))
                      (colon (position #\: s)))
                 (when (and colon (string-equal "content-length" (subseq s 0 colon)))
                   (setf length (or (parse-integer s :start (1+ colon) :junk-allowed t)
                                    0)))))
      (when (plusp length)
        (read-sequence (make-array length :element-type '(unsigned-byte 8)) stream))
      t)))

(defun %drip-write (stream string)
  (write-sequence (babel:string-to-octets string) stream)
  (force-output stream))

(defun %drip-serve (stream script)
  "Answer with a chunked 200 whose chunks follow SCRIPT: (seconds . text) pairs,
   the delay before each chunk. A NIL text means stall that long, then hang up."
  (when (%drip-read-request stream)
    (%drip-write stream (format nil "HTTP/1.1 200 OK~C~CContent-Type: text/event-stream~C~CTransfer-Encoding: chunked~C~CConnection: close~C~C~C~C"
                                #\Return #\Newline #\Return #\Newline
                                #\Return #\Newline #\Return #\Newline
                                #\Return #\Newline))
    (dolist (step script)
      (sleep (car step))
      (unless (cdr step) (return-from %drip-serve))
      (%drip-write stream (format nil "~X~C~C~A~C~C" (length (cdr step))
                                  #\Return #\Newline (cdr step) #\Return #\Newline)))
    (%drip-write stream (format nil "0~C~C~C~C" #\Return #\Newline #\Return #\Newline))))

(defun call-with-drip-origin (script fn)
  (setf *drip-accepts* 0)
  (let* ((server (usocket:socket-listen "127.0.0.1" 0 :reuseaddress t
                                                      :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server))
         (thread
           (bt:make-thread
            (lambda ()
              (loop
                (handler-case
                    (let ((c (usocket:socket-accept server
                                                    :element-type '(unsigned-byte 8))))
                      (incf *drip-accepts*)
                      (bt:make-thread
                       (lambda ()
                         (unwind-protect
                              (ignore-errors (%drip-serve (usocket:socket-stream c) script))
                           (ignore-errors (usocket:socket-close c))))
                       :name "drip-conn"))
                  (error () (return)))))
            :name "drip-origin")))
    (unwind-protect (funcall fn (format nil "http://127.0.0.1:~D/drip" port))
      (ignore-errors (usocket:socket-close server))
      (when (bt:thread-alive-p thread)
        (ignore-errors (bt:destroy-thread thread))))))

(defmacro with-drip-origin ((url-var script) &body body)
  `(call-with-drip-origin ,script (lambda (,url-var) ,@body)))

(defun %steady (n seconds)
  "N chunks, SECONDS apart: `data: tN` lines."
  (loop for i below n collect (cons seconds (format nil "data: t~D~%" i))))

(defun %steady-text (n)
  (format nil "~{~A~}" (loop for i below n collect (format nil "data: t~D~%" i))))

(defun %sync (url timeout &key want-stream (method :get) retry)
  "Blocking SEND with no event loop bound: the backend makes its own."
  (let ((event-protocol:*event-backend* nil)
        (event-protocol:*event-loop* nil))
    (http:request method url :timeout timeout :want-stream want-stream
                             :retry retry)))

(defun %elapsed-since (start)
  (/ (- (get-internal-real-time) start) internal-time-units-per-second))

(defun %timeout-condition (thunk)
  "→ (values condition seconds-taken)."
  (let ((start (get-internal-real-time)))
    (values (handler-case (progn (funcall thunk) nil)
              (http-timeout-error (e) e))
            (%elapsed-since start))))

(deftest-parametrize read-timeout-lets-a-steady-stream-finish
    ((want-stream)
     (t)
     (nil))
  ;; 10 chunks 0.3 s apart: 3 s in all, longer than the 2 s "old" total cap,
  ;; never silent for the 1 s read timeout.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-drip-origin (url (%steady 10 0.3))
      (let ((res (%sync url '(:connect 2 :read 1) :want-stream want-stream)))
        (ok (= 200 (response-status res)))
        (ok (equal (%steady-text 10)
                   (babel:octets-to-string (response-body res) :encoding :utf-8)))))))

(deftest total-timeout-still-caps-the-attempt
  ;; A bare number is :TOTAL, as before: the same stream dies at 1.5 s.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-drip-origin (url (%steady 10 0.3))
      (multiple-value-bind (c secs)
          (%timeout-condition (lambda () (%sync url 1.5 :want-stream t)))
        (ok (typep c 'http-timeout-error))
        (ok (< secs 3.0) (format nil "failed after ~,2F s" secs)))
      (multiple-value-bind (c secs)
          (%timeout-condition (lambda () (%sync url '(:read 1 :total 1.5) :want-stream t)))
        (ok (typep c 'http-timeout-error) "an explicit :total next to :read")
        (ok (< secs 3.0) (format nil "failed after ~,2F s" secs))))))

(deftest-parametrize read-timeout-fails-a-silent-stream
    ((want-stream)
     (t)
     (nil))
  ;; Two chunks, then 6 s of silence: a 1 s read timeout fails in about 1.5 s.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-drip-origin (url (list (cons 0.1 "data: a")
                                 (cons 0.2 "data: b")
                                 (cons 6 nil)))
      (multiple-value-bind (c secs)
          ;; a connect limit far above the read limit (30 s vs 1 s, as
          ;; llm-protocol-openai sends): the read limit still rules
          (%timeout-condition (lambda () (%sync url '(:connect 30 :read 1)
                                                :want-stream want-stream)))
        (ok (typep c 'http-timeout-error))
        (ok (and c (search "read timed out" (princ-to-string c)))
            (format nil "~A" c))
        (ok (< 0.9 secs 4.0) (format nil "failed after ~,2F s" secs))))))

(deftest read-timeout-is-not-retried-for-post
  ;; http-protocol HTTP-RETRY: only idempotent methods are retried
  ;; (cl-repository#142). A POST whose stream went silent is not sent again;
  ;; a GET is.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-drip-origin (url (list (cons 0.1 "data: a") (cons 4 nil)))
      (let ((retry (make-http-retry :total 2 :backoff-factor 0)))
        (ok (typep (%timeout-condition
                    (lambda () (%sync url '(:read 0.5) :method :post :retry retry
                                                       :want-stream t)))
                   'http-timeout-error))
        (ok (= 1 *drip-accepts*) (format nil "POST connections: ~D" *drip-accepts*))
        (setf *drip-accepts* 0)
        (ok (typep (%timeout-condition
                    (lambda () (%sync url '(:read 0.5) :method :get :retry retry
                                                       :want-stream t)))
                   'http-timeout-error))
        (ok (= 3 *drip-accepts*) (format nil "GET connections: ~D" *drip-accepts*))))))

(deftest connect-timeout-applies-to-setup-only
  ;; A routable-nowhere address never answers the SYN: the :connect limit
  ;; fails it, though :read is long. (TEST-NET-1, RFC 5737.)
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (multiple-value-bind (c secs)
        (handler-case
            (%timeout-condition
             (lambda () (%sync "http://192.0.2.1:81/" '(:connect 1 :read 30))))
          (http-error () nil))
      (cond
        ((null c) (skip "the network answered or refused TEST-NET-1"))
        (t
         (ok (search "connect timed out" (princ-to-string c)) (princ-to-string c))
         (ok (< secs 4.0) (format nil "failed after ~,2F s" secs)))))))

(deftest phase-timeouts-shape
  (flet ((pt (x)
           (multiple-value-list
            (http-backend-async::%phase-timeouts
             (make-http-request :method :get :url "http://x/" :timeout x) nil))))
    (ok (equal '(nil nil 180.0d0) (pt 180)) "a number is the total")
    (ok (equal '(30.0d0 300.0d0 nil) (pt '(:connect 30 :read 300))) "no total cap")
    (ok (equal '(30.0d0 300.0d0 600.0d0) (pt '(:connect 30 :read 300 :total 600))))
    (ok (equal '(5.0d0 nil nil) (pt '(:connect 5))) "connect only bounds the setup")
    (ok (equal '(nil nil 30.0d0) (pt nil)) "default")))

(deftest read-timeout-resets-on-every-byte
  ;; #66: one byte a second for 5 s under a 1.5 s read timeout and no total.
  ;; The timeout is the longest wait for the next bytes, not a cap on the
  ;; whole answer.
  (with-async-test (eb el hb)
    (declare (ignore eb el hb))
    (with-drip-origin (url (loop repeat 5 collect (cons 1 "x")))
      (multiple-value-bind (res secs)
          (let ((start (get-internal-real-time)))
            (values (%sync url '(:connect 2 :read 1.5) :want-stream t)
                    (%elapsed-since start)))
        (ok (= 200 (response-status res)))
        (ok (equal "xxxxx" (babel:octets-to-string (response-body res)
                                                   :encoding :utf-8)))
        (ok (> secs 4.5) (format nil "the stream ran ~,2F s, past 1.5 s" secs))))))
