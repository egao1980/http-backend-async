(in-package #:http-backend-async/tests)

;;; #91: HTTP/1 framing leftovers. A raw origin on loopback answers every
;;; request with octets the test gives, written in parts with pauses between
;;; them, then hangs up. Its per-connection threads are joined.

(defun %octets (x)
  (if (stringp x) (babel:string-to-octets x :encoding :latin-1) x))

(defun %crlf-text (&rest lines)
  "LINES joined by CRLF, with the blank line that ends a header block."
  (format nil "~{~A~C~C~}~C~C"
          (loop for l in lines append (list l #\Return #\Newline))
          #\Return #\Newline))

(defun %chunked (octets)
  "OCTETS as one chunk and the last chunk."
  (concatenate '(vector (unsigned-byte 8))
               (%octets (format nil "~X~C~C" (length octets) #\Return #\Newline))
               octets
               (%octets (format nil "~C~C0~C~C~C~C" #\Return #\Newline
                                #\Return #\Newline #\Return #\Newline))))

(defun %read-request-head (stream)
  "The request line of the next request on STREAM (its head is read), or NIL."
  (let ((line (%read-line-octets stream)))
    (when line
      (loop for h = (%read-line-octets stream)
            while (and h (> (length h) 2)))
      (string-right-trim '(#\Return #\Newline) line))))

(defun call-with-raw-origin (fn responder &key (gap 0.1))
  "RESPONDER: (fn request-line) → a list of parts (strings or octets), written
   GAP seconds apart, then the connection is closed. Call FN with the origin's
   base URL; every connection thread is joined before returning."
  (let* ((server (usocket:socket-listen "127.0.0.1" 0 :reuseaddress t
                                                      :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server))
         (lock (bt:make-lock "raw-origin"))
         (threads '())
         (acceptor
           (bt:make-thread
            (lambda ()
              (loop
                (let ((c (handler-case (usocket:socket-accept server
                                                              :element-type '(unsigned-byte 8))
                           (error () (return)))))
                  (bt:with-lock-held (lock)
                    (push (bt:make-thread
                           (lambda ()
                             (unwind-protect
                                  (ignore-errors
                                   (let* ((s (usocket:socket-stream c))
                                          (line (%read-request-head s)))
                                     (when line
                                       (loop for (part . more) on (funcall responder line)
                                             do (write-sequence (%octets part) s)
                                                (force-output s)
                                                (when more (sleep gap))))))
                               (ignore-errors (usocket:socket-close c))))
                           :name "raw-origin-conn")
                          threads)))))
            :name "raw-origin")))
    (unwind-protect
         (funcall fn (format nil "http://127.0.0.1:~D" port))
      (ignore-errors (usocket:socket-close server))
      (ignore-errors (bt:join-thread acceptor))
      (dolist (th (bt:with-lock-held (lock) threads))
        (ignore-errors (bt:join-thread th))))))

(defmacro with-raw-origin ((url-var responder &rest options) &body body)
  `(call-with-raw-origin (lambda (,url-var) ,@body) ,responder ,@options))

(defun %body-text (res)
  (babel:octets-to-string (coerce (response-body res) '(vector (unsigned-byte 8)))
                          :encoding :utf-8 :errorp nil))

(defun %raw-request (method url &key want-stream)
  "Blocking METHOD request with no event loop bound. → the response; a live
   body is read to EOF into it."
  (let* ((client (make-http-client *http-backend* :verify nil))
         (res (let ((event-protocol:*event-backend* nil)
                    (event-protocol:*event-loop* nil))
                (http:request method url :timeout '(:connect 5 :read 5)
                                         :want-stream want-stream :client client)))
         (body (response-body res)))
    (if (streamp body)
        (make-instance 'http-response
                       :status (response-status res)
                       :headers (response-headers res)
                       :body (unwind-protect (http-backend-async::slurp-octets (body-stream res))
                               (close body))
                       :url (response-url res)
                       :request (response-request res))
        res)))

;;; --- 1. Interim (1xx) responses are skipped (RFC 9110 §15.2) -------------

(defun %interim-then-ok (&key split)
  (let ((interim (concatenate 'string
                              (%crlf-text "HTTP/1.1 100 Continue")
                              (%crlf-text "HTTP/1.1 103 Early Hints" "Link: </s.css>; rel=preload")))
        (final (concatenate 'string
                            (%crlf-text "HTTP/1.1 200 OK" "Content-Length: 2" "Connection: close")
                            "ok")))
    (if split (list interim final) (list (concatenate 'string interim final)))))

(deftest-parametrize interim-responses-are-skipped
    ((want-stream split)
     (nil nil)
     (t nil)
     (nil t)
     (t t))
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (with-raw-origin (url (lambda (line) (declare (ignore line)) (%interim-then-ok :split split)))
      (let ((res (handler-case (%raw-request :get (format nil "~A/x" url) :want-stream want-stream)
                   (error (e) e))))
        (ok (and (typep res 'http-response) (= 200 (response-status res)))
            (format nil "the final 200 is the response (got ~A)"
                    (if (typep res 'http-response) (response-status res) res)))
        (ok (and (typep res 'http-response) (equal "ok" (%body-text res)))
            "with its own body")))))

;;; --- 2. Transfer codings other than a final chunked ----------------------

(defun %gzip (text)
  (encode-content-coding :gzip (babel:string-to-octets text :encoding :utf-8)))

(deftest-parametrize gzip-transfer-coding-is-decoded
    ((want-stream te)
     (nil "gzip, chunked")
     (t "gzip, chunked")
     (nil "gzip")
     (t "gzip"))
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (with-raw-origin (url (lambda (line)
                            (declare (ignore line))
                            (list (%crlf-text "HTTP/1.1 200 OK"
                                              (format nil "Transfer-Encoding: ~A" te))
                                  (if (search "chunked" te)
                                      (%chunked (%gzip "hello transfer coding"))
                                      (%gzip "hello transfer coding")))))
      (let ((res (handler-case (%raw-request :get (format nil "~A/x" url) :want-stream want-stream)
                   (error (e) e))))
        (ok (and (typep res 'http-response) (equal "hello transfer coding" (%body-text res)))
            (format nil "Transfer-Encoding: ~A is decoded (got ~S)" te
                    (if (typep res 'http-response) (%body-text res) res)))))))

(deftest chunked-not-final-is-refused
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (with-raw-origin (url (lambda (line)
                            (declare (ignore line))
                            (list (%crlf-text "HTTP/1.1 200 OK" "Transfer-Encoding: chunked, gzip")
                                  (%gzip (format nil "5~C~Chello~C~C0~C~C~C~C"
                                                 #\Return #\Newline #\Return #\Newline
                                                 #\Return #\Newline #\Return #\Newline)))))
      (let ((res (handler-case (%raw-request :get (format nil "~A/x" url)) (error (e) e))))
        (ok (typep res 'http-protocol-error)
            (format nil "an http-protocol-error, not a chunked parse of gzip octets (got ~A)" res))))))

;;; --- 4. A body sink's own error is not "response parse failed" -----------

(define-condition %sink-refusal (error) ())

(deftest a-body-sink-error-is-its-own
  (multiple-value-bind (http headers body finished parse set-body)
      (http-backend-async::make-response-accumulator)
    (declare (ignore http headers body finished))
    (funcall set-body (lambda (data start end)
                        (declare (ignore data start end))
                        (error '%sink-refusal)))
    (let ((e (handler-case
                 (progn
                   (funcall parse (%octets (concatenate 'string
                                                        (%crlf-text "HTTP/1.0 200 OK")
                                                        "close-delimited body")))
                   nil)
               (error (e) e))))
      (ok (typep e '%sink-refusal) (format nil "the sink's condition (got ~S)" (type-of e))))))

;;; --- 5. Coverage: HEAD / 204 / 304 without Content-Length; a
;;; close-delimited redirect ------------------------------------------------

(deftest-parametrize no-body-responses-without-content-length-end-at-the-header-block
    ((method status)
     (:head 200)
     (:get 204)
     (:get 304))
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    ;; The origin keeps the connection open 3 s after the header block: the
    ;; response must not wait for the close.
    (with-raw-origin (url (lambda (line)
                            (declare (ignore line))
                            (list (%crlf-text (format nil "HTTP/1.1 ~D X" status)
                                              "Content-Type: text/plain")
                                  ""))
                          :gap 3)
      (let* ((start (get-internal-real-time))
             (res (handler-case (%raw-request method (format nil "~A/x" url)) (error (e) e)))
             (secs (/ (- (get-internal-real-time) start) internal-time-units-per-second)))
        (ok (and (typep res 'http-response) (= status (response-status res)))
            (format nil "~A → ~D (got ~A)" method status res))
        (ok (< secs 2) (format nil "at the header block, not at the close (~,1F s)" secs))))))

(deftest a-close-delimited-redirect-is-followed
  (with-async-test (eb el hb)
    (declare (ignore eb el))
    (with-raw-origin (url (lambda (line)
                            (if (search "/final" line)
                                (list (%crlf-text "HTTP/1.0 200 OK") "arrived")
                                (list (%crlf-text "HTTP/1.0 302 Found" "Location: /final")
                                      "moved"))))
      (let ((res (handler-case (%raw-request :get (format nil "~A/start" url)) (error (e) e))))
        (ok (and (typep res 'http-response) (= 200 (response-status res))
                 (equal "arrived" (%body-text res)))
            (format nil "the redirect is followed to /final (got ~A)" res))))))
