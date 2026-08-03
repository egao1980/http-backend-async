(in-package #:http-backend-async/tests)

;;; Tiny HTTP/1.1 origin in a background thread (cleartext).

(defvar *fixture-thread* nil)
(defvar *fixture-socket* nil)
(defvar *fixture-port* nil)
(defvar *fixture-handler* nil
  "Function (method path headers body) → (values status header-alist body-octets)")
(defvar *fixture-last-request-target* nil
  "Raw request-target from the last fixture request (origin- or absolute-form).")
(defvar *fixture-keep-alive* nil
  "When T, respond with Connection: keep-alive and accept pipelined/reuse.")
(defvar *fixture-accept-count* 0
  "Number of accepted TCP clients (for connection-pool reuse tests).")
(defvar *fixture-request-count* 0
  "Number of HTTP requests served.")

;;; Shared async test harness (must load before other test files that use it).

(defun %make-event-backend ()
  (let* ((name (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv"))
         (key (intern (string-upcase name) :keyword))
         (sys (ecase key
                (:libuv "event-backend-libuv")
                (:libev "event-backend-libev")))
         (pkg (ecase key
                (:libuv :event-backend-libuv)
                (:libev :event-backend-libev)))
         (maker-name (ecase key
                       (:libuv "MAKE-LIBUV-BACKEND")
                       (:libev "MAKE-LIBEV-BACKEND"))))
    (asdf:load-system sys)
    (funcall (symbol-function (find-symbol maker-name pkg)))))

(defmacro with-async-test ((backend-var loop-var http-backend-var) &body body)
  `(let* ((,backend-var (%make-event-backend))
          (,loop-var (make-event-loop ,backend-var))
          (,http-backend-var (make-async-backend))
          (*event-backend-maker* (lambda () ,backend-var)))
     (with-event-backend (,backend-var)
       (with-event-loop-var (,loop-var)
         (let ((*http-backend* ,http-backend-var))
           ,@body)))))

(defun %await-promise (promise event-backend event-loop &key (timeout 5.0))
  "Run EVENT-LOOP until PROMISE finishes or TIMEOUT. Returns response or signals."
  (let ((result nil)
        (err nil)
        (done nil))
    (blackbird:catcher
      (blackbird:attach promise
                        (lambda (v)
                          (setf result v done t)
                          (stop event-backend event-loop)))
      (error (e)
        (setf err e done t)
        (stop event-backend event-loop)))
    (sleep* event-backend event-loop timeout
            :callback (lambda ()
                        (unless done
                          (setf err (make-condition 'http-timeout-error
                                                    :message "test await timed out")
                                done t)
                          (stop event-backend event-loop))))
    (event-protocol:run event-backend event-loop :stop-when-idle nil)
    (when err (error err))
    result))

(defun %await-stream-promise (promise event-backend event-loop &key (timeout 5.0))
  "Like %AWAIT-PROMISE but keep the loop running while a worker slurps RESPONSE-BODY.
   Returns (values response body-octets)."
  (let ((result nil)
        (octets nil)
        (err nil)
        (done nil))
    (blackbird:catcher
      (blackbird:attach
       promise
       (lambda (res)
         (setf result res)
         (bt:make-thread
          (lambda ()
            (handler-case
                (progn
                  (setf octets (slurp-octets (body-stream res)))
                  (setf done t)
                  (stop event-backend event-loop))
              (error (e)
                (setf err e done t)
                (stop event-backend event-loop))))
          :name "async-want-stream-slurp")))
      (error (e)
        (setf err e done t)
        (stop event-backend event-loop)))
    (sleep* event-backend event-loop timeout
            :callback (lambda ()
                        (unless done
                          (setf err (make-condition 'http-timeout-error
                                                    :message "stream await timed out")
                                done t)
                          (stop event-backend event-loop))))
    (event-protocol:run event-backend event-loop :stop-when-idle nil)
    (when err (error err))
    (values result octets)))

(defun %body-text (res)
  (babel:octets-to-string (response-body res) :encoding :utf-8 :errorp nil))

(defun %read-line-octets (stream)
  (let ((out (make-array 0 :element-type '(unsigned-byte 8)
                           :adjustable t :fill-pointer 0)))
    (loop for b = (read-byte stream nil nil)
          while b
          do (vector-push-extend b out)
             (when (and (>= (length out) 2)
                        (= (aref out (- (length out) 2)) 13)
                        (= (aref out (- (length out) 1)) 10))
               (return)))
    (when (plusp (length out))
      (babel:octets-to-string out :encoding :utf-8 :errorp nil))))

(defun %default-handler (method path headers body)
  "httpbin-shaped paths used by requests CE / cookie tests."
  (declare (ignore method))
  (cond
    ((string= path "/ok")
     (values 200 '(("content-type" . "text/plain"))
             (babel:string-to-octets "ok")))
    ((string= path "/gzip")
     ;; requests test_decompress_gzip — compressed JSON {"gzipped": true}
     (let* ((raw (babel:string-to-octets "{\"gzipped\": true}"))
            (gz (encode-content-coding :gzip raw)))
       (values 200
               '(("content-type" . "application/json")
                 ("content-encoding" . "gzip"))
               gz)))
    ((string= path "/deflate")
     (let* ((raw (babel:string-to-octets "{\"deflated\": true}"))
            (df (encode-content-coding :deflate raw)))
       (values 200
               '(("content-type" . "application/json")
                 ("content-encoding" . "deflate"))
               df)))
    ((string= path "/gzip-case")
     ;; urllib3: Content-Encoding token is case-insensitive
     (let* ((raw (babel:string-to-octets "{\"gzipped\": true}"))
            (gz (encode-content-coding :gzip raw)))
       (values 200
               '(("content-type" . "application/json")
                 ("content-encoding" . "GziP"))
               gz)))
    ((string= path "/echo")
     (values 200 '(("content-type" . "application/octet-stream"))
             (or body #())))
    ((and (>= (length path) 7)
          (string= (subseq path 0 7) "/bytes/"))
     ;; /bytes/N → N octets (0..255 cycling) for stream tests
     (let* ((n (or (parse-integer (subseq path 7) :junk-allowed t) 0))
            (n (max 0 n))
            (buf (make-array n :element-type '(unsigned-byte 8))))
       (loop for i below n do (setf (aref buf i) (mod i 256)))
       (values 200 '(("content-type" . "application/octet-stream")) buf)))
    ;; requests Session cookie persistence (set → echo Cookie header)
    ((string= path "/cookies/set")
     (values 200
             '(("content-type" . "text/plain")
               ("set-cookie" . "session=abc; Path=/"))
             (babel:string-to-octets "set")))
    ((string= path "/cookies")
     (let ((cookie (cdr (assoc "cookie" headers :test #'string-equal))))
       (values 200 '(("content-type" . "text/plain"))
               (babel:string-to-octets (or cookie "")))))
    ((string= path "/redirect/1")
     (values 302
             '(("location" . "/ok")
               ("content-type" . "text/plain"))
             (babel:string-to-octets "go")))
    ((string= path "/redirect/2")
     (values 302
             '(("location" . "/redirect/1")
               ("content-type" . "text/plain"))
             (babel:string-to-octets "go2")))
    ((string= path "/redirect/set-cookie")
     ;; Set-Cookie on redirect hop, then final /cookies echoes Cookie.
     (values 302
             '(("location" . "/cookies")
               ("set-cookie" . "redir=1; Path=/")
               ("content-type" . "text/plain"))
             (babel:string-to-octets "go")))
    ((and (>= (length path) 10)
          (string= (subseq path 0 10) "/absolute/"))
     ;; /absolute/<port>/ok → absolute Location to same fixture host
     (let* ((rest (subseq path 10))
            (slash (position #\/ rest))
            (port* (subseq rest 0 slash))
            (target (subseq rest (1+ slash))))
       (values 302
               (list (cons "location"
                           (format nil "http://127.0.0.1:~A/~A" port* target))
                     (cons "content-type" "text/plain"))
               (babel:string-to-octets "abs"))))
    (t
     (values 404 '(("content-type" . "text/plain"))
             (babel:string-to-octets "nope")))))

(defun %normalize-request-target (target)
  "Origin-form path, or path from absolute-form (HTTP proxy)."
  (cond
    ((or (null target) (zerop (length target))) "/")
    ((or (and (>= (length target) 7)
              (string-equal "http://" target :end2 7))
         (and (>= (length target) 8)
              (string-equal "https://" target :end2 8)))
     (let* ((u (quri:uri target))
            (path (or (quri:uri-path u) "/"))
            (query (quri:uri-query u)))
       (if query (format nil "~A?~A" path query) path)))
    (t target)))

(defun %serve-one (stream)
  (handler-case
      (let* ((req-line (%read-line-octets stream))
             (headers nil)
             (content-length 0))
        (unless req-line (return-from %serve-one nil))
        (loop for line = (%read-line-octets stream)
              while (and line (not (string= line (format nil "~C~C" #\Return #\Newline)))
                         (not (string= line (string #\Newline)))
                         (> (length line) 2))
              do (let* ((s (string-right-trim '(#\Return #\Newline) line))
                        (colon (position #\: s)))
                   (when colon
                     (let ((name (string-downcase (subseq s 0 colon)))
                           (val (string-trim '(#\Space #\Tab) (subseq s (1+ colon)))))
                       (push (cons name val) headers)
                       (when (string= name "content-length")
                         (setf content-length (parse-integer val :junk-allowed t)))))))
        (let* ((parts (split-sequence #\Space (string-right-trim '(#\Return #\Newline) req-line)))
               (method (first parts))
               (raw-target (second parts))
               (path (%normalize-request-target raw-target))
               (body (when (plusp content-length)
                       (let ((buf (make-array content-length
                                              :element-type '(unsigned-byte 8))))
                         (read-sequence buf stream)
                         buf))))
          (setf *fixture-last-request-target* raw-target)
          ;; CONNECT tunnel probe (unit/integration): 200 then stop (no TLS).
          (when (string-equal method "CONNECT")
            (let ((head (babel:string-to-octets
                         (format nil "HTTP/1.1 200 Connection Established~C~C~C~C"
                                 #\Return #\Newline #\Return #\Newline))))
              (write-sequence head stream)
              (force-output stream)
              (return-from %serve-one t)))
          (multiple-value-bind (status hdrs resp-body)
              (funcall (or *fixture-handler* #'%default-handler)
                       method path (nreverse headers) body)
            (incf *fixture-request-count*)
            (let* ((body* (or resp-body #()))
                   (hdr-str
                    (with-output-to-string (s)
                      (format s "HTTP/1.1 ~D OK~C~C" status #\Return #\Newline)
                      (dolist (h hdrs)
                        (format s "~A: ~A~C~C" (car h) (cdr h) #\Return #\Newline))
                      (unless (assoc "content-length" hdrs :test #'string-equal)
                        (format s "Content-Length: ~D~C~C" (length body*)
                                #\Return #\Newline))
                      (format s "Connection: ~A~C~C~C~C"
                              (if *fixture-keep-alive* "keep-alive" "close")
                              #\Return #\Newline #\Return #\Newline)))
                   (head (babel:string-to-octets hdr-str)))
              (write-sequence head stream)
              (write-sequence body* stream)
              (force-output stream)
              *fixture-keep-alive*))))
    (error () nil)))

(defun split-sequence (delimiter string)
  (loop for start = 0 then (1+ pos)
        for pos = (position delimiter string :start start)
        collect (subseq string start (or pos (length string)))
        while pos))

(defun start-http-fixture (&key (host "127.0.0.1") (handler nil) (keep-alive nil))
  (when *fixture-thread*
    (stop-http-fixture))
  ;; SETF (not dynamic bind): fixture thread must see the same value.
  (setf *fixture-handler* handler
        *fixture-keep-alive* keep-alive
        *fixture-accept-count* 0
        *fixture-request-count* 0)
  (let* ((server (usocket:socket-listen host 0
                                        :reuseaddress t
                                        :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server)))
    (setf *fixture-socket* server
          *fixture-port* port
          *fixture-thread*
          (bt:make-thread
           (lambda ()
             (loop
               (when (null *fixture-socket*) (return))
               (handler-case
                   (let ((client (usocket:socket-accept server
                                                        :element-type '(unsigned-byte 8))))
                     (incf *fixture-accept-count*)
                     (unwind-protect
                          (let ((stream (usocket:socket-stream client)))
                            (loop
                              (unless (%serve-one stream) (return))
                              (unless *fixture-keep-alive* (return))))
                       (ignore-errors (usocket:socket-close client))))
                 (error ()
                   (when (null *fixture-socket*) (return))))))
           :name "http-backend-async-fixture"))
    port))

(defun stop-http-fixture ()
  (let ((s *fixture-socket*))
    (setf *fixture-socket* nil
          *fixture-port* nil
          *fixture-handler* nil
          *fixture-keep-alive* nil)
    (when s (ignore-errors (usocket:socket-close s)))
    (when (and *fixture-thread* (bt:thread-alive-p *fixture-thread*))
      (ignore-errors (bt:destroy-thread *fixture-thread*))
      (setf *fixture-thread* nil))))

(defun fixture-url (path)
  (format nil "http://127.0.0.1:~A~A" *fixture-port* path))

(defmacro with-http-fixture ((&optional handler &key keep-alive) &body body)
  `(progn
     (start-http-fixture :handler ,handler :keep-alive ,keep-alive)
     (unwind-protect (progn ,@body)
       (stop-http-fixture))))
