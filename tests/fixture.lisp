(in-package #:http-backend-async/tests)

;;; Tiny HTTP/1.1 origin in a background thread (cleartext).

(defvar *fixture-thread* nil)
(defvar *fixture-socket* nil)
(defvar *fixture-port* nil)
(defvar *fixture-handler* nil
  "Function (method path headers body) → (values status header-alist body-octets)")

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
  (declare (ignore method headers))
  (cond
    ((string= path "/ok")
     (values 200 '(("content-type" . "text/plain"))
             (babel:string-to-octets "ok")))
    ((string= path "/gzip")
     (let* ((raw (babel:string-to-octets "hello-gzip"))
            (gz (encode-content-coding :gzip raw)))
       (values 200
               '(("content-type" . "text/plain")
                 ("content-encoding" . "gzip"))
               gz)))
    ((string= path "/echo")
     (values 200 '(("content-type" . "application/octet-stream"))
             (or body #())))
    (t
     (values 404 '(("content-type" . "text/plain"))
             (babel:string-to-octets "nope")))))

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
               (path (second parts))
               (body (when (plusp content-length)
                       (let ((buf (make-array content-length
                                              :element-type '(unsigned-byte 8))))
                         (read-sequence buf stream)
                         buf))))
          (multiple-value-bind (status hdrs resp-body)
              (funcall (or *fixture-handler* #'%default-handler)
                       method path (nreverse headers) body)
            (let* ((body* (or resp-body #()))
                   (hdr-str
                    (with-output-to-string (s)
                      (format s "HTTP/1.1 ~D OK~C~C" status #\Return #\Newline)
                      (dolist (h hdrs)
                        (format s "~A: ~A~C~C" (car h) (cdr h) #\Return #\Newline))
                      (unless (assoc "content-length" hdrs :test #'string-equal)
                        (format s "Content-Length: ~D~C~C" (length body*)
                                #\Return #\Newline))
                      (format s "Connection: close~C~C~C~C"
                              #\Return #\Newline #\Return #\Newline)))
                   (head (babel:string-to-octets hdr-str)))
              (write-sequence head stream)
              (write-sequence body* stream)
              (force-output stream)))))
    (error () nil)))

(defun split-sequence (delimiter string)
  (loop for start = 0 then (1+ pos)
        for pos = (position delimiter string :start start)
        collect (subseq string start (or pos (length string)))
        while pos))

(defun start-http-fixture (&key (host "127.0.0.1") (handler nil))
  (when *fixture-thread*
    (stop-http-fixture))
  (setf *fixture-handler* handler)
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
                     (unwind-protect
                          (%serve-one (usocket:socket-stream client))
                       (usocket:socket-close client)))
                 (error ()
                   (when (null *fixture-socket*) (return))))))
           :name "http-backend-async-fixture"))
    port))

(defun stop-http-fixture ()
  (let ((s *fixture-socket*))
    (setf *fixture-socket* nil
          *fixture-port* nil
          *fixture-handler* nil)
    (when s (ignore-errors (usocket:socket-close s)))
    (when (and *fixture-thread* (bt:thread-alive-p *fixture-thread*))
      (ignore-errors (bt:destroy-thread *fixture-thread*))
      (setf *fixture-thread* nil))))

(defun fixture-url (path)
  (format nil "http://127.0.0.1:~A~A" *fixture-port* path))

(defmacro with-http-fixture ((&optional handler) &body body)
  `(progn
     (start-http-fixture :handler ,handler)
     (unwind-protect (progn ,@body)
       (stop-http-fixture))))
