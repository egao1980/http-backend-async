(in-package #:http-backend-async)

;;; HTTP/1.1 request build + fast-http response parse.

(defun %uri-host-port (uri)
  (let* ((scheme (or (quri:uri-scheme uri) "http"))
         (host (or (quri:uri-host uri)
                   (error 'http-connection-error :message "URL missing host")))
         (port (or (quri:uri-port uri)
                   (if (string-equal scheme "https") 443 80))))
    (unless (member scheme '("http" "https") :test #'string-equal)
      (error 'http-protocol-error
             :message (format nil "unsupported scheme ~A" scheme)))
    (values host port scheme)))

(defun %header-alist (headers)
  (loop for pair in headers
        for name = (string-downcase
                    (string (if (consp pair) (car pair) pair)))
        for value = (if (consp pair) (cdr pair) nil)
        when value
          collect (cons name (if (stringp value) value (princ-to-string value)))))

(defun %merge-headers (client-headers request-headers)
  (append (%header-alist client-headers)
          (%header-alist request-headers)))

(defun %accept-encoding-header (spec)
  (cond ((null spec) nil)
        ((member spec '(:default t) :test #'eq)
         (default-accept-encoding :as :string))
        ((stringp spec) spec)
        ((listp spec)
         (format nil "~{~(~A~)~^,~}"
                 (mapcar #'normalize-content-coding spec)))
        (t (string spec))))

(defun %method-string (method)
  (string-upcase (string method)))

(defun %prepare-content (content coding)
  "Buffered stream-aware prepare (cl-stack#71). May return a stream."
  (prepare-request-content content :coding coding))

(defun %emit-request-lines (method uri headers &key content-length chunked-p)
  "Shared header serialization → adjustable octet vector (no body)."
  (let* ((path (or (quri:uri-path uri) "/"))
         (query (quri:uri-query uri))
         (target (if query (format nil "~A?~A" path query) path))
         (out (make-array 256 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
         (crlf (babel:string-to-octets (format nil "~C~C" #\Return #\Newline))))
    (labels ((emit (octets)
               (loop for b across octets do (vector-push-extend b out)))
             (emit-line (string)
               (emit (babel:string-to-octets string :encoding :utf-8))
               (emit crlf)))
      (emit-line (format nil "~A ~A HTTP/1.1" (%method-string method) target))
      (dolist (h headers)
        (emit-line (format nil "~A: ~A" (car h) (cdr h))))
      (cond
        (chunked-p
         (unless (assoc "transfer-encoding" headers :test #'string-equal)
           (emit-line "Transfer-Encoding: chunked")))
        ((and content-length (not (assoc "content-length" headers
                                         :test #'string-equal)))
         (emit-line (format nil "Content-Length: ~D" content-length)))
        ((and (null content-length)
              (not (assoc "content-length" headers :test #'string-equal)))
         nil))
      (unless (assoc "connection" headers :test #'string-equal)
        (emit-line "Connection: close"))
      (emit crlf)
      out)))

(defun build-request-header-octets (method uri headers &key chunked-p content-length)
  "HTTP/1.1 request headers only (chunked streaming body follows)."
  (coerce (%emit-request-lines method uri headers
                               :chunked-p chunked-p
                               :content-length content-length)
          '(simple-array (unsigned-byte 8) (*))))

(defun build-request-octets (method uri headers body)
  "Build HTTP/1.1 request octets (headers + materialized BODY vector)."
  (let* ((body* (or body #()))
         (out (%emit-request-lines method uri headers
                                   :content-length (length body*))))
    (loop for b across body* do (vector-push-extend b out))
    (coerce out '(simple-array (unsigned-byte 8) (*)))))

(defun make-chunk-frame (octets &key (start 0) (end (length octets)))
  "Transfer-Encoding: chunked frame for OCTETS[START:END] (size CRLF data CRLF)."
  (let* ((n (- end start))
         (size-line (babel:string-to-octets
                     (format nil "~X~C~C" n #\Return #\Newline)
                     :encoding :utf-8))
         (crlf (babel:string-to-octets (format nil "~C~C" #\Return #\Newline)))
         (out (make-array (+ (length size-line) n (length crlf))
                          :element-type '(unsigned-byte 8))))
    (replace out size-line)
    (replace out octets :start1 (length size-line) :start2 start :end2 end)
    (replace out crlf :start1 (+ (length size-line) n))
    out))

(defparameter +chunked-terminator+
  (babel:string-to-octets (format nil "0~C~C~C~C" #\Return #\Newline
                                  #\Return #\Newline)))

(defun apply-response-content-encoding (body headers &key (decompress t))
  "Decode BODY per Content-Encoding. Unlike dexador, we own the full decode path."
  (let* ((ce (gethash "content-encoding" headers))
         (codings (parse-content-encoding ce)))
    (cond
      ((or (null decompress) (null codings))
       (values body headers))
      (t
       (let* ((decoded (decode-content-codings codings body))
              (ht (let ((n (make-hash-table :test #'equal)))
                    (maphash (lambda (k v) (setf (gethash k n) v)) headers)
                    (remhash "content-encoding" n)
                    (remhash "content-length" n)
                    n)))
         (values decoded ht))))))

(defun make-response-accumulator ()
  "Return (values http headers-ht body-vector finished-fn parser-fn).
   PARSER-FN consumes octet vectors; returns T when message complete."
  (let* ((http (fast-http:make-http-response))
         (headers (make-hash-table :test #'equal))
         (body (make-array 0 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
         (finished nil)
         (parser (fast-http:make-parser
                  http
                  :header-callback
                  (lambda (h)
                    (maphash (lambda (k v)
                               (setf (gethash (string-downcase k) headers) v))
                             h))
                  :body-callback
                  (lambda (data start end)
                    (loop for i from start below end
                          do (vector-push-extend (aref data i) body)))
                  :finish-callback
                  (lambda () (setf finished t)))))
    (values http headers body
            (lambda () finished)
            (lambda (octets &key (start 0) (end (length octets)))
              (funcall parser octets :start start :end end)
              finished))))
