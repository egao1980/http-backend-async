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

(defun %prepare-body (request)
  "Returns (values wire-content extra-headers content-length). Streams only."
  (prepare-request-body request))

(defun %request-target (uri &key absolute-p)
  "Origin-form path?query, or absolute-form for HTTP proxy (RFC 7230)."
  (if absolute-p
      (quri:render-uri uri)
      (let* ((path (or (quri:uri-path uri) "/"))
             (query (quri:uri-query uri)))
        (if query (format nil "~A?~A" path query) path))))

(defun %emit-request-lines (method uri headers &key content-length chunked-p
                                                 absolute-p
                                                 (keep-alive t))
  "Shared header serialization → adjustable octet vector (no body)."
  (let* ((target (%request-target uri :absolute-p absolute-p))
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
        (emit-line (if keep-alive "Connection: keep-alive" "Connection: close")))
      (emit crlf)
      out)))

(defun build-request-header-octets (method uri headers &key chunked-p content-length
                                                         absolute-p
                                                         (keep-alive t))
  "HTTP/1.1 request headers only (chunked streaming body follows)."
  (coerce (%emit-request-lines method uri headers
                               :chunked-p chunked-p
                               :content-length content-length
                               :absolute-p absolute-p
                               :keep-alive keep-alive)
          '(simple-array (unsigned-byte 8) (*))))

(defun build-request-octets (method uri headers body &key absolute-p (keep-alive t))
  "Build HTTP/1.1 request octets (headers + materialized BODY vector)."
  (let* ((body* (or body #()))
         (out (%emit-request-lines method uri headers
                                   :content-length (length body*)
                                   :absolute-p absolute-p
                                   :keep-alive keep-alive)))
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
  "Decode BODY per Content-Encoding.
   Vector path: decode-content-codings.
   Stream path: WRAP-RESPONSE-BODY-STREAM (Gray CE chain)."
  (cond
    ((streamp body)
     (wrap-response-body-stream body headers :decompress decompress))
    (t
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
            (values decoded ht))))))))

(defun make-response-accumulator (&key on-headers)
  "Return (values http headers-ht body-vector finished-fn parser-fn set-body-fn headers-done-fn).

   SET-BODY-FN (fn) replaces the body callback (fn data start end).
   Default body callback accumulates into BODY-VECTOR.
   ON-HEADERS, when non-NIL, is called as (fn http headers) after the header block."
  (let* ((http (fast-http:make-http-response))
         (headers (make-hash-table :test #'equal))
         (body (make-array 0 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
         (finished nil)
         (headers-done nil)
         (body-fn
          (lambda (data start end)
            (loop for i from start below end
                  do (vector-push-extend (aref data i) body))))
         (parser (fast-http:make-parser
                  http
                  :header-callback
                  (lambda (h)
                    (maphash (lambda (k v)
                               (setf (gethash (string-downcase k) headers) v))
                             h)
                    (setf headers-done t)
                    (when on-headers
                      (funcall on-headers http headers)))
                  :body-callback
                  (lambda (data start end)
                    (funcall body-fn data start end))
                  :finish-callback
                  (lambda () (setf finished t)))))
    (values http headers body
            (lambda () finished)
            (lambda (octets &key (start 0) (end (length octets)))
              (funcall parser octets :start start :end end)
              finished)
            (lambda (fn) (setf body-fn fn))
            (lambda () headers-done))))