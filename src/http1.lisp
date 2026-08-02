(in-package #:http-backend-async)

;;; HTTP/1.1 request build + fast-http response parse.

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
  (if (null coding)
      (values (etypecase content
                (null #())
                (stream (slurp-octets content))
                ((or string vector) (coerce-to-octets content)))
              nil)
      (let* ((c (normalize-content-coding coding))
             (octets (etypecase content
                       (null (make-array 0 :element-type '(unsigned-byte 8)))
                       (stream (slurp-octets content))
                       ((or string vector) (coerce-to-octets content))))
             (enc (encode-content-coding c octets)))
        (values enc (string-downcase (symbol-name c))))))

(defun build-request-octets (method uri headers body)
  "Build HTTP/1.1 request octets for absolute-form on origin (host already in headers)."
  (let* ((path (or (quri:uri-path uri) "/"))
         (query (quri:uri-query uri))
         (target (if query (format nil "~A?~A" path query) path))
         (body* (or body #()))
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
      (unless (assoc "content-length" headers :test #'string-equal)
        (when (plusp (length body*))
          (emit-line (format nil "Content-Length: ~D" (length body*)))))
      (unless (assoc "connection" headers :test #'string-equal)
        (emit-line "Connection: close"))
      (emit crlf)
      (when (plusp (length body*))
        (emit body*))
      (coerce out '(simple-array (unsigned-byte 8) (*))))))

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
