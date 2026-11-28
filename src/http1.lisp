(in-package #:http-backend-async)

;;; HTTP/1.1 request build + fast-http response parse.

(defun %uri-host-port (uri)
  (let* ((scheme (or (quri:uri-scheme uri) "http"))
         (host (strip-ipv6-brackets
                (or (quri:uri-host uri)
                    (error 'http-connection-error :message "URL missing host"))))
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
          collect (cons name (if (or (stringp value) (secret-p value))
                                 value
                                 (princ-to-string value)))))

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
  "Origin-form path?query, or absolute-form for HTTP proxy (RFC 7230).
   Absolute-form never includes userinfo (credentials → Proxy-Authorization)."
  (if absolute-p
      (quri:render-uri (quri:copy-uri uri :userinfo nil :fragment nil))
      (let* ((path (or (quri:uri-path uri) "/"))
             (query (quri:uri-query uri)))
        (if query (format nil "~A?~A" path query) path))))

(defun proxy-authorization-value (user password)
  "Proxy-Authorization header value (Basic), or NIL when USER is NIL/empty.
   Matches dexador make-proxy-authorization."
  (when (and user (plusp (length user)))
    (authorization-header-value (list :basic user (or password "")))))

(defun build-connect-request-octets (host port &key proxy-authorization)
  "HTTP CONNECT request octets (dexador write-connect-header).
   HOST:PORT is the origin; PROXY-AUTHORIZATION is the full header value
   (e.g. \"Basic …\") or NIL."
  (let* ((authority (format-host-port host port))
         (out (make-array 128 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
         (crlf (babel:string-to-octets (format nil "~C~C" #\Return #\Newline))))
    (labels ((emit (octets)
               (loop for b across octets do (vector-push-extend b out)))
             (emit-line (string)
               (emit (babel:string-to-octets string :encoding :utf-8))
               (emit crlf)))
      (emit-line (format nil "CONNECT ~A HTTP/1.1" authority))
      (emit-line (format nil "Host: ~A" authority))
      (when proxy-authorization
        (emit-line (format nil "Proxy-Authorization: ~A" (reveal proxy-authorization))))
      (emit crlf)
      (coerce out '(simple-array (unsigned-byte 8) (*))))))

(defun %header-block-end (buf &optional (end (length buf)))
  "Index past CRLFCRLF in BUF[0:END], or NIL if incomplete."
  (loop for i from 0 below (- end 3)
        when (and (= (aref buf i) 13)
                  (= (aref buf (1+ i)) 10)
                  (= (aref buf (+ i 2)) 13)
                  (= (aref buf (+ i 3)) 10))
          return (+ i 4)))

(defun connect-response-ok-p (buf &optional (end (length buf)))
  "True if BUF starts with a successful CONNECT response (2xx).
   Requires a complete header block (CRLFCRLF)."
  (let ((hend (%header-block-end buf end)))
    (when hend
      (let* ((text (babel:octets-to-string buf :encoding :utf-8 :errorp nil
                                           :end (min hend end)))
             (line-end (or (search (format nil "~C~C" #\Return #\Newline) text)
                           (length text)))
             (status-line (subseq text 0 line-end)))
        ;; HTTP/1.x 2xx …
        (and (>= (length status-line) 12)
             (string-equal "HTTP/" status-line :end2 5)
             (let ((sp (position #\Space status-line)))
               (when sp
                 (let* ((code-start (1+ sp))
                        (code-end (or (position #\Space status-line
                                                :start code-start)
                                      (length status-line)))
                        (code (ignore-errors
                               (parse-integer status-line
                                              :start code-start
                                              :end code-end
                                              :junk-allowed t))))
                   (and code (<= 200 code 299))))))))))

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
        (emit-line (format nil "~A: ~A" (car h) (reveal (cdr h)))))
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
         ((or (null decompress) (null codings)
              ;; HEAD, 204 and 304 carry Content-Encoding but no body: there
              ;; is nothing to decode (gzip of zero octets is "Unexpected EOF")
              (zerop (length body)))
          (values body headers))
         (t
          (let* ((decoded (decode-content-codings codings body))
                 (ht (let ((n (make-hash-table :test #'equal)))
                       (maphash (lambda (k v) (setf (gethash k n) v)) headers)
                       (remhash "content-encoding" n)
                       (remhash "content-length" n)
                       n)))
            (values decoded ht))))))))

(defun %status-code-at (buf start)
  "The status code of the status line at START in BUF (HTTP/1.x NNN ...),
   :INCOMPLETE when fewer than its 12 octets are in, or NIL when BUF holds
   no status line there."
  (let ((n (- (length buf) start))
        (prefix #.(map '(vector (unsigned-byte 8)) #'char-code "HTTP/")))
    (cond
      ((not (every #'= prefix (subseq buf start (+ start (min n 5))))) nil)
      ((< n 12) :incomplete)
      (t (let ((digits (subseq buf (+ start 9) (+ start 12))))
           (and (every (lambda (b) (<= 48 b 57)) digits)
                (parse-integer (map 'string #'code-char digits))))))))

(defun %interim-prefix (buf)
  "Where the final response starts in BUF, past any interim (1xx but 101)
   responses: an index, or NIL while more octets are needed to tell."
  (let ((start 0))
    (loop
      (let ((status (%status-code-at buf start)))
        (cond
          ((eq status :incomplete) (return nil))
          ((and (integerp status) (<= 100 status 199) (/= status 101))
           (let ((end (%header-block-end (subseq buf start))))
             (if end
                 (incf start end)
                 (return nil))))
          (t (return start)))))))

(defun %transfer-codings (value)
  "Transfer-Encoding VALUE as lower-case coding names, in header order."
  (loop for part in (uiop:split-string (or value "") :separator ",")
        for name = (string-downcase
                    (string-trim '(#\Space #\Tab)
                                 (subseq part 0 (or (position #\; part) (length part)))))
        when (plusp (length name))
          collect name))

(defun %apply-transfer-codings (headers)
  "Fold the response's transfer codings into HEADERS (a downcased table):
   chunked, which fast-http decodes, stays in Transfer-Encoding; every other
   coding is appended to Content-Encoding, so the body decoding undoes it
   after the content codings it was applied over (RFC 9112 §6.1). A chunked
   that is not the final coding signals HTTP-PROTOCOL-ERROR."
  (let* ((codings (%transfer-codings (gethash "transfer-encoding" headers)))
         (others (remove "chunked" codings :test #'string=)))
    (when (and (member "chunked" codings :test #'string=)
               (string/= "chunked" (car (last codings))))
      (error 'http-protocol-error
             :message (format nil "Transfer-Encoding ~A: chunked is not the final coding"
                              (gethash "transfer-encoding" headers))))
    (when others
      (let ((ce (gethash "content-encoding" headers)))
        (setf (gethash "content-encoding" headers)
              (format nil "~@[~A, ~]~{~A~^, ~}"
                      (and ce (plusp (length (string-trim " " ce))) ce)
                      others)))
      (if (member "chunked" codings :test #'string=)
          (setf (gethash "transfer-encoding" headers) "chunked")
          (remhash "transfer-encoding" headers)))))

(defun make-response-accumulator (&key on-headers no-body-p)
  "Return (values http headers-ht body-vector finished-fn parser-fn set-body-fn
   headers-done-fn close-delimited-fn).

   NO-BODY-P: the request was HEAD. Its response, like any 204 or 304, ends
   with the header block (RFC 9110 §6.4.1, §9.3.2) whatever Content-Length
   says; waiting for that many body octets hung until the timeout.

   Interim responses (1xx but 101: 100 Continue, 103 Early Hints) come
   before the final one and are skipped (RFC 9110 §15.2): fast-http would
   call one the complete response. Octets are held until the status line of
   a final response is in (%INTERIM-PREFIX).

   A response with neither Content-Length nor chunked framing is
   close-delimited (RFC 9112 §6.3 rule 8): its body ends only when the peer
   closes the connection. fast-http calls such a message complete at the end
   of every read, so a pause between the server's writes cut the body (#89).
   Once the header block says so, the octets bypass the parser and go
   straight to the body callback; PARSER-FN called with no octets is the
   peer's EOF and finishes the body. CLOSE-DELIMITED-FN is then true: the
   connection cannot be reused.

   Transfer codings (%TRANSFER-CODINGS): chunked must be the final one, or
   absent (then the body is close-delimited). Any other coding (gzip, as in
   Transfer-Encoding: gzip, chunked) is decoded with the content codings: it
   is appended to the Content-Encoding header the response carries. A
   chunked that is not final signals HTTP-PROTOCOL-ERROR.

   An error of the body callback or of ON-HEADERS is signalled as it is,
   not as fast-http's callback error.

   SET-BODY-FN (fn) replaces the body callback (fn data start end).
   Default body callback accumulates into BODY-VECTOR.
   ON-HEADERS, when non-NIL, is called as (fn http headers) after the header block."
  (let* ((http (fast-http:make-http-response))
         (headers (make-hash-table :test #'equal))
         (body (make-array 0 :element-type '(unsigned-byte 8)
                             :adjustable t :fill-pointer 0))
         (finished nil)
         (headers-done nil)
         (close-delimited nil)
         ;; Octets held until a final status line is in; NIL from then on.
         (prelude (make-array 0 :element-type '(unsigned-byte 8)
                                :adjustable t :fill-pointer 0))
         (escape (list :callback-error))
         (body-fn
           (lambda (data start end)
             (loop for i from start below end
                   do (vector-push-extend (aref data i) body))))
         (parser nil))
    (flet ((guarded (fn)
             ;; fast-http wraps a callback's error in its own; leave by THROW
             ;; instead, so PARSER-FN signals the error itself.
             (lambda (&rest args)
               (handler-case (apply fn args)
                 (error (e) (throw escape e))))))
      (setf parser
            (fast-http:make-parser
             http
             :header-callback
             (guarded
               (lambda (h)
                 (maphash (lambda (k v)
                            (setf (gethash (string-downcase k) headers) v))
                          h)
                 (%apply-transfer-codings headers)
                 (setf headers-done t)
                 (let ((status (fast-http:http-status http)))
                   (cond
                     ((or no-body-p (member status '(204 304)))
                      (setf finished t))
                     ((and (>= status 200)
                           (null (fast-http:http-content-length http))
                           (not (fast-http:http-chunked-p http)))
                      (setf close-delimited t))))
                 (when on-headers
                   (funcall on-headers http headers))))
             :body-callback
             (guarded
               (lambda (data start end)
                 (funcall body-fn data start end)))
             :finish-callback
             ;; A close-delimited body ends at EOF, not where the read did.
             (lambda () (unless close-delimited (setf finished t))))))
    (labels ((feed (octets start end)
               (cond
                 ((not close-delimited)
                  (funcall parser octets :start start :end end))
                 ((= start end)
                  (setf finished t))
                 (t
                  (funcall body-fn octets start end))))
             (feed-guarded (octets start end)
               (let ((failure (catch escape
                                (feed octets start end)
                                nil)))
                 (when failure
                   (error failure))))
             (parse (octets &key (start 0) (end (length octets)))
               (cond
                 ((null prelude)
                  (feed-guarded octets start end))
                 ((= start end)
                  ;; EOF before a final status line: let the parser say so.
                  (let ((held (coerce (shiftf prelude nil) '(simple-array (unsigned-byte 8) (*)))))
                    (when (plusp (length held))
                      (feed-guarded held 0 (length held)))
                    (feed-guarded octets start end)))
                 (t
                  (loop for i from start below end
                        do (vector-push-extend (aref octets i) prelude))
                  (let ((skip (%interim-prefix prelude)))
                    (when skip
                      (let ((held (coerce (shiftf prelude nil) '(simple-array (unsigned-byte 8) (*)))))
                        (feed-guarded held skip (length held)))))))
               finished))
      (values http headers body
              (lambda () finished)
              #'parse
              (lambda (fn) (setf body-fn fn))
              (lambda () headers-done)
              (lambda () close-delimited)))))