(in-package #:http-backend-async)

;;; HTTP redirect helpers (requests "normal" / dexador-shaped).

(defun redirect-status-p (status)
  (member status '(301 302 303 307 308) :test #'=))

(defun redirect-preserves-method-p (status)
  "307/308 keep method+body; 301/302/303 become GET without body."
  (member status '(307 308) :test #'=))

(defun resolve-redirect-uri (current-uri location)
  "Merge LOCATION (string or uri) against CURRENT-URI → absolute quri:uri."
  (let* ((loc (if (typep location 'quri:uri)
                  location
                  (quri:uri (string location))))
         (merged (quri:merge-uris loc current-uri)))
    (unless (member (or (quri:uri-scheme merged) "http")
                    '("http" "https") :test #'string-equal)
      (error 'http-redirect-error
             :message (format nil "disallowed redirect scheme ~A"
                              (quri:uri-scheme merged))))
    merged))

(defun %strip-body-headers (headers)
  (remove-if (lambda (pair)
               (member (car pair)
                       '("content-length" "content-type" "content-encoding"
                         "transfer-encoding")
                       :test #'string-equal))
             headers))

(defun redirect-same-origin-p (from to)
  "FROM and TO (quri URIs) have the same scheme, host and port, default
   ports filled in: a redirect between them keeps Authorization."
  (multiple-value-bind (host port scheme) (%uri-host-port from)
    (multiple-value-bind (to-host to-port to-scheme) (%uri-host-port to)
      (and (string-equal scheme to-scheme)
           (string-equal host to-host)
           (= port to-port)))))

(defun prepare-redirect-hop
    (status from uri method body-octets headers cookie-jar accept-encoding-header)
  "The request a STATUS redirect from FROM to URI (quri URIs, URI resolved
   from Location) makes. → (values method uri headers body host port https)

   307/308 keep METHOD and the body; 301/302/303 send GET without the body
   and the headers that describe it. Host is URI's and Cookie comes from
   COOKIE-JAR for URI. Proxy-Authorization is always dropped: the caller
   adds it back only for an absolute-form hop to an http(s) proxy, never
   inside a CONNECT tunnel to the origin (requests' rebuild_proxies, #82).
   On another origin (scheme, host or port) Authorization goes too, as
   http-backend-dexador drops it."
  (let* ((new-method (if (redirect-preserves-method-p status) method :get))
         (new-body (if (redirect-preserves-method-p status)
                       body-octets
                       (make-array 0 :element-type '(unsigned-byte 8))))
         (drop (if (redirect-same-origin-p from uri)
                   '("host" "cookie" "proxy-authorization")
                   '("host" "cookie" "proxy-authorization" "authorization")))
         (headers (remove-if (lambda (pair) (member (car pair) drop :test #'string-equal))
                             headers)))
    (multiple-value-bind (host port scheme) (%uri-host-port uri)
      (unless (redirect-preserves-method-p status)
        (setf headers (%strip-body-headers headers)))
      (when (and accept-encoding-header
                 (not (assoc "accept-encoding" headers :test #'string-equal)))
        (setf headers (acons "accept-encoding" accept-encoding-header headers)))
      (setf headers
            (acons "host"
                   (if (or (and (string-equal scheme "http") (= port 80))
                           (and (string-equal scheme "https") (= port 443)))
                       host
                       (format nil "~A:~A" host port))
                   headers)
            headers (inject-cookie-header headers cookie-jar
                                          (quri:render-uri uri)))
      (values new-method uri headers new-body
              host port (string-equal scheme "https")))))