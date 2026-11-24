(in-package #:http-backend-async)

;;; HTTP redirect helpers (requests "normal" / dexador-shaped).

(defun redirect-status-p (status)
  (member status '(301 302 303 307 308) :test #'=))

(defun redirect-method (status method)
  "The method a STATUS redirect of METHOD goes out with (RFC 9110 §15.4, as
   the Fetch standard has it): 303 makes anything but HEAD a GET, 301 and 302
   make only POST a GET (#84). 307, 308 and every other case keep METHOD.
   requests also makes a 302 of any method but HEAD a GET; the RFC does not."
  (let ((name (string-upcase (string method))))
    (if (or (and (= status 303) (string/= name "HEAD"))
            (and (member status '(301 302)) (string= name "POST")))
        :get
        method)))

(defun redirect-keeps-body-p (status method)
  "T when a STATUS redirect of METHOD sends the request body again: the
   method stays (REDIRECT-METHOD) and STATUS is not 303, whose GET or HEAD
   only retrieves. RFC 9110 §15.4: resend the request, and drop the body and
   its headers only when the method became GET or HEAD."
  (and (/= status 303)
       (eq (redirect-method status method) method)))

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

   The method is REDIRECT-METHOD's: 303 makes anything but HEAD a GET, 301
   and 302 make only POST a GET. When REDIRECT-KEEPS-BODY-P the body goes
   again; otherwise the hop has no body and none of the headers that
   describe it. Host is URI's and Cookie comes from COOKIE-JAR for URI.
   Proxy-Authorization is always dropped: the caller adds it back only for
   an absolute-form hop to an http(s) proxy, never inside a CONNECT tunnel
   to the origin (requests' rebuild_proxies, #82). On another origin
   (scheme, host or port) Authorization goes too, as http-backend-dexador
   drops it."
  (let* ((new-method (redirect-method status method))
         (new-body (if (redirect-keeps-body-p status method)
                       body-octets
                       (make-array 0 :element-type '(unsigned-byte 8))))
         (drop (if (redirect-same-origin-p from uri)
                   '("host" "cookie" "proxy-authorization")
                   '("host" "cookie" "proxy-authorization" "authorization")))
         (headers (remove-if (lambda (pair) (member (car pair) drop :test #'string-equal))
                             headers)))
    (multiple-value-bind (host port scheme) (%uri-host-port uri)
      (unless (redirect-keeps-body-p status method)
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