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

(defun prepare-redirect-hop (status uri method body-octets headers cookie-jar
                             accept-encoding-header)
  "After Location resolve: return (values method uri headers body host port https)."
  (let* ((new-method (if (redirect-preserves-method-p status) method :get))
         (new-body (if (redirect-preserves-method-p status) body-octets #()))
         (headers (copy-list headers)))
    (multiple-value-bind (host port scheme) (%uri-host-port uri)
      (setf headers (remove "host" headers :key #'car :test #'string-equal)
            headers (remove "cookie" headers :key #'car :test #'string-equal))
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