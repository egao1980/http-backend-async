(in-package #:http-backend-async/tests)

;;; Proxy wire form (dexador#202): CONNECT, Proxy-Authorization, absolute-form.

(deftest connect-request-octets
  (let ((req (build-connect-request-octets "example.com" 443)))
    (let ((text (babel:octets-to-string req :encoding :utf-8)))
      (ok (search "CONNECT example.com:443 HTTP/1.1" text))
      (ok (search "Host: example.com:443" text))
      (ok (not (search "Proxy-Authorization" text)))))
  (let* ((auth (proxy-authorization-value "u" "p"))
         (req (build-connect-request-octets "example.com" 443
                                            :proxy-authorization auth))
         (text (babel:octets-to-string req :encoding :utf-8)))
    (ok (search "Proxy-Authorization: Basic " text))
    (ok (string= auth (proxy-authorization-value "u" "p")))))

(deftest connect-response-ok
  (let ((ok (babel:string-to-octets
             (format nil "HTTP/1.1 200 Connection Established~C~C~C~C"
                     #\Return #\Newline #\Return #\Newline)))
        (fail (babel:string-to-octets
               (format nil "HTTP/1.1 407 Proxy Auth Required~C~C~C~C"
                       #\Return #\Newline #\Return #\Newline)))
        (partial (babel:string-to-octets "HTTP/1.1 200 OK")))
    (ok (connect-response-ok-p ok))
    (ok (not (connect-response-ok-p fail)))
    (ok (not (connect-response-ok-p partial)))))

(deftest proxy-authorization-basic
  (ok (null (proxy-authorization-value nil nil)))
  (ok (null (proxy-authorization-value "" "x")))
  (let ((v (proxy-authorization-value "alice" "s3cret")))
    (ok (string= "Basic " (subseq v 0 6)))
    (ok (string= v (authorization-header-value '(:basic "alice" "s3cret"))))))

(defvar *proxy-test-saw-auth* nil)

(defun %proxy-auth-handler (method path headers body)
  (declare (ignore method body))
  (when (assoc "proxy-authorization" headers :test #'string-equal)
    (setf *proxy-test-saw-auth* t))
  (%default-handler "GET" path headers nil))

(deftest http-proxy-absolute-form-and-auth
  "Cleartext via HTTP proxy: absolute-form + Proxy-Authorization (dexador)."
  (setf *proxy-test-saw-auth* nil
        *fixture-last-request-target* nil)
  (with-http-fixture (#'%proxy-auth-handler)
    (with-async-test (eb el hb)
      (let* ((proxy (format nil "http://user:secret@127.0.0.1:~A" *fixture-port*))
             (url (fixture-url "/ok"))
             (client (make-http-client hb :proxy proxy :pool nil))
             (res (%await-promise (get-async url :client client) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "ok") (response-body res)))
        (ok *proxy-test-saw-auth*)
        (ok (and *fixture-last-request-target*
                 (string-equal "http://" *fixture-last-request-target* :end2 7)))
        (ok (search "/ok" *fixture-last-request-target*))))))
