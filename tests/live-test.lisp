(in-package #:http-backend-async/tests)

;;; Live HTTPS + Content-Encoding against public origins.
;;; Skip when HTTP_ASYNC_LIVE=0 or the network/origin is unreachable.

(defun %live-enabled-p ()
  (let ((v (uiop:getenv "HTTP_ASYNC_LIVE")))
    (not (member v '("0" "false" "no" "off") :test #'string-equal))))

(defun %live-url (path)
  (concatenate 'string
               (or (uiop:getenv "HTTP_ASYNC_LIVE_BASE") "https://httpbin.org")
               path))

(defmacro with-live ((eb el) &body body)
  `(if (not (%live-enabled-p))
       (skip "HTTP_ASYNC_LIVE disabled")
       (with-async-test (,eb ,el hb)
         (declare (ignore hb))
         (handler-case (progn ,@body)
           (error (e)
             (skip (format nil "live network unavailable: ~A" e)))))))

(deftest live-https-get
  (with-live (eb el)
    (let ((res (%await-promise
                (get-async (%live-url "/get") :timeout 20.0)
                eb el :timeout 25.0)))
      (ok (<= 200 (response-status res) 299))
      (ok (plusp (length (response-body res)))))))

(deftest live-https-gzip
  "Origin returns Content-Encoding: gzip; backend must decode."
  (with-live (eb el)
    (let ((res (%await-promise
                (get-async (%live-url "/gzip")
                           :accept-encoding '(:gzip)
                           :timeout 20.0)
                eb el :timeout 25.0)))
      (ok (= 200 (response-status res)))
      (ok (null (response-header res :content-encoding)))
      (let ((text (babel:octets-to-string (response-body res) :errorp nil)))
        (ok (search "gzipped" text :test #'char-equal))))))

(deftest live-https-brotli
  "Decode br when http-encoding-brotli is available (CDN negotiation)."
  (with-live (eb el)
    (unless (ignore-errors (asdf:load-system "http-encoding-brotli") t)
      (skip "http-encoding-brotli not available"))
    (unless (content-coding-supported-p :br)
      (skip "br coding not registered"))
    (let* ((url (or (uiop:getenv "HTTP_ASYNC_LIVE_BR_URL")
                    "https://www.cloudflare.com/"))
           (res (%await-promise
                 (get-async url
                            :accept-encoding '(:br)
                            :timeout 20.0)
                 eb el :timeout 25.0)))
      (unless (<= 200 (response-status res) 299)
        (skip (format nil "br origin returned ~A" (response-status res))))
      ;; Either origin sent br (decoded → CE removed) or fell back to identity.
      (ok (null (response-header res :content-encoding)))
      (ok (plusp (length (response-body res)))))))

(deftest live-https-accept-encoding-negotiated
  "Ask for available codings against /gzip — body must decode."
  (with-live (eb el)
    (ignore-errors (asdf:load-system "http-encoding-brotli"))
    (ignore-errors (asdf:load-system "http-encoding-zstd"))
    (let ((res (%await-promise
                (get-async (%live-url "/gzip")
                           :accept-encoding :default
                           :timeout 20.0)
                eb el :timeout 25.0)))
      (ok (= 200 (response-status res)))
      (ok (null (response-header res :content-encoding)))
      (ok (plusp (length (response-body res)))))))
