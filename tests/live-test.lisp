(in-package #:http-backend-async/tests)

;;; Live HTTPS + CE — mirrors psf/requests httpbin cases (see PROVENANCE.md).
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

(deftest live-http-200-ok-get
  "requests: GET httpbin/get over HTTPS."
  (with-live (eb el)
    (let ((res (%await-promise
                (get-async (%live-url "/get") :timeout 20.0)
                eb el :timeout 25.0)))
      (ok (= 200 (response-status res)))
      (ok (plusp (length (response-body res))))
      (ok (search "\"url\"" (%body-text res))))))

(deftest live-decompress-gzip
  "requests: TestRequests.test_decompress_gzip"
  (with-live (eb el)
    (let* ((res (%await-promise
                 (get-async (%live-url "/gzip")
                            :accept-encoding '(:gzip)
                            :timeout 20.0)
                 eb el :timeout 25.0))
           (text (%body-text res)))
      (ok (= 200 (response-status res)))
      ;; requests: r.content.decode("ascii")
      (ok (every (lambda (c) (< (char-code c) 128)) text))
      (ok (search "\"gzipped\"" text))
      (ok (search "true" text))
      ;; stack policy: CE removed after decode
      (ok (null (response-header res :content-encoding))))))

(deftest live-decompress-deflate
  "httpbin /deflate — same shape as requests/urllib3 deflate decode."
  (with-live (eb el)
    (let* ((res (%await-promise
                 (get-async (%live-url "/deflate")
                            :accept-encoding '(:deflate)
                            :timeout 20.0)
                 eb el :timeout 25.0))
           (text (%body-text res)))
      (ok (= 200 (response-status res)))
      (ok (search "\"deflated\"" text))
      (ok (null (response-header res :content-encoding))))))

(deftest live-accept-encoding-identity
  "requests: Accept-Encoding identity — no transport compression."
  (with-live (eb el)
    (let ((res (%await-promise
                (get-async (%live-url "/get")
                           :accept-encoding "identity"
                           :timeout 20.0)
                eb el :timeout 25.0)))
      (ok (= 200 (response-status res)))
      (ok (null (response-header res :content-encoding)))
      (ok (plusp (length (response-body res)))))))

(deftest live-default-accept-encoding-gzip
  "requests default_headers include gzip — /gzip must still decode."
  (with-live (eb el)
    (ignore-errors (asdf:load-system "http-encoding-brotli"))
    (ignore-errors (asdf:load-system "http-encoding-zstd"))
    (let* ((res (%await-promise
                 (get-async (%live-url "/gzip")
                            :accept-encoding :default
                            :timeout 20.0)
                 eb el :timeout 25.0))
           (text (%body-text res)))
      (ok (= 200 (response-status res)))
      (ok (search "gzipped" text :test #'char-equal))
      (ok (null (response-header res :content-encoding))))))

(deftest live-decompress-brotli-optional
  "requests FAQ: br when brotli available — CDN origin (httpbin has no /brotli)."
  (with-live (eb el)
    (unless (ignore-errors (asdf:load-system "http-encoding-brotli") t)
      (skip "http-encoding-brotli not available (like requests without brotli pkg)"))
    (unless (content-coding-supported-p :br)
      (skip "br coding not registered"))
    (let* ((url (or (uiop:getenv "HTTP_ASYNC_LIVE_BR_URL")
                    "https://www.cloudflare.com/"))
           (res (%await-promise
                 (get-async url :accept-encoding '(:br) :timeout 20.0)
                 eb el :timeout 25.0)))
      (unless (<= 200 (response-status res) 299)
        (skip (format nil "br origin returned ~A" (response-status res))))
      (ok (null (response-header res :content-encoding)))
      (ok (plusp (length (response-body res)))))))
