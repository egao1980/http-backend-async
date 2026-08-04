;;;; HTTP/2 preference + (optional) live ALPN negotiation.

(in-package #:http-backend-async/tests)

(deftest http2-cleartext-forced-signals
  (testing "forced :http/2 on http:// is not available (no h2c yet)"
    (with-async-test (eb el backend)
      (declare (ignore eb el))
      (let* ((client (make-http-client backend :http-version :http/2))
             (req (make-http-request :url "http://example.test/"
                                     :http-version :http/2)))
        ;; Fail before TCP when cleartext + forced :http/2 (no h2c yet).
        (ok (signals (send backend client req)
                     'http-version-not-available))))))

(deftest http2-alpn-helpers
  (ok (equal '("h2" "http/1.1") (alpn-protocols-for-version :auto)))
  (ok (eq :http/2 (http-version-from-alpn "h2")))
  (ok (equal '(:http/1.1 :http/2)
             (backend-http-versions (make-async-backend)))))

#+ (or)
(deftest http2-live-nghttp2
  "Live: requires network. Enable with HTTP_ASYNC_H2_LIVE."
  (when (uiop:getenv "HTTP_ASYNC_H2_LIVE")
    (with-async-test (eb el backend)
      (declare (ignore eb el))
      (let* ((client (make-http-client backend :http-version :http/2
                                       :verify t))
             (req (make-http-request :url "https://www.cloudflare.com/"
                                     :http-version :auto
                                     :timeout 20.0))
             (res (send backend client req)))
        (ok (member (response-http-version res) '(:http/1.1 :http/2) :test #'eq))
        (ok (<= 200 (response-status res) 399))))))
