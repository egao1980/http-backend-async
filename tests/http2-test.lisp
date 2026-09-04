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

(deftest http2-streaming-hooks-feed-data
  (let* ((s (make-instance 'async-h2-stream-hooks))
         (got nil))
    (setf (http-backend-async:h2-stream-on-data s)
          (lambda (data start end)
            (setf got (subseq data start end))))
    (http-backend-async::%h2-streaming-apply-data s #(10 20 30 40) 1 3)
    (ok (equalp #(20 30) got))
    (ok (zerop (http-backend-async:h2-stream-pending-window s)))))

(deftest http2-streaming-hooks-hold-window
  (let ((s (make-instance 'async-h2-stream-hooks)))
    (setf (http-backend-async:h2-stream-hold-window-p s) t)
    (http-backend-async::%h2-streaming-apply-data s #(1 2 3) 0 3)
    (ok (= 3 (http-backend-async:h2-stream-pending-window s)))
    (http-backend-async:h2-stream-release-window s)
    (ok (zerop (http-backend-async:h2-stream-pending-window s)))
    (ok (not (http-backend-async:h2-stream-hold-window-p s)))))

(deftest http2-buf-append
  (let ((buf (make-array 0 :element-type '(unsigned-byte 8)
                         :adjustable t :fill-pointer 0)))
    (http-backend-async::h2-buf-append buf #(1 2 3 4) 1 4)
    (ok (equalp #(2 3 4) buf))))

(deftest http2-live-want-stream
  "Live H2 :want-stream — gate with HTTP_ASYNC_H2_LIVE=1."
  (if (not (uiop:getenv "HTTP_ASYNC_H2_LIVE"))
      (skip "HTTP_ASYNC_H2_LIVE unset")
      (with-async-test (eb el backend)
        (declare (ignore backend))
        (multiple-value-bind (res octets)
            (%await-stream-promise
             (http:stream-async :get "https://www.cloudflare.com/"
                                :http-version :http/2
                                :timeout 20.0)
             eb el :timeout 25.0)
          (ok (member (response-http-version res) '(:http/1.1 :http/2) :test #'eq))
          (ok (streamp (response-body res)))
          (ok (plusp (length octets)))))))

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
