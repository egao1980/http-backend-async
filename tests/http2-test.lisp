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

(defun %count-preface (octets)
  (let ((needle (babel:string-to-octets "PRI * HTTP/2.0"))
        (count 0)
        (start 0))
    (loop for pos = (search needle octets :start2 start)
          while pos
          do (incf count)
             (setf start (+ pos (length needle))))
    count))

(deftest h2-second-stream-omits-connection-preface
  "A second H2-OPEN-REQUEST on the same session is a new stream, not a new
   connection. The client preface is written only when the session is created."
  (if (not (ensure-http2))
      (skip "http2/client not loadable")
      (let* ((pump (make-instance 'async-h2-pump-stream))
             (session (make-async-h2-session
                       pump
                       :stream-class 'http-backend-async::async-h2-streaming-client-stream))
             (uri (quri:uri "https://example.test/a")))
        (ok (http-backend-async::h2-session-idle-p session))
        (h2-open-request session :get uri nil)
        (let ((first (http-backend-async::h2-pump-take-out pump)))
          (ok (= 1 (%count-preface first))))
        ;; Stream 1 is still open: do not treat the session as reusable.
        (ok (not (http-backend-async::h2-session-idle-p session)))
        (ok (not (http-backend-async::h2-session-reusable-p session)))
        ;; Peer SETTINGS (empty) + HEADERS :status 200, END_STREAM, on stream 1.
        ;; Static-table index 8 is :status 200 (HPACK indexed, 0x88).
        (http-backend-async::h2-pump-feed-in
         pump
         #(0 0 0 4 0 0 0 0 0
           0 0 1 1 5 0 0 0 1
           #x88))
        (http-backend-async::h2-process-pending session)
        (ok (http-backend-async::h2-session-idle-p session))
        (h2-open-request session :get (quri:uri "https://example.test/b") nil)
        (let ((second (http-backend-async::h2-pump-take-out pump)))
          (ok (plusp (length second)))
          (ok (zerop (%count-preface second)))))))

(deftest h2-goaway-session-is-not-reusable
  "GOAWAY is not a queryable flag on the http2 connection. DO-GOAWAY records
   it, and a parse error while draining also refuses reuse."
  (if (not (ensure-http2))
      (skip "http2/client not loadable")
      (let* ((pump (make-instance 'async-h2-pump-stream))
             (session (make-async-h2-session pump)))
        ;; length=8 type=GOAWAY (0x07), the prefix seen as INVALID-VERSION
        ;; when an h1 writer was pointed at an h2 socket.
        (http-backend-async::h2-pump-feed-in
         pump
         #(0 0 8 7 0 0 0 0 0
           0 0 0 0
           0 0 0 0))
        (ok (not (http-backend-async::h2-session-reusable-p session)))
        (ok (http-backend-async::h2-connection-saw-goaway-p
             (http-backend-async::async-h2-session-connection session))))))

(deftest h2-open-request-streaming-body
  "HEADERS without END_STREAM, then DATA chunks."
  (if (not (ensure-http2))
      (skip "http2/client not loadable")
      (let* ((pump (make-instance 'async-h2-pump-stream))
             (session (make-async-h2-session pump))
             (uri (quri:uri "https://example.test/upload"))
             (stream (h2-open-request session :post uri
                                      '(("content-type" . "application/octet-stream"))
                                      :end-stream nil)))
        (ok stream)
        (h2-write-data session stream #(1 2 3) :end-stream nil)
        (h2-write-data session stream #(4) :end-stream t)
        (ok (plusp (length (http-backend-async::h2-pump-take-out pump)))))))

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

(deftest http2-live-sequential-same-client
  "Two h2 GETs on one client/pool: the second must not replay HTTP/1.1 on a
   pooled ALPN=h2 socket (INVALID-VERSION on GOAWAY). HTTP_ASYNC_H2_LIVE=1."
  (if (not (uiop:getenv "HTTP_ASYNC_H2_LIVE"))
      (skip "HTTP_ASYNC_H2_LIVE unset")
      (with-async-test (eb el backend)
        (let ((client (make-http-client backend :http-version :http/2 :verify t)))
          (dotimes (i 2)
            (let ((res (%await-promise
                        (http:get-async "https://www.cloudflare.com/"
                                        :client client :timeout 20.0)
                        eb el :timeout 25.0)))
              (ok (eq :http/2 (response-http-version res))
                  (format nil "request ~D over h2" (1+ i)))
              (ok (<= 200 (response-status res) 399))))))))

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
