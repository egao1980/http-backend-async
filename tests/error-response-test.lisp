(in-package #:http-backend-async/tests)

;;; Error responses and cut-off responses, read the way the LLM providers
;;; (llm-protocol-anthropic / -openai) read them.

;;; #74 / http-protocol#50: a 401 from api.anthropic.com, sent with
;;; :WANT-STREAM T. The provider asks the body's element type before it reads
;;; the error JSON; the live ASYNC-BODY-INPUT-STREAM had no method for it.

(defparameter +anthropic-401-json+
  "{\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid x-api-key\"}}")

(defun %provider-body-string (body)
  "%COERCE-BODY-STRING of llm-protocol-anthropic: characters or octets?"
  (if (subtypep (stream-element-type body) 'character)
      (with-output-to-string (out)
        (loop for c = (read-char body nil nil)
              while c
              do (write-char c out)))
      (babel:octets-to-string
       (coerce (loop for b = (read-byte body nil nil)
                     while b
                     collect b)
               '(vector (unsigned-byte 8)))
       :encoding :utf-8)))

(deftest async-body-input-stream-is-an-octet-stream
  (let ((s (http-backend-async::make-async-body-input-stream)))
    (ok (equal '(unsigned-byte 8) (stream-element-type s)))
    (ok (input-stream-p s))
    (ok (null (clear-input s)) "CLEAR-INPUT is a no-op")
    (ok (not (listen s)) "nothing queued yet")
    (http-backend-async::async-body-feed s (babel:string-to-octets "ok"))
    (ok (listen s))
    (ok (null (clear-input s)))
    (http-backend-async::async-body-eof s)
    (ok (equal "ok" (%provider-body-string s)) "CLEAR-INPUT dropped nothing")
    (close s)
    (ok (not (open-stream-p s)))))

(deftest want-stream-error-body-reads-like-a-provider
  (with-async-test (eb el hb)
    (let ((http-protocol:*default-connection-pool* nil))
      (with-http-fixture ((lambda (method path headers body)
                            (declare (ignore method path headers body))
                            (values 401 '(("content-type" . "application/json")
                                          ("request-id" . "req_test"))
                                    (babel:string-to-octets +anthropic-401-json+))))
        (let* ((res (let ((event-protocol:*event-backend* nil)
                          (event-protocol:*event-loop* nil))
                      (http:request :post (fixture-url "/v1/messages")
                                    :content "{}" :want-stream t
                                    :timeout '(:connect 5 :read 5))))
               (body (response-body res)))
          (unwind-protect
               (progn
                 (ok (= 401 (response-status res)))
                 (ok (typep body 'buffered-binary-input-stream))
                 (let ((live (buffered-stream-source body)))
                   (ok (typep live 'http-backend-async::async-body-input-stream))
                   (ok (equal '(unsigned-byte 8) (stream-element-type live))
                       "the live body under the buffer answers too"))
                 (ok (equal +anthropic-401-json+ (%provider-body-string body))))
            (close body)))))))

;;; #64: a response cut off by EOF (a ghcr blob pull dropped mid-body) is
;;; HTTP-INCOMPLETE-RESPONSE, a connection error, so :RETRY resends a GET.

(defun %truncating-handler (truncated)
  "The first TRUNCATED answers promise 64 octets, send 10 and hang up; later
   answers are whole."
  (let ((n 0)
        (full (make-array 64 :element-type '(unsigned-byte 8) :initial-element 65)))
    (lambda (method path headers body)
      (declare (ignore method path headers body))
      (if (<= (incf n) truncated)
          (values 200 '(("content-type" . "application/octet-stream")
                        ("content-length" . "64"))
                  (subseq full 0 10))
          (values 200 '(("content-type" . "application/octet-stream")) full)))))

(defun %sync-condition (thunk)
  (handler-case (progn (funcall thunk) nil)
    (http-error (e) e)))

(deftest truncated-response-is-an-incomplete-response
  (with-async-test (eb el hb)
    (let ((http-protocol:*default-connection-pool* nil))
      (with-http-fixture ((%truncating-handler 1))
        (let ((c (%sync-condition (lambda () (%sync (fixture-url "/blob") 5)))))
          (ok (typep c 'http-incomplete-response) (format nil "~S: ~A" (type-of c) c))
          (ok (typep c 'http-connection-error) "a transport failure")
          (ok (typep c 'http-protocol-error) "still the protocol error it was")
          (ok (and (typep c 'http-incomplete-response)
                   (plusp (http-incomplete-response-octets c)))
              "the head and 10 body octets arrived"))))))

(deftest truncated-get-is-retried
  (with-async-test (eb el hb)
    (let ((http-protocol:*default-connection-pool* nil)
          (retry (make-http-retry :total 2 :backoff-factor 0)))
      (with-http-fixture ((%truncating-handler 1))
        (let ((res (%sync (fixture-url "/blob") 5 :retry retry)))
          (ok (= 200 (response-status res)))
          (ok (= 64 (length (response-body res))))
          (ok (= 2 *fixture-request-count*)))))))

(deftest truncated-post-is-not-retried
  (with-async-test (eb el hb)
    (let ((http-protocol:*default-connection-pool* nil)
          (retry (make-http-retry :total 2 :backoff-factor 0)))
      (with-http-fixture ((%truncating-handler 1))
        (ok (typep (%sync-condition
                    (lambda () (%sync (fixture-url "/blob") 5 :method :post :retry retry)))
                   'http-incomplete-response))
        (ok (= 1 *fixture-request-count*) "a POST is not resent")))))
