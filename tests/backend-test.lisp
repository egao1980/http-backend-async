(in-package #:http-backend-async/tests)

(defun %make-event-backend ()
  (let* ((name (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv"))
         (key (intern (string-upcase name) :keyword))
         (sys (ecase key
                (:libuv "event-backend-libuv")
                (:libev "event-backend-libev")))
         (pkg (ecase key
                (:libuv :event-backend-libuv)
                (:libev :event-backend-libev)))
         (maker-name (ecase key
                       (:libuv "MAKE-LIBUV-BACKEND")
                       (:libev "MAKE-LIBEV-BACKEND"))))
    (asdf:load-system sys)
    (funcall (symbol-function (find-symbol maker-name pkg)))))

(defmacro with-async-test ((backend-var loop-var http-backend-var) &body body)
  `(let* ((,backend-var (%make-event-backend))
          (,loop-var (make-event-loop ,backend-var))
          (,http-backend-var (make-async-backend))
          (*event-backend-maker* (lambda () ,backend-var)))
     (with-event-backend (,backend-var)
       (with-event-loop-var (,loop-var)
         (let ((*http-backend* ,http-backend-var))
           ,@body)))))

(defun %await-promise (promise event-backend event-loop &key (timeout 5.0))
  "Run EVENT-LOOP until PROMISE finishes or TIMEOUT. Returns response or signals."
  (let ((result nil)
        (err nil)
        (done nil))
    (blackbird:catcher
      (blackbird:attach promise
                        (lambda (v)
                          (setf result v done t)
                          (stop event-backend event-loop)))
      (error (e)
        (setf err e done t)
        (stop event-backend event-loop)))
    (sleep* event-backend event-loop timeout
            :callback (lambda ()
                        (unless done
                          (setf err (make-condition 'http-timeout-error
                                                    :message "test await timed out")
                                done t)
                          (stop event-backend event-loop))))
    (event-protocol:run event-backend event-loop :stop-when-idle nil)
    (when err (error err))
    result))

(defun %body-text (res)
  (babel:octets-to-string (response-body res) :encoding :utf-8 :errorp nil))

;;; --- requests-shaped local fixture (httpbin paths) ---

(deftest test-http-200-ok-get
  "requests: TestRequests.test_HTTP_200_OK_GET_ALTERNATIVE (local fixture)."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((res (%await-promise (get-async (fixture-url "/ok")) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "ok") (response-body res)))))))

(deftest test-decompress-gzip
  "requests: TestRequests.test_decompress_gzip — body decodes after CE gzip."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((res (%await-promise
                   (get-async (fixture-url "/gzip") :accept-encoding '(:gzip))
                   eb el))
             (text (%body-text res)))
        (ok (= 200 (response-status res)))
        ;; Our policy: strip CE after decode (stricter than stock requests headers).
        (ok (null (response-header res :content-encoding)))
        (ok (search "gzipped" text :test #'char-equal))
        ;; requests asserts r.content.decode("ascii") succeeds
        (ok (every (lambda (c) (< (char-code c) 128)) text))))))

(deftest test-decompress-deflate
  "urllib3 test_decode_deflate / httpbin /deflate."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((res (%await-promise
                   (get-async (fixture-url "/deflate") :accept-encoding '(:deflate))
                   eb el))
             (text (%body-text res)))
        (ok (= 200 (response-status res)))
        (ok (null (response-header res :content-encoding)))
        (ok (search "deflated" text :test #'char-equal))))))

(deftest test-content-encoding-case-insensitive
  "urllib3 test_decode_deflate_case_insensitve — CE token case."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((res (%await-promise
                   (get-async (fixture-url "/gzip-case") :accept-encoding '(:gzip))
                   eb el))
             (text (%body-text res)))
        (ok (= 200 (response-status res)))
        (ok (search "gzipped" text :test #'char-equal))))))

(deftest test-post-echo
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((payload (babel:string-to-octets "ping"))
             (res (%await-promise
                   (post-async (fixture-url "/echo") :content payload) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp payload (response-body res)))))))

(deftest test-cancel-in-flight
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (let* ((url (fixture-url "/ok"))
             (client (make-http-client hb))
             (req (make-http-request :method :get :url url))
             (canceled nil)
             (handle
              (send-async
               hb client req
               :callback (lambda (r) (declare (ignore r)))
               :error-callback
               (lambda (c)
                 (when (typep c 'http-canceled)
                   (setf canceled t))
                 (stop eb el)))))
        (cancel-request hb handle)
        (defer eb el (lambda () (stop eb el)))
        (event-protocol:run eb el :stop-when-idle t)
        (ok (async-request-canceled-p handle))
        (ok (null canceled))))))

(deftest test-session-cookie-jar
  "requests Session: Set-Cookie persisted and sent on next request."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (http:with-client (client)
        (let ((res1 (%await-promise
                     (get-async (fixture-url "/cookies/set") :client client)
                     eb el)))
          (ok (= 200 (response-status res1)))
          (ok (= 1 (length (response-cookies res1))))
          (ok (plusp (length (cl-cookie:cookie-jar-cookies
                              (http-client-cookie-jar client))))))
        ;; Fresh event loop for second hop (fixture still up).
        (let* ((eb2 (%make-event-backend))
               (el2 (make-event-loop eb2)))
          (with-event-backend (eb2)
            (with-event-loop-var (el2)
              (let* ((res2 (%await-promise
                            (get-async (fixture-url "/cookies") :client client)
                            eb2 el2))
                     (text (%body-text res2)))
                (ok (= 200 (response-status res2)))
                (ok (search "session=abc" text))))))))))

(deftest test-redirect-follow
  "requests: follow 302 → final 200; history has the redirect hop."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((res (%await-promise (get-async (fixture-url "/redirect/1")) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "ok") (response-body res)))
        (ok (= 1 (length (response-history res))))
        (ok (= 302 (response-status (first (response-history res)))))))))

(deftest test-redirect-chain
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((res (%await-promise (get-async (fixture-url "/redirect/2")) eb el)))
        (ok (= 200 (response-status res)))
        (ok (= 2 (length (response-history res))))))))

(deftest test-redirect-max-zero
  "max-redirects 0 → return 302, empty history."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((res (%await-promise
                  (get-async (fixture-url "/redirect/1") :max-redirects 0)
                  eb el)))
        (ok (= 302 (response-status res)))
        (ok (null (response-history res)))))))

(deftest test-redirect-exceeded
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (ok (signals
           (%await-promise
            (get-async (fixture-url "/redirect/2") :max-redirects 1)
            eb el)
           'http-redirect-error)))))

(deftest test-redirect-set-cookie
  "Set-Cookie on redirect hop is sent to the final URL."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (http:with-client (client)
        (let* ((res (%await-promise
                     (get-async (fixture-url "/redirect/set-cookie")
                                :client client)
                     eb el))
               (text (%body-text res)))
          (ok (= 200 (response-status res)))
          (ok (search "redir=1" text)))))))

(deftest test-redirect-absolute
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((url (fixture-url (format nil "/absolute/~A/ok" *fixture-port*)))
             (res (%await-promise (get-async url) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "ok") (response-body res)))))))
