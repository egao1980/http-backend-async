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
