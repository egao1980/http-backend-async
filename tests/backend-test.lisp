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

(deftest get-ok-lib-matrix
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((url (fixture-url "/ok"))
             (res (%await-promise (get-async url) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "ok") (response-body res)))))))

(deftest get-gzip-decoded
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((url (fixture-url "/gzip"))
             (res (%await-promise (get-async url) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp (babel:string-to-octets "hello-gzip") (response-body res)))
        (ok (null (response-header res :content-encoding)))))))

(deftest post-echo
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let* ((payload (babel:string-to-octets "ping"))
             (url (fixture-url "/echo"))
             (res (%await-promise
                   (post-async url :content payload) eb el)))
        (ok (= 200 (response-status res)))
        (ok (equalp payload (response-body res)))))))

(deftest cancel-in-flight
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
        ;; Drive a tick so cleanup can settle; request should not complete OK.
        (defer eb el (lambda () (stop eb el)))
        (event-protocol:run eb el :stop-when-idle t)
        (ok (async-request-canceled-p handle))
        (ok (null canceled)))))) ; cancel-request does not invoke error-callback
