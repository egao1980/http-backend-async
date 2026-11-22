(in-package #:http-backend-async/tests)

;;; #62: event-backend-libuv calls an IO callback as (:ERROR code) when the
;;; poll fails. A one-argument callback turned that call into "invalid number
;;; of arguments: 2", libuv only warned, and the request never completed: the
;;; caller waited for its own timeout (an hour in cl-repo).
;;;
;;; Every test here is bounded: %AWAIT-PROMISE gives up after a few seconds
;;; with HTTP-TIMEOUT-ERROR, which is the failure these tests rule out.

(defvar *inject-poll-error* nil
  "When true, the next IO callback of a socket registered while it was true
   is delivered as (:ERROR -104), as libuv reports a reset peer. Cleared once
   used.")

(defmethod event-protocol:register-io :around
    ((backend event-protocol:event-backend) loop fd direction callback &key)
  (if *inject-poll-error*
      (let ((wrapped (lambda (status &rest more)
                       (if *inject-poll-error*
                           (progn (setf *inject-poll-error* nil)
                                  (funcall callback :error -104))
                           (apply callback status more)))))
        (call-next-method backend loop fd direction wrapped))
      (call-next-method)))

(defun %await-condition (promise eb el &key (timeout 5.0))
  "Await PROMISE; return (values condition-or-nil seconds)."
  (let ((start (get-internal-real-time)))
    (values (handler-case (progn (%await-promise promise eb el :timeout timeout)
                                 nil)
              (error (e) e))
            (/ (- (get-internal-real-time) start)
               internal-time-units-per-second))))

(deftest poll-error-fails-request-promptly
  "A poll error on a fresh connection fails the request with
   HTTP-CONNECTION-ERROR carrying the backend status, at once."
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (let ((*inject-poll-error* t))
        (multiple-value-bind (c seconds)
            (%await-condition (get-async (fixture-url "/ok")) eb el)
          (ok (typep c 'http-connection-error)
              (format nil "got ~S, want HTTP-CONNECTION-ERROR" c))
          (ok (not (typep c 'http-timeout-error)))
          (ok (search "-104" (princ-to-string c)))
          (ok (< seconds 3)))))))

(deftest poll-error-on-stale-pooled-socket-redials
  "A poll error on a pooled socket before any response octet is a stale
   keep-alive connection: the GET goes out again on a new connection."
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture (nil :keep-alive t)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (ok (= 200 (response-status
                      (%await-promise (get-async (fixture-url "/ok") :client client)
                                      eb el))))
          (let ((*inject-poll-error* t))
            (let ((res (%await-promise (get-async (fixture-url "/ok") :client client)
                                       eb el)))
              (ok (= 200 (response-status res)))
              (ok (string= "ok" (%body-text res)))))
          (pool-clear pool))))))

;;; A real poll error: the peer resets the socket in the middle of the
;;; response (SO_LINGER 0, then close, sends RST). On Linux libuv reports the
;;; POLLERR as (:ERROR UV_EBADF); elsewhere recv fails with ECONNRESET. Either
;;; way the request fails with HTTP-CONNECTION-ERROR, promptly.

(defun %set-linger-zero (usock)
  "Make closing USOCK send RST instead of FIN."
  (let ((fd (http-backend-async::socket-fd usock)))
    (multiple-value-bind (level optname)
        #+linux (values 1 13)
        #-linux (values #xffff #x0080)
      #+(or windows win32)
      (cffi:with-foreign-object (l :unsigned-short 2)
        (setf (cffi:mem-aref l :unsigned-short 0) 1
              (cffi:mem-aref l :unsigned-short 1) 0)
        (cffi:foreign-funcall "setsockopt" :uintptr fd :int level :int optname
                              :pointer l :int 4 :int))
      #-(or windows win32)
      (cffi:with-foreign-object (l :int 2)
        (setf (cffi:mem-aref l :int 0) 1
              (cffi:mem-aref l :int 1) 0)
        (cffi:foreign-funcall "setsockopt" :int fd :int level :int optname
                              :pointer l :int 8 :int)))))

(defun %start-reset-server ()
  "Origin that answers with half a response, then resets the connection.
   Returns (values port thread listener)."
  (let* ((server (usocket:socket-listen "127.0.0.1" 0
                                        :reuseaddress t
                                        :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port server)))
    (values
     port
     (bt:make-thread
      (lambda ()
        (ignore-errors
         (let* ((client (usocket:socket-accept server
                                               :element-type '(unsigned-byte 8)))
                (stream (usocket:socket-stream client)))
           (loop for line = (%read-line-octets stream)
                 until (or (null line) (string= line (format nil "~C~C" #\Return #\Newline))))
           (write-sequence
            (babel:string-to-octets
             (format nil "HTTP/1.1 200 OK~C~CContent-Length: 1000~C~C~C~Cpartial"
                     #\Return #\Newline #\Return #\Newline #\Return #\Newline))
            stream)
           (force-output stream)
           (sleep 0.2)
           (%set-linger-zero client)
           (usocket:socket-close client))))
      :name "http-backend-async-reset-origin")
     server)))

(deftest peer-reset-mid-response-fails-promptly
  (multiple-value-bind (port thread server) (%start-reset-server)
    (unwind-protect
         (with-async-test (eb el hb)
           (multiple-value-bind (c seconds)
               (%await-condition
                (get-async (format nil "http://127.0.0.1:~D/reset" port))
                eb el :timeout 8.0)
             (ok (typep c 'http-connection-error)
                 (format nil "got ~S, want HTTP-CONNECTION-ERROR" c))
             (ok (not (typep c 'http-timeout-error)))
             (ok (< seconds 5))))
      (ignore-errors (usocket:socket-close server))
      (ignore-errors (bt:join-thread thread)))))
