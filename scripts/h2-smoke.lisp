(asdf:load-system :http-protocol :force t)
(asdf:load-system :http-backend-async :force t)
(asdf:load-system :event-backend-libuv :force t)
(ql:quickload :http2 :silent t)

(setf http-backend-async:*event-backend-maker*
      (lambda () (event-backend-libuv:make-libuv-backend)))

(defun try-url (url)
  (let* ((maker http-backend-async:*event-backend-maker*)
         (eb (funcall maker))
         (el (event-protocol:make-event-loop eb)))
    (event-protocol:with-event-backend (eb)
      (event-protocol:with-event-loop-var (el)
        (handler-case
            (let* ((backend (http-backend-async:make-async-backend))
                   (client (http-protocol:make-http-client
                            backend :http-version :http/2))
                   (req (http-protocol:make-http-request
                         :url url :http-version :http/2 :timeout 20.0))
                   (res (http-protocol:send backend client req)))
              (format t "~a => ~a ~a len=~a~%"
                      url
                      (http-protocol:response-status res)
                      (http-protocol:response-http-version res)
                      (length (http-protocol:response-body res))))
          (error (e)
            (format t "~a ERR ~a~%" url e)
            (let ((dbg (find-symbol "DEBUG-DATA" :http2/core))
                  (code (find-symbol "ERROR-CODE" :http2/core)))
              (when (and dbg (slot-exists-p e dbg) (slot-boundp e dbg))
                (format t "  debug=~s~%"
                        (babel:octets-to-string (slot-value e dbg) :errorp nil)))
              (when (and code (slot-exists-p e code) (slot-boundp e code))
                (format t "  code=~s~%" (slot-value e code))))))))))

(try-url "https://nghttp2.org/")
(uiop:quit 0)
