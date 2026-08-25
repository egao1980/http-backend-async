;;;; Event backend is a matrix env, not an .asd constant.

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: event backend ~a -> ~a~%" backend event-sys)
  (push event-sys cl-repository-ci-lib:*extra-with*))
