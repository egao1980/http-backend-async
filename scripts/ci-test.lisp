;;;; Phase 2: fresh image + overlay OpenSSL on loader path, then test.

(setf *debugger-hook*
      (lambda (c h)
        (declare (ignore h))
        (format *error-output* "~&UNHANDLED: ~A~%" c)
        (uiop:quit 1)))

(setf asdf:*compile-file-failure-behaviour* :warn)

(defun call-with-ci-muffles (fn)
  #+sbcl
  (handler-bind ((sb-ext:defconstant-uneql
                  (lambda (c)
                    (declare (ignore c))
                    (let ((r (find-restart 'continue)))
                      (when r (invoke-restart r))))))
    (funcall fn))
  #-sbcl
  (funcall fn))

(call-with-ci-muffles (lambda () (asdf:load-system "cl-repository-client")))

(cl-repository-client/asdf-integration:configure-asdf-source-registry)
(cl-repository-client/asdf-integration:load-system-init-files)

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: test with event backend ~a~%" backend)
  (call-with-ci-muffles
   (lambda ()
     (dolist (n '("rove" "fast-http" "babel" "usocket" "bordeaux-threads"
                  "blackbird" "trivial-gray-streams" "cl-cookie"))
       (unless (asdf:find-system n nil)
         (format t "~&; ci: ql fallback ~a~%" n)
         (ql:quickload n :silent t)))
     (asdf:load-system "cl+ssl")
     (asdf:load-system "cl-stack-ssl")
     (asdf:load-system event-sys)
     (asdf:load-system "http-backend-async")
     (asdf:test-system "http-backend-async"))))

(uiop:quit 0)
