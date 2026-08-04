;;;; Phase 1: install SUT dependency closure via cl-repository-client.
;;;; Event backend + cl-stack-ssl + soft http2/ws libs are CI :with extras
;;;; (not in the main .asd). No ASDF-load of overlays here — see ci-test.

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

(defun ci-record-installed-version (system env-var)
  (let ((ver (cl-repo:installed-system-version system))
        (env (uiop:getenv "GITHUB_ENV")))
    (when (and ver env)
      (with-open-file (out env :direction :output :if-exists :append :if-does-not-exist :create)
        (format out "~a=~a~%" env-var ver))
      (format t "~&; ci: ~a=~a~%" env-var ver))))

(call-with-ci-muffles (lambda () (asdf:load-system "cl-repository-client")))

(cl-repo:add-registry "https://ghcr.io" :namespace "egao1980/cl-systems" :priority :prepend)

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend))))
       (with (list event-sys
                   "cl-stack-ssl"
                   ;; Soft deps used by websocket tests (loaded optionally at runtime).
                   "http2/client"
                   "fast-websocket"
                   "http-encoding-brotli"
                   "cl-stack-brotli")))
  (format t "~&; ci: event backend ~a -> ~a~%" backend event-sys)
  (call-with-ci-muffles
   (lambda ()
     (cl-repo:ensure-system-dependencies "http-backend-async"
       :also-tests t
       :with with
       :sources '(("babel" :ql)
                  ("trivial-features" :ql)
                  ("cl-unicode" :ql)))
     (ci-record-installed-version "cl-stack-ssl" "CL_STACK_SSL_VERSION"))))

(format t "~&; ci: install phase done~%")
(uiop:quit 0)
