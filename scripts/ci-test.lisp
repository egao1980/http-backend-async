;;;; CI: install deps via cl-repository-client, then test this checkout.
;;;; Bootstrap (Roswell + .cl-repository checkout) is outside this file.
;;;; Event backend selected by HTTP_ASYNC_EVENT_BACKEND (libuv|libev).

(setf *debugger-hook*
      (lambda (c h)
        (declare (ignore h))
        (format *error-output* "~&UNHANDLED: ~A~%" c)
        (uiop:quit 1)))

(asdf:load-system "cl-repository-client")

(defun ci-load (name &key version)
  (format t "~&; ci: cl-repo load ~a~@[:~a~]~%" name version)
  (if version
      (cl-repo:load-system name :version version)
      (cl-repo:load-system name)))

(defun ci-ensure-ql (&rest names)
  "QL only for systems not yet published to egao1980/cl-systems."
  (dolist (name names)
    (unless (asdf:find-system name nil)
      (format t "~&; ci: ql fallback (unpublished) ~a~%" name)
      (ql:quickload name :silent t))))

(cl-repo:add-registry "https://ghcr.io" :namespace "egao1980/cl-systems" :priority :prepend)

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: event backend ~a → ~a~%" backend event-sys)
  ;; cl+ssl OCI name is cl-plus-ssl (GHCR forbids '+').
  (ci-load "cl-plus-ssl" :version "latest")
  (ci-load "cl-stack-ssl" :version "3.4.1")
  (ci-load "http-protocol" :version "0.1.0")
  (ci-load "event-protocol" :version "0.1.0")
  (ci-load event-sys :version "0.1.0")
  (ci-load "http-encoding-chipz" :version "0.1.0")
  (ci-load "http-encoding-brotli" :version "0.1.0")
  (ci-load "cl-stack-brotli" :version "1.2.0")
  (ci-load "quri" :version "0.7.1")
  (ci-load "chipz" :version "0.8")
  (ci-load "salza2" :version "2.1")
  (ci-load "cffi" :version "677cabae64b181330a3bbbda9c11891a2a8edcdc")
  (ci-load "alexandria" :version "1.0.1")
  (ci-ensure-ql "rove" "fast-http" "babel" "usocket" "bordeaux-threads"
                "blackbird" "trivial-gray-streams" "cl-cookie"))

(asdf:test-system "http-backend-async")
(uiop:quit 0)
