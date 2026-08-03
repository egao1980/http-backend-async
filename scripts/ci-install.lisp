;;;; Phase 1: install OCI deps with system OpenSSL. Fresh image loads natives later.

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

(defparameter *ci-ql-sources*
  '(("babel" :ql)
    ("trivial-features" :ql)
    ("cl-unicode" :ql))
  "QL pins: babel already bootstrapped; cl-unicode OCI v0.1.6 lacks idna-mapping.")

(cl-repo:add-registry "https://ghcr.io" :namespace "egao1980/cl-systems" :priority :prepend)

(defun ci-install (oci-name &key (version "latest"))
  (format t "~&; ci: install ~a:~a~%" oci-name version)
  (cl-repository-client/installer:install-system
   "https://ghcr.io" (format nil "egao1980/cl-systems/~a" oci-name) version)
  (cl-repository-client/asdf-integration:configure-asdf-source-registry))

(defun ci-load (name &key version)
  (format t "~&; ci: cl-repo load ~a~@[:~a~]~%" name version)
  (call-with-ci-muffles
   (lambda ()
     (if version
         (cl-repo:load-system name :version version :sources *ci-ql-sources*)
         (cl-repo:load-system name :sources *ci-ql-sources*)))))

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (cl-stack-ssl-version (or (uiop:getenv "CL_STACK_SSL_VERSION") "3.4.1"))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: event backend ~a → ~a~%" backend event-sys)
  (call-with-ci-muffles
   (lambda ()
     (ci-install "cl-plus-ssl" :version "latest")
     (ci-install "cl-stack-ssl" :version cl-stack-ssl-version)
     (let ((setup (probe-file
                   (merge-pathnames
                    (format nil "cl-stack-ssl/~a/src/setup.lisp" cl-stack-ssl-version)
                    (cl-repository-client/installer:systems-root)))))
       (when setup
         (let* ((text (uiop:read-file-string setup))
                (fixed (search "(defconstant +openssl-version+" text :test #'char-equal)))
           (when fixed
             (setf text (concatenate 'string
                                     (subseq text 0 fixed)
                                     "(defparameter +openssl-version+"
                                     (subseq text (+ fixed (length "(defconstant +openssl-version+")))))
             (with-open-file (out setup :direction :output :if-exists :supersede)
               (write-string text out))
             (format t "~&; ci: patched ~a~%" setup)))))
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
     (dolist (n '("rove" "fast-http" "babel" "usocket" "bordeaux-threads"
                  "blackbird" "trivial-gray-streams" "cl-cookie"))
       (unless (asdf:find-system n nil)
         (format t "~&; ci: ql fallback ~a~%" n)
         (ql:quickload n :silent t))))))

(format t "~&; ci: install phase done~%")
(uiop:quit 0)
