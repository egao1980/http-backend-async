;;;; Phase 1: fetch OCI deps with system OpenSSL. Do NOT ASDF-load / ql:quickload
;;;; mid-flight — loading cffi/babel/event backends rewires foreign state and
;;;; breaks subsequent dexador HTTPS (CL+SSL::PEM-PASSWORD-CALLBACK).
;;;; Phase 2 (ci-test.lisp) loads everything with overlay OpenSSL on the loader path.

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

(defun ci-on-disk-p (name)
  (cl-repository-client/quickload::system-already-installed-p name))

(defun ci-fetch (name &key version)
  "Resolve + install NAME (and OCI plan) without ASDF-load or ql:quickload."
  (format t "~&; ci: fetch ~a~@[:~a~]~%" name version)
  (cl-repository-client/source-policy:call-with-policy-overrides
   *ci-ql-sources* nil nil nil
   (lambda ()
     (cl-repository-client/protected-systems:ensure-snapshot)
     (cl-repository-client/digest-cache:load-digest-cache)
     (let ((plan (cl-repository-client/quickload::compute-install-plan
                  (list name) :version version)))
       (dolist (entry plan)
         (let ((n (car entry))
               (ver (cdr entry)))
           (unless (or (cl-repository-client/source-policy:system-denied-p n)
                       (and (ci-on-disk-p n)
                            (let ((iv (cl-repository-client/quickload::installed-system-version n)))
                              (and iv (string= iv (princ-to-string ver))))))
             (format t "~&; ci: ensure-installed ~a~@[:~a~]~%" n ver)
             (let ((result (cl-repository-client/quickload::ensure-system-installed
                            n :version ver)))
               (when result
                 (cl-repository-client/asdf-integration:configure-asdf-source-registry))))))
       ;; Do NOT try-quicklisp-fallback here — ql:quickload loads cffi/babel and
       ;; breaks GHCR pulls. Unpublished deps are ql'd once at the end.
       (when cl-repository-client/quickload::*missing-deps-accumulator*
         (format t "~&; ci: deferring ql fallback: ~{~a~^, ~}~%"
                 cl-repository-client/quickload::*missing-deps-accumulator*)))))
  (cl-repository-client/asdf-integration:configure-asdf-source-registry)
  (unless (ci-on-disk-p name)
    (error "ci-fetch: ~a not on disk after install" name)))

(defun ci-patch-stack-ssl (&optional (version "3.4.1"))
  "Patch stale OCI source (DEFCONSTANT -> DEFPARAMETER) until republished."
  (let ((setup (probe-file
                (merge-pathnames
                 (format nil "cl-stack-ssl/~a/src/setup.lisp" version)
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
          (format t "~&; ci: patched ~a~%" setup))))))

(let* ((backend (string-downcase (or (uiop:getenv "HTTP_ASYNC_EVENT_BACKEND") "libuv")))
       (cl-stack-ssl-version (or (uiop:getenv "CL_STACK_SSL_VERSION") "3.4.1"))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: event backend ~a -> ~a~%" backend event-sys)
  (call-with-ci-muffles
   (lambda ()
     (ci-install "cl-plus-ssl" :version "latest")
     ;; All GHCR pulls before any ql:quickload / ASDF load.
     (ci-fetch "http-protocol" :version "0.1.0")
     (ci-fetch "http-encoding-chipz" :version "0.1.0")
     (ci-fetch "http-encoding-brotli" :version "0.1.0")
     (ci-fetch "cl-stack-brotli" :version "1.2.0")
     (ci-fetch "quri" :version "0.7.1")
     (ci-fetch "chipz" :version "0.8")
     (ci-fetch "salza2" :version "2.1")
     (ci-fetch "alexandria" :version "1.0.1")
     (ci-fetch "cffi" :version "677cabae64b181330a3bbbda9c11891a2a8edcdc")
     (ci-fetch "event-protocol" :version "0.1.0")
     (ci-fetch event-sys :version "0.1.0")
     (ci-install "cl-stack-ssl" :version cl-stack-ssl-version)
     (ci-patch-stack-ssl cl-stack-ssl-version)
     ;; QL only after OCI HTTPS is done (image will be discarded before tests).
     (dolist (n '("rove" "fast-http" "babel" "usocket" "bordeaux-threads"
                  "blackbird" "trivial-gray-streams" "cl-cookie" "cl-unicode"))
       (unless (or (ci-on-disk-p n) (asdf:find-system n nil))
         (format t "~&; ci: ql fallback ~a~%" n)
         (ql:quickload n :silent t))))))

(format t "~&; ci: install phase done~%")
(uiop:quit 0)
