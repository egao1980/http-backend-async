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

(defun ci-newest-tag (oci-name)
  "Newest version tag on ghcr.io/egao1980/cl-systems/NAME (excludes 'latest')."
  (let* ((token (or (uiop:getenv "GITHUB_TOKEN") (uiop:getenv "GH_TOKEN")))
         (auth (when token
                 (cl-oci-client/auth:make-auth-config
                  :username (or (uiop:getenv "GITHUB_ACTOR") "x-access-token")
                  :password token)))
         (reg (cl-oci-client/registry:make-registry "https://ghcr.io" :auth auth))
         (repo (format nil "egao1980/cl-systems/~a" oci-name))
         (tags (cl-oci-client/content-discovery:list-tags reg repo))
         (version-tags (remove "latest" tags :test #'string=)))
    (or (cl-repository-client/version-utils:select-preferred-version version-tags)
        (first tags)
        (error "ci-newest-tag: no tags for ~a" oci-name))))

(defun ci-install (oci-name &key version)
  "Install OCI package. VERSION nil → newest published version tag."
  (let ((version (or version (ci-newest-tag oci-name))))
    (format t "~&; ci: install ~a:~a~%" oci-name version)
    (cl-repository-client/installer:install-system
     "https://ghcr.io" (format nil "egao1980/cl-systems/~a" oci-name) version)
    (cl-repository-client/asdf-integration:configure-asdf-source-registry)
    version))

(defun ci-on-disk-p (name)
  (cl-repository-client/quickload::system-already-installed-p name))

(defun ci-fetch (name &key version)
  "Resolve + install NAME (and OCI plan) without ASDF-load or ql:quickload.
   VERSION nil → newest published tag (cl-repo :latest)."
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

(defun ci-patch-stack-ssl (&optional version)
  "Patch stale OCI source (DEFCONSTANT -> DEFPARAMETER) until republished."
  (let* ((root (cl-repository-client/installer:systems-root))
         (setup
           (or (when version
                 (probe-file
                  (merge-pathnames
                   (format nil "cl-stack-ssl/~a/src/setup.lisp" version) root)))
               (first (directory
                       (merge-pathnames "cl-stack-ssl/*/src/setup.lisp" root))))))
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
       (cl-stack-ssl-version (uiop:getenv "CL_STACK_SSL_VERSION"))
       (event-sys (cond ((string= backend "libuv") "event-backend-libuv")
                        ((string= backend "libev") "event-backend-libev")
                        (t (error "Unknown HTTP_ASYNC_EVENT_BACKEND: ~a" backend)))))
  (format t "~&; ci: event backend ~a -> ~a~%" backend event-sys)
  (call-with-ci-muffles
   (lambda ()
     (ci-install "cl-plus-ssl" :version "latest") ; real :latest tag
     ;; All GHCR pulls before any ql:quickload / ASDF load.
     ;; Omit :version → cl-repo picks newest published tag.
     (ci-fetch "http-protocol" :version "0.3.0")
     (ci-fetch "http-encoding-chipz")
     (ci-fetch "http-encoding-brotli")
     (ci-fetch "cl-stack-brotli")
     (ci-fetch "quri")
     (ci-fetch "chipz")
     (ci-fetch "salza2")
     (ci-fetch "alexandria")
     (ci-fetch "cffi")
     (ci-fetch "event-protocol")
     (ci-fetch event-sys)
     (handler-case (ci-fetch "ws-protocol" :version "0.2.0")
       (error (e)
         (format t "~&; ci: ws-protocol OCI unavailable (~A) — git fallback~%" e)
         (let* ((root (cl-repository-client/installer:systems-root))
                (dest (merge-pathnames "ws-protocol/0.2.0/" root)))
           (unless (probe-file (merge-pathnames "ws-protocol.asd" dest))
             (ensure-directories-exist dest)
             (uiop:run-program
              (list "git" "clone" "--depth" "1"
                    "https://github.com/egao1980/ws-protocol.git"
                    (uiop:native-namestring dest))
              :output t :error-output t))
           (cl-repository-client/asdf-integration:configure-asdf-source-registry))))
     (let ((ssl-ver (ci-install "cl-stack-ssl" :version cl-stack-ssl-version)))
       (ci-patch-stack-ssl ssl-ver)
       (when (uiop:getenv "GITHUB_ENV")
         (with-open-file (out (uiop:getenv "GITHUB_ENV")
                              :direction :output
                              :if-exists :append :if-does-not-exist :create)
           (format out "CL_STACK_SSL_VERSION=~a~%" ssl-ver))))
     ;; QL only after OCI HTTPS is done (image will be discarded before tests).
     (dolist (n '("rove" "fast-http" "babel" "usocket" "bordeaux-threads"
                  "blackbird" "trivial-gray-streams" "cl-cookie" "cl-unicode"
                  "cl-base64" "http2"))
       (unless (or (ci-on-disk-p n) (asdf:find-system n nil))
         (format t "~&; ci: ql fallback ~a~%" n)
         (ql:quickload n :silent t))))))

(format t "~&; ci: install phase done~%")
(uiop:quit 0)
