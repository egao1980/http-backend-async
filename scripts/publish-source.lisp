;;; Source-only OCI publish for http-backend-async (no native overlays).
;;; Env: PKG_VERSION OCI_REGISTRY OCI_NAMESPACE GITHUB_ACTOR GITHUB_TOKEN
;;;      PKG_SOURCE_DIR (default: cwd) SKIP_CATALOG (default true)

(require :asdf)
(asdf:initialize-source-registry
 '(:source-registry
   (:tree (:home ".local/share/cl-systems/"))
   :inherit-configuration))
(load (merge-pathnames "quicklisp/setup.lisp" (user-homedir-pathname)))
(ql:quickload :cl-repository-packager :silent t)

(defun env (name &optional default)
  (or (uiop:getenv name) default))

(defun sync-asdf-version! (source-dir version)
  (let ((asd (merge-pathnames "http-backend-async.asd" source-dir)))
    (with-open-file (in asd :direction :input)
      (let ((lines (loop for line = (read-line in nil nil) while line collect line)))
        (with-open-file (out asd :direction :output :if-exists :supersede)
          (dolist (line lines)
            (write-line (if (search ":version \"" line)
                            (format nil "  :version \"~a\"" version)
                            line)
                        out)))))))

(let* ((name "http-backend-async")
       (version (env "PKG_VERSION" "0.2.1"))
       (source-dir (uiop:ensure-directory-pathname
                    (env "PKG_SOURCE_DIR" (namestring (uiop:getcwd)))))
       (_ (sync-asdf-version! source-dir version))
       (registry (env "OCI_REGISTRY" "ghcr.io"))
       (namespace (string-downcase (env "OCI_NAMESPACE" "egao1980/cl-systems")))
       (registry-url (format nil "https://~a" registry))
       (skip-catalog (string-equal "true" (env "SKIP_CATALOG" "true")))
       (auth (cl-oci-client/auth:make-auth-config
              :username (env "GITHUB_ACTOR")
              :password (env "GITHUB_TOKEN")))
       (reg (cl-oci-client/registry:make-registry registry-url :auth auth))
       (spec (make-instance 'cl-repository-packager/build-matrix:package-spec
               :name name
               :version version
               :source-dir source-dir
               :license "MIT"
               :description "Async http-protocol backend on event-protocol (register-io)"
               :author "egao1980"
               :depends-on '("http-protocol" "event-protocol" "ws-protocol"
                             "http-encoding-chipz" "quri" "fast-http" "babel"
                             "alexandria" "bordeaux-threads" "trivial-gray-streams"
                             "cffi" "usocket" "cl+ssl")
               :provides '("http-backend-async")))
       (result (cl-repository-packager/build-matrix:build-package spec)))
  (declare (ignore _))
  (format t "~%Publishing ~a:~a (source-only) to ~a/~a~%"
          name version registry-url namespace)
  (cl-repository-packager/publisher:publish-package
   reg namespace version result spec :skip-catalog skip-catalog)
  (format t "Published ~a/~a/~a:~a~%" registry namespace name version))
