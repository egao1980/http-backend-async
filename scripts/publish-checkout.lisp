;;; Publish this checkout to ghcr.io/egao1980/cl-systems via auto-package-spec.
;;; Env: GITHUB_ACTOR, GITHUB_TOKEN, PKG_SYSTEM, optional PKG_VERSION / OCI_NAMESPACE.
;;;
;;; Load packager from ~/.local/share/cl-systems first — do NOT put the project
;;; tree on the ASDF registry until after quickload (project systems like quri
;;; can shadow QL and pull unpublished deps into the packager graph).

(require :asdf)

(asdf:initialize-source-registry
 `(:source-registry
   (:tree (:home ".local/share/cl-systems/"))
   :inherit-configuration))
(load (merge-pathnames "quicklisp/setup.lisp" (user-homedir-pathname)))
(ql:quickload '(:cl-repository-packager :cl-oci-client) :silent t)

(asdf:initialize-source-registry
 `(:source-registry
   (:tree ,(uiop:getcwd))
   (:tree (:home ".local/share/cl-systems/"))
   :inherit-configuration))

(defun env (name &optional default)
  (or (uiop:getenv name) default))

(let* ((system-name (env "PKG_SYSTEM" "http-backend-async"))
       (version (or (env "PKG_VERSION")
                    (asdf:component-version (asdf:find-system system-name))))
       (registry-url (env "OCI_REGISTRY" "ghcr.io"))
       (namespace (string-downcase (env "OCI_NAMESPACE" "egao1980/cl-systems")))
       (auth (cl-oci-client/auth:make-auth-config
              :username (env "GITHUB_ACTOR" "x-access-token")
              :password (or (env "GITHUB_TOKEN")
                            (error "GITHUB_TOKEN required"))))
       (reg (cl-oci-client/registry:make-registry
             (format nil "https://~a" registry-url) :auth auth))
       (spec (cl-repository-packager/asdf-plugin:auto-package-spec system-name))
       (result nil))
  ;; One GHCR package per owning repo — never auto-publish *-test secondaries
  ;; (those are often still ACL-linked to cl-stack-systems).
  (setf (cl-repository-packager/build-matrix:package-spec-provides spec)
        (list system-name))
  (setf (cl-repository-packager/build-matrix:package-spec-version spec) version)
  (setf result (cl-repository-packager/build-matrix:build-package spec))
  (format t "~&Publishing ~a/~a:~a~%" namespace system-name version)
  (cl-repository-packager/publisher:publish-package
   reg namespace version result spec :skip-catalog t)
  (format t "~&Published ~a/~a:~a~%" namespace system-name version))
