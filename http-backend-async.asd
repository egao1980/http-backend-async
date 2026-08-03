(defsystem "http-backend-async"
  :version "0.1.0"
  :description "Async http-protocol backend on event-protocol (register-io)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("http-protocol"
               "event-protocol"
               "http-encoding-chipz"
               "quri"
               "fast-http"
               "babel"
               "alexandria"
               "bordeaux-threads"
               "trivial-gray-streams"
               "cffi"
               "usocket"
               "cl+ssl")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "pool")
               (:file "socket")
               (:file "socks")
               (:file "async-body-stream")
               (:file "http1")
               (:file "redirect")
               (:file "tls")
               (:file "backend")
               (:file "sync"))
  :in-order-to ((test-op (test-op "http-backend-async/tests"))))

(defsystem "http-backend-async/tests"
  :depends-on ("http-backend-async" "rove" "bordeaux-threads" "usocket")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "fixture")
               (:file "pool-test")
               (:file "proxy-test")
               (:file "socks-test")
               (:file "backend-test")
               (:file "live-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
