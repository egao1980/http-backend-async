(defsystem "http-backend-async"
  :version "0.2.9"
  :description "Async http-protocol backend on event-protocol (register-io)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("http-protocol"
               "event-protocol"
               "ws-protocol"
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
  :properties
  (:cl-repo
   (:ci (:with ("cl-stack-ssl" "http2" "fast-websocket"
                "http-encoding-brotli" "cl-stack-brotli")
         :load-before-test ("cl+ssl" "cl-stack-ssl")
         :record-versions (("cl-stack-ssl" . "CL_STACK_SSL_VERSION")))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "pool")
               (:file "socket")
               (:file "socks")
               (:file "async-body-stream")
               (:file "http1")
               (:file "http2")
               (:file "redirect")
               (:file "tls")
               (:file "backend")
               (:file "websocket")
               (:file "sync"))
  :in-order-to ((test-op (test-op "http-backend-async/tests"))))

(defsystem "http-backend-async/tests"
  :depends-on ("http-backend-async" "rove" "bordeaux-threads" "usocket" "ws-protocol")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "fixture")
               (:file "pool-test")
               (:file "proxy-test")
               (:file "socks-test")
               (:file "backend-test")
               (:file "http2-test")
               (:file "websocket-test")
               (:file "live-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
