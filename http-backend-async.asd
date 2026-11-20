(defsystem "http-backend-async"
  :version "0.2.21"
  :description "Async http-protocol backend on event-protocol (register-io)"
  :author "egao1980"
  :license "MIT"
  :depends-on ((:version "http-protocol" "0.4.0")
               (:version "event-protocol" "0.2.2")
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
               ;; not cl+ssl directly: cl-stack-ssl points cl+ssl at its own
               ;; OpenSSL before cl+ssl loads (a bare cl+ssl took Homebrew's)
               "cl-stack-ssl")
  :properties
  (:cl-repo
   (:ci (:with ("http2" "fast-websocket"
                "http-encoding-brotli" "cl-stack-brotli")
         ;; The event backend is a matrix choice: a local `cl-repo test`
         ;; installs and loads the same one CI does.
         :env (("HTTP_ASYNC_EVENT_BACKEND" :default "libuv"
                :install t :load-before-test t
                :systems (("libuv" "event-backend-libuv")
                          ("libev" "event-backend-libev"))))
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
               (:file "h2-mux")
               (:file "backend")
               (:file "websocket")
               (:file "sync"))
  :in-order-to ((test-op (test-op "http-backend-async/tests"))))

(defsystem "http-backend-async/tests"
  :depends-on ("http-backend-async" "rove" "bordeaux-threads" "usocket" "ws-protocol"
               "trivial-gray-streams")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "fixture")
               (:file "pool-test")
               (:file "keep-alive-test")
               (:file "redact-test")
               (:file "proxy-test")
               (:file "socks-test")
               (:file "backend-test")
               (:file "http2-test")
               (:file "h2-mux-test")
               (:file "websocket-test")
               (:file "live-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
