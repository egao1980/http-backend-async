(defpackage #:http-backend-async
  (:use #:cl #:http-protocol)
  (:import-from #:ws-protocol
                #:ws-backend
                #:backend-ws-transports
                #:backend-supports-ws-transport-p
                #:connect
                #:make-http2-websocket-connect-headers
                #:ws-transport-not-available
                #:make-ws-client)
  (:import-from #:event-protocol
                #:*event-backend*
                #:*event-loop*
                #:with-event-backend
                #:with-event-loop-var
                #:make-event-loop
                #:run
                #:stop
                #:defer
                #:sleep*
                #:cancel
                #:register-io
                #:update-io)
  (:import-from #:alexandria #:when-let)
  (:export #:async-backend
           #:make-async-backend
           #:*event-backend-maker*
           #:*async-body-queue-limit*
           #:async-request-handle
           #:async-request-canceled-p
           #:lru-connection-pool
           #:make-lru-connection-pool
           #:async-pooled-connection
           #:make-async-pooled-connection
           #:response-keeps-alive-p
           #:build-connect-request-octets
           #:connect-response-ok-p
           #:proxy-authorization-value
           #:ensure-http2
           #:async-h2-pump-stream
           #:async-h2-session
           #:make-async-h2-session
           #:h2-session-enable-connect-protocol-p
           #:h2-enable-connect-protocol-p
           #:make-extended-connect-ws-headers
           #:tls-selected-alpn))