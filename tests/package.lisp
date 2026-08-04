(defpackage #:http-backend-async/tests
  (:use #:cl
        #:rove
        #:http-protocol
        #:http
        #:http-backend-async)
  (:shadowing-import-from #:http #:get #:delete #:trace #:stream)
  (:import-from #:ws-protocol
                #:make-ws-client
                #:backend-ws-transports
                #:backend-supports-ws-transport-p
                #:ws-transport-not-available)
  (:import-from #:event-protocol
                #:with-event-backend
                #:with-event-loop-var
                #:make-event-loop
                #:stop
                #:defer
                #:sleep*))
