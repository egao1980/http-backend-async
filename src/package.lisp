(defpackage #:http-backend-async
  (:use #:cl
        #:http-protocol
        #:sb-bsd-sockets)
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
                #:register-io)
  (:import-from #:alexandria #:when-let)
  (:export #:async-backend
           #:make-async-backend
           #:*event-backend-maker*
           #:async-request-handle
           #:async-request-canceled-p))
