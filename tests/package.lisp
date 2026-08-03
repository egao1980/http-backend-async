(defpackage #:http-backend-async/tests
  (:use #:cl
        #:rove
        #:http-protocol
        #:http
        #:http-backend-async)
  (:shadowing-import-from #:http #:get #:delete #:trace)
  (:import-from #:event-protocol
                #:with-event-backend
                #:with-event-loop-var
                #:make-event-loop
                #:stop
                #:defer
                #:sleep*))
