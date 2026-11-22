(in-package #:http-backend-async)

;;; Blocking SEND on the async engine: run the event loop until the attempt
;;; finishes. Retries use protocol HTTP-RETRY / HTTP-TIMEOUT.

(defun %await-send-async (backend client request &key (timeout-seconds 30.0))
  "Run SEND-ASYNC to completion on a fresh/bound event loop. Returns HTTP-RESPONSE.
   :WANT-STREAM T slurps the body on a worker (urllib3 preload_content shape) so the
   event loop can keep feeding until EOF.
   TIMEOUT-SECONDS caps the whole wait; NIL means no cap here (SEND-ASYNC's
   connect / read watchdog still ends a stalled request)."
  (multiple-value-bind (event-backend event-loop owned-p) (%ensure-event-context)
    (let ((result nil)
          (err nil)
          (done nil)
          (in-run nil)
          (want-stream-p (http-request-want-stream request)))
      (labels ((finish (res)
                 (setf result res done t)
                 (stop event-backend event-loop))
               (fail (c)
                 (setf err c done t)
                 (stop event-backend event-loop))
               (on-response (res)
                 (if (and want-stream-p (streamp (response-body res)))
                     (submit event-backend event-loop
                             (lambda () (slurp-octets (body-stream res)))
                             :callback
                             (lambda (octets)
                               (finish
                                (make-instance 'http-response
                                               :status (response-status res)
                                               :headers (response-headers res)
                                               :body octets
                                               :url (response-url res)
                                               :http-version (response-http-version res)
                                               :cookies (response-cookies res)
                                               :history (response-history res)
                                               :request (response-request res))))
                             :error-callback #'fail)
                     (finish res))))
        (unwind-protect
             (with-event-backend (event-backend)
               (with-event-loop-var (event-loop)
                 (send-async
                  backend client request
                  :callback #'on-response
                  :error-callback #'fail)
                 (when timeout-seconds
                   (sleep* event-backend event-loop timeout-seconds
                           :callback (lambda ()
                                       (unless done
                                         (fail (make-condition
                                                'http-timeout-error
                                                :message
                                                (format nil "sync await timed out after ~,1F s"
                                                        timeout-seconds)))))))
                 (setf in-run t)
                 (event-protocol:run event-backend event-loop :stop-when-idle nil)
                 (setf in-run nil)))
          ;; A loop made for this call is this call's to release: each open
          ;; libuv loop keeps a descriptor, so one per SEND exhausted
          ;; FD_SETSIZE in long-lived clients. Never close it from an unwind
          ;; out of RUN itself (libuv is mid-iteration there).
          (when (and owned-p (not in-run))
            (close-event-loop event-backend event-loop))))
      (when err (error err))
      result)))
(defun %sleep-retry (seconds)
  (when (and seconds (plusp seconds))
    (sleep seconds)))

(defmethod send ((backend async-backend) client request &key)
  "Blocking send: await SEND-ASYNC with HTTP-RETRY backoff."
  (let* ((retry (effective-retry request client))
         ;; Only a :TOTAL cap bounds the wait: a streamed body that keeps
         ;; arriving under a :READ (idle) timeout is never cut off.
         (total-s (%timeout-seconds request client))
         (attempt 0)
         (last-condition nil)
         (last-response nil))
    (loop
      (incf attempt)
      (handler-case
          (let ((res (%await-send-async backend client request
                                        :timeout-seconds total-s)))
            (setf last-response res)
            (let ((status (response-status res)))
              (if (retry-should-retry-p retry attempt
                                        (http-request-method request)
                                        :status status
                                        :response res)
                  (progn
                    (release-response-connection res :abort t)
                    (%sleep-retry (retry-delay-seconds retry attempt
                                                       :response res)))
                  (return-from send res))))
        (http-error (e)
          (setf last-condition e)
          (if (retry-should-retry-p retry attempt
                                    (http-request-method request)
                                    :condition e)
              (%sleep-retry (retry-delay-seconds retry attempt))
              (error e))))
      (when (>= (1- attempt) (retry-total retry))
        (if last-condition
            (error last-condition)
            (return-from send last-response))))))
