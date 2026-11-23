(in-package #:http-backend-async)

;;; Blocking SEND on the async engine: run the event loop until the attempt
;;; finishes. Retries use protocol HTTP-RETRY / HTTP-TIMEOUT.
;;;
;;; :WANT-STREAM T returns as soon as the headers arrive, with the live
;;; ASYNC-BODY-INPUT-STREAM as the body (#70). On a loop made for this call,
;;; a pump thread keeps running the loop until the body ends (EOF, error,
;;; CLOSE by the reader, or a timeout). When the caller bound a loop
;;; (event-protocol:*event-loop*), a streamed request gets a private loop of
;;; the same backend instead: another thread must never run the caller's.

(defun %spawn-stream-pump (backend event-backend event-loop handle feed
                           total-timer)
  "Run EVENT-LOOP on a new thread until FEED finishes, then end the request
   and close the loop (it was made for this call)."
  (flet ((pump ()
           (unwind-protect
                (with-event-backend (event-backend)
                  (with-event-loop-var (event-loop)
                    (loop until (async-body-finished-p feed)
                          do (event-protocol:run event-backend event-loop
                                                 :stop-when-idle nil))))
             ;; The reader CLOSEd early (or the body failed): stop the IO;
             ;; a request that completed is not touched (CANCELED-P).
             (ignore-errors (cancel-request backend handle))
             (when total-timer
               (ignore-errors (cancel event-backend total-timer)))
             (ignore-errors (close-event-loop event-backend event-loop)))))
    (bt:make-thread #'pump :name "http-backend-async stream pump")))

(defun %stream-pump-p (event-backend event-loop)
  "T when this backend can hand its loop to a pump thread (WAKE-CALL)."
  (handler-case (progn (event-protocol:wake-call event-backend event-loop (lambda ())) t)
    (error () nil)))

(defun %await-event-context (want-stream-p)
  "(values event-backend event-loop owned-p) for one blocking SEND. A
   streamed request on a loop the caller bound gets a private loop of the
   same backend, so its pump never runs the caller's loop."
  (multiple-value-bind (eb el owned-p) (%ensure-event-context)
    (if (or owned-p (not want-stream-p))
        (values eb el owned-p)
        (let ((private (make-event-loop eb)))
          (if (%stream-pump-p eb private)
              (values eb private t)
              (progn (close-event-loop eb private)
                     (values eb el nil)))))))

(defun %await-send-async (backend client request &key (timeout-seconds 30.0))
  "Run SEND-ASYNC to completion on a fresh/bound event loop. Returns HTTP-RESPONSE.
   :WANT-STREAM T returns at the headers; the body is the live stream, fed
   by a pump thread until it ends. Read it to EOF or CLOSE it. With a loop
   the caller bound, a streamed request runs on a private loop of the same
   event backend (pumped and closed like a fresh one); an event backend
   without WAKE-CALL reads the body whole on a worker first.
   TIMEOUT-SECONDS caps the whole wait, body included; NIL means no cap here
   (SEND-ASYNC's connect / read watchdog still ends a stalled request)."
  (multiple-value-bind (event-backend event-loop owned-p)
      (%await-event-context (http-request-want-stream request))
    (let* ((result nil)
           (err nil)
           (done nil)
           (in-run nil)
           (handle nil)
           (total-timer nil)
           (pumped nil)
           (want-stream-p (http-request-want-stream request))
           (live-p (and want-stream-p owned-p
                        (%stream-pump-p event-backend event-loop))))
      (labels ((finish (res)
                 (setf result res done t)
                 (stop event-backend event-loop))
               (fail (c)
                 (setf err c done t)
                 (stop event-backend event-loop))
               (on-total-timeout ()
                 (let ((c (make-condition
                           'http-timeout-error
                           :message (format nil "sync await timed out after ~,1F s"
                                            timeout-seconds))))
                   (cond
                     ((not done) (fail c))
                     ;; Streaming past the headers: end the body with it.
                     ((and handle (async-request-body-feed handle))
                      (async-body-abort (async-request-body-feed handle) c)
                      (cancel-request backend handle)))))
               (on-response (res)
                 (cond
                   ((not (and want-stream-p (streamp (response-body res))))
                    (finish res))
                   (live-p (finish res))
                   (t
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
                            :error-callback #'fail)))))
        (unwind-protect
             (with-event-backend (event-backend)
               (with-event-loop-var (event-loop)
                 (setf handle
                       (send-async
                        backend client request
                        :callback #'on-response
                        :error-callback #'fail))
                 (when timeout-seconds
                   (setf total-timer
                         (sleep* event-backend event-loop timeout-seconds
                                 :callback #'on-total-timeout)))
                 (setf in-run t)
                 (event-protocol:run event-backend event-loop :stop-when-idle nil)
                 (setf in-run nil)
                 ;; Headers are in and the body still runs: hand the loop to
                 ;; a pump. The watch stops RUN on the pump once the body
                 ;; ends; NIL means it already ended, so close here.
                 (let ((feed (and live-p result (not err)
                                  (typep handle 'async-request-handle)
                                  (async-request-body-feed handle))))
                   (when (and feed
                              (async-body-watch-finish
                               feed
                               (lambda ()
                                 (ignore-errors
                                  (event-protocol:wake-call event-backend event-loop
                                             (lambda ()
                                               (stop event-backend event-loop)))))))
                     (%spawn-stream-pump backend event-backend event-loop
                                         handle feed total-timer)
                     (setf pumped t)))))
          ;; A loop made for this call is this call's to release: each open
          ;; libuv loop keeps a descriptor, so one per SEND exhausted
          ;; FD_SETSIZE in long-lived clients. Never close it from an unwind
          ;; out of RUN itself (libuv is mid-iteration there). A pumped loop
          ;; is closed by its pump.
          (when (and owned-p (not in-run) (not pumped))
            (close-event-loop event-backend event-loop))))
      (when err (error err))
      result)))

(defun %discard-response-body (res)
  "Drop a response that will be retried: a live stream body is CLOSEd (its
   pump ends the request); a pooled one goes back to the pool."
  (let ((b (response-body res)))
    (when (typep b 'stream)
      (ignore-errors (close b :abort t))))
  (release-response-connection res :abort t))

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
                    (%discard-response-body res)
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
