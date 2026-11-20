(in-package #:http-backend-async/tests)

;;; #54: credentials in request headers never print -- not in backtrace frame
;;; args, not in condition reports -- yet go out on the wire unchanged.

(defparameter +redact-token+ "sk-hba-redact-7f3c9a1e5d")

(defun %token-free-p (string)
  (not (search +redact-token+ string)))

(defun %bearer () (concatenate 'string "Bearer " +redact-token+))

;;; A header value whose printing records the backtrace. %HEADER-ALIST prints
;;; it while the backend builds the request, so the trace holds the backend's
;;; frames with the request headers as their arguments.

(defvar *probe-trace* nil)
(defvar *in-probe* nil)

(defclass trace-probe () ())

(defmethod print-object ((p trace-probe) stream)
  (unless (or *in-probe* *probe-trace*)
    (let ((*in-probe* t))
      (setf *probe-trace*
            (with-output-to-string (s)
              (uiop:print-backtrace :stream s :count 80)))))
  (write-string "probe" stream))

(deftest secret-headers-reach-wire-but-not-frames
  (let ((seen nil))
    (setf *probe-trace* nil)
    (with-http-fixture ((lambda (method path headers body)
                          (declare (ignore method path body))
                          (setf seen (list (cdr (assoc "authorization" headers
                                                       :test #'string-equal))
                                           (cdr (assoc "x-api-key" headers
                                                       :test #'string-equal))))
                          (values 200 '(("content-type" . "text/plain"))
                                  (babel:string-to-octets "ok"))))
      (with-async-test (eb el hb)
        (declare (ignore hb))
        (let ((res (%await-promise
                    (post-async (fixture-url "/v1/chat")
                                :content "{}"
                                :auth (list :bearer (make-secret +redact-token+))
                                :headers (list (cons "x-api-key"
                                                     (make-secret +redact-token+))
                                               (cons "x-probe"
                                                     (make-instance 'trace-probe))))
                    eb el)))
          (ok (= 200 (response-status res))))))
    ;; the server got the real values
    (ok (equal (list (%bearer) +redact-token+) seen))
    ;; the frames that built the request printed the headers, redacted
    (ok (stringp *probe-trace*))
    (ok (search "SECRET" *probe-trace*))
    (ok (search "x-api-key" *probe-trace*))
    (ok (%token-free-p *probe-trace*))))

;;; An upload whose source fails inside the event loop: record the backtrace
;;; there, then check it and the condition the caller gets. The caller passes
;;; plain strings; the backend wraps credential headers itself.

(defvar *upload-trace* nil)

(defclass failing-upload (trivial-gray-streams:fundamental-binary-input-stream) ())

(defmethod stream-element-type ((s failing-upload)) '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-sequence
    ((s failing-upload) seq start end &key)
  (declare (ignore seq start end))
  (setf *upload-trace* (with-output-to-string (o)
                         (uiop:print-backtrace :stream o :count 80)))
  (error "upload source failed"))

(defmethod trivial-gray-streams:stream-read-byte ((s failing-upload))
  (error "upload source failed"))

(deftest failing-request-condition-and-frames-are-token-free
  (setf *upload-trace* nil)
  (with-http-fixture ()
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((err nil))
        (handler-case
            (%await-promise
             (post-async (fixture-url "/echo")
                         :content (make-instance 'failing-upload)
                         :auth (list :bearer +redact-token+)
                         :headers (list (cons "x-api-key" +redact-token+)
                                        (cons "cookie" +redact-token+)))
             eb el)
          (error (e) (setf err e)))
        (ok (typep err 'http-error))
        (ok (%token-free-p (format nil "~A ~S" err err)))
        (ok (stringp *upload-trace*))
        (ok (%token-free-p *upload-trace*))))))

(deftest backend-keeps-secrets
  (ok (backend-keeps-secrets-p (make-async-backend))))
