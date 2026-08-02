(in-package #:http-backend-async)

;;; TLS via cl+ssl (+ optional cl-stack-ssl overlay).
;;; After async TCP connect, handshake + HTTP exchange run to completion on the
;;; loop thread (cl+ssl socket-BIO waits internally). True nonblocking SSL later.

(defun ensure-tls ()
  "Load cl-stack-ssl when present, else bare cl+ssl."
  (or (ignore-errors (asdf:load-system "cl-stack-ssl") t)
      (asdf:load-system "cl+ssl")
      t))

(defun %verify-arg (verify)
  (cond ((null verify) nil)
        ((eq verify t) :required)
        (t verify)))

(defun make-tls-stream (sock host &key (verify t))
  "Blocking TLS client stream over SOCK for HOST (SNI)."
  (ensure-tls)
  (setf (non-blocking-mode sock) nil)
  (let ((base (socket-make-stream sock
                                  :input t
                                  :output t
                                  :element-type '(unsigned-byte 8)
                                  :buffering :none
                                  :auto-close nil)))
    (handler-case
        (cl+ssl:make-ssl-client-stream
         base
         :hostname host
         :verify (%verify-arg verify)
         :unwrap-stream-p t)
      (error (e)
        (ignore-errors (close base))
        (error 'http-tls-error
               :message (format nil "TLS handshake failed for ~A: ~A" host e))))))

(defun tls-write-all (ssl octets)
  (write-sequence octets ssl)
  (force-output ssl))

(defun tls-read-some (ssl buffer)
  "Read into BUFFER. Returns bytes read, or 0 on EOF."
  (let ((n (read-sequence buffer ssl)))
    n))

(defun tls-close (ssl)
  (when ssl
    (ignore-errors (close ssl :abort t))))
