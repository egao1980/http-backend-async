(in-package #:http-backend-async)

;;; TLS via cl+ssl (+ optional cl-stack-ssl overlay) over a usocket stream.
;;; After TCP connect, handshake + HTTP exchange run to completion on the
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

(defun make-tls-stream (usock host &key (verify t))
  "Blocking TLS client stream over USOCKET for HOST (SNI)."
  (ensure-tls)
  (set-socket-nonblocking usock nil)
  (let ((base (socket-byte-stream usock)))
    (handler-case
        (cl+ssl:make-ssl-client-stream
         base
         :hostname host
         :verify (%verify-arg verify)
         :unwrap-stream-p t)
      (error (e)
        (error 'http-tls-error
               :message (format nil "TLS handshake failed for ~A: ~A" host e))))))

(defun tls-write-all (ssl octets)
  (write-sequence octets ssl)
  (force-output ssl))

(defun tls-read-some (ssl buffer)
  "Read into BUFFER. Returns bytes read, or 0 on EOF."
  (handler-case
      (read-sequence buffer ssl)
    (end-of-file () 0)))

(defun tls-close (ssl)
  (when ssl
    (ignore-errors (close ssl :abort t))))
