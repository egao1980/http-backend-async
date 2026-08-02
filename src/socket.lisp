(in-package #:http-backend-async)

;;; Portable TCP via usocket (not an ASDF dep on sb-bsd-sockets / iolib).
;;; iolib is Unix-only; usocket covers Windows + unix.
;;;
;;; Cleartext path: usocket connect → OS FD for event-protocol:register-io →
;;; nonblocking send/recv on the implementation socket (SBCL/CCL/…).
;;; TLS path: usocket stream (blocking) → cl+ssl.

(defun %soft (package name)
  (let ((s (find-symbol name package)))
    (when (and s (fboundp s)) s)))

(defun %soft-setf (package name)
  (let ((s (find-symbol name package)))
    (when (and s (fboundp `(setf ,s))) `(setf ,s))))

(defun tcp-connect (host port &key timeout)
  "Blocking TCP connect → STREAM-USOCKET. Call from the event-loop thread."
  (handler-case
      (apply #'usocket:socket-connect
             host port
             :element-type '(unsigned-byte 8)
             (when timeout (list :timeout timeout)))
    (usocket:ns-error (e)
      (error 'http-connection-error
             :message (format nil "DNS failed for ~A: ~A" host e)))
    (usocket:socket-error (e)
      (error 'http-connection-error
             :message (format nil "connect ~A:~A failed: ~A" host port e)))
    (error (e)
      (error 'http-connection-error
             :message (format nil "connect ~A:~A failed: ~A" host port e)))))

(defun %impl-socket (usock)
  (usocket:socket usock))

(defun socket-fd (usock)
  "OS descriptor for event-protocol:register-io."
  (let ((impl (%impl-socket usock)))
    (or
     (let ((fn (%soft :sb-bsd-sockets "SOCKET-FILE-DESCRIPTOR")))
       (when fn (ignore-errors (funcall fn impl))))
     (let ((fn (%soft :ccl "SOCKET-DEVICE")))
       (when fn (ignore-errors (funcall fn impl))))
     (let ((fn (%soft :openmcl-socket "SOCKET-OS-FD")))
       (when fn (ignore-errors (funcall fn impl))))
     (let ((fn (%soft :comm "GET-FD-FROM-SOCKET")))
       (when fn (ignore-errors (funcall fn impl))))
     (error 'http-connection-error
            :message "cannot extract socket fd for register-io on this Lisp"))))

(defun set-socket-nonblocking (usock nonblocking)
  "Best-effort nonblocking mode for cleartext register-io I/O."
  (let* ((impl (%impl-socket usock))
         (setf-fn (%soft-setf :sb-bsd-sockets "NON-BLOCKING-MODE")))
    (when setf-fn
      (funcall (fdefinition setf-fn) nonblocking impl)))
  usock)

(defun socket-byte-stream (usock)
  (usocket:socket-stream usock))

(defun %would-block-p (condition)
  (let ((msg (princ-to-string condition)))
    (or (search "WOULD-BLOCK" msg :test #'char-equal)
        (search "EAGAIN" msg :test #'char-equal)
        (search "EWOULDBLOCK" msg :test #'char-equal)
        (search "Resource temporarily unavailable" msg :test #'char-equal)
        (search "operation in progress" msg :test #'char-equal))))

(defun socket-send-octets (usock octets start end)
  "Write OCTETS[START,END). Returns bytes sent, or NIL if would-block."
  (let* ((len (- end start))
         (impl (%impl-socket usock))
         (send (%soft :sb-bsd-sockets "SOCKET-SEND")))
    (when (zerop len) (return-from socket-send-octets 0))
    (cond
      (send
       ;; SBCL SOCKET-SEND has no offset — displace for partial sends.
       (let ((buf (if (zerop start)
                      octets
                      (make-array len
                                  :element-type '(unsigned-byte 8)
                                  :displaced-to octets
                                  :displaced-index-offset start)))
             (eintr (%soft :sb-bsd-sockets "INTERRUPTED-ERROR"))
             (op-in-prog (%soft :sb-bsd-sockets "OPERATION-IN-PROGRESS")))
         (handler-case
             (funcall send impl buf len)
           (error (e)
             (cond
               ((and eintr (typep e eintr)) nil)
               ((and op-in-prog (typep e op-in-prog)) nil)
               ((%would-block-p e) nil)
               (t (error 'http-connection-error
                         :message (format nil "socket send failed: ~A" e))))))))
      (t
       (let ((stream (socket-byte-stream usock)))
         (handler-case
             (progn
               (write-sequence octets stream :start start :end end)
               (force-output stream)
               len)
           ((or usocket:socket-error stream-error) (e)
             (if (%would-block-p e)
                 nil
                 (error 'http-connection-error
                        :message (format nil "socket send failed: ~A" e))))))))))

(defun socket-recv-octets (usock buffer)
  "Read into BUFFER. Returns byte count, 0 on EOF, NIL if would-block."
  (let* ((impl (%impl-socket usock))
         (recv (%soft :sb-bsd-sockets "SOCKET-RECEIVE")))
    (cond
      (recv
       (let ((eintr (%soft :sb-bsd-sockets "INTERRUPTED-ERROR")))
         (handler-case
             (multiple-value-bind (buf n)
                 (funcall recv impl buffer (length buffer))
               (declare (ignore buf))
               ;; NIL length → would-block on nonblocking SBCL sockets
               n)
           (error (e)
             (cond
               ((and eintr (typep e eintr)) nil)
               ((%would-block-p e) nil)
               (t (error 'http-connection-error
                         :message (format nil "socket recv failed: ~A" e))))))))
      (t
       (let ((stream (socket-byte-stream usock)))
         (handler-case
             (read-sequence buffer stream)
           (end-of-file () 0)
           ((or usocket:socket-error stream-error) (e)
             (if (%would-block-p e)
                 nil
                 (error 'http-connection-error
                        :message (format nil "socket recv failed: ~A" e))))))))))

(defun close-socket (usock)
  (when usock
    (ignore-errors (usocket:socket-close usock))))
