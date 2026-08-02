(in-package #:http-backend-async)

;;; Nonblocking TCP over sb-bsd-sockets. Event interest via event-protocol:register-io.

(defun resolve-ipv4 (host)
  "Return a 4-octet vector for HOST (name or dotted IPv4)."
  (handler-case
      (host-ent-address (get-host-by-name host))
    (error (e)
      (error 'http-connection-error
             :message (format nil "DNS failed for ~A: ~A" host e)))))

(defun make-nonblocking-tcp ()
  (let ((sock (make-instance 'inet-socket :type :stream :protocol :tcp)))
    (setf (non-blocking-mode sock) t)
    sock))

(defun socket-fd (sock)
  (socket-file-descriptor sock))

(defun begin-connect (sock host port)
  "Start nonblocking connect. Returns :connected or :pending."
  (let ((addr (resolve-ipv4 host)))
    (handler-case
        (progn
          (socket-connect sock addr port)
          :connected)
      (operation-in-progress ()
        :pending)
      (interrupted-error ()
        :pending)
      (error (e)
        (error 'http-connection-error
               :message (format nil "connect ~A:~A failed: ~A" host port e))))))

(defun socket-send-octets (sock octets start end)
  "Send OCTETS[START,END). Returns bytes sent, or NIL if would-block.
   SBCL SOCKET-SEND has no offset — use a displaced vector for partial sends."
  (let* ((len (- end start))
         (buf (if (zerop start)
                  octets
                  (make-array len
                              :element-type '(unsigned-byte 8)
                              :displaced-to octets
                              :displaced-index-offset start))))
    (handler-case
        (socket-send sock buf len)
      (interrupted-error ()
        nil)
      (socket-error (e)
        (error 'http-connection-error
               :message (format nil "socket send failed: ~A" e))))))

(defun socket-recv-octets (sock buffer)
  "Receive into BUFFER. Returns byte count, 0 on EOF, NIL if would-block.
   SOCKET-RECEIVE → (values buffer length peer)."
  (handler-case
      (multiple-value-bind (buf n)
          (socket-receive sock buffer (length buffer))
        (declare (ignore buf))
        ;; NIL length → would-block on nonblocking sockets
        n)
    (interrupted-error ()
      nil)
    (socket-error (e)
      (error 'http-connection-error
             :message (format nil "socket recv failed: ~A" e)))))

(defun close-socket (sock)
  (when sock
    (ignore-errors (socket-close sock))))
