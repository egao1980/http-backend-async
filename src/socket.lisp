(in-package #:http-backend-async)

;;; Portable TCP via usocket (not an ASDF dep on sb-bsd-sockets / iolib).
;;; iolib is Unix-only; usocket covers Windows + unix.
;;;
;;; Cleartext + TLS: nonblocking connect (SBCL) → OS FD for
;;; event-protocol:register-io → nonblocking send/recv / SSL_* on the FD.

(defun %soft (package name)
  (let ((s (find-symbol name package)))
    (when (and s (fboundp s)) s)))

(defun %soft-class (package name)
  (let ((s (find-symbol name package)))
    (when (and s (find-class s nil)) s)))

(defun %soft-setf (package name)
  (let ((s (find-symbol name package)))
    (when (and s (fboundp `(setf ,s))) `(setf ,s))))

(defun tcp-connect (host port &key timeout)
  "Blocking TCP connect → STREAM-USOCKET. Fallback when NB connect unavailable."
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
  "Best-effort nonblocking mode for register-io I/O."
  (let* ((impl (%impl-socket usock))
         (setf-fn (%soft-setf :sb-bsd-sockets "NON-BLOCKING-MODE")))
    (when setf-fn
      (funcall (fdefinition setf-fn) nonblocking impl)))
  usock)

(defun socket-byte-stream (usock)
  (usocket:socket-stream usock))

(defun %would-block-p (condition)
  "True for EAGAIN/EWOULDBLOCK/EINPROGRESS and Windows WSAEWOULDBLOCK.
   SBCL on Windows often reports WSAEWOULDBLOCK (10035) as EINTR with
   message 'A non-blocking socket operation could not be completed immediately.'"
  (let ((msg (princ-to-string condition)))
    (or (search "WOULD-BLOCK" msg :test #'char-equal)
        (search "EAGAIN" msg :test #'char-equal)
        (search "EWOULDBLOCK" msg :test #'char-equal)
        (search "WSAEWOULDBLOCK" msg :test #'char-equal)
        (search "Resource temporarily unavailable" msg :test #'char-equal)
        (search "operation in progress" msg :test #'char-equal)
        (search "EINPROGRESS" msg :test #'char-equal)
        (search "could not be completed immediately" msg :test #'char-equal)
        ;; WSAEWOULDBLOCK via SBCL interrupted-error / EINTR wording
        (and (search "EINTR" msg :test #'char-equal)
             (or (search "non-blocking" msg :test #'char-equal)
                 (search "immediately" msg :test #'char-equal))))))

(defun %in-progress-condition-p (condition)
  (let ((op (%soft-class :sb-bsd-sockets "OPERATION-IN-PROGRESS"))
        (eintr (%soft-class :sb-bsd-sockets "INTERRUPTED-ERROR")))
    (or (and op (typep condition op))
        ;; Windows NB connect: WSAEWOULDBLOCK often typed as INTERRUPTED-ERROR
        (and eintr (typep condition eintr))
        (%would-block-p condition))))

;;; --- nonblocking connect (SBCL) -------------------------------------------

#+sbcl
(defun %resolve-inet-addresses (host)
  "All A/AAAA addresses for HOST (same family mix as get-host-by-name)."
  (handler-case
      (let* ((ent (sb-bsd-sockets:get-host-by-name
                   (if (stringp host) host (princ-to-string host))))
             (addrs (or (sb-bsd-sockets:host-ent-addresses ent)
                        (list (sb-bsd-sockets:host-ent-address ent)))))
        (unless addrs
          (error 'http-connection-error
                 :message (format nil "DNS returned no addresses for ~A" host)))
        addrs)
    (http-connection-error (e) (error e))
    (error (e)
      (error 'http-connection-error
             :message (format nil "DNS failed for ~A: ~A" host e)))))

#+sbcl
(defun %ensure-connected-stream (usock)
  "Attach a binary stream once TCP is connected."
  (let ((stream (usocket:socket-stream usock))
        (impl (%impl-socket usock)))
    (when (or (null stream)
              (not (streamp stream))
              (not (open-stream-p stream)))
      ;; :none — we I/O via socket-send/receive + register-io, not the stream.
      ;; :full buffering on Windows can desync keep-alive reuse (INVALID-VERSION).
      (setf (usocket:socket-stream usock)
            (sb-bsd-sockets:socket-make-stream
             impl
             :input t :output t :buffering :none
             :element-type '(unsigned-byte 8)))))
  usock)

;; SOL_SOCKET / SO_ERROR — enough for connect-completion check.
(defconstant +sol-socket+
  #+darwin 65535
  #+linux 1
  #+(or win32 windows) #xffff
  #-(or darwin linux win32 windows) 1)

(defconstant +so-error+
  #+darwin 4103
  #+linux 4
  #+(or win32 windows) #x1007
  #-(or darwin linux win32 windows) 4)

(defun %socket-so-error (fd)
  "Return SO_ERROR errno for FD, or NIL if getsockopt unavailable/fails."
  (handler-case
      (cffi:with-foreign-objects ((err :int) (len :int))
        (setf (cffi:mem-ref len :int) (cffi:foreign-type-size :int))
        (let ((rc (cffi:foreign-funcall "getsockopt" :int fd
                                        :int +sol-socket+ :int +so-error+
                                        :pointer err :pointer len :int)))
          (when (zerop rc)
            (cffi:mem-ref err :int))))
    (error () nil)))

#+sbcl
(defun %try-connect-addr (addr port)
  "Nonblocking connect to one ADDR.
Returns (values usock :connected|:pending), or (values nil error) on immediate failure."
  (let* ((ipv6 (= 16 (length addr)))
         (sock (make-instance (if ipv6
                                  'sb-bsd-sockets:inet6-socket
                                  'sb-bsd-sockets:inet-socket)
                              :type :stream :protocol :tcp))
         (usock nil)
         (ok nil))
    (unwind-protect
         (progn
           (ignore-errors
             (setf (sb-bsd-sockets:sockopt-tcp-nodelay sock) t))
           (setf (sb-bsd-sockets:non-blocking-mode sock) t)
           (setf usock (usocket::make-stream-socket
                        :socket sock
                        :stream usocket::*dummy-stream*))
           (handler-case
               (progn
                 (sb-bsd-sockets:socket-connect sock addr port)
                 (%ensure-connected-stream usock)
                 (setf ok t)
                 (values usock :connected))
             (error (e)
               (if (%in-progress-condition-p e)
                   (progn
                     (setf ok t)
                     (values usock :pending))
                   (values nil e)))))
      (unless ok
        (ignore-errors (sb-bsd-sockets:socket-close sock))))))

(defun tcp-connect-nb (host port)
  "Start TCP connect without blocking the event loop when possible.

Returns (values usocket status) where STATUS is:
  :connected — ready for I/O
  :pending   — arm register-io :write, then TCP-CONNECT-FINISH

On SBCL, tries each DNS address until one connects or goes pending
(immediate refusals/unreachable skip to the next; EINPROGRESS stops the walk)."
  #+sbcl
  (return-from tcp-connect-nb
    (let ((addrs (%resolve-inet-addresses host))
          (last-err nil))
      (dolist (addr addrs
               (error 'http-connection-error
                      :message (format nil "connect ~A:~A failed: ~A"
                                       host port
                                       (or last-err "no addresses"))))
        (multiple-value-bind (usock status-or-err)
            (%try-connect-addr addr port)
          (if usock
              (return (values usock status-or-err))
              (setf last-err status-or-err))))))
  #-sbcl
  (let ((usock (tcp-connect host port)))
    (set-socket-nonblocking usock t)
    (values usock :connected)))

(defun tcp-connect-finish (usock)
  "After writable interest: finish nonblocking connect.
Returns T when connected. Signals HTTP-CONNECTION-ERROR on failure.
Returns NIL if connect still pending (spurious wakeup)."
  #+sbcl
  (let* ((impl (%impl-socket usock))
         (fd (ignore-errors (sb-bsd-sockets:socket-file-descriptor impl)))
         (err (and fd (%socket-so-error fd))))
    (cond
      ((and err (plusp err))
       (error 'http-connection-error
              :message (format nil "connect failed: errno ~A" err)))
      (t
       (handler-case
           (progn
             (sb-bsd-sockets:socket-peername impl)
             (%ensure-connected-stream usock)
             (setf (sb-bsd-sockets:non-blocking-mode impl) t)
             t)
         (error (e)
           (let ((msg (princ-to-string e)))
             (cond
               ((or (search "EINPROGRESS" msg :test #'char-equal)
                    (search "not connected" msg :test #'char-equal)
                    (search "ENOTCONN" msg :test #'char-equal)
                    (search "EINVAL" msg :test #'char-equal))
                nil)
               (t
                (error 'http-connection-error
                       :message (format nil "connect failed: ~A" e))))))))))
  #-sbcl
  (progn
    (set-socket-nonblocking usock t)
    t))

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
             (eintr (%soft-class :sb-bsd-sockets "INTERRUPTED-ERROR"))
             (op-in-prog (%soft-class :sb-bsd-sockets "OPERATION-IN-PROGRESS")))
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
       (let ((eintr (%soft-class :sb-bsd-sockets "INTERRUPTED-ERROR")))
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
