(in-package #:http-backend-async)

;;; Async TLS via cl+ssl, loaded through cl-stack-ssl (its OpenSSL).
;;; Socket-BIO on a nonblocking FD; SSL_connect / SSL_read / SSL_write driven
;;; by event-protocol:register-io (WANT_READ / WANT_WRITE). Quicklisp cl+ssl
;;; has no MemoryBIO — FD BIO + WANT_* is the asyncio-equivalent path.

(defun ensure-tls ()
  "TLS is ready: cl-stack-ssl, and through it cl+ssl, load with this system.
   Kept for callers (CI hooks) that call it before the first request."
  t)

(defun %verify-arg (verify)
  (cond ((null verify) nil)
        ((eq verify t) :required)
        (t verify)))

(defstruct (tls-session (:constructor %make-tls-session))
  stream    ; cl+ssl::ssl-stream wrapping integer FD (socket BIO)
  handle    ; SSL*
  host
  verify
  (alpn nil) ; negotiated ALPN protocol string or NIL
  (handshake-done-p nil :type boolean)
  ;; Tunneled session (TLS inside TLS: an https origin through an https://
  ;; proxy, #53). The SSL* reads and writes two memory BIOs; TRANSPORT, the
  ;; TLS session to the proxy, carries their ciphertext. NIL: socket BIO.
  (transport nil)
  (rbio nil)          ; BIO* the SSL* reads ciphertext from (owned by the SSL*)
  (wbio nil)          ; BIO* the SSL* writes ciphertext to (owned by the SSL*)
  (out nil)           ; ciphertext drained from WBIO, not yet through TRANSPORT
  (out-pos 0 :type fixnum)
  (written nil)       ; SSL_write count whose ciphertext still waits in OUT
  (scratch nil))       ; cl+ssl buffer for BIO_read / BIO_write

(defun %set-alpn-protos (handle protocols)
  "Offer ALPN PROTOCOLS on SSL* HANDLE (cl+ssl wire format)."
  (when protocols
    (cffi:with-foreign-string ((string len)
                               (cl+ssl::make-alpn-proto-string protocols))
      (cl+ssl::ssl-set-alpn-protos handle string (1- len)))))

(defun tls-selected-alpn (session)
  "ALPN protocol selected after handshake, or NIL."
  (or (tls-session-alpn session)
      (when (and session (tls-session-stream session))
        (ignore-errors
          (cl+ssl:get-selected-alpn-protocol (tls-session-stream session))))))

(defun make-tls-session (fd host &key (verify t) (alpn-protocols '("h2" "http/1.1")))
  "Create SSL* + socket BIO on nonblocking FD. Does not run handshake.

   ALPN-PROTOCOLS — list of protocol names offered (RFC 7301), default prefer h2."
  (ensure-tls)
  (cl+ssl:ensure-initialized)
  (let* ((verify* (%verify-arg verify))
         (stream (make-instance 'cl+ssl::ssl-stream :socket fd))
         (handle (cl+ssl::ssl-new cl+ssl::*ssl-global-context*)))
    (when (cffi:null-pointer-p handle)
      (error 'http-tls-error :message "SSL_new failed"))
    (handler-case
        (progn
          (when host
            (cffi:with-foreign-string (chostname host)
              (cl+ssl::ssl-set-tlsext-host-name handle chostname)))
          (%set-alpn-protos handle alpn-protocols)
          (cl+ssl::install-handle-and-bio stream handle fd t)
          (cl+ssl::ssl-set-connect-state handle)
          (%make-tls-session :stream stream
                             :handle handle
                             :host host
                             :verify verify*))
      (error (e)
        (ignore-errors (cl+ssl::ssl-free handle))
        (setf (cl+ssl::ssl-stream-handle stream) nil)
        (error 'http-tls-error
               :message (format nil "TLS setup failed for ~A: ~A" host e))))))

(defun %new-memory-bio ()
  "A fresh memory BIO (BIO_s_mem). Empty, a read returns -1 with the retry
   flag set: the SSL* sees WANT_READ, not EOF."
  (let ((bio (cl+ssl::bio-new (cffi:foreign-funcall "BIO_s_mem" :pointer))))
    (when (cffi:null-pointer-p bio)
      (error 'http-tls-error :message "BIO_new(BIO_s_mem) failed"))
    bio))

(defun make-tunneled-tls-session (transport host &key (verify t) (alpn-protocols '("h2" "http/1.1")))
  "TLS to HOST inside TRANSPORT, an established TLS-SESSION: an https origin
   through the CONNECT tunnel of an https:// proxy (TLS inside TLS, #53), as
   urllib3 does. The SSL* reads and writes two memory BIOs and TRANSPORT
   carries their ciphertext, so the proxy relays octets it cannot read.
   VERIFY and ALPN-PROTOCOLS as for MAKE-TLS-SESSION: the certificate is
   checked against HOST, the origin. Does not run the handshake.

   The session's :WANT-READ / :WANT-WRITE are those of TRANSPORT's socket;
   TLS-CLOSE closes TRANSPORT too."
  (check-type transport tls-session)
  (cl+ssl:ensure-initialized)
  (let* ((verify* (%verify-arg verify))
         (stream (make-instance 'cl+ssl::ssl-stream :socket nil))
         (handle (cl+ssl::ssl-new cl+ssl::*ssl-global-context*)))
    (when (cffi:null-pointer-p handle)
      (error 'http-tls-error :message "SSL_new failed"))
    (handler-case
        (let ((rbio (%new-memory-bio))
              (wbio (%new-memory-bio)))
          ;; The SSL* owns both BIOs from here: SSL_free frees them.
          (cl+ssl::ssl-set-bio handle rbio wbio)
          (when host
            (cffi:with-foreign-string (chostname host)
              (cl+ssl::ssl-set-tlsext-host-name handle chostname)))
          (%set-alpn-protos handle alpn-protocols)
          (setf (cl+ssl::ssl-stream-handle stream) handle)
          (cl+ssl::ssl-set-connect-state handle)
          (%make-tls-session :stream stream
                             :handle handle
                             :host host
                             :verify verify*
                             :transport transport
                             :rbio rbio
                             :wbio wbio
                             :out (make-array 0 :element-type '(unsigned-byte 8)
                                                :adjustable t :fill-pointer 0)
                             :scratch (cl+ssl::make-buffer 16384)))
      (error (e)
        (ignore-errors (cl+ssl::ssl-free handle))
        (setf (cl+ssl::ssl-stream-handle stream) nil)
        (error 'http-tls-error
               :message (format nil "TLS setup failed for ~A (tunnel): ~A" host e))))))

(defun %tunnel-drain (session)
  "Move the ciphertext SESSION's SSL* wrote to its write BIO onto OUT."
  (let ((scratch (tls-session-scratch session))
        (out (tls-session-out session))
        (wbio (tls-session-wbio session)))
    (loop
      (let ((n (cl+ssl::with-pointer-to-vector-data (ptr scratch)
                 (cffi:foreign-funcall "BIO_read" :pointer wbio :pointer ptr
                                                  :int (length scratch) :int))))
        (unless (plusp n)
          (return))
        (let* ((start (fill-pointer out))
               (end (+ start n)))
          (when (> end (array-dimension out 0))
            (adjust-array out (max end (* 2 (array-dimension out 0)))))
          (setf (fill-pointer out) end)
          (replace out scratch :start1 start :end2 n))))))

(defun %tunnel-flush (session)
  "Write SESSION's pending ciphertext through its transport session.
   → NIL when all of it went out, else the transport's :WANT-READ or
   :WANT-WRITE. OUT is refilled only once it is empty, so a transport write
   that blocked is retried with the same octets, as SSL_write requires."
  (let ((out (tls-session-out session))
        (transport (tls-session-transport session)))
    (loop
      (when (>= (tls-session-out-pos session) (fill-pointer out))
        (setf (fill-pointer out) 0
              (tls-session-out-pos session) 0)
        (%tunnel-drain session)
        (when (zerop (fill-pointer out))
          (return nil)))
      (multiple-value-bind (n want)
          (tls-write-octets transport out (tls-session-out-pos session)
                            (fill-pointer out))
        (cond
         ((null n) (return want))
         ((zerop n) (return :want-write))
         (t (incf (tls-session-out-pos session) n)))))))

(defun %tunnel-fill (session)
  "Move ciphertext from the transport session into SESSION's read BIO.
   → :OK, :EOF (the transport ended), or the transport's :WANT-READ or
   :WANT-WRITE."
  (let ((scratch (tls-session-scratch session)))
    (multiple-value-bind (n want)
        (tls-read-octets (tls-session-transport session) scratch)
      (cond
       ((null n) (or want :want-read))
       ((zerop n) :eof)
       (t
        (let ((written (cl+ssl::with-pointer-to-vector-data (ptr scratch)
                         (cffi:foreign-funcall "BIO_write"
                                               :pointer (tls-session-rbio session)
                                               :pointer ptr :int n :int))))
          (unless (eql written n)
            (error 'http-tls-error
                   :message (format nil "BIO_write took ~A of ~A octets" written n))))
        :ok)))))

(defun %tunnel-step (session func args)
  "%SSL-STEP for a tunneled SESSION. Ciphertext the SSL* writes goes out
   through the transport; ciphertext it waits for comes in from it.
   → (:ok n) | (:want-read n) | (:want-write n) | (:eof 0), the wants being
   the transport socket's."
  (let ((handle (tls-session-handle session)))
    ;; Earlier output first: the peer may be waiting for it.
    (let ((want (%tunnel-flush session)))
      (when want
        (return-from %tunnel-step (values want -1))))
    (loop
      (let ((ret (apply func handle args)))
        (when (plusp ret)
          ;; Best effort: what stays in OUT goes before the next call.
          (%tunnel-flush session)
          (return (values :ok ret)))
        (let ((err (cl+ssl::ssl-get-error handle ret)))
          (cond
           ((or (eql err cl+ssl::+ssl-error-want-read+)
                (eql err cl+ssl::+ssl-error-want-write+))
            ;; Our flight (ClientHello, Finished) must reach the peer before
            ;; it can answer.
            (let ((want (%tunnel-flush session)))
              (when want
                (return (values want ret))))
            (when (eql err cl+ssl::+ssl-error-want-read+)
              (let ((status (%tunnel-fill session)))
                (case status
                  (:ok nil)
                  (:eof (return (values :eof 0)))
                  (t (return (values status ret)))))))
           ((eql err cl+ssl::+ssl-error-zero-return+)
            (return (values :eof 0)))
           ((and (eql err cl+ssl::+ssl-error-syscall+) (zerop ret))
            (return (values :eof 0)))
           ((eql err cl+ssl::+ssl-error-syscall+)
            (error 'tls-connection-error
                   :message (format nil "SSL syscall error ret=~A (tunnel)" ret)))
           (t
            (handler-case
                (cl+ssl::ssl-signal-error handle func err ret)
              (error (e)
                (error 'http-tls-error
                       :message (format nil "TLS error: ~A" e)))))))))))

(define-condition tls-connection-error (http-tls-error http-connection-error) ()
  (:documentation "SSL_ERROR_SYSCALL with ret -1: the socket under the TLS session
   failed (the peer reset it). Both a TLS error and a connection error, so a
   stale pooled HTTPS socket is retried like a cleartext one (#57)."))

(defun %ssl-step (session func &rest args)
  "One nonblocking SSL_* call → (:ok n) | (:want-read n) | (:want-write n) | (:eof 0)."
  (when (tls-session-transport session)
    (return-from %ssl-step (%tunnel-step session func args)))
  (let* ((stream (tls-session-stream session))
         (handle (tls-session-handle session))
         (ret (let ((cl+ssl::*bio-socket* (cl+ssl::ssl-stream-socket stream)))
                (apply func handle args))))
    (cond
      ((plusp ret) (values :ok ret))
      (t
       (let ((err (cl+ssl::ssl-get-error handle ret)))
         (case err
           (#.cl+ssl::+ssl-error-want-read+ (values :want-read ret))
           (#.cl+ssl::+ssl-error-want-write+ (values :want-write ret))
           (#.cl+ssl::+ssl-error-zero-return+ (values :eof 0))
           (#.cl+ssl::+ssl-error-syscall+
            (if (zerop ret)
                (values :eof 0)
                (error 'tls-connection-error
                       :message (format nil "SSL syscall error ret=~A" ret))))
           (t
            (handler-case
                (cl+ssl::ssl-signal-error handle func err ret)
              (error (e)
                (error 'http-tls-error
                       :message (format nil "TLS error: ~A" e)))))))))))

(defun tls-handshake-step (session)
  "Advance SSL_connect. Returns :done | :want-read | :want-write."
  (when (tls-session-handshake-done-p session)
    (return-from tls-handshake-step :done))
  (multiple-value-bind (status ret)
      (%ssl-step session #'cl+ssl::ssl-connect)
    (declare (ignore ret))
    (ecase status
      (:ok
       (handler-case
           (cl+ssl::maybe-verify-client-stream
            (tls-session-stream session)
            (tls-session-verify session)
            (tls-session-host session))
         (error (e)
           (error 'http-tls-error
                  :message (format nil "TLS verify failed for ~A: ~A"
                                   (tls-session-host session) e))))
       (setf (tls-session-handshake-done-p session) t
             (tls-session-alpn session)
             (ignore-errors
               (cl+ssl:get-selected-alpn-protocol (tls-session-stream session))))
       :done)
      ((:want-read :want-write) status)
      (:eof
       (error 'http-tls-error :message "TLS handshake EOF")))))

(defun tls-write-octets (session octets start end)
  "SSL_write. Returns bytes written, or (values NIL :want-read|:want-write).

   On a tunneled session a count means the octets' ciphertext went through
   the transport, as on a direct one it means the socket took it: a caller
   that goes on to read never leaves its request behind in the tunnel. While
   the transport blocks, the count of the SSL_write already done waits for
   the retry, which comes with the same arguments as any SSL_write retry."
  (let ((len (- end start))
        (tunnel (tls-session-transport session)))
    (when (zerop len) (return-from tls-write-octets 0))
    (when (and tunnel (tls-session-written session))
      (let ((want (%tunnel-flush session)))
        (when want
          (return-from tls-write-octets (values nil want))))
      (return-from tls-write-octets (shiftf (tls-session-written session) nil)))
    (let* ((stream (tls-session-stream session))
           (ssl-buf (cl+ssl::ssl-stream-output-buffer stream))
           (n (min len (length ssl-buf))))
      (replace ssl-buf octets :start2 start :end2 (+ start n))
      (cl+ssl::with-pointer-to-vector-data (ptr ssl-buf)
        (multiple-value-bind (status ret)
            (%ssl-step session #'cl+ssl::ssl-write ptr n)
          (ecase status
            (:ok
             (let ((want (and tunnel (%tunnel-flush session))))
               (cond
                (want
                 (setf (tls-session-written session) ret)
                 (values nil want))
                (t ret))))
            ((:want-read :want-write) (values nil status))
            (:eof
             (error 'http-tls-error :message "TLS write EOF"))))))))

(defun tls-read-octets (session buffer)
  "SSL_read into BUFFER. Returns count, 0 on EOF, or (values NIL :want-*)."
  (let* ((stream (tls-session-stream session))
         (ssl-buf (cl+ssl::ssl-stream-input-buffer stream))
         (n (min (length buffer) (length ssl-buf))))
    (cl+ssl::with-pointer-to-vector-data (ptr ssl-buf)
      (multiple-value-bind (status ret)
          (%ssl-step session #'cl+ssl::ssl-read ptr n)
        (ecase status
          (:ok
           (replace buffer ssl-buf :end1 ret)
           ret)
          ((:want-read :want-write) (values nil status))
          (:eof 0))))))

(defun tls-pending-p (session)
  "True when OpenSSL holds decrypted bytes SSL_read has not returned yet. The
   socket will not signal them: the record is already off the wire.

   A tunneled session also counts what sits between it and the socket:
   ciphertext in its BIOs or in OUT, and what its transport holds. A read
   moves it on (OUT goes out first)."
  (let ((handle (and session (tls-session-handle session))))
    (and handle
         (or (plusp (cffi:foreign-funcall "SSL_pending" :pointer handle :int))
             (let ((transport (tls-session-transport session)))
               (and transport
                    (or (< (tls-session-out-pos session)
                           (fill-pointer (tls-session-out session)))
                        (plusp (cffi:foreign-funcall "BIO_ctrl_pending"
                                                     :pointer (tls-session-rbio session)
                                                     :size))
                        (plusp (cffi:foreign-funcall "BIO_ctrl_pending"
                                                     :pointer (tls-session-wbio session)
                                                     :size))
                        (tls-pending-p transport)))))
         t)))

(defun tls-close (session)
  "Free SSL* without closing the underlying FD (owned by usocket). A tunneled
   session frees its memory BIOs (the SSL* owns them) and closes its
   transport session too."
  (when session
    (let ((stream (tls-session-stream session))
          (handle (tls-session-handle session)))
      (when (and handle stream (cl+ssl::ssl-stream-handle stream))
        ;; abort: skip SSL_shutdown (would block / need WANT_* pumping)
        (ignore-errors (cl+ssl::ssl-free handle))
        (setf (cl+ssl::ssl-stream-handle stream) nil
              (tls-session-handle session) nil
              (tls-session-rbio session) nil
              (tls-session-wbio session) nil)))
    (let ((transport (tls-session-transport session)))
      (when transport
        (setf (tls-session-transport session) nil)
        (tls-close transport)))))

;;; Legacy sync helpers (unused by async path; kept for ad-hoc REPL)

(defun make-tls-stream (usock host &key (verify t))
  "Blocking TLS client stream (run-to-completion). Prefer MAKE-TLS-SESSION."
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
