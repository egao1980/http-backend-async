(in-package #:http-backend-async)

;;; Async TLS via cl+ssl (+ optional cl-stack-ssl overlay).
;;; Socket-BIO on a nonblocking FD; SSL_connect / SSL_read / SSL_write driven
;;; by event-protocol:register-io (WANT_READ / WANT_WRITE). Quicklisp cl+ssl
;;; has no MemoryBIO — FD BIO + WANT_* is the asyncio-equivalent path.

(defun ensure-tls ()
  "Load cl-stack-ssl when present, else bare cl+ssl."
  (or (ignore-errors (asdf:load-system "cl-stack-ssl") t)
      (asdf:load-system "cl+ssl")
      t))

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
  (handshake-done-p nil :type boolean))

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

(defun %ssl-step (session func &rest args)
  "One nonblocking SSL_* call → (:ok n) | (:want-read n) | (:want-write n) | (:eof 0)."
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
                (error 'http-tls-error
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
  "SSL_write. Returns bytes written, or (values NIL :want-read|:want-write)."
  (let ((len (- end start)))
    (when (zerop len) (return-from tls-write-octets 0))
    (let* ((stream (tls-session-stream session))
           (ssl-buf (cl+ssl::ssl-stream-output-buffer stream))
           (n (min len (length ssl-buf))))
      (replace ssl-buf octets :start2 start :end2 (+ start n))
      (cl+ssl::with-pointer-to-vector-data (ptr ssl-buf)
        (multiple-value-bind (status ret)
            (%ssl-step session #'cl+ssl::ssl-write ptr n)
          (ecase status
            (:ok ret)
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

(defun tls-close (session)
  "Free SSL* without closing the underlying FD (owned by usocket)."
  (when session
    (let ((stream (tls-session-stream session))
          (handle (tls-session-handle session)))
      (when (and handle stream (cl+ssl::ssl-stream-handle stream))
        ;; abort: skip SSL_shutdown (would block / need WANT_* pumping)
        (ignore-errors (cl+ssl::ssl-free handle))
        (setf (cl+ssl::ssl-stream-handle stream) nil
              (tls-session-handle session) nil)))))

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
