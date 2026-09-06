(in-package #:http-backend-async)

;;; SOCKS5 CONNECT client (RFC 1928 + optional RFC 1929 user/pass).
;;; Used after TCP connect to the proxy; tunnel then carries HTTP or TLS.

(defun %socks-fail (msg)
  (error 'http-connection-error :message msg))

(defun socks5-greeting (&key username)
  "Client greeting: no-auth and optionally username/password."
  (if username
      (make-array 4 :element-type '(unsigned-byte 8)
                  :initial-contents '(#x05 #x02 #x00 #x02))
      (make-array 3 :element-type '(unsigned-byte 8)
                  :initial-contents '(#x05 #x01 #x00))))

(defun socks5-userpass-request (username password)
  (let* ((u (babel:string-to-octets (or username "") :encoding :utf-8))
         (p (babel:string-to-octets (or password "") :encoding :utf-8))
         (ul (length u))
         (pl (length p)))
    (when (or (> ul 255) (> pl 255))
      (%socks-fail "SOCKS5 username/password too long"))
    (let ((out (make-array (+ 3 ul pl) :element-type '(unsigned-byte 8))))
      (setf (aref out 0) 1
            (aref out 1) ul)
      (replace out u :start1 2)
      (setf (aref out (+ 2 ul)) pl)
      (replace out p :start1 (+ 3 ul))
      out)))

(defun %ipv4-dotted-host-p (host)
  "True when HOST is a dotted IPv4 literal (not IPv6)."
  (and (stringp host)
       (not (find #\: host))
       (let ((parts (uiop:split-string host :separator ".")))
         (and (= 4 (length parts))
              (every (lambda (p)
                       (and (plusp (length p))
                            (<= (length p) 3)
                            (every #'digit-char-p p)
                            (<= 0 (parse-integer p) 255)))
                     parts)))))

(defun %socks5-connect-ipv4 (host hi lo)
  (let* ((parts (mapcar #'parse-integer
                        (uiop:split-string host :separator ".")))
         (out (make-array 10 :element-type '(unsigned-byte 8))))
    (setf (aref out 0) #x05
          (aref out 1) #x01          ; CONNECT
          (aref out 2) #x00
          (aref out 3) #x01)         ; IPv4
    (loop for i from 0 below 4
          do (setf (aref out (+ 4 i)) (nth i parts)))
    (setf (aref out 8) hi (aref out 9) lo)
    out))

(defun %socks5-connect-domain (host hi lo)
  (let* ((name (babel:string-to-octets host :encoding :utf-8))
         (n (length name)))
    (when (> n 255)
      (%socks-fail "SOCKS5 hostname too long"))
    (let ((out (make-array (+ 7 n) :element-type '(unsigned-byte 8))))
      (setf (aref out 0) #x05
            (aref out 1) #x01
            (aref out 2) #x00
            (aref out 3) #x03        ; DOMAINNAME
            (aref out 4) n)
      (replace out name :start1 5)
      (setf (aref out (+ 5 n)) hi
            (aref out (+ 6 n)) lo)
      out)))

(defun socks5-connect-request (host port &key remote-dns)
  "Build SOCKS5 CONNECT. :REMOTE-DNS T (socks5h) always sends ATYP 3
   so the proxy resolves HOST. Otherwise IPv4 literals use ATYP 1."
  (let* ((port (if (integerp port) port (parse-integer (princ-to-string port))))
         (hi (ldb (byte 8 8) port))
         (lo (ldb (byte 8 0) port))
         (host (strip-ipv6-brackets host)))
    (if (and (not remote-dns) (%ipv4-dotted-host-p host))
        (%socks5-connect-ipv4 host hi lo)
        (%socks5-connect-domain host hi lo))))

(defun socks5-reply-ok-p (buf &key (start 0))
  "True if BUF[START…] is a successful CONNECT reply (VERN CMD=0)."
  (and (>= (- (length buf) start) 4)
       (= (aref buf start) #x05)
       (zerop (aref buf (1+ start)))))

(defun socks5-reply-length (buf &key (start 0))
  "Full length of a SOCKS5 reply starting at START, or NIL if incomplete."
  (when (>= (- (length buf) start) 4)
    (let ((atyp (aref buf (+ start 3))))
      (case atyp
        (#x01 (when (>= (- (length buf) start) 10) 10))
        (#x03 (when (>= (- (length buf) start) 5)
                (let ((n (aref buf (+ start 4))))
                  (when (>= (- (length buf) start) (+ 7 n))
                    (+ 7 n)))))
        (#x04 (when (>= (- (length buf) start) 22) 22))
        (t nil)))))
