(in-package #:http-backend-async/tests)

(defun %socks-port (req)
  (+ (ash (aref req (- (length req) 2)) 8)
     (aref req (1- (length req)))))

(defun %socks-domain (req)
  (babel:octets-to-string (subseq req 5 (+ 5 (aref req 4)))
                          :encoding :utf-8))

(deftest socks5-connect-request-domain
  (let ((req (http-backend-async::socks5-connect-request "example.com" 443)))
    (ok (= #x05 (aref req 0)))
    (ok (= #x01 (aref req 1)))          ; CONNECT
    (ok (= #x03 (aref req 3)))          ; DOMAINNAME
    (ok (= (length "example.com") (aref req 4)))
    (ok (= 443 (%socks-port req)))))

(deftest socks5-connect-request-ipv4
  (let ((req (http-backend-async::socks5-connect-request "1.2.3.4" 80)))
    (ok (= #x01 (aref req 3)))
    (ok (equalp #(1 2 3 4) (subseq req 4 8)))
    (ok (= 80 (+ (ash (aref req 8) 8) (aref req 9))))))

(deftest socks5-connect-request-remote-dns-domain
  "socks5h: hostname is ATYP 3 (domain), never a resolved IPv4."
  (let ((req (http-backend-async::socks5-connect-request
              "example.com" 443 :remote-dns t)))
    (ok (= #x03 (aref req 3)))
    (ok (string= "example.com" (%socks-domain req)))
    (ok (= 443 (%socks-port req)))))

(deftest socks5-connect-request-remote-dns-ipv4-literal
  "socks5h: even a dotted IPv4 is ATYP 3 so the proxy sees the name."
  (let ((req (http-backend-async::socks5-connect-request
              "1.2.3.4" 80 :remote-dns t)))
    (ok (= #x03 (aref req 3)))
    (ok (= 7 (aref req 4)))
    (ok (string= "1.2.3.4" (%socks-domain req)))
    (ok (= 80 (%socks-port req)))))

(deftest socks5-connect-request-local-dns-ipv4
  "socks5: IPv4 literal stays ATYP 1."
  (let ((req (http-backend-async::socks5-connect-request
              "1.2.3.4" 80 :remote-dns nil)))
    (ok (= #x01 (aref req 3)))
    (ok (equalp #(1 2 3 4) (subseq req 4 8)))))
