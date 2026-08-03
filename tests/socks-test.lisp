(in-package #:http-backend-async/tests)

(deftest socks5-connect-request-domain
  (let ((req (http-backend-async::socks5-connect-request "example.com" 443)))
    (ok (= #x05 (aref req 0)))
    (ok (= #x01 (aref req 1)))          ; CONNECT
    (ok (= #x03 (aref req 3)))          ; DOMAINNAME
    (ok (= (length "example.com") (aref req 4)))
    (ok (= 443 (+ (ash (aref req (- (length req) 2)) 8)
                  (aref req (1- (length req))))))))

(deftest socks5-connect-request-ipv4
  (let ((req (http-backend-async::socks5-connect-request "1.2.3.4" 80)))
    (ok (= #x01 (aref req 3)))
    (ok (equalp #(1 2 3 4) (subseq req 4 8)))
    (ok (= 80 (+ (ash (aref req 8) 8) (aref req 9))))))
