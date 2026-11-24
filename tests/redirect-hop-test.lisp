;;;; #84: the method and body a redirect hop goes out with (RFC 9110 §15.4).
;;;; #85: a redirect on a kept-alive connection gives it back to the pool,
;;;; so a same-origin hop reuses it.

(in-package #:http-backend-async/tests)

(defvar *redirect-hops-seen* nil
  "(method path body) of each request the redirect fixture served, newest
   first. Set, not bound: the fixture thread pushes to it.")

(defun %redirect-method-handler (method path headers body)
  "/<status> answers <status> with Location /target; /target answers 200
   \"ok\". A HEAD gets the same Content-Length and no body, as a server sends."
  (declare (ignore headers))
  (push (list (string-upcase method) path
              (if body (babel:octets-to-string body :encoding :utf-8) ""))
        *redirect-hops-seen*)
  (let* ((targetp (string= path "/target"))
         (octets (babel:string-to-octets (if targetp "ok" "go")))
         (fields (list (cons "content-type" "text/plain")
                       (cons "content-length" (princ-to-string (length octets))))))
    (values (if targetp 200 (parse-integer path :start 1))
            (if targetp fields (acons "location" "/target" fields))
            (if (string-equal method "HEAD") #() octets))))

(deftest-parametrize redirect-hop-method
    ((status method content want-method want-content)
     (301 :head nil "HEAD" "")
     (302 :head nil "HEAD" "")
     (303 :head nil "HEAD" "")
     (301 :post "x=1" "GET" "")
     (302 :post "x=1" "GET" "")
     (303 :post "x=1" "GET" "")
     (303 :put "x=1" "GET" "")
     (301 :put "x=1" "PUT" "x=1")
     (302 :delete nil "DELETE" "")
     (307 :post "x=1" "POST" "x=1")
     (308 :put "x=1" "PUT" "x=1"))
  (setf *redirect-hops-seen* nil)
  (with-http-fixture (#'%redirect-method-handler)
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((res (%await-promise
                  (apply #'request-async method
                         (fixture-url (format nil "/~D" status))
                         (when content (list :content content)))
                  eb el)))
        (ok (= 200 (response-status res))
            (format nil "~D ~A: final status ~D" status method (response-status res)))
        (ok (equal (list want-method "/target" want-content) (first *redirect-hops-seen*))
            (format nil "~D ~A: the target saw ~S" status method (first *redirect-hops-seen*)))
        (ok (= (if (eq method :head) 0 2) (length (response-body res)))
            (format nil "~D ~A: response body ~S" status method (response-body res)))))))

(deftest redirect-refuses-to-resend-a-streamed-upload
  "A redirect that keeps the body cannot send a streamed upload again: its
   octets are gone, so the hop fails at once. A multipart upload has a known
   length; only a chunked one was refused, and the hop went out with a
   Content-Length and no body."
  (with-http-fixture (#'%redirect-method-handler)
    (with-async-test (eb el hb)
      (declare (ignore hb))
      (let ((err (handler-case
                     (progn
                       (%await-promise
                        (post-async (fixture-url "/307")
                                    :files `(("upload" . ,(make-http-file
                                                           "x=1" :filename "a.txt"))))
                        eb el :timeout 3)
                       nil)
                   (error (e) e))))
        (ok (typep err 'http-error) (format nil "got ~S" err))
        (ok (and err (search "replay" (princ-to-string err)))
            (format nil "the hop failed with: ~A" err))))))

(defun %same-origin-redirect-handler (method path headers body)
  "/a → 302 to /b, /b → 200 \"b\"; both say Keep-Alive: timeout=7."
  (declare (ignore method headers body))
  (if (string= path "/a")
      (values 302
              '(("location" . "/b") ("keep-alive" . "timeout=7"))
              (babel:string-to-octets "go"))
      (values 200
              '(("content-type" . "text/plain") ("keep-alive" . "timeout=7"))
              (babel:string-to-octets "b"))))

(deftest same-origin-redirect-reuses-the-connection
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture (#'%same-origin-redirect-handler :keep-alive t)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool))
               (res (%await-promise (get-async (fixture-url "/a") :client client)
                                    eb el)))
          (ok (= 200 (response-status res)))
          (ok (string= "b" (%body-text res)))
          (ok (= 1 (length (response-history res))))
          (ok (= 2 *fixture-request-count*))
          (ok (= 1 *fixture-accept-count*)
              (format nil "~D connections accepted for /a → /b on one keep-alive origin"
                      *fixture-accept-count*))
          (pool-clear pool))))))

(defun %pooled-connections (pool)
  "Every connection idle in POOL, an LRU-CONNECTION-POOL: (key . connection)."
  (loop for key being the hash-keys of (http-backend-async::lru-pool-table pool)
          using (hash-value entries)
        append (mapcar (lambda (entry)
                         (cons key (http-backend-async::lru-entry-connection entry)))
                       entries)))

(deftest cross-origin-redirect-pools-the-first-connection
  "A redirect to another origin gives the first connection back to the pool,
   with the redirect response's Keep-Alive timeout and its own scheme."
  (let ((http-protocol:*default-connection-pool* nil))
    (multiple-value-bind (other-port thread) (%one-shot-server "other")
      (with-http-fixture ((lambda (method path headers body)
                            (declare (ignore method path headers body))
                            (values 302
                                    (list (cons "location"
                                                (format nil "http://127.0.0.1:~D/x"
                                                        other-port))
                                          (cons "keep-alive" "timeout=7"))
                                    (babel:string-to-octets "go")))
                          :keep-alive t)
        (with-async-test (eb el hb)
          (let* ((pool (make-lru-connection-pool :max-size 4))
                 (client (make-http-client hb :pool pool))
                 (res (%await-promise (get-async (fixture-url "/away") :client client)
                                      eb el))
                 (pooled (%pooled-connections pool))
                 (conn (cdr (first pooled))))
            (ok (string= "other" (%body-text res)))
            (ok (= 1 (length pooled)) (format nil "pooled after the hop: ~S" pooled))
            (ok (and conn (eql 7 (http-backend-async::async-conn-idle-timeout conn)))
                "the pooled connection keeps the redirect response's Keep-Alive timeout")
            (ok (and conn (not (http-backend-async::async-conn-https-p conn)))
                "the pooled connection is the http:// origin's")
            (pool-clear pool))))
      (bt:join-thread thread))))
