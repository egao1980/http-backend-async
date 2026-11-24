(in-package #:http-backend-async/tests)

;;;; #82: each hop of a redirect through a proxy is built for its own URL.
;;;;
;;;; http:// goes to the proxy in absolute-form with Proxy-Authorization.
;;;; https:// opens a CONNECT tunnel (Proxy-Authorization on the CONNECT) and
;;;; sends origin-form inside it with no Proxy-Authorization: the proxy
;;;; credentials never reach the origin. A hop to another origin (scheme,
;;;; host or port) loses Authorization and Cookie, as http-backend-dexador
;;;; does since #34; the cookie jar adds what it holds for the new URL. Each
;;;; hop picks its proxy again (a per-scheme :proxy table).
;;;;
;;;; The fixture proxies listen on 127.0.0.1 only and play every origin
;;;; themselves, so origin.test and other.test are never resolved. One is
;;;; plain (http://), one TLS (https://, tests/certs/proxy.crt). They take
;;;; one connection at a time and close it after one answer. The credentials
;;;; are made up: proxy user / secret, origin alice / made-up.

(defstruct (%hop (:conc-name %hop-))
  (proxy nil)   ; the fixture proxy that took the connection: :http or :https
  (connect nil) ; the CONNECT head it read, or NIL
  (head "")     ; the request head: read by the proxy, or inside the tunnel
  (url nil))    ; the URL the fixture answered as, or NIL when it names none

(defstruct (%hop-log (:conc-name %log-))
  (lock (bt:make-lock "proxy-redirect-hops"))
  (hops '())    ; newest first
  (errors '()))

(defun %log-push (log item)
  "Record ITEM, a %HOP or a condition, in LOG."
  (bt:with-lock-held ((%log-lock log))
    (if (typep item '%hop)
        (push item (%log-hops log))
        (push (princ-to-string item) (%log-errors log)))))

(defun %request-target (head)
  "The request-target of HEAD's request line, or the empty string."
  (let* ((sp1 (position #\Space head))
         (sp2 (and sp1 (position #\Space head :start (1+ sp1)))))
    (if sp2 (subseq head (1+ sp1) sp2) "")))

(defun %tunnel-origin (authority)
  "https://AUTHORITY, the origin a CONNECT to AUTHORITY reaches, without :443."
  (let ((colon (position #\: authority :from-end t)))
    (if (and colon (string= ":443" authority :start2 colon))
        (format nil "https://~A" (subseq authority 0 colon))
        (format nil "https://~A" authority))))

(defun %first-line (text)
  (subseq text 0 (or (position #\Return text) (length text))))

(defun %answer-route (stream routes url)
  "Answer the request for URL on STREAM: the redirect ROUTES has for it as
   (URL status location), else 200 ok; 400 when URL is NIL (a proxy cannot
   route an origin-form request). Connection: close."
  (let* ((route (assoc url routes :test #'equal))
         (status (cond (route (second route)) (url 200) (t 400)))
         (body (if (= status 200) "ok" "")))
    (write-sequence
     (apply #'%wire
            (format nil "HTTP/1.1 ~D ~A" status
                    (case status (200 "OK") (400 "Bad Request") (t "Redirect")))
            (append (when route
                      (list (format nil "Location: ~A" (third route))))
                    (list "Content-Type: text/plain"
                          (format nil "Content-Length: ~D" (length body))
                          "Connection: close")))
     stream)
    (write-sequence (babel:string-to-octets body) stream)
    (finish-output stream)))

(defun %serve-proxied-hop (kind socket routes log)
  "Serve one connection on SOCKET, a binary stream, as a proxy of KIND:
   TLS first for :https, then CONNECT (TLS to the origin inside the tunnel)
   or a request to forward. Log the hop in LOG and answer it from ROUTES."
  (let ((stream socket)
        (origin nil))
    (unwind-protect
         (progn
           (when (eq kind :https)
             (setf stream (cl+ssl:make-ssl-server-stream
                           socket
                           :certificate (%test-cert "proxy.crt")
                           :key (%test-cert "proxy.key")
                           :unwrap-stream-p nil)))
           (let* ((head (%read-head stream))
                  (target (%request-target head)))
             (cond
               ((zerop (length head)))
               ((%starts-with-p "CONNECT " head)
                (write-sequence (%wire "HTTP/1.1 200 Connection established") stream)
                (finish-output stream)
                (setf origin (cl+ssl:make-ssl-server-stream
                              stream
                              :certificate (%test-cert "origin.crt")
                              :key (%test-cert "origin.key")
                              :unwrap-stream-p nil))
                (let* ((inner (%read-head origin))
                       (inner-target (%request-target inner))
                       (url (if (%starts-with-p "/" inner-target)
                                (concatenate 'string (%tunnel-origin target) inner-target)
                                inner-target)))
                  (%log-push log (make-%hop :proxy kind :connect head :head inner :url url))
                  (%answer-route origin routes url)))
               (t
                (let ((url (and (%starts-with-p "http" target) target)))
                  (%log-push log (make-%hop :proxy kind :head head :url url))
                  (%answer-route stream routes url))))))
      (when origin
        (ignore-errors (close origin)))
      (unless (eq stream socket)
        (ignore-errors (close stream))))))

(defstruct (%fixture-proxy (:conc-name %fp-))
  kind listener thread (stop nil))

(defun %start-fixture-proxy (kind routes log)
  "A loopback proxy of KIND (:http plain, :https TLS) answering ROUTES, one
   connection at a time, until %STOP-FIXTURE-PROXY."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0
                                          :reuse-address t
                                          :element-type '(unsigned-byte 8)))
         (proxy (make-%fixture-proxy :kind kind :listener listener)))
    (setf (%fp-thread proxy)
          (bt:make-thread
           (lambda ()
             (loop until (%fp-stop proxy)
                   do (let ((conn nil))
                        (unwind-protect
                             (handler-case
                                 (when (usocket:wait-for-input listener :timeout 0.1
                                                                        :ready-only t)
                                   (setf conn (usocket:socket-accept
                                               listener :element-type '(unsigned-byte 8)))
                                   (ignore-errors
                                    (setf (usocket:socket-option conn :receive-timeout) 10))
                                   (%serve-proxied-hop kind (usocket:socket-stream conn)
                                                       routes log))
                               (error (e) (%log-push log e)))
                          (when conn
                            (ignore-errors (usocket:socket-close conn)))))))
           :name (format nil "proxy-redirect-fixture-~(~A~)" kind)))
    proxy))

(defun %stop-fixture-proxy (proxy &key (seconds 10))
  (setf (%fp-stop proxy) t)
  (let ((deadline (+ (get-internal-real-time)
                     (* seconds internal-time-units-per-second))))
    (loop while (and (bt:thread-alive-p (%fp-thread proxy))
                     (< (get-internal-real-time) deadline))
          do (sleep 0.02)))
  (when (bt:thread-alive-p (%fp-thread proxy))
    (ignore-errors (bt:destroy-thread (%fp-thread proxy))))
  (ignore-errors (usocket:socket-close (%fp-listener proxy))))

(defun %fixture-proxy-url (proxy)
  (format nil "~(~A~)://user:secret@127.0.0.1:~D"
          (%fp-kind proxy) (usocket:get-local-port (%fp-listener proxy))))

(defparameter +origin-credentials+ '(:basic "alice" "made-up")
  "The :auth every test request carries.")

(defun %origin-cookie-jar ()
  "A cookie jar holding session=made-up for origin.test only (path /)."
  (let ((jar (cl-cookie:make-cookie-jar)))
    (cl-cookie:merge-cookies
     jar (list (cl-cookie:make-cookie :name "session" :value "made-up"
                                      :origin-host "origin.test" :path "/")))
    jar))

(defun %redirect-through (chain proxies)
  "GET the first URL of CHAIN through the fixture proxies with Authorization
   and the origin.test cookie, following its redirects. PROXIES is :http or
   :https (that proxy for every hop) or :per-scheme (http:// through the
   plain proxy, https:// through the TLS one).
   → (values response-or-condition hops errors), hops oldest first."
  (let* ((routes (remove-if-not #'second chain))
         (log (make-%hop-log))
         (plain (%start-fixture-proxy :http routes log))
         (tls (%start-fixture-proxy :https routes log))
         (res nil))
    (unwind-protect
         (setf res
               (with-async-test (eb el hb)
                 (let ((client (make-http-client
                                hb
                                :proxy (ecase proxies
                                         (:http (%fixture-proxy-url plain))
                                         (:https (%fixture-proxy-url tls))
                                         (:per-scheme
                                          (list (cons "http" (%fixture-proxy-url plain))
                                                (cons "https" (%fixture-proxy-url tls)))))
                                :verify nil
                                :pool nil)))
                   (handler-case
                       (%await-promise (get-async (first (first chain))
                                                  :client client
                                                  :auth +origin-credentials+
                                                  :cookies (%origin-cookie-jar))
                                       eb el :timeout 15)
                     (error (e) e)))))
      (%stop-fixture-proxy plain)
      (%stop-fixture-proxy tls))
    (values res (reverse (%log-hops log)) (reverse (%log-errors log)))))

(defun %hop-proxy-kind (proxies url)
  "The fixture proxy a hop to URL should go through."
  (if (eq proxies :per-scheme)
      (if (%starts-with-p "https:" url) :https :http)
      proxies))

(defun %same-origin-url-p (a b)
  "A and B have the same scheme, host and port (http-backend-dexador's rule)."
  (let ((a (quri:uri a))
        (b (quri:uri b)))
    (and (string-equal (quri:uri-scheme a) (quri:uri-scheme b))
         (string-equal (quri:uri-host a) (quri:uri-host b))
         (eql (quri:uri-port a) (quri:uri-port b)))))

(defun %check-hop (hop url kind authorization-p)
  "HOP, as the fixture saw it, went to URL as it should: through the KIND
   proxy, in the form and with the proxy headers URL's scheme needs, with
   Authorization when AUTHORIZATION-P, with the cookie when URL is on
   origin.test."
  (let* ((u (quri:uri url))
         (host (quri:uri-host u))
         (head (if hop (%hop-head hop) ""))
         (connect (and hop (%hop-connect hop)))
         (cookie-p (and (string-equal host "origin.test") t))
         (origin-auth (authorization-header-value +origin-credentials+)))
    (testing url
      (ok (eq kind (and hop (%hop-proxy hop)))
          (format nil "through the ~(~A~) proxy (got ~S)" kind (and hop (%hop-proxy hop))))
      (cond
        ((string-equal "https" (quri:uri-scheme u))
         (ok (and connect
                  (%starts-with-p (format nil "CONNECT ~A:~D HTTP/1.1" host (quri:uri-port u))
                                  connect))
             (format nil "CONNECT to the proxy (it read ~S)" (%first-line (or connect head))))
         (ok (and connect (search +test-proxy-credentials+ connect))
             "Proxy-Authorization on the CONNECT")
         (ok (%starts-with-p (format nil "GET ~A HTTP/1.1" (quri:uri-path u)) head)
             (format nil "origin-form inside the tunnel (~S)" (%first-line head)))
         (ok (not (or (search "proxy-authorization" (string-downcase head))
                      (search +test-proxy-credentials+ head)))
             "no Proxy-Authorization inside the tunnel: the origin never sees it"))
        (t
         (ok (null connect) (format nil "no CONNECT for http:// (~S)" connect))
         (ok (%starts-with-p (format nil "GET ~A HTTP/1.1" url) head)
             (format nil "absolute-form to the proxy (~S)" (%first-line head)))
         (ok (search +test-proxy-credentials+ head)
             "Proxy-Authorization to the proxy")))
      (ok (eq authorization-p (and (search origin-auth head) t))
          (if authorization-p
              "Authorization while the origin stays the same"
              "no Authorization once a hop went to another origin"))
      (ok (eq cookie-p (and (search "session=made-up" head) t))
          (if cookie-p
              "the jar's origin.test cookie"
              "no origin.test cookie on another host")))))

(defparameter *proxy-redirect-chains*
  '((:http-to-https ("http://origin.test/a" 301 "https://origin.test/b")
     ("https://origin.test/b"))
    (:https-to-http ("https://origin.test/a" 301 "http://origin.test/b")
     ("http://origin.test/b"))
    (:http-to-https-other-host ("http://origin.test/a" 302 "https://other.test/b")
     ("https://other.test/b"))
    (:https-to-http-other-host ("https://origin.test/a" 302 "http://other.test/b")
     ("http://other.test/b"))
    (:http-same-origin ("http://origin.test/a" 302 "/b")
     ("http://origin.test/b"))
    (:https-same-origin ("https://origin.test/a" 307 "/b")
     ("https://origin.test/b"))
    (:http-https-http ("http://origin.test/a" 301 "https://origin.test/b")
     ("https://origin.test/b" 302 "http://origin.test/c")
     ("http://origin.test/c")))
  "Redirect chains, (id (url [status location]) …): one entry per hop, the
   last one answers 200 ok.")

(defparameter *proxy-redirect-rows*
  (append
   (loop for proxies in '(:http :https)
         append (loop for (id . chain) in *proxy-redirect-chains*
                      collect (list proxies id chain)))
   (loop for (id . chain) in *proxy-redirect-chains*
         when (member id '(:http-to-https :https-to-http :http-https-http))
           collect (list :per-scheme id chain)))
  "(proxies id chain) rows: every chain through each proxy, and the chains
   that change scheme with a proxy per scheme.")

(deftest-parametrize proxy-redirect-hops
    ((proxies id chain)
     :ids (mapcar (lambda (row) (format nil "~(~A-~A~)" (first row) (second row)))
                  *proxy-redirect-rows*)
     :rows *proxy-redirect-rows*)
  (multiple-value-bind (res hops errors) (%redirect-through chain proxies)
    (ok (%ok-response-p res)
        (format nil "~(~A via ~A~): 200 ok at the end (got ~A)" id proxies
                (if (typep res 'http-response) (response-status res) res)))
    (ok (= (length chain) (length hops))
        (format nil "~D hops through the proxies (~S)" (length chain) (mapcar #'%hop-url hops)))
    (when (typep res 'http-response)
      (ok (= (1- (length chain)) (length (response-history res)))
          "every redirect in the history"))
    (let ((authorization-p t)
          (previous nil))
      (loop for (url) in chain
            for i from 0
            do (when previous
                 (setf authorization-p (and authorization-p
                                            (%same-origin-url-p previous url))))
               (%check-hop (nth i hops) url (%hop-proxy-kind proxies url) authorization-p)
               (setf previous url)))
    (ok (null errors) (format nil "fixture errors: ~S" errors))))

(deftest-parametrize hop-route-forms
    ((proxy url form host port tls-p)
     (nil "http://origin.test/a" :direct "origin.test" 80 nil)
     (nil "https://origin.test:8443/a" :direct "origin.test" 8443 nil)
     ("http://user:secret@127.0.0.1:3128" "http://origin.test/a"
      :absolute-form "127.0.0.1" 3128 nil)
     ("http://user:secret@127.0.0.1:3128" "https://origin.test/a"
      :connect "127.0.0.1" 3128 nil)
     ("https://user:secret@127.0.0.1:8443" "http://origin.test/a"
      :absolute-form "127.0.0.1" 8443 t)
     ("https://user:secret@127.0.0.1:8443" "https://origin.test/a"
      :connect "127.0.0.1" 8443 t)
     ("socks5h://user:secret@127.0.0.1:1080" "https://origin.test/a"
      :socks5 "127.0.0.1" 1080 nil))
  (let ((route (http-backend-async::hop-route proxy (quri:uri url))))
    (ok (eq form (http-backend-async::route-form route))
        (format nil "~A via ~A: ~S" url proxy (http-backend-async::route-form route)))
    (ok (equal host (http-backend-async::route-host route)) "dials the proxy, or the origin")
    (ok (eql port (http-backend-async::route-port route)))
    (ok (eq tls-p (http-backend-async::route-tls-p route)) "TLS to an https:// proxy")
    (ok (equal (and proxy "user") (http-backend-async::route-user route)))
    (ok (eq (eq form :absolute-form)
            (and (assoc "proxy-authorization"
                        (http-backend-async::add-proxy-authorization '() route)
                        :test #'string-equal)
                 t))
        "Proxy-Authorization in the request head for absolute-form only")))

(deftest-parametrize prepare-redirect-hop-credentials
    ((to authorization-p)
     ("http://origin.test/b" t)
     ("http://origin.test:80/b" t)
     ("https://origin.test/b" nil)
     ("http://origin.test:8080/b" nil)
     ("http://other.test/b" nil))
  (multiple-value-bind (method uri headers)
      (http-backend-async::prepare-redirect-hop
       302 (quri:uri "http://origin.test/a") (quri:uri to) :get #()
       '(("authorization" . "Basic YWxpY2U6bWFkZS11cA==")
         ("proxy-authorization" . "Basic dXNlcjpzZWNyZXQ=")
         ("x-kept" . "yes"))
       (cl-cookie:make-cookie-jar) nil)
    (declare (ignore method uri))
    (ok (eq authorization-p (and (assoc "authorization" headers :test #'string-equal) t))
        (if authorization-p "Authorization kept on the same origin" "Authorization dropped"))
    (ok (null (assoc "proxy-authorization" headers :test #'string-equal))
        "Proxy-Authorization never carries over: the caller adds it per hop")
    (ok (assoc "x-kept" headers :test #'string-equal))))
