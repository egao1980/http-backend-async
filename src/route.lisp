(in-package #:http-backend-async)

;;; How each hop of a request travels (#82). The first hop and every redirect
;;; ask HOP-ROUTE again: a hop to another scheme or host gets its own proxy,
;;; request form, proxy headers and pool key.

(defstruct (hop-route (:conc-name route-))
  "How one hop of a request travels. PROXY is the proxy URL RESOLVE-PROXY
   named for the hop's URL, or NIL: direct. HOST and PORT are what to dial:
   the origin, or the proxy. FORM is :DIRECT; :ABSOLUTE-FORM, an http://
   origin through an http(s) proxy (the request goes to the proxy, with
   Proxy-Authorization); :CONNECT, an https:// origin through one (CONNECT
   with Proxy-Authorization, then TLS to the origin inside the tunnel); or
   :SOCKS5. TLS-P: an https:// proxy, reached over TLS first (#53). USER and
   PASSWORD are the proxy URL's credentials, REMOTE-DNS-P says a socks5h://
   proxy resolves the host. POOL-KEY scopes pooled connections to the
   origin and the proxy."
  (proxy nil)
  (pool-key nil)
  (host nil)
  (port nil)
  (form :direct)
  (tls-p nil)
  (user nil)
  (password nil)
  (remote-dns-p nil))

(defun hop-route (proxy uri)
  "The HOP-ROUTE of a request to URI (a quri URI) through PROXY, the proxy
   URL RESOLVE-PROXY named for URI, or NIL for none. An http(s) proxy takes
   an http:// origin in absolute-form and an https:// one through CONNECT,
   as dexador; an https:// proxy is reached over TLS either way, checked
   against its own host name. Signals UNSUPPORTED-OPERATION for a SOCKS4
   proxy."
  (multiple-value-bind (host port scheme) (%uri-host-port uri)
    (if (null proxy)
        (make-hop-route :pool-key (pool-key scheme host port) :host host :port port)
        (multiple-value-bind (pscheme phost pport user password)
            (parse-proxy-uri proxy)
          (let ((key (pool-key scheme host port :proxy proxy)))
            (ecase (proxy-kind proxy)
              ((:http :https)
               (make-hop-route :proxy proxy :pool-key key :host phost :port pport
                               :form (if (string-equal scheme "https")
                                         :connect
                                         :absolute-form)
                               :tls-p (proxy-tls-p proxy)
                               :user user :password password))
              (:socks5
               (make-hop-route :proxy proxy :pool-key key :host phost :port pport
                               :form :socks5
                               :user user :password password
                               :remote-dns-p (socks-remote-dns-p pscheme)))
              (:socks4
               (error 'unsupported-operation
                      :operation :socks4-proxy
                      :message "SOCKS4 not implemented; use socks5:// or socks5h://"))))))))

(defun route-absolute-form-p (route)
  "T when ROUTE sends the request to an http(s) proxy in absolute-form."
  (eq (route-form route) :absolute-form))

(defun add-proxy-authorization (headers route)
  "HEADERS with Proxy-Authorization from ROUTE's proxy credentials when the
   request goes to the proxy in absolute-form and HEADERS has none. Never
   for a CONNECT tunnel: its credentials go on the CONNECT, and the request
   inside it reaches the origin."
  (let ((value (and (route-absolute-form-p route)
                    (not (assoc "proxy-authorization" headers :test #'string-equal))
                    (proxy-authorization-value (route-user route) (route-password route)))))
    (if value
        (acons "proxy-authorization" (make-secret value) headers)
        headers)))
