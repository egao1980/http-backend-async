(in-package #:http-backend-async)

;;; WebSocket via this backend — RFC 8441 Extended CONNECT (planned).
;;;
;;; CLOS: ASYNC-BACKEND is a WS-BACKEND. Transport list is empty until the
;;; Extended CONNECT + RFC 6455 framing path is wired on the H2 pump.
;;; Peer SETTINGS_ENABLE_CONNECT_PROTOCOL is already recorded on
;;; ASYNC-H2-CLIENT-CONNECTION (see http2.lisp).
;;;
;;; Until then: use ws-backend-websocket-driver (:http/1.1 Upgrade) or
;;; http-backend-winhttp (WinHTTP WebSocket API) for live WS.

(defmethod backend-ws-transports ((backend async-backend))
  "No live WS transport yet — Extended CONNECT wire is the next milestone."
  (declare (ignore backend))
  '())

(defmethod connect ((backend async-backend) client url &key transport)
  "Present so specialize sites exist; :BEFORE already rejects empty transports."
  (declare (ignore client url transport))
  (error 'ws-transport-not-available
         :requested :http/2
         :negotiated nil
         :message
         (format nil
                 "http-backend-async WebSocket not implemented yet ~
                  (RFC 8441 Extended CONNECT). Use ws-backend-websocket-driver ~
                  or http-backend-winhttp. transports=~S"
                 (backend-ws-transports backend))))

(defun make-extended-connect-ws-headers (url &key headers protocols scheme)
  "RFC 8441 header alist for a future Extended CONNECT open.

   Thin wrapper over MAKE-HTTP2-WEBSOCKET-CONNECT-HEADERS so HTTP backends
   share one header-field policy."
  (make-http2-websocket-connect-headers
   url headers :scheme scheme :protocols protocols))
