(in-package #:http-backend-async/tests)

(defclass fake-conn ()
  ((alive :initarg :alive :accessor fake-conn-alive :initform t)
   (closed :initform nil :accessor fake-conn-closed)))

(defmethod connection-alive-p ((c fake-conn))
  (fake-conn-alive c))

(defmethod pool-discard ((pool lru-connection-pool) (c fake-conn))
  (setf (fake-conn-closed c) t))

(deftest lru-pool-acquire-release
  (let* ((pool (make-lru-connection-pool :max-size 2))
         (k (pool-key "http" "example.com" 80))
         (a (make-instance 'fake-conn))
         (b (make-instance 'fake-conn))
         (c (make-instance 'fake-conn))
         (evicted nil))
    (pool-release pool k a :on-evict (lambda (x) (setf evicted x)))
    (ok (eq a (pool-acquire pool k)))
    (ok (null (pool-acquire pool k)))
    (pool-release pool k a :on-evict (lambda (x) (setf evicted x)))
    (pool-release pool k b)
    (pool-release pool k c)
    (ok (eq a evicted))
    (ok (fake-conn-closed evicted))
    (pool-clear pool)))

(deftest constructor-registers-default
  (ok (functionp http-protocol:*connection-pool-constructor*))
  (let ((http-protocol:*default-connection-pool* nil))
    (let ((p (ensure-default-connection-pool :max-size 4)))
      (ok (typep p 'lru-connection-pool))
      (ok (eq p http-protocol:*default-connection-pool*)))))

(deftest response-keeps-alive-parse
  (let ((ht (make-hash-table :test #'equal)))
    (setf (gethash "connection" ht) "keep-alive")
    (ok (response-keeps-alive-p ht 1.1))
    (setf (gethash "connection" ht) "close")
    (ok (not (response-keeps-alive-p ht 1.1)))
    (remhash "connection" ht)
    (ok (response-keeps-alive-p ht 1.1))
    (ok (not (response-keeps-alive-p ht 1.0)))))

(defclass h1-pool-stub (async-pooled-connection) ())

(defmethod connection-alive-p ((c h1-pool-stub))
  (http-backend-async::async-conn-alive-p c))

(defclass h2-pool-stub (async-pooled-h2-connection) ())

(defmethod connection-alive-p ((c h2-pool-stub))
  (http-backend-async::async-h2-conn-alive-p c))

(defun %make-h2-stub (event-loop)
  (make-instance 'h2-pool-stub
                 :socket nil
                 :https t
                 :session (list :session)
                 :pump (list :pump)
                 :event-loop event-loop))

(deftest h2-pool-entry-not-handed-to-h1
  "An h2 entry lives under the |h2 key. The HTTP/1.1 acquire never receives it,
   and a stray h2 object on the plain key is discarded rather than adopted."
  (let* ((pool (make-lru-connection-pool :max-size 4))
         (base (pool-key "https" "example.com" 443))
         (h2-key (h2-pool-key base))
         (loop-a (cons :loop :a))
         (loop-b (cons :loop :b))
         (h2 (%make-h2-stub loop-a))
         (h1 (make-instance 'h1-pool-stub :socket nil :https t)))
    (ok (string= (concatenate 'string base "|h2") h2-key))
    (pool-release pool h2-key h2)
    ;; Plain key does not see the h2 entry.
    (ok (null (pool-acquire pool base)))
    (multiple-value-bind (conn kind)
        (http-backend-async::acquire-pooled-connection
         pool base loop-a :https t :version-pref :http/1.1)
      (ok (null conn))
      (ok (null kind))
      (ok (not (typep conn 'async-pooled-h2-connection))))
    ;; Still pooled for a matching loop on the h2 key.
    (multiple-value-bind (conn kind)
        (http-backend-async::acquire-pooled-connection
         pool base loop-a :https t :version-pref :http/2)
      (ok (eq conn h2))
      (ok (eq kind :h2))
      (ok (typep conn 'async-pooled-h2-connection))
      (ok (not (typep conn 'async-pooled-connection))))
    ;; Different event-loop: discard that entry and do not hand it back.
    (pool-release pool h2-key h2)
    (multiple-value-bind (conn kind)
        (http-backend-async::acquire-pooled-connection
         pool base loop-b :https t :version-pref :auto)
      (ok (null conn))
      (ok (null kind)))
    (ok (not (http-backend-async::async-h2-conn-alive-p h2)))
    (ok (null (pool-acquire pool h2-key)))
    ;; Stray h2 object on the plain key is not adopted as HTTP/1.1.
    (let ((stray (%make-h2-stub loop-a)))
      (pool-release pool base stray)
      (multiple-value-bind (conn kind)
          (http-backend-async::acquire-pooled-connection
           pool base loop-a :https nil :version-pref :http/1.1)
        (ok (null conn))
        (ok (null kind))
        (ok (not (typep conn 'async-pooled-h2-connection))))
      (ok (not (http-backend-async::async-h2-conn-alive-p stray)))
      (ok (null (pool-acquire pool base))))
    ;; HTTP/1.1 pooling still returns the h1 entry. Forced :http/2 does not.
    (pool-release pool base h1)
    (multiple-value-bind (conn kind)
        (http-backend-async::acquire-pooled-connection
         pool base loop-a :https t :version-pref :http/2)
      (ok (null conn))
      (ok (null kind)))
    (ok (eq h1 (pool-acquire pool base)))
    (pool-release pool base h1)
    (multiple-value-bind (conn kind)
        (http-backend-async::acquire-pooled-connection
         pool base loop-a :https nil :version-pref :http/1.1)
      (ok (eq conn h1))
      (ok (eq kind :http/1.1))
      (ok (typep conn 'async-pooled-connection)))
    (pool-clear pool)))

(deftest fixture-pool-reuses-tcp
  "Two GETs with keep-alive fixture → one TCP accept, two HTTP requests."
  (let ((http-protocol:*default-connection-pool* nil))
    (with-http-fixture
        ((lambda (method path headers body)
           (declare (ignore method headers body))
           (values 200
                   '(("content-type" . "text/plain"))
                   (babel:string-to-octets path)))
         :keep-alive t)
      (with-async-test (eb el hb)
        (let* ((pool (make-lru-connection-pool :max-size 4))
               (client (make-http-client hb :pool pool)))
          (let ((r1 (%await-promise
                     (get-async (fixture-url "/a") :client client) eb el)))
            (ok (= 200 (response-status r1)))
            (ok (equalp (babel:string-to-octets "/a") (response-body r1))))
          (let ((r2 (%await-promise
                     (get-async (fixture-url "/b") :client client) eb el)))
            (ok (= 200 (response-status r2)))
            (ok (string= "/b" (babel:octets-to-string (response-body r2)
                                                      :encoding :utf-8))))
          (ok (= 1 *fixture-accept-count*))
          (ok (= 2 *fixture-request-count*))
          (pool-clear pool))))))
