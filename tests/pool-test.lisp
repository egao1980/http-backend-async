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
            (ok (= 200 (response-status r1))))
          (let ((r2 (%await-promise
                     (get-async (fixture-url "/b") :client client) eb el)))
            (ok (= 200 (response-status r2)))
            (ok (string= "/b" (babel:octets-to-string (response-body r2)
                                                      :encoding :utf-8))))
          (ok (= 1 *fixture-accept-count*))
          (ok (= 2 *fixture-request-count*))
          (pool-clear pool))))))
