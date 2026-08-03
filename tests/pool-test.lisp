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
