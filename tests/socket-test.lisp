(in-package #:http-backend-async/tests)

;;; #60: the soft lookups must answer NIL for a package this Lisp lacks
;;; (SB-BSD-SOCKETS on CCL), not signal PACKAGE-ERROR before the next
;;; implementation's fallback is tried.

(defparameter +absent-package+ "HTTP-BACKEND-ASYNC-NO-SUCH-PACKAGE-60")

(deftest test-soft-lookups-absent-package
  (ok (null (find-package +absent-package+)))
  (ok (null (http-backend-async::%soft +absent-package+ "SOCKET-FILE-DESCRIPTOR")))
  (ok (null (http-backend-async::%soft-class +absent-package+ "INTERRUPTED-ERROR")))
  (ok (null (http-backend-async::%soft-setf +absent-package+ "NON-BLOCKING-MODE"))))

(deftest test-soft-lookups-present-package
  (ok (eq 'car (http-backend-async::%soft :cl "CAR")))
  (ok (eq 'error (http-backend-async::%soft-class :cl "ERROR")))
  (ok (equal '(setf car) (http-backend-async::%soft-setf :cl "CAR")))
  (ok (null (http-backend-async::%soft :cl "NO-SUCH-SYMBOL-60"))))
