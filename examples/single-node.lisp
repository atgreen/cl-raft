;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT

(require :asdf)

(let ((root (uiop:pathname-parent-directory-pathname
             (uiop:pathname-directory-pathname *load-pathname*))))
  (pushnew root asdf:*central-registry* :test #'equal))

(asdf:load-system :cl-raft)

(let* ((state (make-hash-table :test #'equal))
       (node (cl-raft:make-raft-node
              :id "node-a"
              :peers nil
              :heartbeat-timeout 1
              :timeout-fn (lambda () 3)
              :send-fn (lambda (peer message)
                         (declare (ignore peer message)))
              :apply-fn (lambda (command)
                          (setf (gethash (second command) state)
                                (third command))))))
  (cl-raft:raft-reset-election-timer node)
  (loop repeat 4 do (cl-raft:raft-tick node))
  (assert (cl-raft:raft-leader-p node))
  (cl-raft:raft-propose node '(:set "message" "hello from Raft"))
  (assert (string= "hello from Raft" (gethash "message" state)))
  (format t "~A~%" (gethash "message" state)))
