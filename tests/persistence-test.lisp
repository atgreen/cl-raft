;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT

(in-package #:cl-raft/tests)
(in-suite cl-raft)

(test raft-compaction-trims-log-and-preserves-access
  (let ((node (make-standalone-node)))
    (dotimes (i 5) (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 1 :command (list :set "x" i))))
    (setf (cl-raft:raft-node-commit-index node) 5)
    (is (= 5 (cl-raft:raft-last-log-index node)))
    (cl-raft:raft-compact node 3)
    (is (= 3 (cl-raft:raft-node-snapshot-index node)))
    (is (= 1 (cl-raft:raft-node-snapshot-term node)))
    (is (= 5 (cl-raft:raft-last-log-index node)))            ; absolute length unchanged by compaction
    (is (null (cl-raft:raft-log-entry node 2)))              ; compacted away
    (is (null (cl-raft:raft-log-entry node 3)))              ; == snapshot boundary
    (is (= 1 (cl-raft:raft-log-term node 3)))                ; term still answerable at the boundary
    (let ((e4 (cl-raft:raft-log-entry node 4)))
      (is (not (null e4)))                                 ; tail intact
      (is (= 4 (cl-raft:raft-entry-index e4))))))

(test raft-crash-recovery-restores-hard-state-and-log
  (with-raft-temp-dir (path)
    ;; Write a WAL: hard state + three log entries, then "crash" (close).
    (multiple-value-bind (node kv) (make-standalone-node :storage (cl-raft:open-file-raft-storage path))
      (declare (ignore kv))
      (setf (cl-raft:raft-node-current-term node) 7 (cl-raft:raft-node-voted-for node) "peer-b")
      (cl-raft:raft-persist-hard-state node)
      (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 7 :command '(:set "a" 1)))
      (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 7 :command '(:set "b" 2)))
      (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 7 :command '(:set "a" 3)))
      (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node)))
    ;; Recover a fresh node from the same WAL.
    (let ((node2 (make-standalone-node)))
      (cl-raft:recover-raft-node node2 path)
      (is (= 7 (cl-raft:raft-node-current-term node2)))
      (is (equal "peer-b" (cl-raft:raft-node-voted-for node2)))
      (is (= 3 (cl-raft:raft-last-log-index node2)))
      (is (equal '(:set "a" 3) (cl-raft:raft-entry-command (cl-raft:raft-log-entry node2 3))))
      (is (= 7 (cl-raft:raft-log-term node2 2)))
      (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node2)))))

(test raft-recovery-restores-snapshot-state
  (with-raft-temp-dir (path)
    (multiple-value-bind (node kv) (make-standalone-node :storage (cl-raft:open-file-raft-storage path)
                                                         :snapshot/restore t)
      (dotimes (i 5) (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 2 :command (list :set "x" i))))
      (setf (cl-raft:raft-node-commit-index node) 3)
      (cl-raft:raft-apply-committed node)                    ; only the prefix through entry 3 is applied
      (is (= 2 (gethash "x" kv)))
      (cl-raft:raft-compact node 3)                          ; persists snapshot image of kv
      (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node)))
    ;; Recover: snapshot image restores the state machine; the tail survives.
    (multiple-value-bind (node2 kv2) (make-standalone-node :snapshot/restore t)
      (cl-raft:recover-raft-node node2 path)
      (is (= 2 (gethash "x" kv2)) "state machine restored from snapshot")
      (is (= 3 (cl-raft:raft-node-snapshot-index node2)))
      (is (= 5 (cl-raft:raft-last-log-index node2)))         ; entries 4,5 survived compaction
      (is (not (null (cl-raft:raft-log-entry node2 4))))
      (is (= 3 (cl-raft:raft-node-last-applied node2)))      ; applied up to the snapshot boundary
      (setf (cl-raft:raft-node-commit-index node2) 5)
      (cl-raft:raft-apply-committed node2)
      (is (= 4 (gethash "x" kv2)) "tail is applied once after the exact prefix snapshot")
      (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node2)))))

