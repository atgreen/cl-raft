;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT
;;;;
;;;; Availability features: election barrier, CheckQuorum, randomized election
;;;; timeouts, leadership transfer, batched WAL syncs, and ReadIndex reads.

(in-package #:cl-raft/tests)
(in-suite cl-raft)

(defun rsim-leader (sim)
  (let ((leaders (rsim-leaders sim)))
    (is (= 1 (length leaders)) "exactly one leader, got ~S" leaders)
    (first leaders)))

;;; Election barrier ---------------------------------------------------------

(test raft-new-leader-commits-a-barrier
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (dolist (id '("a" "b" "c"))
      (let ((node (rsim-node sim id)))
        (is (= 1 (cl-raft:raft-node-commit-index node)) "~A committed the barrier" id)
        (is (equal '(:raft-barrier) (cl-raft:raft-entry-command (cl-raft:raft-log-entry node 1))))))
    (is (null (gethash "anything" (rsim-kv sim "a"))) "barrier never reaches apply-fn")))

(test raft-single-node-commits-its-barrier-at-once
  (let ((node (make-standalone-node)))
    (cl-raft:raft-become-candidate node)
    (is (cl-raft:raft-leader-p node))
    (is (= 1 (cl-raft:raft-node-commit-index node)))
    (is (= 1 (cl-raft:raft-node-last-applied node)))))

;;; CheckQuorum --------------------------------------------------------------

(test raft-isolated-leader-steps-down
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (is (equal "a" (rsim-leader sim)))
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    ;; Within an election timeout (3 ticks for a) plus one period of slack.
    (rsim-run sim 7)
    (is (not (cl-raft:raft-leader-p (rsim-node sim "a"))) "isolated leader stepped down")
    (multiple-value-bind (index ok reason) (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" 1))
      (is (null index)) (is (null ok))
      (is (eq :not-leader reason) "clients on the minority side fail fast"))
    ;; Stepping down did not bump a's term: it cannot disrupt the majority on heal.
    (let ((term (cl-raft:raft-node-current-term (rsim-node sim "a"))))
      (rsim-run sim 20)
      (is (= term (cl-raft:raft-node-current-term (rsim-node sim "a"))) "Pre-Vote keeps a's term"))
    (setf (rsim-groups sim) nil)
    (rsim-run sim 10)
    (is (= 1 (length (rsim-leaders sim))))
    (is (not (member "a" (rsim-leaders sim) :test #'equal)))))

(test raft-healthy-leader-keeps-leadership
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (let ((term (cl-raft:raft-node-current-term (rsim-node sim "a"))))
      (rsim-run sim 100)
      (is (equal '("a") (rsim-leaders sim)))
      (is (= term (cl-raft:raft-node-current-term (rsim-node sim "a")))))))

(test raft-leader-with-one-peer-down-keeps-quorum
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (setf (rsim-groups sim) '(("a" "b") ("c")))
    (rsim-run sim 30)
    (is (cl-raft:raft-leader-p (rsim-node sim "a")) "a and b are still a majority")))

(test raft-check-quorum-can-be-disabled
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (setf (cl-raft:raft-node-check-quorum (rsim-node sim "a")) nil)
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    (rsim-run sim 20)
    (is (cl-raft:raft-leader-p (rsim-node sim "a")) "legacy behavior: keeps the role")))

;;; Randomized election timeouts ---------------------------------------------

(defun default-timeout-node (id &optional (base 10))
  (cl-raft:make-raft-node :id id :peers nil :election-timeout-base base
                          :send-fn (lambda (to msg) (declare (ignore to msg)))))

(defun draw-timeouts (node n)
  (loop repeat n
        collect (progn (cl-raft:raft-reset-election-timer node)
                       (cl-raft:raft-node-election-timeout node))))

(test raft-default-election-timeouts-are-randomized
  (let* ((a (draw-timeouts (default-timeout-node "a") 200))
         (b (draw-timeouts (default-timeout-node "b") 200)))
    (is (every (lambda (v) (<= 10 v 19)) a) "drawn from [base, 2*base)")
    (is (< 5 (length (remove-duplicates a))) "values actually vary")
    (is (not (equal a b)) "independently constructed nodes draw independent sequences"))
  (is (every (lambda (v) (<= 50 v 99)) (draw-timeouts (default-timeout-node "c" 50) 100))
      "ELECTION-TIMEOUT-BASE scales the range"))

(test raft-explicit-timeout-fn-still-wins
  (let ((node (cl-raft:make-raft-node :id "a" :peers nil :timeout-fn (lambda () 7)
                                      :send-fn (lambda (to msg) (declare (ignore to msg))))))
    (is (equal '(7 7 7) (draw-timeouts node 3)))))

(test raft-randomized-cluster-elects-one-leader
  (dotimes (trial 20)
    (let* ((ids '("a" "b" "c" "d" "e")) (queue nil) (nodes nil))
      (dolist (id ids)
        (let ((node (cl-raft:make-raft-node
                     :id id :peers (remove id ids :test #'equal)
                     :send-fn (lambda (to msg) (setf queue (nconc queue (list (cons to msg))))))))
          (cl-raft:raft-reset-election-timer node)
          (push (cons id node) nodes)))
      (dotimes (tick 60)
        (dolist (entry nodes) (cl-raft:raft-tick (cdr entry)))
        (loop while queue
              do (destructuring-bind (to . msg) (pop queue)
                   (cl-raft:raft-handle-message (cdr (assoc to nodes :test #'equal)) msg))))
      (is (= 1 (count-if #'cl-raft:raft-leader-p nodes :key #'cdr)) "trial ~D" trial))))

;;; Leadership transfer -------------------------------------------------------

(test raft-transfer-leadership-to-up-to-date-peer
  (let ((sim (three-node-sim)) (cb nil))
    (rsim-run sim 8)
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "before") (lambda (r) (setf cb r)))
    (rsim-run sim 2)
    (is (equal "before" cb))
    (let ((term (cl-raft:raft-node-current-term (rsim-node sim "a"))))
      ;; The whole handoff happens in message delivery, not timeouts.
      (is (eq t (cl-raft:raft-transfer-leadership (rsim-node sim "a") "c")))
      (rsim-drain sim)
      (is (equal '("c") (rsim-leaders sim)) "c took over without waiting for a timeout")
      (is (= (1+ term) (cl-raft:raft-node-current-term (rsim-node sim "c"))))
      (is (null (cl-raft:raft-node-lead-transferee (rsim-node sim "a")))))
    (rsim-run sim 2)
    (cl-raft:raft-propose (rsim-node sim "c") '(:set "k" "after"))
    (rsim-run sim 2)
    (dolist (id '("a" "b" "c")) (is (equal "after" (gethash "k" (rsim-kv sim id)))))))

(test raft-transfer-catches-up-lagging-target-first
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    ;; c misses three committed writes.
    (setf (rsim-groups sim) '(("a" "b") ("c")))
    (dotimes (i 3) (cl-raft:raft-propose (rsim-node sim "a") (list :set "k" i)))
    (rsim-run sim 1)
    (setf (rsim-groups sim) nil)
    (let ((behind (cl-raft:raft-last-log-index (rsim-node sim "c"))))
      (is (< behind (cl-raft:raft-last-log-index (rsim-node sim "a"))) "c is behind"))
    (cl-raft:raft-transfer-leadership (rsim-node sim "a") "c")
    (rsim-drain sim)
    (rsim-run sim 2)
    (is (equal '("c") (rsim-leaders sim)))
    (is (= 2 (gethash "k" (rsim-kv sim "c"))) "c was caught up before taking over")))

(test raft-transfer-pauses-proposals
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (setf (rsim-groups sim) '(("a" "b") ("c")))   ; keep the transfer from completing
    (cl-raft:raft-transfer-leadership (rsim-node sim "a") "c")
    (multiple-value-bind (index ok reason) (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" 1))
      (declare (ignore ok))
      (is (null index))
      (is (eq :transferring reason)))
    (is (eq :transferring (nth-value 2 (cl-raft:raft-propose-batch (rsim-node sim "a") '((:set "k" 1))))))
    (is (eq :transferring (nth-value 1 (cl-raft:raft-propose-config (rsim-node sim "a") :remove "b"))))))

(test raft-failed-transfer-is-abandoned
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (setf (rsim-groups sim) '(("a" "b") ("c")))   ; target unreachable
    (cl-raft:raft-transfer-leadership (rsim-node sim "a") "c")
    (rsim-run sim 6)
    (is (equal '("a") (rsim-leaders sim)) "a keeps leading with b")
    (is (null (cl-raft:raft-node-lead-transferee (rsim-node sim "a"))) "transfer abandoned")
    (is (numberp (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "resumed"))))
    (rsim-run sim 2)
    (is (equal "resumed" (gethash "k" (rsim-kv sim "b"))))))

(test raft-transfer-rejects-bad-targets
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (is (eq :self (nth-value 1 (cl-raft:raft-transfer-leadership (rsim-node sim "a") "a"))))
    (is (eq :not-voter (nth-value 1 (cl-raft:raft-transfer-leadership (rsim-node sim "a") "zz"))))
    (is (eq :not-leader (nth-value 1 (cl-raft:raft-transfer-leadership (rsim-node sim "b") "c"))))))

;;; Batched WAL syncs ---------------------------------------------------------

(defclass sync-counting-raft-storage (cl-raft:file-raft-storage)
  ((syncs :initform 0 :accessor storage-syncs)))

(defmethod cl-raft::file-raft-append-sync ((storage sync-counting-raft-storage) fd)
  (declare (ignore fd))
  (incf (storage-syncs storage))
  (call-next-method))

(test raft-batched-append-syncs-once-and-recovers
  (with-raft-temp-dir (path)
    (let ((storage (cl-raft:open-file-raft-storage path)))
      (change-class storage 'sync-counting-raft-storage)
      (let ((node (make-standalone-node :storage storage)))
        (cl-raft:raft-log-append-entries
         node (loop for i below 50 collect (cl-raft:make-raft-entry :term 3 :command (list :set "x" i))))
        (is (= 1 (storage-syncs storage)) "fifty entries, one fsync")
        (is (= 50 (cl-raft:raft-last-log-index node)))
        (cl-raft:close-file-raft-storage storage)))
    (let ((node (make-standalone-node)))
      (cl-raft:recover-raft-node node path)
      (is (= 50 (cl-raft:raft-last-log-index node)))
      (is (equal '(:set "x" 49) (cl-raft:raft-entry-command (cl-raft:raft-log-entry node 50))))
      (is (= 3 (cl-raft:raft-log-term node 1)))
      (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node)))))

(test raft-follower-persists-replicated-batch-with-one-sync
  (with-raft-temp-dir (path)
    (let* ((storage (cl-raft:open-file-raft-storage path))
           (node (make-standalone-node :storage storage)))
      (change-class storage 'sync-counting-raft-storage)
      (cl-raft:raft-handle-message
       node (list :type :append-entries :term 1 :from "leader" :prev-log-index 0 :prev-log-term 0
                  :leader-commit 0
                  :entries (loop for i from 1 to 20
                                 collect (cl-raft:make-raft-entry :term 1 :index i
                                                                  :command (list :set "y" i)))))
      (is (= 20 (cl-raft:raft-last-log-index node)))
      ;; One sync for the batch, plus the hard-state write adopting term 1.
      (is (= 2 (storage-syncs storage)) "batch shares one sync, got ~D" (storage-syncs storage))
      ;; A conflicting suffix is truncated once and the replacement batch synced once.
      (setf (storage-syncs storage) 0)
      (cl-raft:raft-handle-message
       node (list :type :append-entries :term 2 :from "leader2" :prev-log-index 10 :prev-log-term 1
                  :leader-commit 0
                  :entries (loop for i from 11 to 15
                                 collect (cl-raft:make-raft-entry :term 2 :index i
                                                                  :command (list :set "y" (- i))))))
      (is (= 15 (cl-raft:raft-last-log-index node)))
      (is (= 2 (cl-raft:raft-log-term node 15)))
      (is (= 3 (storage-syncs storage)) "hard state, truncate, one batch; got ~D" (storage-syncs storage))
      (cl-raft:close-file-raft-storage storage))))

(test raft-propose-batch-commits-all-with-callbacks
  (let ((sim (three-node-sim)) (results nil))
    (rsim-run sim 8)
    (multiple-value-bind (first last)
        (cl-raft:raft-propose-batch (rsim-node sim "a") '((:set "p" 1) (:set "q" 2) (:set "p" 3))
                                    (list (lambda (r) (push (list :p1 r) results))
                                          nil
                                          (lambda (r) (push (list :p3 r) results))))
      (is (= 2 first)) (is (= 4 last)))
    (rsim-run sim 2)
    (is (equal '((:p1 1) (:p3 3)) (reverse results)))
    (dolist (id '("a" "b" "c"))
      (is (= 3 (gethash "p" (rsim-kv sim id))))
      (is (= 2 (gethash "q" (rsim-kv sim id)))))
    (is (eq :not-leader (nth-value 2 (cl-raft:raft-propose-batch (rsim-node sim "b") '((:set "z" 0))))))))

;;; ReadIndex -----------------------------------------------------------------

(test raft-read-index-sees-prior-writes
  (let ((sim (three-node-sim)) (seen :never-called))
    (rsim-run sim 8)
    (let* ((acked nil)
           (write-index (cl-raft:raft-propose (rsim-node sim "a") '(:set "r" "v1")
                                              (lambda (r) (setf acked r)))))
      ;; Linearizability covers writes acknowledged before the read begins.
      (rsim-drain sim)
      (is (equal "v1" acked))
      (is (eq t (cl-raft:raft-read-index
                 (rsim-node sim "a")
                 (lambda (index)
                   (setf seen (list index (gethash "r" (rsim-kv sim "a"))))))))
      (rsim-drain sim)
      (is (consp seen))
      (is (>= (first seen) write-index) "read index covers the acknowledged write")
      (is (equal "v1" (second seen)) "state machine reflects it when the callback runs"))))

(test raft-read-index-single-node-is-immediate
  (let ((node (make-standalone-node)) (seen nil))
    (cl-raft:raft-become-candidate node)
    (cl-raft:raft-propose node '(:set "s" 9))
    (is (eq t (cl-raft:raft-read-index node (lambda (index) (setf seen index)))))
    (is (= 2 seen))))

(test raft-read-index-refused-off-leader
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (is (eq :not-leader (nth-value 1 (cl-raft:raft-read-index (rsim-node sim "b") #'identity))))))

(test raft-read-index-not-served-by-deposed-leader
  ;; With CheckQuorum off, a partitioned leader keeps its role. Its reads must
  ;; still never be served: no quorum confirms its leadership, and when it learns
  ;; of the new term the read fails rather than returning stale data.
  (let ((sim (three-node-sim)) (seen :never-called))
    (rsim-run sim 8)
    (setf (cl-raft:raft-node-check-quorum (rsim-node sim "a")) nil)
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    (is (eq t (cl-raft:raft-read-index (rsim-node sim "a") (lambda (index) (setf seen index)))))
    (rsim-run sim 15)
    (is (cl-raft:raft-leader-p (rsim-node sim "a")) "stale leader still thinks it leads")
    (is (eq :never-called seen) "but cannot confirm the read")
    (let ((leader (first (remove "a" (rsim-leaders sim) :test #'equal))))
      (cl-raft:raft-propose (rsim-node sim leader) '(:set "r" "new")))
    (rsim-run sim 2)
    (setf (rsim-groups sim) nil)
    (rsim-run sim 4)
    (is (null seen) "read fails once a discovers the newer term")))

(test raft-read-index-fails-on-check-quorum-step-down
  (let ((sim (three-node-sim)) (seen :never-called))
    (rsim-run sim 8)
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    (cl-raft:raft-read-index (rsim-node sim "a") (lambda (index) (setf seen index)))
    (rsim-run sim 7)
    (is (null seen) "pending read fails when the isolated leader steps down")))
