;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT

(in-package #:cl-raft/tests)
(in-suite cl-raft)

;;; A versioned key/value state machine with compare-and-swap. Commands:
;;;   (:set key val)              -> val
;;;   (:cas key expected new)     -> :ok | :conflict   (expected NIL means "absent")
(defun raft-kv-apply (kv command)
  (ecase (first command)
    (:set (setf (gethash (second command) kv) (third command)))
    (:cas (if (equal (gethash (second command) kv) (third command))
              (progn (setf (gethash (second command) kv) (fourth command)) :ok)
              :conflict))))

(defstruct (rsim (:constructor %make-rsim)) nodes kvs (queue '()) (groups nil))

(defun rsim-node (sim id) (cdr (assoc id (rsim-nodes sim) :test #'equal)))
(defun rsim-kv (sim id) (cdr (assoc id (rsim-kvs sim) :test #'equal)))

(defun rsim-deliverable (sim from to)
  (let ((groups (rsim-groups sim)))
    (or (null groups)
        (loop for g in groups
                thereis (and (member from g :test #'equal) (member to g :test #'equal))))))

(defun rsim-drain (sim &optional (max 200000))
  (loop for steps from 0 below max
        while (rsim-queue sim)
        do (destructuring-bind (to . msg) (pop (rsim-queue sim))
             (when (rsim-deliverable sim (getf msg :from) to)
               (cl-raft:raft-handle-message (rsim-node sim to) msg)))))

(defun rsim-tick (sim)
  (dolist (entry (rsim-nodes sim)) (cl-raft:raft-tick (cdr entry)))
  (rsim-drain sim))

(defun rsim-run (sim ticks) (dotimes (i ticks) (rsim-tick sim)))

(defun rsim-tick-one-at-a-time (sim stop-node)
  "One tick, delivering messages singly and stopping the moment STOP-NODE leads,
   so a test can act while a witness holds its brief leadership."
  (dolist (entry (rsim-nodes sim)) (cl-raft:raft-tick (cdr entry)))
  (loop while (and (rsim-queue sim) (not (cl-raft:raft-leader-p stop-node)))
        do (destructuring-bind (to . msg) (pop (rsim-queue sim))
             (when (rsim-deliverable sim (getf msg :from) to)
               (cl-raft:raft-handle-message (rsim-node sim to) msg)))))

(defun make-rsim (ids timeouts
                  &key (state-factory (lambda () (make-hash-table :test 'equal)))
                       (apply-maker (lambda (state) (lambda (command) (raft-kv-apply state command)))))
  "IDS is a list of member ids; TIMEOUTS an alist id->election-timeout (distinct,
   to make elections deterministic). Each node gets its own state machine built
   from STATE-FACTORY + APPLY-MAKER (default: a KV hash)."
  (let ((sim (%make-rsim)))
    (dolist (id ids)
      (let* ((state (funcall state-factory))
             (to (cdr (assoc id timeouts :test #'equal)))
             (node (cl-raft:make-raft-node
                    :id id :peers (remove id ids :test #'equal)
                    :heartbeat-timeout 1
                    :timeout-fn (let ((v to)) (lambda () v))
                    :send-fn (lambda (dest message)
                               (setf (rsim-queue sim)
                                     (nconc (rsim-queue sim) (list (cons dest message)))))
                    :apply-fn (funcall apply-maker state))))
        (cl-raft:raft-reset-election-timer node)
        (push (cons id node) (rsim-nodes sim))
        (push (cons id state) (rsim-kvs sim))))
    sim))

(defun rsim-leaders (sim)
  (loop for (id . node) in (rsim-nodes sim)
        when (cl-raft:raft-leader-p node) collect id))

(defun three-node-sim ()
  (make-rsim '("a" "b" "c") '(("a" . 3) ("b" . 5) ("c" . 7))))

(test raft-elects-a-single-leader
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (is (equal '("a") (rsim-leaders sim)))                       ; lowest timeout wins, exactly one
    (let ((terms (loop for (id . n) in (rsim-nodes sim) collect (cl-raft:raft-node-current-term n))))
      (is (apply #'= terms))                                     ; all on the same term
      (is (plusp (first terms))))
    (dolist (id '("b" "c"))
      (is (equal "a" (cl-raft:raft-node-leader-id (rsim-node sim id)))))))

(test raft-replicates-and-applies
  (let ((sim (three-node-sim)))
    (rsim-run sim 8)
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "x" 1))
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "x" 2))
    (rsim-run sim 6)
    (dolist (id '("a" "b" "c"))
      (is (= 2 (gethash "x" (rsim-kv sim id))) "node ~A has x=2" id)
      ;; Index 1 is the leader's election barrier, then the two writes.
      (is (= 3 (cl-raft:raft-node-commit-index (rsim-node sim id)))))))

(test raft-linearizable-compare-and-swap
  (let ((sim (three-node-sim)) (r1 nil) (r2 nil))
    (rsim-run sim 8)
    (cl-raft:raft-propose (rsim-node sim "a") '(:cas "y" nil "A") (lambda (r) (setf r1 r)))
    (cl-raft:raft-propose (rsim-node sim "a") '(:cas "y" nil "B") (lambda (r) (setf r2 r)))
    (rsim-run sim 6)
    ;; Exactly one CAS wins; the serialized order makes it deterministic.
    (is (eq :ok r1))
    (is (eq :conflict r2))
    (dolist (id '("a" "b" "c"))
      (is (equal "A" (gethash "y" (rsim-kv sim id))) "node ~A converged to y=A" id))))

(test raft-leader-failover-preserves-committed-and-drops-uncommitted
  (let ((sim (three-node-sim)) (minority-cb nil))
    (rsim-run sim 8)
    (is (equal '("a") (rsim-leaders sim)))
    ;; Commit an entry under leader a, replicated everywhere.
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "v1"))
    (rsim-run sim 6)
    (dolist (id '("a" "b" "c")) (is (equal "v1" (gethash "k" (rsim-kv sim id)))))
    ;; Partition a away from the majority {b,c}.
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    ;; a is now a minority leader: a proposal here must NOT commit.
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "vX") (lambda (r) (setf minority-cb r)))
    (rsim-run sim 10)
    (is (null minority-cb) "minority leader must not commit")
    ;; The majority side elects a new leader and makes progress.
    (let ((new-leaders (remove "a" (rsim-leaders sim) :test #'equal)))
      (is (= 1 (length new-leaders)))
      (let ((b (rsim-node sim (first new-leaders))))
        (cl-raft:raft-propose b '(:set "k" "v2"))
        (rsim-run sim 8)
        ;; Majority side committed v2.
        (dolist (id (second (rsim-groups sim)))
          (is (equal "v2" (gethash "k" (rsim-kv sim id)))))))
    ;; Heal: a learns of the higher term, steps down, and converges; its
    ;; uncommitted vX is discarded.
    (setf (rsim-groups sim) nil)
    (rsim-run sim 12)
    (is (= 1 (length (rsim-leaders sim))) "exactly one leader after heal")
    (dolist (id '("a" "b" "c"))
      (is (equal "v2" (gethash "k" (rsim-kv sim id))) "node ~A converged to k=v2" id))
    (let ((terms (loop for (id . n) in (rsim-nodes sim) collect (cl-raft:raft-node-current-term n))))
      (is (apply #'= terms) "all members on one term after heal"))
    (is (null minority-cb) "discarded proposal's callback never resolves")))

(test raft-deposed-leader-callback-not-resolved-by-replacement-entry
  ;; A deposed leader's uncommitted entry is overwritten at the SAME index by the
  ;; new leader's entry. Applying that replacement must not hand its result to
  ;; the client that proposed the discarded entry.
  (let ((sim (three-node-sim)) (stale-cb :never-called) (fresh-cb :never-called))
    (rsim-run sim 8)
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "v1"))
    (rsim-run sim 6)
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    (let ((stale-term (cl-raft:raft-node-current-term (rsim-node sim "a")))
          (stale-index (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "vX")
                                             (lambda (r) (setf stale-cb r)))))
      (rsim-run sim 10)
      (let ((leader (first (remove "a" (rsim-leaders sim) :test #'equal))))
        (cl-raft:raft-propose (rsim-node sim leader) '(:set "k" "v2")
                              (lambda (r) (setf fresh-cb r))))
      (rsim-run sim 8)
      (setf (rsim-groups sim) nil)
      (rsim-run sim 12)
      ;; A later-term entry (the new leader's election barrier) now occupies it.
      (is (< stale-term (cl-raft:raft-entry-term
                         (cl-raft:raft-log-entry (rsim-node sim "a") stale-index)))
          "a later-term entry replaced the stale proposal"))
    (is (equal "v2" (gethash "k" (rsim-kv sim "a"))) "old leader applied the replacement")
    (is (equal "v2" fresh-cb) "new leader's client receives its own result")
    (is (eq :never-called stale-cb) "discarded proposal's callback is not resolved")))

(test raft-deposed-leader-callback-resolves-if-its-entry-survives
  ;; Leadership loss alone must not drop a callback: if the deposed leader's
  ;; entry reached a majority, the next leader commits it, and the original
  ;; client still learns its result.
  (let ((sim (three-node-sim)) (cb :never-called))
    (rsim-run sim 8)
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "kept") (lambda (r) (setf cb r)))
    ;; Deliver the append to b and c but drop their replies, so a never learns
    ;; the entry reached a majority; then cut a off.
    (loop while (rsim-queue sim)
          do (destructuring-bind (to . msg) (pop (rsim-queue sim))
               (unless (equal to "a")
                 (cl-raft:raft-handle-message (rsim-node sim to) msg))))
    (is (eq :never-called cb) "old leader has not observed the commit")
    (setf (rsim-groups sim) '(("a") ("b" "c")))
    (rsim-run sim 12)
    ;; No client writes on the new leader: its election barrier alone must commit
    ;; the inherited entry (Raft §5.4.2).
    (setf (rsim-groups sim) nil)
    (rsim-run sim 12)
    (dolist (id '("a" "b" "c")) (is (equal "kept" (gethash "k" (rsim-kv sim id)))))
    (is (equal "kept" cb) "callback resolves when the entry itself commits")))

;;; ---------------------------------------------------------------------------
;;; Durability + compaction (podster-c2d.2).

(defun make-standalone-node (&key kv storage snapshot/restore)
  "A single raft-node with a no-op transport and a KV state machine."
  (let ((kv (or kv (make-hash-table :test 'equal))))
    (values
     (cl-raft:make-raft-node
      :id "n1" :peers nil
      :send-fn (lambda (to msg) (declare (ignore to msg)))
      :apply-fn (lambda (command) (raft-kv-apply kv command))
      :storage storage
      :snapshot-fn (when snapshot/restore
                     (lambda () (loop for k being the hash-keys of kv using (hash-value v)
                                      collect (cons k v))))
      :restore-fn (when snapshot/restore
                    (lambda (state) (clrhash kv) (loop for (k . v) in state do (setf (gethash k kv) v)))))
     kv)))

(defmacro with-raft-temp-dir ((path) &body body)
  `(let ((,path (namestring (merge-pathnames (format nil "cl-raft-~36R/wal" (random (expt 36 10)))
                                             (uiop:temporary-directory)))))
     (unwind-protect (progn ,@body)
       (ignore-errors (uiop:delete-directory-tree
                       (uiop:pathname-directory-pathname ,path) :validate t :if-does-not-exist :ignore)))))


(test a-follower-weighs-replicated-entries-by-their-own-estimator
  ;; An application's proposal estimate may reserve for a waiting client: gr8s
  ;; charges every proposal two memory pages. Charged that for each replicated
  ;; entry, a 32 MB budget held 450 entries, and a recovering follower -- which
  ;; cannot compact until it has caught up -- never caught up with a leader that
  ;; committed more than that while a snapshot was sent and installed.
  (flet ((first-refused (node count)
           (cl-raft::raft-replication-capacity-index
            node 0 (loop for index from 1 to count
                         collect (cl-raft:make-raft-entry :index index :term 1 :command (list :put "k" index))))))
    (let ((node (cl-raft:make-raft-node :id "follower")))
      (setf (cl-raft::raft-node-retained-byte-limit node) (* 1024 1024)
            (cl-raft::raft-node-uncommitted-weight-fn node)
            (lambda (command) (declare (ignore command)) 65536))
      (is (null (first-refused node 10)))
      (is (eql 1 (first-refused node 100)) "at a proposal's price the budget holds fifteen entries")
      (setf (cl-raft::raft-node-retained-weight-fn node)
            (lambda (command) (declare (ignore command)) 100))
      (is (null (first-refused node 100)))
      (is (null (first-refused node 4000)))
      (is (eql 1 (first-refused node 5000)) "the budget still binds, at the entry's own size"))))

;;; ---------------------------------------------------------------------------
;;; Two voters and a witness: the witness stands in when it is what keeps the
;;; surviving voter from winning.

(defun witness-sim ()
  "Voters a and b with witness w. a has the shortest timeout and leads first."
  (let ((sim (make-rsim '("a" "b" "w") '(("a" . 3) ("b" . 5) ("w" . 7)))))
    (dolist (id '("a" "b" "w"))
      (cl-raft:raft-configure (rsim-node sim id) '(("a" . :voter) ("b" . :voter) ("w" . :witness))))
    (rsim-run sim 8)
    sim))

(defun witness-sim-with-b-behind ()
  "The witness-sim after a has committed x=1 with w alone: b never received it."
  (let ((sim (witness-sim)) (acknowledged nil))
    (setf (rsim-groups sim) '(("a" "w") ("b")))
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "x" 1) (lambda (r) (setf acknowledged r)))
    (rsim-run sim 2)
    (values sim acknowledged)))

(test a-witness-ahead-of-the-surviving-voter-hands-it-the-log
  ;; The leader and the witness are a majority, so a write they alone hold is
  ;; committed and acknowledged. When that leader fails, the other voter is behind
  ;; the witness; the witness must refuse it, and used to leave the cluster with
  ;; no leader until the failed one came back.
  (multiple-value-bind (sim acknowledged) (witness-sim-with-b-behind)
    (is (eql 1 acknowledged) "the client was told its write is saved")
    (is (null (gethash "x" (rsim-kv sim "b"))) "b does not have it")
    (is (> (cl-raft::raft-last-log-index (rsim-node sim "w"))
           (cl-raft::raft-last-log-index (rsim-node sim "b"))))
    (setf (rsim-groups sim) '(("b" "w") ("a")))        ; the leader fails
    (rsim-run sim 30)
    (is (cl-raft:raft-leader-p (rsim-node sim "b")) "the surviving voter leads")
    (is (not (cl-raft:raft-leader-p (rsim-node sim "w"))))
    (is (eql 1 (gethash "x" (rsim-kv sim "b"))) "and holds the acknowledged write")
    (let ((saved nil))
      (cl-raft:raft-propose (rsim-node sim "b") '(:set "y" 2) (lambda (r) (setf saved r)))
      (rsim-run sim 4)
      (is (eql 2 saved) "and accepts new writes"))
    (setf (rsim-groups sim) nil)                       ; the old leader returns
    (rsim-run sim 10)
    (is (equal '("b") (rsim-leaders sim)))
    (is (eql 2 (gethash "y" (rsim-kv sim "a"))))))

(test a-witness-level-with-the-surviving-voter-just-votes
  (let ((sim (witness-sim)))
    (cl-raft:raft-propose (rsim-node sim "a") '(:set "x" 1))
    (rsim-run sim 4)
    (let ((witness-term nil))
      (setf (rsim-groups sim) '(("b" "w") ("a")))
      ;; b times out, asks, and wins on the witness's vote: the witness never leads.
      (dotimes (i 30)
        (rsim-tick sim)
        (when (cl-raft:raft-leader-p (rsim-node sim "w")) (setf witness-term t)))
      (is (null witness-term))
      (is (cl-raft:raft-leader-p (rsim-node sim "b"))))))

(test a-witness-does-not-stand-in-while-it-hears-a-leader
  ;; b is cut off from the leader and behind; the witness still follows a.
  (multiple-value-bind (sim acknowledged) (witness-sim-with-b-behind)
    (declare (ignore acknowledged))
    (let ((term (cl-raft:raft-node-current-term (rsim-node sim "a"))))
      (setf (rsim-groups sim) '(("a" "w") ("b" "w")))
      (rsim-run sim 40)
      (is (equal '("a") (rsim-leaders sim)))
      (is (= term (cl-raft:raft-node-current-term (rsim-node sim "a"))) "no election disturbed it"))))

(test a-witness-stands-in-only-for-a-voter-it-can-serve
  (multiple-value-bind (sim acknowledged) (witness-sim-with-b-behind)
    (declare (ignore acknowledged))
    (let* ((w (rsim-node sim "w"))
           (ask (lambda (&rest overrides)
                  (cl-raft::raft-witness-should-stand-in-p
                   w (append overrides
                             (list :type :pre-vote :from "b"
                                   :term (1+ (cl-raft:raft-node-current-term w))
                                   :last-log-index (cl-raft::raft-last-log-index (rsim-node sim "b"))
                                   :last-log-term (cl-raft::raft-last-log-term (rsim-node sim "b"))))))))
      (is (not (funcall ask)) "it still hears the leader")
      (setf (cl-raft::raft-node-leader-contact-elapsed w) 1000)
      (is (funcall ask))
      (is (not (funcall ask :term (cl-raft:raft-node-current-term w))) "a stale round")
      (is (not (funcall ask :from "stranger")))
      (is (not (funcall ask :last-log-index (1- (cl-raft::raft-node-snapshot-index w))))
          "the voter needs entries the witness no longer has")
      (setf (cl-raft::raft-node-snapshot-index w) (cl-raft::raft-last-log-index w))
      (is (not (funcall ask)) "after discarding them it cannot help")
      (is (not (cl-raft::raft-witness-should-stand-in-p
                (rsim-node sim "b") (list :type :pre-vote :from "a" :term 99 :last-log-index 0 :last-log-term 0)))
          "a voter never stands in"))))

(test a-witness-that-leads-takes-no-proposal-and-gives-up-within-an-election-timeout
  (multiple-value-bind (sim acknowledged) (witness-sim-with-b-behind)
    (declare (ignore acknowledged))
    (let ((w (rsim-node sim "w")) (led 0))
      (setf (rsim-groups sim) '(("b" "w") ("a")))
      ;; Run until the witness wins, then cut it off from the voter it would serve.
      (loop repeat 30 until (cl-raft:raft-leader-p w) do (rsim-tick-one-at-a-time sim w))
      (is (cl-raft:raft-leader-p w))
      (setf (rsim-groups sim) '(("a") ("b") ("w")))
      (is (equal '(nil nil :transferring)
                 (multiple-value-list (cl-raft:raft-propose w '(:set "z" 9)))))
      (is (equal '(nil :not-leader) (multiple-value-list (cl-raft:raft-read-index w #'identity))))
      (is (equal '(nil :transferring)
                 (multiple-value-list (cl-raft:raft-propose-config w :add "c" :role :learner))))
      (is (zerop (hash-table-count (cl-raft::raft-node-pending-members w)))
          "and admits no member")
      (loop while (cl-raft:raft-leader-p w) repeat 20 do (rsim-tick sim) (incf led))
      (is (not (cl-raft:raft-leader-p w)))
      (is (<= led 7) "its election timeout")
      (is (null (gethash "z" (rsim-kv sim "b")))))))

(test a-witness-brings-a-voter-far-behind-level-within-its-one-election-timeout
  ;; The witness leads for one election timeout (seven ticks here). Backing up one
  ;; entry per round would not cover forty entries; the voter says where its log ends.
  (let ((sim (witness-sim)))
    (setf (rsim-groups sim) '(("a" "w") ("b")))
    (dotimes (i 40)
      (cl-raft:raft-propose (rsim-node sim "a") (list :set "x" i)))
    (rsim-run sim 2)
    (is (>= (- (cl-raft::raft-last-log-index (rsim-node sim "w"))
               (cl-raft::raft-last-log-index (rsim-node sim "b")))
            40))
    (setf (rsim-groups sim) '(("b" "w") ("a")))
    (rsim-run sim 30)
    (is (cl-raft:raft-leader-p (rsim-node sim "b")))
    (is (eql 39 (gethash "x" (rsim-kv sim "b"))))))
