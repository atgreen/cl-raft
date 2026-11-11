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
      (is (= 2 (cl-raft:raft-node-commit-index (rsim-node sim id)))))))

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
    (let ((stale-index (cl-raft:raft-propose (rsim-node sim "a") '(:set "k" "vX")
                                             (lambda (r) (setf stale-cb r)))))
      (rsim-run sim 10)
      (let* ((leader (first (remove "a" (rsim-leaders sim) :test #'equal)))
             (fresh-index (cl-raft:raft-propose (rsim-node sim leader) '(:set "k" "v2")
                                                (lambda (r) (setf fresh-cb r)))))
        (is (= stale-index fresh-index) "replacement entry occupies the stale index")))
    (rsim-run sim 8)
    (setf (rsim-groups sim) nil)
    (rsim-run sim 12)
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
    ;; A new leader commits prior-term entries only via an entry of its own term
    ;; (Raft §5.4.2), so it proposes a barrier.
    (let ((leader (first (remove "a" (rsim-leaders sim) :test #'equal))))
      (cl-raft:raft-propose (rsim-node sim leader) '(:raft-barrier)))
    (rsim-run sim 4)
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

