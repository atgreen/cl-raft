;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT
;;;;
;;;; Purpose:
;;;; A pure Common Lisp Raft consensus core.
;;;; (podster-c2d.1). It implements leader election, log replication, and
;;;; commit/apply per the Raft paper (§5), with the network transport and the
;;;; logical clock injected -- so the identical core runs under a deterministic
;;;; in-process simulator (tests: partitions, leader kills) and, later, under an
;;;; ag-gRPC transport adapter. No etcd, no external consensus.
;;;;
;;;; Indexing: the log is 1-based; index 0 is a sentinel with term 0. The
;;;; replicated state machine is supplied as APPLY-FN, invoked exactly once per
;;;; committed entry in log order; its result is handed back to the client that
;;;; proposed that entry (enabling linearizable compare-and-swap).

(in-package #:cl-raft)

(defstruct raft-entry
  (term 0 :type fixnum)
  (index 0 :type fixnum)
  command)

(defstruct raft-node
  id                                    ; this member's id
  peers                                 ; list of the other members' ids
  (role :follower)                      ; :follower | :candidate | :leader
  (current-term 0 :type fixnum)
  (voted-for nil)
  (log (make-array 0 :adjustable t :fill-pointer 0))  ; vector of RAFT-ENTRY after snapshot-index
  (commit-index 0 :type fixnum)
  (last-applied 0 :type fixnum)
  ;; Optional disk-only committed-prefix cache; never part of recovered state.
  (catchup-byte-limit nil :type (or null (integer 1 *)))
  ;; Bound retry encoding and per-peer outbox payload while a follower is behind.
  ;; A single legacy large command may exceed the budget and is sent alone.
  (append-entry-limit 64 :type (integer 1 *))
  (append-byte-budget (* 1024 1024) :type (integer 1 *))
  ;; Application coordinators opt in; generic/legacy Raft callers keep their
  ;; existing command-size contract. Protocol/configuration traffic can recover
  ;; quorum even when the application budget is full.
  (uncommitted-byte-limit nil :type (or null (integer 1 *)))
  ;; Optional coordinator backpressure while checkpoints lag committed writes.
  (retained-byte-limit nil :type (or null (integer 1 *)))
  (uncommitted-weight-fn #'raft-command-wire-weight :type function)
  ;; compaction: entries <= snapshot-index are discarded; snapshot-term is the
  ;; term of the last entry the snapshot includes. log[0] is index snapshot-index+1.
  (snapshot-index 0 :type fixnum)
  (snapshot-term 0 :type fixnum)
  snapshot-state                       ; immutable envelope at snapshot-index
  snapshot-pending                     ; durable install awaiting application restore
  leader-id
  ;; membership (podster-c2d.6). ROLES maps member-id -> :voter | :witness |
  ;; :learner; PEERS stays the quorum+replication set (voters+witnesses, so the
  ;; proven quorum/commit paths are unchanged for an all-voter cluster).
  ;; PENDING-MEMBERS are being added: replicated to, but not counted for quorum
  ;; until the config entry commits. A witness votes and counts for quorum but
  ;; never starts an election; a learner neither votes-weight nor leads.
  (roles (make-hash-table :test 'equal))
  ;; Committed intent for learners that requested eventual voter membership.
  (promotion-intents (make-hash-table :test 'equal))
  (self-role :voter)
  (pending-members (make-hash-table :test 'equal))   ; id -> role
  ;; DEPARTING-MEMBERS are peers a committed config just dropped: the leader
  ;; keeps replicating the commit to them (id -> config-entry index) until they
  ;; apply it and learn they are removed, so they stop disrupting the new quorum.
  (departing-members (make-hash-table :test 'equal))
  (removed nil)
  ;; Transport glue (ignored by the deterministic in-process sim, where SEND-FN
  ;; routes by id). PEER-ADDRESSES maps member-id -> (host port), carried in the
  ;; config command so every member learns where to reach a joiner; ON-CONFIGURED
  ;; (if set) runs after a membership change so the transport can reconcile links.
  (peer-addresses (make-hash-table :test 'equal))
  ;; id -> HTTP API base URL, replicated in the config so every member can
  ;; redirect a write to the leader's API (podster-c2d.7.3).
  (api-urls (make-hash-table :test 'equal))
  on-configured
  ;; leader volatile
  (next-index (make-hash-table :test 'equal))
  (match-index (make-hash-table :test 'equal))
  ;; Current-leadership applied receipts: peer -> (term . applied-index).
  ;; Log match alone does not establish that application restore has completed.
  (peer-applied (make-hash-table :test 'equal))
  ;; A capacity-limited receiver must install a checkpoint instead of replaying
  ;; more cached prefix. Startup may keep its local compactor gated until catch-up.
  (peer-snapshot-required (make-hash-table :test 'equal))
  ;; candidate volatile
  (votes (make-hash-table :test 'equal))
  ;; pre-candidate volatile (Pre-Vote, Raft dissertation §9.6): a node that times
  ;; out first asks whether it COULD win, at a hypothetical next term, WITHOUT
  ;; incrementing its own term. Only a quorum of grants promotes it to a real
  ;; candidate. This stops a joining or partitioned member from bumping the term
  ;; and deposing a healthy leader (podster-8r7). PRE-VOTE-TERM is the hypothetical
  ;; term of the round in flight, or NIL when not pre-campaigning.
  (pre-votes (make-hash-table :test 'equal))
  pre-vote-term
  ;; logical timers (ticks)
  (election-elapsed 0 :type fixnum)
  (heartbeat-elapsed 0 :type fixnum)
  (election-timeout 10 :type fixnum)
  ;; Ticks since this node last heard a valid AppendEntries from the leader, reset
  ;; only by that heartbeat (NOT by starting a pre-election). Pre-Vote grants are
  ;; keyed on this, not ELECTION-ELAPSED, so a node pre-electing does not reset its
  ;; own leader-liveness view and a follower still hearing the leader refuses to
  ;; grant -- which is what protects the leader. Starts "stale" so a fresh cluster
  ;; (no leader yet) elects promptly (podster-8r7).
  (leader-contact-elapsed 1000000 :type fixnum)
  (heartbeat-timeout 1 :type fixnum)
  ;; injectables
  send-fn                               ; (lambda (to message-plist))
  apply-fn                              ; (lambda (command) -> result)
  storage                               ; a RAFT-STORAGE for durable hard-state/log, or NIL
  snapshot-fn                           ; (lambda () -> opaque state-machine snapshot)
  restore-fn                            ; (lambda (snapshot)) restores the state machine
  (timeout-fn (lambda () 10))           ; -> next election timeout (randomized in prod)
  ;; leader: log index -> (term . client-callback), resolved on apply only when
  ;; the committed entry at that index still carries the proposing term.
  (pending (make-hash-table :test 'eql)))

;;; ---------------------------------------------------------------------------
;;; Durable storage protocol (implemented by file-raft-storage in raft-log.lisp;
;;; NIL = volatile, used by the deterministic simulator). Raft requires
;;; currentTerm, votedFor and the log to survive a crash.

(defgeneric raft-storage-save-hard-state (storage term vote)
  (:method ((storage null) term vote) (declare (ignore term vote)) nil))
(defgeneric raft-storage-append-entry (storage entry)
  (:method ((storage null) entry) (declare (ignore entry)) nil))
(defgeneric raft-storage-truncate (storage index)
  (:method ((storage null) index) (declare (ignore index)) nil))
(defgeneric raft-storage-save-commit-index (storage index)
  (:method ((storage null) index) (declare (ignore index)) nil))
(defgeneric raft-storage-save-snapshot (storage index term state)
  (:method ((storage null) index term state) (declare (ignore index term state)) nil))
(defgeneric raft-storage-cache-prefix (storage node index byte-limit)
  (:method (storage node index byte-limit) (declare (ignore storage node index byte-limit)) nil))
(defgeneric raft-storage-read-catchup (storage node next)
  (:method (storage node next) (declare (ignore storage node next)) nil))
(defgeneric raft-storage-clear-catchup (storage)
  (:method (storage) (declare (ignore storage)) nil))
(defgeneric raft-storage-catchup-usage (storage)
  (:method (storage) (declare (ignore storage)) (values 0 0)))
(defgeneric raft-storage-replace-snapshot (storage index term state hard-term vote entries &key commit-index)
  (:documentation "Persist a snapshot with exact hard state and log tail. ENTRIES
is a borrowed sequence; retain only entries after INDEX. Caller holds the node lock.")
  (:method (storage index term state hard-term vote entries &key (commit-index index))
    (declare (ignore hard-term vote entries))
    (raft-storage-save-snapshot storage index term state)
    (raft-storage-save-commit-index storage commit-index)))

(defun raft-persist-hard-state (node)
  "Persist NODE's current term and vote to durable storage so they survive a
   crash (Raft §5). A no-op when NODE has no storage (the volatile simulator)."
  (raft-storage-save-hard-state (raft-node-storage node)
                                (raft-node-current-term node) (raft-node-voted-for node)))

(defun raft-set-hard-state (node term vote)
  "Persist TERM and VOTE before publishing either field in memory."
  (raft-storage-save-hard-state (raft-node-storage node) term vote)
  (setf (raft-node-current-term node) term
        (raft-node-voted-for node) vote))

;;; ---------------------------------------------------------------------------
(defun raft-set-commit-index (node index)
  "Persist a proven advancing commit boundary before publishing or applying it."
  (when (> index (raft-node-commit-index node))
    (raft-storage-save-commit-index (raft-node-storage node) index)
    (setf (raft-node-commit-index node) index)))

;;; Log helpers (1-based, offset by snapshot-index for compaction).

(defun raft-log-length (node)
  "Absolute index of the last entry (snapshot-index + in-memory tail length)."
  (+ (raft-node-snapshot-index node) (fill-pointer (raft-node-log node))))

(defun raft-log-entry (node index)
  "The RAFT-ENTRY at absolute INDEX, or NIL if compacted/out of range."
  (let ((pos (- index (raft-node-snapshot-index node) 1)))
    (when (and (>= pos 0) (< pos (fill-pointer (raft-node-log node))))
      (aref (raft-node-log node) pos))))

(defun raft-log-term (node index)
  "Term of the entry at absolute INDEX; 0 at index 0; snapshot-term at/below the
   snapshot boundary."
  (cond ((zerop index) 0)
        ((= index (raft-node-snapshot-index node)) (raft-node-snapshot-term node))
        ((< index (raft-node-snapshot-index node)) (raft-node-snapshot-term node))
        (t (let ((e (raft-log-entry node index))) (if e (raft-entry-term e) 0)))))

(defun raft-last-log-index (node)
  "The absolute index of NODE's last log entry (the snapshot boundary when the
   in-memory tail is empty)."
  (raft-log-length node))
(defun raft-last-log-term (node)
  "The term of NODE's last log entry (snapshot-term when the tail is empty, 0 for
   a wholly empty log)."
  (raft-log-term node (raft-log-length node)))

(defun raft-log-truncate (node index)
  "Durably drop uncommitted entries at INDEX and beyond, releasing their payloads."
  (when (<= index (raft-node-commit-index node))
    (error "Cannot truncate committed Raft entries at ~D" index))
  (let* ((log (raft-node-log node))
         (end (fill-pointer log))
         (start (min end (max 0 (- index (raft-node-snapshot-index node) 1)))))
    ;; A storage error must leave the existing log available for retry/recovery.
    (raft-storage-truncate (raft-node-storage node) index)
    (loop for i from start below end do (setf (aref log i) nil))
    (setf (fill-pointer log) start)))

(defun raft-log-append (node entry)
  "Persist ENTRY before publishing it in NODE's log."
  (setf (raft-entry-index entry) (1+ (raft-last-log-index node)))
  ;; Certify the old boundary even if this first configuration never commits.
  ;; Recovery can then distinguish an uncommitted tail from a legacy WAL whose
  ;; committed configuration boundary was never recorded.
  (when (and (consp (raft-entry-command entry)) (eq :raft-config (car (raft-entry-command entry))))
    (raft-storage-save-commit-index (raft-node-storage node) (raft-node-commit-index node)))
  (raft-storage-append-entry (raft-node-storage node) entry)
  (vector-push-extend entry (raft-node-log node))
  entry)

;;; ---------------------------------------------------------------------------
;;; Snapshot / compaction.

(defun raft-snapshot-envelope-p (state)
  (and (consp state) (eq :cl-raft-snapshot (first state)) (eql 1 (second state))))

(defun raft-capture-snapshot (node application boundary)
  "Freeze configuration at the same boundary as APPLICATION, including learners."
  (flet ((alist (table)
           (sort (loop for key being the hash-keys of table using (hash-value value)
                       collect (cons key (copy-tree value))) #'string< :key #'car)))
    (list :cl-raft-snapshot 1 :application application
          :application-present (and (= boundary (raft-node-last-applied node))
                                    (or (not (null (raft-node-snapshot-fn node))) (null (raft-node-apply-fn node))))
          :members (if (plusp (hash-table-count (raft-node-roles node)))
                       (alist (raft-node-roles node)) (raft-current-config node))
          :addresses (alist (raft-node-peer-addresses node))
          :api-urls (alist (raft-node-api-urls node))
          :promotions (alist (raft-node-promotion-intents node)))))

(defun raft-restore-snapshot-contents (node state)
  "Restore application and configuration. Legacy local snapshots contain only application data."
  (when (and (raft-snapshot-envelope-p state) (not (getf (cddr state) :application-present)))
    (error "Snapshot lacks an exact application/configuration boundary; replay its complete log instead"))
  (when (raft-snapshot-envelope-p state)
    (raft-validate-promotion-intents (getf (cddr state) :members) (getf (cddr state) :promotions)))
  (when (raft-node-restore-fn node)
    (funcall (raft-node-restore-fn node)
             (if (raft-snapshot-envelope-p state) (getf (cddr state) :application) state)))
  (when (raft-snapshot-envelope-p state)
    (clrhash (raft-node-peer-addresses node))
    (clrhash (raft-node-api-urls node))
    (raft-adopt-config node (getf (cddr state) :members)
                           (getf (cddr state) :addresses)
                           (getf (cddr state) :api-urls)
                           (getf (cddr state) :promotions))))

(defun raft-publish-snapshot (node index term state entries)
  "Publish an already durable snapshot. Restore callbacks must support retries."
  (raft-restore-snapshot-contents node state)
  (let* ((log (raft-node-log node)) (end (fill-pointer log)) (count 0))
    (map nil (lambda (entry)
               (when (> (raft-entry-index entry) index)
                 (if (< count (length log)) (setf (aref log count) entry)
                     (vector-push-extend entry log))
                 (incf count))) entries)
    (loop for i from count below end do (setf (aref log i) nil))
    (setf (fill-pointer log) count))
  ;; Entries covered by an installed snapshot are never applied here, so their
  ;; results are unknowable locally; release any callbacks waiting on them.
  (loop for pending-index being the hash-keys of (raft-node-pending node)
        when (<= pending-index index) collect pending-index into stale
        finally (dolist (i stale) (remhash i (raft-node-pending node))))
  (setf (raft-node-snapshot-index node) index (raft-node-snapshot-term node) term
        (raft-node-snapshot-state node) state (raft-node-last-applied node) index
        (raft-node-commit-index node) index))

(defun raft-resume-snapshot-install (node)
  (when (raft-node-snapshot-pending node)
    (destructuring-bind (index term state entries) (raft-node-snapshot-pending node)
      (raft-publish-snapshot node index term state entries)
      (setf (raft-node-snapshot-pending node) nil))))

(defstruct (raft-prepared-compaction
             (:constructor %make-raft-prepared-compaction (owner index term state)))
  (owner nil :read-only t)
  (index 0 :type fixnum :read-only t)
  (term 0 :type fixnum :read-only t)
  (state nil :read-only t))

(defun raft-prepare-compaction (node)
  "Freeze an exact applied boundary and configuration without application I/O.
The caller holds the node lock and freezes its application view in the same
critical section. It must retain that view until publication or abandonment.
Returns NIL if no newer boundary is available or an installation is pending."
  (let ((index (raft-node-last-applied node)))
    (when (or (raft-node-snapshot-pending node) (<= index (raft-node-snapshot-index node)))
      (return-from raft-prepare-compaction nil))
    (unless (and (<= index (raft-node-commit-index node))
                 (or (raft-node-snapshot-fn node) (null (raft-node-apply-fn node))))
      (error "Prepared compaction requires an exact committed application boundary"))
    (%make-raft-prepared-compaction node index (raft-log-term node index)
                                    (raft-capture-snapshot node nil index))))

(defun raft-persist-compaction (node upto term state)
  "Persist a checked boundary before changing the live log. Caller owns the node lock."
  (let ((drop (- upto (raft-node-snapshot-index node))))
    (when (raft-node-catchup-byte-limit node)
      (raft-storage-cache-prefix (raft-node-storage node) node upto (raft-node-catchup-byte-limit node)))
    (raft-storage-replace-snapshot
     (raft-node-storage node) upto term state
     (raft-node-current-term node) (raft-node-voted-for node) (raft-node-log node)
     :commit-index (raft-node-commit-index node))
    (let* ((log (raft-node-log node)) (end (fill-pointer log)) (remaining (- end drop)))
      (loop for i from drop below end do (setf (aref log (- i drop)) (aref log i)))
      ;; A fill pointer does not hide array slots from the garbage collector.
      (loop for i from remaining below end do (setf (aref log i) nil))
      (setf (fill-pointer log) remaining))
    (setf (raft-node-snapshot-index node) upto
          (raft-node-snapshot-term node) term
          (raft-node-snapshot-state node) state)))

(defun raft-commit-prepared-compaction (node prepared application)
  "Publish APPLICATION from PREPARED's frozen view, under the node lock.
The caller serializes/retains the application outside the lock. Newer applied
entries and configuration remain live and survive in the durable suffix. Returns
NIL if a newer snapshot or pending installation superseded this preparation."
  (check-type prepared raft-prepared-compaction)
  (unless (eq node (raft-prepared-compaction-owner prepared))
    (error "Prepared compaction belongs to another node"))
  (let ((index (raft-prepared-compaction-index prepared)))
    (when (or (<= index (raft-node-snapshot-index node)) (raft-node-snapshot-pending node))
      (return-from raft-commit-prepared-compaction nil))
    (unless (and (<= index (raft-node-last-applied node) (raft-node-commit-index node))
                 (eql (raft-prepared-compaction-term prepared) (raft-log-term node index)))
      (error "Prepared compaction no longer matches the committed prefix"))
    (let ((state (copy-list (raft-prepared-compaction-state prepared))))
      (setf (getf (cddr state) :application) application)
      (raft-persist-compaction node index (raft-prepared-compaction-term prepared) state)
      t)))

(defun raft-compact (node upto)
  "Persist a snapshot before releasing its committed prefix. When SNAPSHOT-FN
captures current application state, UPTO must equal LAST-APPLIED: that callback
cannot reconstruct an arbitrary older application revision."
  (when (and (> upto (raft-node-snapshot-index node)) (<= upto (raft-node-commit-index node)))
    (when (and (raft-node-snapshot-fn node) (/= upto (raft-node-last-applied node)))
      (error "A Raft application snapshot must cover exactly last-applied ~D, not ~D"
             (raft-node-last-applied node) upto))
    (let* ((term (raft-log-term node upto))
           (state (raft-capture-snapshot
                   node (and (raft-node-snapshot-fn node) (funcall (raft-node-snapshot-fn node))) upto)))
      ;; Snapshot construction or persistence may fail; keep the complete old log
      ;; and boundary until the durable replacement succeeds.
      (raft-persist-compaction node upto term state))))

(defun raft-restore-snapshot (node index term state)
  "Restore a newer snapshot into memory; the caller owns durable installation.
Stale snapshots cannot roll back committed state. Snapshot transport must persist
an atomic replacement, including its log-tail policy, before calling this helper."
  (when (<= index (raft-node-commit-index node))
    (return-from raft-restore-snapshot nil))
  (raft-publish-snapshot node index term state #())
  t)

(defun raft-majority (node)
  "The quorum size: a strict majority of the cluster, floor(N/2)+1 for N = this
   node plus its peers."
  (1+ (floor (1+ (length (raft-node-peers node))) 2)))  ; floor((N+1)/2)+ -> strict majority of N

(defun raft-cluster-size (node)
  "The cluster size: this node plus its peers."
  (1+ (length (raft-node-peers node))))

(defun raft-member-role (node id)
  "ID's role in NODE's cluster (:voter, :witness or :learner), defaulting to
   :voter when unrecorded."
  (gethash id (raft-node-roles node) :voter))

(defun raft-replicate-ids (node)
  "Everyone the leader replicates to: quorum peers, members being added, and
   members being removed (until they apply the config and learn they are out)."
  (remove-duplicates
   (append (raft-node-peers node)
           (loop for id being the hash-keys of (raft-node-roles node) using (hash-value role)
                 when (and (eq role :learner) (not (equal id (raft-node-id node)))) collect id)
           (loop for id being the hash-keys of (raft-node-pending-members node) collect id)
           (loop for id being the hash-keys of (raft-node-departing-members node) collect id))
   :test #'equal))

(defun raft-configure (node members &optional (default-self-role :voter))
  "Set NODE's membership from MEMBERS, an alist (id . role) for the whole cluster
   (including self). PEERS becomes the voters+witnesses except self; self-role and
   the removed flag follow MEMBERS. Not required for an all-voter cluster that
   passed :peers to MAKE-RAFT-NODE -- it is for witnesses/learners/config change."
  (clrhash (raft-node-roles node))
  (clrhash (raft-node-promotion-intents node))
  (clrhash (raft-node-peer-snapshot-required node))
  (dolist (m members) (setf (gethash (car m) (raft-node-roles node)) (cdr m)))
  (let ((self (assoc (raft-node-id node) members :test #'equal)))
    (setf (raft-node-self-role node) (if self (cdr self) default-self-role)
          (raft-node-removed node) (not self)
          (raft-node-peers node)
          (loop for (id . role) in members
                unless (equal id (raft-node-id node))
                  when (member role '(:voter :witness)) collect id))))

(defun raft-current-config (node)
  "The current membership as an alist (id . role), including self."
  (if (plusp (hash-table-count (raft-node-roles node)))
      (loop for id being the hash-keys of (raft-node-roles node) using (hash-value role) collect (cons id role))
      (cons (cons (raft-node-id node) (raft-node-self-role node))
            (loop for id in (raft-node-peers node) collect (cons id (raft-member-role node id))))))

;;; ---------------------------------------------------------------------------
;;; Role transitions.

(defun raft-reset-election-timer (node)
  "Restart NODE's election timer: zero the elapsed count and draw a fresh
   timeout from its timeout-fn (randomized in production)."
  (setf (raft-node-election-elapsed node) 0
        (raft-node-election-timeout node) (funcall (raft-node-timeout-fn node))))

(defun raft-become-follower (node term &optional leader-id)
  "Revert NODE to a follower at TERM (optionally knowing LEADER-ID): clear its
   vote only on a higher term, abandon in-flight membership delivery, persist the
   hard state, and restart the election timer."
  (raft-set-hard-state node term (when (= term (raft-node-current-term node))
                                  (raft-node-voted-for node)))
  (setf (raft-node-role node) :follower
        (raft-node-leader-id node) leader-id
        (raft-node-pre-vote-term node) nil         ; abandon any in-flight pre-election
        ;; Stepping down without a known leader means no recent leader contact, so
        ;; a fresh pre-election may proceed; adopting a leader resets contact below.
        (raft-node-leader-contact-elapsed node) (if leader-id 0 1000000))
  (clrhash (raft-node-pending-members node))   ; abandon any in-flight add on leadership loss
  (clrhash (raft-node-departing-members node)) ; a new leader restarts removal delivery
  (raft-reset-election-timer node))

(defun raft-send (node to message)
  "Hand MESSAGE to NODE's injected transport for delivery to member TO."
  (funcall (raft-node-send-fn node) to message))

(defun raft-broadcast (node message-fn)
  "Send each peer the message (MESSAGE-FN peer) returns, via NODE's transport.
   Peers only -- not members being added or dropped."
  (dolist (peer (raft-node-peers node))
    (raft-send node peer (funcall message-fn peer))))

(defun raft-begin-pre-election (node)
  "Pre-Vote (Raft dissertation §9.6): without touching term, role, or vote, ask
   peers whether this node could win at the next term. Reset the election timer,
   grant self, and -- unless a one-node quorum already suffices -- broadcast a
   :pre-vote for term (1+ current). Only a quorum of grants promotes to a real
   candidate; short of that the term is never bumped, so a member that cannot win
   (it is behind, or a healthy leader still rules) cannot depose the leader
   (podster-8r7)."
  (when (eq (raft-node-self-role node) :voter)
    (setf (raft-node-pre-vote-term node) (1+ (raft-node-current-term node)))
    (clrhash (raft-node-pre-votes node))
    (setf (gethash (raft-node-id node) (raft-node-pre-votes node)) t)
    (raft-reset-election-timer node)
    (if (>= (hash-table-count (raft-node-pre-votes node)) (raft-majority node))
        (raft-become-candidate node)
        (raft-broadcast node
                        (lambda (peer)
                          (declare (ignore peer))
                          (list :type :pre-vote
                                :term (raft-node-pre-vote-term node)
                                :from (raft-node-id node)
                                :last-log-index (raft-last-log-index node)
                                :last-log-term (raft-last-log-term node)))))))

(defun raft-become-candidate (node)
  "Start an election: advance to the next term as a candidate, vote for self,
   persist the hard state, restart the election timer, and request votes from
   every peer. Wins at once in a single-node cluster."
  (raft-set-hard-state node (1+ (raft-node-current-term node)) (raft-node-id node))
  (setf (raft-node-pre-vote-term node) nil)
  (setf (raft-node-role node) :candidate)
  (setf (raft-node-leader-id node) nil)
  (clrhash (raft-node-votes node))
  (setf (gethash (raft-node-id node) (raft-node-votes node)) t)
  (raft-reset-election-timer node)
  (raft-broadcast node
                  (lambda (peer)
                    (declare (ignore peer))
                    (list :type :request-vote
                          :term (raft-node-current-term node)
                          :from (raft-node-id node)
                          :last-log-index (raft-last-log-index node)
                          :last-log-term (raft-last-log-term node))))
  ;; A single-node cluster wins immediately.
  (raft-maybe-become-leader node))

(defun raft-maybe-become-leader (node)
  "Promote NODE to leader when it is still a candidate and has collected a quorum
   of votes; otherwise a no-op."
  (when (and (eq (raft-node-role node) :candidate)
             (>= (hash-table-count (raft-node-votes node)) (raft-majority node)))
    (raft-become-leader node)))

(defun raft-become-leader (node)
  "Assume leadership: reset the per-peer next/match-index for everyone replicated
   to and broadcast an immediate AppendEntries to assert the new term."
  (setf (raft-node-role node) :leader
        (raft-node-leader-id node) (raft-node-id node))
  (clrhash (raft-node-next-index node))
  (clrhash (raft-node-match-index node))
  (clrhash (raft-node-peer-applied node))
  (clrhash (raft-node-peer-snapshot-required node))
  (dolist (peer (raft-replicate-ids node))
    (setf (gethash peer (raft-node-next-index node)) (1+ (raft-last-log-index node))
          (gethash peer (raft-node-match-index node)) 0))
  ;; Replicate immediately to assert leadership.
  (raft-broadcast-append node))

;;; ---------------------------------------------------------------------------
;;; Leader replication.

(defun raft-command-wire-weight (command)
  "Conservative printed-wire weight for ordinary Raft command data, without
materializing its encoded string. Strings commonly dominate encoded commands."
  (typecase command
    (null 3)
    (string (+ 2 (* 6 (length command))))
    (cons (+ 3 (raft-command-wire-weight (car command))
               (raft-command-wire-weight (cdr command))))
    (vector (+ 3 (loop for value across command sum (1+ (raft-command-wire-weight value)))))
    (symbol (+ 32 (length (symbol-name command))))
    (integer (+ 4 (integer-length command)))
    (t 128)))

(defun raft-command-memory-weight (command)
  "Conservative ordinary-command heap weight before a producer's allocator reserve.
Use four bytes per character even if this instance is narrow: decoding a WAL or
RPC can widen the same string. This is distinct from printed-wire expansion."
  (typecase command
    (null 0)
    (string (+ 32 (* 4 (length command))))
    (cons (+ 16 (raft-command-memory-weight (car command))
                (raft-command-memory-weight (cdr command))))
    (vector (+ 32 (* 8 (length command))
               (loop for value across command sum (raft-command-memory-weight value))))
    (symbol 0)
    (integer (if (typep command 'fixnum) 0 (+ 32 (ceiling (integer-length command) 8))))
    (t 128)))

(defun raft-append-message (node peer)
  "Build a bounded AppendEntries batch beginning at PEER's next-index.
Retries never encode the entire retained suffix. One oversized command travels
alone so existing persisted commands remain deliverable."
  (let* ((next (gethash peer (raft-node-next-index node) (1+ (raft-last-log-index node))))
         (cached (when (and (raft-node-catchup-byte-limit node)
                            (not (gethash peer (raft-node-peer-snapshot-required node)))
                            (<= next (raft-node-snapshot-index node)))
                   (raft-storage-read-catchup (raft-node-storage node) node next))))
    (when cached
      (return-from raft-append-message
        (list :type :append-entries :term (raft-node-current-term node) :from (raft-node-id node)
              :prev-log-index (1- next) :prev-log-term (car cached) :entries (cdr cached)
              :leader-commit (raft-node-commit-index node))))
    (let* ((prev-index (1- next))
         (bytes 0)
         (entries
           (loop for i from (max next (1+ (raft-node-snapshot-index node))) to (raft-last-log-index node)
                 for count from 0
                 for entry = (raft-log-entry node i)
                 for weight = (+ 128 (raft-command-wire-weight (raft-entry-command entry)))
                 while (and (< count (raft-node-append-entry-limit node))
                            (or (zerop count) (<= (+ bytes weight) (raft-node-append-byte-budget node))))
                 do (incf bytes weight)
                 collect entry)))
    (when (<= next (raft-node-snapshot-index node))
      (unless (raft-snapshot-envelope-p (raft-node-snapshot-state node))
        (error "Snapshot lacks configuration; create a fresh configured snapshot before transfer"))
      (unless (getf (cddr (raft-node-snapshot-state node)) :application-present)
        (error "Snapshot lacks application state; provide a snapshot callback before transfer"))
      (return-from raft-append-message
        (list :type :install-snapshot :term (raft-node-current-term node) :from (raft-node-id node)
              :last-index (raft-node-snapshot-index node) :last-term (raft-node-snapshot-term node)
              :state (raft-node-snapshot-state node))))
    (list :type :append-entries
          :term (raft-node-current-term node)
          :from (raft-node-id node)
          :prev-log-index prev-index
          :prev-log-term (raft-log-term node prev-index)
          :entries entries
          :leader-commit (raft-node-commit-index node)))))

(defun raft-broadcast-append (node)
  "Send every replicated member (peers, joiners, and departing members) an
   AppendEntries and reset the heartbeat timer. Stops tracking a departing member
   once its match-index shows it has stored its own removal entry."
  (setf (raft-node-heartbeat-elapsed node) 0)
  (dolist (peer (raft-replicate-ids node))
    (raft-send node peer (raft-append-message node peer)))
  ;; A departing member that already holds its removal entry has now also been
  ;; sent the commit for it (this append carries leader-commit), so it will apply
  ;; the change and learn it is out. Stop tracking it. If it has not yet stored
  ;; the entry (match below the removal index), keep sending until it does.
  (loop for peer being the hash-keys of (raft-node-departing-members node) using (hash-value at)
        when (>= (gethash peer (raft-node-match-index node) 0) at)
          collect peer into done
        finally (dolist (peer done) (remhash peer (raft-node-departing-members node)))))

(defun raft-advance-commit (node)
  "Leader: advance commit-index to the highest N replicated on a majority whose
   entry is from the current term (Raft §5.4.2)."
  (loop for n from (raft-last-log-index node) downto (1+ (raft-node-commit-index node))
        when (and (= (raft-log-term node n) (raft-node-current-term node))
                  (>= (1+ (loop for peer in (raft-node-peers node)
                                count (>= (gethash peer (raft-node-match-index node) 0) n)))
                      (raft-majority node)))
          do (raft-set-commit-index node n)
             (return)))

;;; ---------------------------------------------------------------------------
;;; Apply committed entries to the state machine.

(defun raft-apply-committed (node)
  "Apply every newly committed entry to the state machine in log order, advancing
   last-applied up to commit-index. A :raft-config entry reconfigures membership
   instead of calling apply-fn; each entry's result resolves and clears any
   pending client callback registered for its index."
  (loop while (< (raft-node-last-applied node) (raft-node-commit-index node))
        do (let* ((index (1+ (raft-node-last-applied node)))
                  (entry (raft-log-entry node index))
                  (command (raft-entry-command entry))
                  (result
                    (if (and (consp command) (eq (car command) :raft-config))
                        ;; A membership change is applied to the config, not the
                        ;; user state machine. Every member applies it in log
                        ;; order, so all converge on the same membership.
                        (progn (raft-adopt-config node (getf (cdr command) :members)
                                                  (getf (cdr command) :addresses)
                                                  (getf (cdr command) :api-urls)
                                                  (getf (cdr command) :promotions))
                               :configured)
                        (if (equal command '(:raft-barrier)) :barrier
                            (and (raft-node-apply-fn node) (funcall (raft-node-apply-fn node) command)))))
                  (pending (gethash index (raft-node-pending node))))
             ;; A failed application must be retried at this exact log position.
             ;; Callback failures, in contrast, must never apply the command twice.
             (setf (raft-node-last-applied node) index)
             (when pending
               (remhash index (raft-node-pending node))
               ;; A deposed leader's uncommitted entry may have been replaced by a
               ;; different entry at the same index. That entry's result belongs to
               ;; another client, so the stale callback is dropped, never resolved.
               (when (= (car pending) (raft-entry-term entry))
                 (funcall (cdr pending) result))))))

;;; ---------------------------------------------------------------------------
;;; Client proposal (leader only).

(defun raft-capacity-exempt-command-p (command)
  (or (member command '((:raft-barrier) (:store-recovery-barrier)) :test #'equal)
      (and (consp command) (eq :raft-config (car command)))))

(defun raft-proposal-capacity-reason (node command)
  "Check admission before append using the node's configured command estimator."
  (let ((limit (raft-node-uncommitted-byte-limit node))
        (retained-limit (raft-node-retained-byte-limit node)))
    (when (and (or limit retained-limit) (not (raft-capacity-exempt-command-p command)))
      (let* ((bytes (+ 128 (funcall (raft-node-uncommitted-weight-fn node) command)))
             (retained bytes)
             (pending-start (max 0 (- (raft-node-commit-index node) (raft-node-snapshot-index node)))))
        (when (or (and limit (> bytes limit)) (and retained-limit (> bytes retained-limit)))
          (return-from raft-proposal-capacity-reason :proposal-too-large))
        (loop for offset from (if retained-limit 0 pending-start)
                below (length (raft-node-log node))
              for entry = (aref (raft-node-log node) offset)
              for weight = (+ 128 (funcall (raft-node-uncommitted-weight-fn node) (raft-entry-command entry)))
              do (incf retained weight)
                 (when (>= offset pending-start) (incf bytes weight))
                 (when (or (and limit (> bytes limit)) (and retained-limit (> retained retained-limit)))
                   (return-from raft-proposal-capacity-reason :capacity)))))))

(defun raft-propose (node command &optional callback)
  "Leader appends COMMAND and replicates it. CALLBACK (if given) is invoked with
   the state-machine result once the entry commits. Returns (values index t) on
   the leader, or (values nil nil reason) on refusal. Capacity refusal does not
   append, persist, send, or retain CALLBACK."
  (when (and (eq (raft-node-role node) :leader) (not (raft-node-snapshot-pending node)))
    (let ((reason (raft-proposal-capacity-reason node command)))
      (when reason (return-from raft-propose (values nil nil reason)))))
  (if (and (eq (raft-node-role node) :leader) (not (raft-node-snapshot-pending node)))
      (let ((entry (raft-log-append node (make-raft-entry :term (raft-node-current-term node)
                                                          :command command))))
        (when callback
          (setf (gethash (raft-entry-index entry) (raft-node-pending node))
                (cons (raft-entry-term entry) callback)))
        (setf (gethash (raft-node-id node) (raft-node-match-index node)) (raft-entry-index entry))
        (raft-broadcast-append node)
        (raft-advance-commit node)
        (raft-apply-committed node)
        (values (raft-entry-index entry) t))
      (values nil nil :not-leader)))

;;; ---------------------------------------------------------------------------
;;; Membership change (single-member, effective on commit).

(defun raft-validate-promotion-intents (members promotions)
  "Promotion metadata must name distinct committed learners and request voters."
  (let ((seen (make-hash-table :test 'equal)))
    (dolist (entry promotions)
      (unless (and (consp entry) (stringp (car entry)) (eq :voter (cdr entry))
                   (eq :learner (cdr (assoc (car entry) members :test #'equal)))
                   (not (gethash (car entry) seen)))
        (error "Invalid learner promotion intent"))
      (setf (gethash (car entry) seen) t)))
  t)

(defun raft-adopt-config (node members &optional addresses api-urls promotions)
  "Adopt a committed membership MEMBERS (alist id->role). Every member runs this
   in log order, so all converge. A member that finds itself gone steps down.
   ADDRESSES (alist id->(host port)) teaches the transport where to reach peers;
   API-URLS (alist id->url) teaches the HTTP layer where to redirect writes."
  (raft-validate-promotion-intents members promotions)
  (dolist (a addresses)
    (setf (gethash (car a) (raft-node-peer-addresses node)) (cdr a)))
  (dolist (a api-urls)
    (when (cdr a) (setf (gethash (car a) (raft-node-api-urls node)) (cdr a))))
  (let ((old-peers (raft-node-peers node))
        (leading (eq (raft-node-role node) :leader)))
    (raft-configure node members)
    (dolist (entry promotions)
      (setf (gethash (copy-seq (car entry)) (raft-node-promotion-intents node)) (cdr entry)))
    (clrhash (raft-node-pending-members node))  ; any added member is now a real peer
    ;; Keep delivering this commit to peers the change just dropped, so they
    ;; apply it, mark themselves removed, and stop holding elections. (Only the
    ;; leader replicates; a departed member clears its own set when it steps.)
    (when leading
      (dolist (id old-peers)
        (unless (member id (raft-node-peers node) :test #'equal)
          (setf (gethash id (raft-node-departing-members node)) (raft-node-commit-index node))))))
  (when (raft-node-removed node)
    (clrhash (raft-node-departing-members node))
    (when (eq (raft-node-role node) :leader)
      (raft-become-follower node (raft-node-current-term node))))
  (when (raft-node-on-configured node)
    (funcall (raft-node-on-configured node) node)))

(defun raft-config-change-in-flight-p (node)
  "Find retained, unapplied configuration entries, including after leader change."
  (or (plusp (hash-table-count (raft-node-pending-members node)))
      (loop for index from (1+ (raft-node-last-applied node)) to (raft-last-log-index node)
            for entry = (raft-log-entry node index)
            for command = (and entry (raft-entry-command entry))
            thereis (and (consp command) (eq :raft-config (first command))))))

(defun raft-peer-ready-for-promotion-p (node id)
  "A learner must acknowledge application of this leader's current-term commit.
Caller serializes access to NODE. Receipt evidence is volatile and reacquired
following leadership changes; old peers without applied receipts cannot qualify."
  (let ((boundary (raft-node-commit-index node))
        (receipt (gethash id (raft-node-peer-applied node))))
    (and (eq :leader (raft-node-role node))
         (eq :learner (gethash id (raft-node-roles node)))
         (not (raft-node-removed node))
         (not (raft-config-change-in-flight-p node))
         (plusp boundary)
         (= (raft-node-current-term node) (raft-log-term node boundary))
         (= boundary (raft-node-last-applied node))
         receipt (= (car receipt) (raft-node-current-term node))
         (>= (cdr receipt) boundary)
         (>= (gethash id (raft-node-match-index node) 0) boundary))))

(defun raft-propose-config (node &key add remove (role :voter) address api-url callback promotion-role)
  "Leader-only single-member change. ADD = a member id to add with ROLE (:voter
   or :witness) and ADDRESS = its (host port) so every member can reach it;
   REMOVE = a member id to drop (not self). Only one change may be in flight. The
   new member is replicated to immediately but is not counted for quorum until
   the config entry commits (effective-on-commit). Returns (values index t) when
   the change is proposed, else (values nil reason)."
  (unless (or (null promotion-role) (and add (eq role :learner) (eq promotion-role :voter)))
    (error "Promotion intent requires adding a learner with voter intent"))
  (cond
    ((not (eq (raft-node-role node) :leader)) (values nil :not-leader))
    ((raft-config-change-in-flight-p node) (values nil :change-in-flight))
    ((and add (eq role :voter) (eq :learner (gethash add (raft-node-roles node)))
          (not (raft-peer-ready-for-promotion-p node add)))
     (values nil :learner-not-ready))
    ((and remove (equal remove (raft-node-id node))) (values nil :cannot-remove-self))
    (t
     (let ((members (raft-current-config node))
           (promotions (loop for id being the hash-keys of (raft-node-promotion-intents node)
                               using (hash-value desired)
                             unless (or (and (equal id add) (or promotion-role (not (eq role :learner))))
                                        (equal id remove))
                               collect (cons id desired))))
       (cond
         (add (setf members (cons (cons add role)
                                  (remove add members :key #'car :test #'equal)))
              (when address (setf (gethash add (raft-node-peer-addresses node)) address))
              (when api-url (setf (gethash add (raft-node-api-urls node)) api-url))
              ;; Replicate to the joiner now (not yet quorum) so it receives the
              ;; config entry that promotes it. The transport needs its link before
              ;; the append goes out, so reconcile immediately.
              (setf (gethash add (raft-node-pending-members node)) role
                    (gethash add (raft-node-next-index node)) (1+ (raft-last-log-index node))
                    (gethash add (raft-node-match-index node)) 0)
              (when (raft-node-on-configured node)
                (funcall (raft-node-on-configured node) node)))
         (remove (setf members (remove remove members :key #'car :test #'equal))))
       (when promotion-role (push (cons add promotion-role) promotions))
       (raft-validate-promotion-intents members promotions)
       ;; Carry the whole address book (and API URLs) so a just-joined member
       ;; (and every other) learns where to reach each peer when this commits.
       (raft-propose node (list :raft-config :members members
                                :addresses (raft-config-addresses node)
                                :api-urls (raft-config-api-urls node)
                                :promotions promotions)
                     callback)))))

(defun raft-config-addresses (node)
  "The current member address book as an alist id->(host port) for a config entry."
  (loop for id being the hash-keys of (raft-node-peer-addresses node) using (hash-value addr)
        collect (cons id addr)))

(defun raft-config-api-urls (node)
  "The current member API-URL book as an alist id->url for a config entry."
  (loop for id being the hash-keys of (raft-node-api-urls node) using (hash-value url)
        collect (cons id url)))

;;; ---------------------------------------------------------------------------
;;; Message handling.

(defun raft-step-down-if-stale (node term)
  "If TERM exceeds ours, adopt it and revert to follower. Returns T if stepped."
  (when (> term (raft-node-current-term node))
    (raft-become-follower node term)
    t))

(defun raft-handle-request-vote (node msg)
  "Handle a RequestVote: step down first if MSG's term is newer, then grant the
   vote only when the terms match, this node has not already voted for someone
   else, and the candidate's log is at least as up-to-date (Raft §5.4.1). A
   granted vote is recorded, persisted, and resets the election timer; either way
   a reply goes back to the candidate."
  (raft-step-down-if-stale node (getf msg :term))
  (let* ((term (getf msg :term))
         (candidate (getf msg :from))
         (up-to-date (or (> (getf msg :last-log-term) (raft-last-log-term node))
                         (and (= (getf msg :last-log-term) (raft-last-log-term node))
                              (>= (getf msg :last-log-index) (raft-last-log-index node)))))
         (grant (and (= term (raft-node-current-term node))
                     (member (raft-node-voted-for node) (list nil candidate) :test #'equal)
                     up-to-date)))
    (when grant
      (raft-set-hard-state node term candidate)
      (raft-reset-election-timer node))
    (raft-send node candidate
               (list :type :request-vote-reply :term (raft-node-current-term node)
                     :from (raft-node-id node) :granted grant))))

(defun raft-handle-request-vote-reply (node msg)
  "Handle a RequestVote reply: step down if MSG's term is newer, else -- while
   still a candidate for this term -- count a granted vote and promote to leader
   on reaching quorum."
  (raft-step-down-if-stale node (getf msg :term))
  (when (and (eq (raft-node-role node) :candidate)
             (= (getf msg :term) (raft-node-current-term node))
             (getf msg :granted))
    (setf (gethash (getf msg :from) (raft-node-votes node)) t)
    (raft-maybe-become-leader node)))

(defun raft-handle-pre-vote (node msg)
  "Answer a Pre-Vote: grant iff this node is not itself the leader, its own
   election timer has expired (it sees no live leader), the candidate's log is at
   least as up-to-date (Raft §5.4.1), and the candidate's hypothetical term is not
   behind ours. Granting changes neither our term nor our vote, and never resets
   our election timer -- so a flood of pre-votes cannot keep a follower from timing
   out, and a follower still hearing a leader's heartbeats refuses, which is what
   protects a healthy leader (podster-8r7)."
  (let* ((up-to-date (or (> (getf msg :last-log-term) (raft-last-log-term node))
                         (and (= (getf msg :last-log-term) (raft-last-log-term node))
                              (>= (getf msg :last-log-index) (raft-last-log-index node)))))
         (grant (and (not (eq (raft-node-role node) :leader))
                     (>= (raft-node-leader-contact-elapsed node) (raft-node-election-timeout node))
                     (> (getf msg :term) (raft-node-current-term node))
                     up-to-date)))
    (raft-send node (getf msg :from)
               (list :type :pre-vote-reply :term (raft-node-current-term node)
                     :pre-term (getf msg :term) :from (raft-node-id node) :granted grant))))

(defun raft-handle-pre-vote-reply (node msg)
  "Count a Pre-Vote grant for the round in flight; on a quorum, become a real
   candidate for that term. A reply revealing a higher term means this node is
   behind, so adopt it and stand down. Replies for a stale round are ignored."
  (raft-step-down-if-stale node (getf msg :term))
  (when (and (raft-node-pre-vote-term node)
             (eql (getf msg :pre-term) (raft-node-pre-vote-term node))
             (getf msg :granted)
             (not (eq (raft-node-role node) :leader)))
    (setf (gethash (getf msg :from) (raft-node-pre-votes node)) t)
    (when (>= (hash-table-count (raft-node-pre-votes node)) (raft-majority node))
      (raft-become-candidate node))))

(defun raft-replication-capacity-index (node prev-index entries)
  "Return the first new index if a replicated suffix exceeds the retention budget.
Matching duplicates cost nothing; a conflicting suffix replaces the old one.
Protocol-only traffic remains available to establish a recovery commit."
  (let ((limit (raft-node-retained-byte-limit node)))
    (when limit
      (loop for remaining on entries for index from (1+ prev-index)
            for entry = (car remaining)
            when (or (> index (raft-last-log-index node))
                     (/= (raft-log-term node index) (raft-entry-term entry)))
              do (when (<= index (raft-node-commit-index node))
                   (error "Replication cannot overwrite the committed prefix"))
                 (when (some (lambda (entry) (not (raft-capacity-exempt-command-p (raft-entry-command entry))))
                             remaining)
                   (let ((bytes 0) (weight (raft-node-uncommitted-weight-fn node)))
                     (loop for old across (raft-node-log node)
                           while (< (raft-entry-index old) index)
                           do (incf bytes (+ 128 (funcall weight (raft-entry-command old)))))
                     (dolist (new remaining)
                       (incf bytes (+ 128 (funcall weight (raft-entry-command new)))))
                     (when (> bytes limit) (return-from raft-replication-capacity-index index))))
                 (return nil)))))

(defun raft-handle-append-entries (node msg)
  "Handle an AppendEntries from a leader: reject (success nil) when MSG's term is
   stale or the previous-entry log-match check fails; otherwise accept leadership
   for the term, append or overwrite the entries, advance the commit index
   (capped at the last new entry), apply, and reply with the matched index. A
   valid leader resets the election timer."
  (let ((term (getf msg :term)) (leader (getf msg :from)))
    (raft-step-down-if-stale node term)
    (cond
      ((< term (raft-node-current-term node))
       (raft-send node leader (list :type :append-entries-reply
                                    :term (raft-node-current-term node) :from (raft-node-id node)
                                    :success nil :match-index 0)))
      (t
       ;; Valid leader for our term.
       (when (eq (raft-node-role node) :candidate) (raft-become-follower node term leader))
       (setf (raft-node-leader-id node) leader)
       (setf (raft-node-leader-contact-elapsed node) 0)   ; heard the leader: Pre-Vote stickiness
       (raft-reset-election-timer node)
       (let ((prev-index (getf msg :prev-log-index))
             (prev-term (getf msg :prev-log-term)))
         (when (< prev-index (raft-node-snapshot-index node))
           (raft-send node leader (list :type :append-entries-reply
                                       :term (raft-node-current-term node) :from (raft-node-id node)
                                       :success nil :match-index 0
                                       :next-index (1+ (raft-node-snapshot-index node))))
           (return-from raft-handle-append-entries nil))
         (if (or (> prev-index (raft-last-log-index node))
                 (and (> prev-index 0)
                      (/= (raft-log-term node prev-index) prev-term)))
             ;; Log mismatch: reject so the leader backs up.
             (raft-send node leader (list :type :append-entries-reply
                                          :term (raft-node-current-term node) :from (raft-node-id node)
                                          :success nil :match-index 0))
             (progn
               (let ((next (raft-replication-capacity-index node prev-index (getf msg :entries))))
                 (when next
                   ;; PREV-INDEX matched: that prefix can become committed even
                   ;; though we cannot retain this new batch yet. Otherwise a
                   ;; full uncommitted tail could never become compactable.
                   (when (> (min (getf msg :leader-commit) prev-index) (raft-node-commit-index node))
                     (raft-set-commit-index node (min (getf msg :leader-commit) prev-index)))
                   (raft-apply-committed node)
                   (raft-send node leader (list :type :append-entries-reply
                                               :term (raft-node-current-term node) :from (raft-node-id node)
                                               :success nil :match-index 0 :next-index next
                                               :capacity-limited t))
                   (return-from raft-handle-append-entries nil)))
               ;; Append/overwrite, skipping entries we already agree on.
               (loop for entry in (getf msg :entries)
                     for i from (1+ prev-index)
                     do (cond ((> i (raft-log-length node)) (raft-log-append node (copy-raft-entry entry)))
                              ((/= (raft-log-term node i) (raft-entry-term entry))
                               (raft-log-truncate node i)
                               (raft-log-append node (copy-raft-entry entry)))))
               (let ((last-new (+ prev-index (length (getf msg :entries)))))
                 (when (> (getf msg :leader-commit) (raft-node-commit-index node))
                   (raft-set-commit-index node (min (getf msg :leader-commit) last-new)))
                 (raft-apply-committed node)
                 (raft-send node leader (list :type :append-entries-reply
                                              :term (raft-node-current-term node)
                                              :from (raft-node-id node)
                                              :success t :match-index last-new
                                              :applied-index (min last-new (raft-node-last-applied node))))))))))))

(defun raft-handle-append-entries-reply (node msg)
  "Handle an AppendEntries reply as leader: on success record the peer's
   match/next-index and advance the commit index (applying anything newly
   committed); on failure back the peer's next-index up by one for the next
   round. Steps down first if MSG's term is newer."
  (raft-step-down-if-stale node (getf msg :term))
  (when (and (eq (raft-node-role node) :leader)
             (= (getf msg :term) (raft-node-current-term node)))
    (let ((peer (getf msg :from)))
      (if (getf msg :success)
          (progn
            (setf (gethash peer (raft-node-match-index node)) (getf msg :match-index)
                  (gethash peer (raft-node-next-index node)) (1+ (getf msg :match-index)))
            (when (>= (getf msg :match-index) (raft-node-snapshot-index node))
              (remhash peer (raft-node-peer-snapshot-required node)))
            (let ((applied (getf msg :applied-index))
                  (matched (getf msg :match-index)))
              (when (and (member peer (raft-replicate-ids node) :test #'equal)
                         (typep applied '(integer 0 #.most-positive-fixnum))
                         (typep matched '(integer 0 #.most-positive-fixnum))
                         (<= applied matched (raft-last-log-index node)))
                (let ((prior (gethash peer (raft-node-peer-applied node))))
                  (setf (gethash peer (raft-node-peer-applied node))
                        (cons (raft-node-current-term node)
                              (max applied (if (and prior (= (car prior) (raft-node-current-term node)))
                                               (cdr prior) 0)))))))
            (raft-advance-commit node)
            (raft-apply-committed node))
          ;; Back up and retry on the next replication round.
          (progn
            (setf (gethash peer (raft-node-next-index node))
                  (min (1+ (raft-last-log-index node))
                       (max (1+ (gethash peer (raft-node-match-index node) 0))
                            (or (getf msg :next-index)
                                (1- (gethash peer (raft-node-next-index node) 1))))))
            ;; Only a newer durable checkpoint can free the receiver's prefix.
            ;; Keep this preference across ordinary heartbeat retries; the next
            ;; successful snapshot/catch-up or membership/leadership reset clears it.
            (when (and (getf msg :capacity-limited)
                       (member peer (raft-replicate-ids node) :test #'equal)
                       (<= (gethash peer (raft-node-next-index node))
                           (raft-node-snapshot-index node)))
              (setf (gethash peer (raft-node-peer-snapshot-required node)) t)))))))

(defun raft-validate-snapshot-envelope (state)
  (unless (raft-snapshot-envelope-p state) (error "Unsupported snapshot envelope"))
  (unless (getf (cddr state) :application-present) (error "Snapshot lacks application state"))
  (raft-validate-promotion-intents (getf (cddr state) :members) (getf (cddr state) :promotions))
  (let ((members (getf (cddr state) :members)) (seen (make-hash-table :test 'equal)))
    (unless (consp members) (error "Snapshot has no membership"))
    (dolist (member members)
      (unless (and (consp member) (stringp (car member))
                   (member (cdr member) '(:voter :witness :learner))
                   (not (gethash (car member) seen)))
        (error "Invalid snapshot member"))
      (setf (gethash (car member) seen) t)))
  (dolist (entry (getf (cddr state) :addresses))
    (unless (and (consp entry) (stringp (car entry)) (listp (cdr entry))
                 (= 2 (length (cdr entry))) (stringp (second entry))
                 (typep (third entry) '(integer 1 65535)))
      (error "Invalid snapshot address")))
  (dolist (entry (getf (cddr state) :api-urls))
    (unless (and (consp entry) (stringp (car entry)) (stringp (cdr entry)))
      (error "Invalid snapshot API URL"))))

(defun raft-handle-install-snapshot (node msg)
  "Durably install a complete snapshot; transport assembles bounded chunks first."
  (let ((term (getf msg :term)) (leader (getf msg :from))
        (index (getf msg :last-index)) (last-term (getf msg :last-term))
        (state (getf msg :state)))
    (raft-step-down-if-stale node term)
    (when (< term (raft-node-current-term node))
      (raft-send node leader (list :type :install-snapshot-reply :term (raft-node-current-term node)
                                  :from (raft-node-id node) :success nil :match-index 0))
      (return-from raft-handle-install-snapshot nil))
    (unless (eq :follower (raft-node-role node)) (raft-become-follower node term leader))
    (setf (raft-node-leader-id node) leader (raft-node-leader-contact-elapsed node) 0)
    (raft-reset-election-timer node)
    (unless (and (typep index '(integer 1 *)) (typep last-term '(integer 0 *)) (<= last-term term))
      (error "Invalid snapshot boundary"))
    (when (> index (raft-node-commit-index node))
      (raft-validate-snapshot-envelope state)
      (when (and (raft-node-apply-fn node) (null (raft-node-restore-fn node)))
        (error "Application snapshots require a restore callback"))
      (let ((tail (if (= last-term (raft-log-term node index)) (raft-node-log node) #())))
        (raft-storage-replace-snapshot (raft-node-storage node) index last-term state
                                      (raft-node-current-term node) (raft-node-voted-for node) tail
                                      :commit-index index)
        (setf (raft-node-snapshot-pending node) (list index last-term state tail))
        (raft-resume-snapshot-install node)))
    (raft-send node leader (list :type :install-snapshot-reply :term (raft-node-current-term node)
                                :from (raft-node-id node) :success t :match-index index))))

(defun raft-handle-install-snapshot-reply (node msg)
  (raft-step-down-if-stale node (getf msg :term))
  (when (and (eq :leader (raft-node-role node))
             (= (getf msg :term) (raft-node-current-term node)) (getf msg :success)
             (member (getf msg :from) (raft-replicate-ids node) :test #'equal)
             (<= 0 (getf msg :match-index) (raft-last-log-index node)))
    (let ((peer (getf msg :from)) (index (getf msg :match-index)))
      (when (>= index (raft-node-snapshot-index node))
        (remhash peer (raft-node-peer-snapshot-required node)))
      (setf (gethash peer (raft-node-match-index node))
            (max index (gethash peer (raft-node-match-index node) 0))
            (gethash peer (raft-node-next-index node))
            (max (1+ index) (gethash peer (raft-node-next-index node) 1)))
      (raft-advance-commit node)
      (raft-apply-committed node))))

(defun raft-handle-message (node msg)
  "Dispatch MSG to the handler for its :type (request-vote and its reply,
   append-entries and its reply). Signals on an unknown type."
  (when (raft-node-snapshot-pending node) (raft-resume-snapshot-install node))
  (ecase (getf msg :type)
    (:pre-vote (raft-handle-pre-vote node msg))
    (:pre-vote-reply (raft-handle-pre-vote-reply node msg))
    (:request-vote (raft-handle-request-vote node msg))
    (:request-vote-reply (raft-handle-request-vote-reply node msg))
    (:append-entries (raft-handle-append-entries node msg))
    (:append-entries-reply (raft-handle-append-entries-reply node msg))
    (:install-snapshot (raft-handle-install-snapshot node msg))
    (:install-snapshot-reply (raft-handle-install-snapshot-reply node msg))))

;;; ---------------------------------------------------------------------------
;;; Logical clock.

(defun raft-tick (node)
  "Advance one logical tick: voter candidates/followers may start an election,
   leaders send heartbeats. A witness or learner never starts an election (so it
   never becomes leader); a removed member does nothing."
  (unless (or (raft-node-removed node) (raft-node-snapshot-pending node))
    (case (raft-node-role node)
      ((:follower :candidate)
       (incf (raft-node-leader-contact-elapsed node))   ; time since a leader heartbeat (Pre-Vote)
       (when (eq (raft-node-self-role node) :voter)
         (when (>= (incf (raft-node-election-elapsed node)) (raft-node-election-timeout node))
           (raft-begin-pre-election node))))
      (:leader
       (when (>= (incf (raft-node-heartbeat-elapsed node)) (raft-node-heartbeat-timeout node))
         (raft-broadcast-append node))))))

(defun raft-leader-p (node)
  "True when NODE currently holds the leader role."
  (eq (raft-node-role node) :leader))
