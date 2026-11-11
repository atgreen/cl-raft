;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT

(in-package #:cl-raft/tests)
(in-suite cl-raft)

(defun raft-wal-size (path)
  (with-open-file (stream path :element-type '(unsigned-byte 8)) (file-length stream)))

(test raft-file-checkpoints-reclaim-history-and-retain-exact-tail
  (with-raft-temp-dir (path)
    (let* ((storage (cl-raft:open-file-raft-storage path))
           (node (make-standalone-node :storage storage :snapshot/restore t)))
      (unwind-protect
           (progn
             (setf (cl-raft:raft-node-current-term node) 7 (cl-raft:raft-node-voted-for node) "peer")
             (cl-raft:raft-persist-hard-state node)
             (dotimes (round 5)
               (dotimes (i 20)
                 (cl-raft:raft-log-append node
                   (cl-raft:make-raft-entry :term 7 :command
                     (list :set "x" (list round i (make-string 8192 :initial-element #\x))))))
               (let* ((boundary (- (cl-raft:raft-last-log-index node) 2))
                      (before (raft-wal-size path)))
                 (setf (cl-raft:raft-node-commit-index node) boundary)
                 (cl-raft:raft-apply-committed node)
                 (cl-raft:raft-compact node boundary)
                 (is (< (raft-wal-size path) (/ before 3)))
                 (is (< (raft-wal-size path) 26000))
                 (let ((recovered (cl-raft:load-raft-log path)))
                   (is (= 7 (cl-raft:raft-recovered-term recovered)))
                   (is (equal "peer" (cl-raft:raft-recovered-vote recovered)))
                   (is (= boundary (cl-raft:raft-recovered-snapshot-index recovered)))
                   (is (equal (list (1+ boundary) (+ boundary 2))
                              (mapcar #'cl-raft:raft-entry-index (cl-raft:raft-recovered-entries recovered)))))))
             (is (= #o600 (logand #o777 (sb-posix:stat-mode (sb-posix:stat path)))))
             ;; Exercise truncation and replacement after checkpointing; writes
             ;; must reach the renamed inode rather than the discarded stream.
             (cl-raft:raft-log-truncate node 100)
             (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 8 :command '(:set "x" "replacement")))
             (cl-raft:close-file-raft-storage storage)
             (multiple-value-bind (node2 kv2) (make-standalone-node :snapshot/restore t)
               (unwind-protect
                    (progn
                      (cl-raft:recover-raft-node node2 path)
                      (is (= 98 (cl-raft:raft-node-last-applied node2)))
                      (is (= 100 (cl-raft:raft-last-log-index node2)))
                      (is (= 8 (cl-raft:raft-log-term node2 100)))
                      (setf (cl-raft:raft-node-commit-index node2) 100)
                      (cl-raft:raft-apply-committed node2)
                      (is (equal "replacement" (gethash "x" kv2))))
                 (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node2)))))
        (cl-raft:close-file-raft-storage storage)))))

(test raft-wal-owner-excludes-second-open-before-recovery-or-restore
  (with-raft-temp-dir (path)
    (let ((owner (cl-raft:open-file-raft-storage path)))
      (unwind-protect
           (progn
             (cl-raft:raft-storage-save-hard-state owner 7 "first")
             (let ((before (uiop:read-file-string path)) (validated nil) (restored nil)
                   (node (cl-raft:make-raft-node :id "second")))
               (setf (cl-raft:raft-node-restore-fn node) (lambda (state) (declare (ignore state)) (setf restored t)))
               (signals error (cl-raft:open-file-raft-storage path))
               (signals error (cl-raft:recover-raft-node node path :validate-fn
                                (lambda (state) (declare (ignore state)) (setf validated t))))
               (is (null validated))
               (is (null restored))
               (is (= 0 (cl-raft:raft-node-current-term node)))
               (is (equal before (uiop:read-file-string path)))
               (is (= 7 (cl-raft:raft-recovered-term (cl-raft:load-raft-log path))))))
        (cl-raft:close-file-raft-storage owner))
      (cl-raft:close-file-raft-storage owner)
      (let ((next (cl-raft:open-file-raft-storage path)))
        (unwind-protect
             (progn (cl-raft:raft-storage-save-hard-state next 8 "next")
                    (is (= 8 (cl-raft:raft-recovered-term (cl-raft:load-raft-log path)))))
          (cl-raft:close-file-raft-storage next))))))

(test raft-wal-owner-survives-checkpoint-inode-replacement
  (with-raft-temp-dir (path)
    (let* ((owner (cl-raft:open-file-raft-storage path))
           (node (make-standalone-node :storage owner :snapshot/restore t)))
      (unwind-protect
           (progn
             (dotimes (i 3)
               (cl-raft:raft-log-append node (cl-raft:make-raft-entry :term 1 :command (list :set "x" i))))
             (setf (cl-raft:raft-node-commit-index node) 3)
             (cl-raft:raft-apply-committed node)
             (let ((inode (sb-posix:stat-ino (sb-posix:stat path))))
               (cl-raft:raft-compact node 3)
               (is (/= inode (sb-posix:stat-ino (sb-posix:stat path)))))
             (signals error (cl-raft:open-file-raft-storage path))
             (is (= 3 (cl-raft:raft-recovered-snapshot-index (cl-raft:load-raft-log path)))))
        (cl-raft:close-file-raft-storage owner)))))

(test raft-wal-recovery-failures-release-ownership
  (with-raft-temp-dir (path)
    (let ((owner (cl-raft:open-file-raft-storage path)))
      (cl-raft:raft-storage-save-snapshot owner 3 1 '(:state))
      (cl-raft:close-file-raft-storage owner))
    (dolist (phase '(:validation :restore))
      (let ((node (cl-raft:make-raft-node :id "failed")))
        (setf (cl-raft:raft-node-restore-fn node)
              (lambda (state) (declare (ignore state)) (error "Injected restore failure")))
        (signals error (cl-raft:recover-raft-node node path :validate-fn
                         (lambda (state) (declare (ignore state)) (if (eq phase :validation) nil t))))
        (is (null (cl-raft:raft-node-storage node)))
        (let ((next (cl-raft:open-file-raft-storage path)))
          (is (not (null next)))
          (cl-raft:close-file-raft-storage next))))))

(defun raft-test-record-octets (form)
  (trivial-utf-8:string-to-utf-8-bytes
   (with-output-to-string (out) (cl-raft::raft-wal-write-frame form out))))

(defun raft-test-write-octets (path octets &optional (end (length octets)))
  (ensure-directories-exist path)
  (with-open-file (out path :direction :output :element-type '(unsigned-byte 8)
                           :if-exists :supersede :if-does-not-exist :create)
    (write-sequence octets out :end end)))

(test raft-wal-frames-use-the-standalone-format-identity
  (let ((frame (raft-test-record-octets '(:hard 1 nil))))
    (is (string= "@CLRAFTW1 "
                 (map 'string #'code-char (subseq frame 0 10))))))

(test raft-framed-wal-recovers-every-short-tail-byte-boundary
  (with-raft-temp-dir (path)
    (let* ((baseline (concatenate '(vector (unsigned-byte 8))
                      (raft-test-record-octets '(:hard 17 "acknowledged"))
                      (raft-test-record-octets '(:entry 1 17 (:set "x" "kept"))))))
      (dolist (pending (list '(:hard 18 "unacknowledged")
                            (list :entry 2 18 (list :set "x" (format nil "éλ😀~%payload")))
                            '(:truncate 1)))
        (let* ((tail (raft-test-record-octets pending))
               (complete (concatenate '(vector (unsigned-byte 8)) baseline tail)))
          (loop for cut from 1 below (length tail)
                do (raft-test-write-octets path complete (+ (length baseline) cut))
                   (let ((size (raft-wal-size path)))
                     (signals error (cl-raft:load-raft-log path))
                     (is (= size (raft-wal-size path))))
                   (let ((node (cl-raft:make-raft-node :id "recover")))
                     (unwind-protect
                          (progn
                            (cl-raft:recover-raft-node node path)
                            (is (= 17 (cl-raft:raft-node-current-term node)))
                            (is (= 1 (cl-raft:raft-last-log-index node)))
                            (is (equal '(:set "x" "kept")
                                       (cl-raft:raft-entry-command (cl-raft:raft-log-entry node 1))))
                            (is (= (length baseline) (raft-wal-size path)))
                            (cl-raft:raft-storage-save-hard-state (cl-raft:raft-node-storage node) 19 "after-repair")
                            (is (= 19 (cl-raft:raft-recovered-term (cl-raft:load-raft-log path)))))
                       (when (cl-raft:raft-node-storage node)
                         (cl-raft:close-file-raft-storage (cl-raft:raft-node-storage node)))))))))))

(test raft-framed-wal-rejects-complete-corruption-without-truncation
  (with-raft-temp-dir (path)
    (let* ((first (raft-test-record-octets '(:hard 17 "first")))
           (second (raft-test-record-octets '(:entry 1 17 (:set "x" "second")))))
      (dolist (position '(0 10 27 92 157 164))
        (let ((damaged (concatenate '(vector (unsigned-byte 8)) first second)))
          (setf (aref damaged position) (if (= (aref damaged position) (char-code #\0)) (char-code #\1) (char-code #\0)))
          (raft-test-write-octets path damaged)
          (signals error (cl-raft:load-raft-log path))
          (let ((node (cl-raft:make-raft-node :id "bad")))
            (signals error (cl-raft:recover-raft-node node path))
            (is (null (cl-raft:raft-node-storage node))))
          (is (= (length damaged) (raft-wal-size path)))
          (with-open-file (in path :element-type '(unsigned-byte 8))
            (let ((after (make-array (length damaged) :element-type '(unsigned-byte 8))))
              (read-sequence after in)
              (is (equalp damaged after)))))))))

(test raft-framed-wal-supports-legacy-prefix-and-fails-closed-on-legacy-torn-tail
  (with-raft-temp-dir (path)
    (ensure-directories-exist path)
    (with-open-file (out path :direction :output :if-does-not-exist :create)
      (write-line "(:hard 7 \"legacy\")" out))
    (let ((storage (cl-raft:open-file-raft-storage path)))
      (unwind-protect
           (cl-raft:raft-storage-append-entry storage
             (cl-raft:make-raft-entry :index 1 :term 7 :command (list :set "x" (format nil "éλ😀~%kept"))))
        (cl-raft:close-file-raft-storage storage)))
    (let ((recovered (cl-raft:load-raft-log path)))
      (is (= 7 (cl-raft:raft-recovered-term recovered)))
      (is (equal (list :set "x" (format nil "éλ😀~%kept"))
                 (cl-raft:raft-entry-command (first (cl-raft:raft-recovered-entries recovered))))))
    (with-open-file (out path :direction :output :if-exists :append)
      (write-string "(:hard 8 \"partial" out))
    (let ((before (raft-wal-size path)) (node (cl-raft:make-raft-node :id "legacy")))
      (signals end-of-file (cl-raft:recover-raft-node node path))
      (is (= before (raft-wal-size path)))
      (is (null (cl-raft:raft-node-storage node))))))



(test raft-short-payload-with-later-valid-frame-fails-closed
  (with-raft-temp-dir (path)
    (let* ((first (raft-test-record-octets (list :entry 1 1 (make-string 1000 :initial-element #\x))))
           (later (raft-test-record-octets '(:hard 2 "acknowledged-later")))
           (damaged (concatenate '(vector (unsigned-byte 8)) (subseq first 0 180) later)))
      (raft-test-write-octets path damaged)
      (let ((node (cl-raft:make-raft-node :id "interior")))
        (signals error (cl-raft:recover-raft-node node path))
        (is (null (cl-raft:raft-node-storage node))))
      (is (= (length damaged) (raft-wal-size path))))))

(defclass repair-observing-raft-storage (cl-raft:file-raft-storage)
  ((failure :initarg :failure :initform nil :reader repair-sync-failure)
   (sizes :initform nil :accessor repair-sync-sizes)))

(defmethod cl-raft::file-raft-tail-sync ((storage repair-observing-raft-storage) fd)
  (push (sb-posix:stat-size (sb-posix:fstat fd)) (repair-sync-sizes storage))
  (when (repair-sync-failure storage) (error "Injected tail repair fsync failure"))
  (call-next-method))

