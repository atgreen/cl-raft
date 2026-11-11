;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT

(asdf:defsystem "cl-raft"
  :description "A transport-independent Raft consensus state machine"
  :author "Anthony Green <green@moxielogic.com>"
  :license "MIT"
  :version "0.1.0"
  :serial t
  :components ((:module "src" :serial t
                :components ((:file "package") (:file "core"))))
  :in-order-to ((test-op (test-op "cl-raft/tests"))))

(asdf:defsystem "cl-raft/tests"
  :depends-on ("cl-raft/wal" "fiveam")
  :serial t
  :components ((:module "tests" :serial t
                :components ((:file "package") (:file "core-test")
                             (:file "persistence-test")
                             (:file "wal-hardening-test")
                             (:file "availability-test"))))
  :perform (test-op (operation component)
             (declare (ignore operation component))
             (unless (uiop:symbol-call :fiveam :run!
                                       (find-symbol "CL-RAFT" "CL-RAFT/TESTS"))
               (error "cl-raft tests failed"))))

(asdf:defsystem "cl-raft/wal"
  :description "Durable SBCL/Linux file storage for cl-raft"
  :depends-on ("cl-raft" "cffi" "flexi-streams" "ironclad" "trivial-utf-8")
  :serial t
  :components ((:module "src" :components ((:file "log")))))
