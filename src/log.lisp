;;;; SPDX-FileCopyrightText: 2026 Anthony Green <green@moxielogic.com>
;;;; SPDX-License-Identifier: MIT
;;;;
;;;; Purpose:
;;;; Durable, crash-recoverable persistence for the Raft core (podster-c2d.2).
;;;; Raft must persist currentTerm, votedFor and the log before acting on them,
;;;; or a crashed-then-recovered member can violate safety. This is a Clobber-
;;;; style append-only write-ahead log: each mutation is one checksummed frame
;;;; containing a readable Lisp record, flushed and fsync'd to a per-member file.
;;;; Recovery also reads legacy unframed records, but never repairs their tails.
;;;; Snapshot records fold a committed prefix into compacted state so the log
;;;; does not grow without bound.
;;;;
;;;; Record forms:
;;;;   (:hard term vote)
;;;;   (:entry index term command)
;;;;   (:commit index)               ; proven durable commit watermark
;;;;   (:truncate index)             ; drop entries at/after INDEX
;;;;   (:snapshot index term state)  ; compaction boundary + state-machine image

(in-package #:cl-raft)

(defclass file-raft-storage ()
  ((path :initarg :path :reader file-raft-storage-path)
   (stream :initarg :stream :initform nil :accessor file-raft-storage-stream)
   ;; Only scalar byte offsets/terms, never command or payload references.
   (entry-frames :initform (make-hash-table) :accessor file-raft-storage-entry-frames)
   (catchup-segments :initform nil :accessor file-raft-storage-catchup-segments)
   (catchup-error :initform nil :accessor file-raft-storage-catchup-error)
   (ownership-fd :initarg :ownership-fd :initform nil :accessor file-raft-storage-ownership-fd)))

(defstruct raft-catchup-segment fd first last previous-term bytes)

(defmethod raft-storage-catchup-usage ((storage file-raft-storage))
  (values (reduce #'+ (file-raft-storage-catchup-segments storage)
                  :key #'raft-catchup-segment-bytes :initial-value 0)
          (length (file-raft-storage-catchup-segments storage))))

(defmethod raft-storage-clear-catchup ((storage file-raft-storage))
  (sb-sys:without-interrupts
    (let ((segments (file-raft-storage-catchup-segments storage)))
      (setf (file-raft-storage-catchup-segments storage) nil)
      (dolist (segment segments) (ignore-errors (sb-posix:close (raft-catchup-segment-fd segment)))))))

(defun raft-catchup-evict (storage extra limit)
  "Reserve EXTRA physical bytes and one segment, evicting oldest segments first."
  (loop while (and (file-raft-storage-catchup-segments storage)
                   (or (> (+ extra (raft-storage-catchup-usage storage)) limit)
                       (>= (length (file-raft-storage-catchup-segments storage)) 128)))
        do (let* ((segments (file-raft-storage-catchup-segments storage)) (oldest (car (last segments))))
             (sb-sys:without-interrupts
               (setf (file-raft-storage-catchup-segments storage) (butlast segments))
               (sb-posix:close (raft-catchup-segment-fd oldest))))))

(defun open-file-raft-storage (path &key prepare-fn)
  "Own and open an append-only WAL. A stable sidecar lease survives checkpoint
replacement. Never unlink the lease file: that would allow competing owners.
PREPARE-FN receives the canonical path under its lease, before data-file creation."
  (when (and prepare-fn (not (functionp prepare-fn))) (error "Invalid WAL preparation callback"))
  (ensure-directories-exist path)
  (let* ((absolute (merge-pathnames path))
         (target (merge-pathnames (file-namestring absolute)
                                 (truename (uiop:pathname-directory-pathname absolute))))
         (lease (concatenate 'string (namestring target) ".lock"))
         (ownership-fd nil) (stream nil) (completed nil))
    (unwind-protect
         (progn
           (setf ownership-fd
                 (sb-posix:open lease (logior sb-posix:o-rdwr sb-posix:o-creat sb-posix:o-nofollow) #o600))
           (unless (zerop (cffi:foreign-funcall "fcntl" :int ownership-fd :int 2 :int 1 :int))
             (error "Cannot protect Raft WAL lease across exec"))
           (let ((stat (sb-posix:fstat ownership-fd)))
             (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                          (= 1 (sb-posix:stat-nlink stat))
                          (= (sb-posix:geteuid) (sb-posix:stat-uid stat)))
               (error "Unsafe Raft WAL lease file")))
           (unless (zerop (cffi:foreign-funcall "flock" :int ownership-fd :int 6 :int))
             (error "Another owner holds Raft WAL ~A" target)) ; LOCK_EX | LOCK_NB
           (sb-posix:fchmod ownership-fd #o600)
           (when prepare-fn (funcall prepare-fn target))
           (let ((fd (sb-posix:open (namestring target)
                                   (logior sb-posix:o-wronly sb-posix:o-creat
                                           sb-posix:o-append sb-posix:o-nofollow) #o600)))
             (handler-case
                 (progn
                   (let ((stat (sb-posix:fstat fd)))
                     (unless (and (sb-posix:s-isreg (sb-posix:stat-mode stat))
                                  (= 1 (sb-posix:stat-nlink stat)))
                       (error "Unsafe Raft WAL file")))
                   (sb-posix:fchmod fd #o600)
                   (sb-posix:fsync fd)
                   (setf stream (sb-sys:make-fd-stream fd :output t :element-type 'character
                                                     :external-format :utf-8 :auto-close t)))
               (error (condition) (sb-posix:close fd) (error condition))))
           ;; Publish newly created WAL/lease names before serving peers.
           (let ((fd (sb-posix:open (namestring (uiop:pathname-directory-pathname target))
                                    (logior sb-posix:o-rdonly sb-posix:o-directory) 0)))
             (unwind-protect (sb-posix:fsync fd) (sb-posix:close fd)))
           (prog1 (make-instance 'file-raft-storage :path target :stream stream
                                                  :ownership-fd ownership-fd)
             (setf completed t)))
      (unless completed
        (when stream (ignore-errors (close stream :abort t)))
        (when ownership-fd (sb-posix:close ownership-fd))))))

;;; Framing protects record boundaries without building a whole printed record.
;;; A counting SHA-256 sink is the first printer pass; the second writes the WAL.
(defclass raft-wal-digest-stream (sb-gray:fundamental-binary-output-stream)
  ((digest :initform (ironclad:make-digest :sha256) :reader wal-stream-digest)
   (count :initform 0 :accessor wal-stream-count)
   (buffer :initform (make-array 4096 :element-type '(unsigned-byte 8)) :reader wal-stream-buffer)
   (used :initform 0 :accessor wal-stream-used)))

(defmethod stream-element-type ((stream raft-wal-digest-stream)) '(unsigned-byte 8))

(defun wal-stream-flush (stream)
  (when (plusp (wal-stream-used stream))
    (ironclad:update-digest (wal-stream-digest stream) (wal-stream-buffer stream)
                            :end (wal-stream-used stream))
    (setf (wal-stream-used stream) 0)))

(defmethod sb-gray:stream-write-byte ((stream raft-wal-digest-stream) byte)
  (setf (aref (wal-stream-buffer stream) (wal-stream-used stream)) byte)
  (incf (wal-stream-used stream))
  (incf (wal-stream-count stream))
  (when (= (wal-stream-used stream) (length (wal-stream-buffer stream))) (wal-stream-flush stream))
  byte)

(defmethod sb-gray:stream-write-sequence ((stream raft-wal-digest-stream) sequence &optional (start 0) end)
  (wal-stream-flush stream)
  (let ((end (or end (length sequence))))
    (ironclad:update-digest (wal-stream-digest stream) sequence :start start :end end)
    (incf (wal-stream-count stream) (- end start)))
  sequence)

(defun raft-wal-form-metrics (form &optional output)
  (let ((sink (make-instance 'raft-wal-digest-stream)))
    (with-open-stream (text (flexi-streams:make-flexi-stream sink :external-format :utf-8))
      (let ((printer (if output (make-broadcast-stream output text) text)))
        (prin1 form printer)
        (terpri printer))
      (finish-output text)
      (wal-stream-flush sink)
      (values (wal-stream-count sink)
              (ironclad:byte-array-to-hex-string (ironclad:produce-digest (wal-stream-digest sink)))))))

(defun raft-wal-text-sha256 (text)
  (ironclad:byte-array-to-hex-string
   (ironclad:digest-sequence :sha256 (trivial-utf-8:string-to-utf-8-bytes text))))

(defmacro define-constant (name value &optional documentation)
  "Like DEFCONSTANT, but safe to re-evaluate when VALUE is EQUAL but not EQL to
the current binding -- as strings and vectors always are across compilations.
Loading the compiled file a second time would otherwise signal DEFCONSTANT-UNEQL."
  (let ((new (gensym "VALUE")))
    `(defconstant ,name
       (let ((,new ,value))
         (if (and (boundp ',name) (equal (symbol-value ',name) ,new))
             (symbol-value ',name)
             ,new))
       ,@(when documentation (list documentation)))))

(define-constant +raft-wal-frame-magic+ "@CLRAFTW1 ")
(defconstant +raft-wal-header-length+ 157)

(defun raft-wal-write-frame (form stream)
  (let ((*print-readably* t) (*print-circle* nil) (*print-pretty* nil)
        (*print-base* 10) (*print-radix* nil) (*print-case* :upcase)
        (*package* (find-package :cl-raft)))
    (multiple-value-bind (length digest) (raft-wal-form-metrics form)
      (unless (< 0 length (expt 16 16)) (error "Raft WAL record length is out of range"))
      (let ((prefix (format nil "~A~16,'0X ~A " +raft-wal-frame-magic+ length digest)))
        (write-string prefix stream)
        (write-string (raft-wal-text-sha256 prefix) stream)
        (terpri stream))
      (multiple-value-bind (actual-length actual-digest) (raft-wal-form-metrics form stream)
        (unless (and (= length actual-length) (equal digest actual-digest))
          (error "Raft WAL record changed during serialization"))))))

(defun raft-wal-header-character-p (character position)
  (cond ((< position (length +raft-wal-frame-magic+))
         (char= character (char +raft-wal-frame-magic+ position)))
        ((member position '(26 91)) (char= character #\Space))
        ((= position 156) (char= character #\Newline))
        (t (not (null (digit-char-p character 16))))))

(defun raft-wal-valid-header-p (header)
  (and (= (length header) +raft-wal-header-length+)
       (loop for character across header for i from 0
             always (raft-wal-header-character-p character i))
       (string-equal (raft-wal-text-sha256 (subseq header 0 92)) (subseq header 92 156))))

(defun raft-wal-later-header-p (binary start)
  "A short payload containing another valid header is ambiguous, not repairable.
Scan with bounded overlap, including candidates that cross a read-buffer edge."
  (file-position binary start)
  (let ((buffer (make-array (+ 65536 (1- +raft-wal-header-length+)) :element-type '(unsigned-byte 8)))
        (carry 0) (magic +raft-wal-frame-magic+))
    (loop for end = (read-sequence buffer binary :start carry)
          do (loop for i from 0 to (- end +raft-wal-header-length+)
                   when (loop for j below (length magic)
                              always (= (aref buffer (+ i j)) (char-code (char magic j))))
                     do (let ((header (map 'string #'code-char (subseq buffer i (+ i +raft-wal-header-length+)))))
                          (when (raft-wal-valid-header-p header) (return-from raft-wal-later-header-p t))))
             (when (= end carry) (return nil))
             (setf carry (min end (1- +raft-wal-header-length+)))
             (replace buffer buffer :start1 0 :start2 (- end carry) :end2 end))))

(defun raft-wal-require-record-form (form)
  "Reject malformed records, including atoms that resemble reader status values."
  (let* ((arity (and (consp form) (list-length form)))
         (expected (and arity (case (first form) (:hard 3) (:entry 4) (:truncate 2) (:commit 2) (:snapshot 4)))))
    (unless (and expected (= arity expected)
                 (integerp (second form)) (<= 0 (second form))
                 (or (not (eq (first form) :entry)) (plusp (second form)))
                 (or (not (member (first form) '(:entry :snapshot)))
                     (and (integerp (third form)) (<= 0 (third form)))))
      (error "Malformed Raft WAL record"))
    form))

(defun raft-wal-read-record (input binary)
  "Return FORM, or :EOF, or :TORN with its frame boundary as a second value.
Legacy forms are read strictly; only explicit frame boundaries can be repaired."
  (let ((first (peek-char t input nil :eof)))
    (when (eq first :eof) (return-from raft-wal-read-record :eof))
    (unless (eql first #\@) (return-from raft-wal-read-record (raft-wal-require-record-form (read input))))
    (let ((start (file-position input))
          (header (make-string +raft-wal-header-length+)))
      (dotimes (i +raft-wal-header-length+)
        (let ((character (read-char input nil :eof)))
          (when (eq character :eof) (return-from raft-wal-read-record (values :torn start)))
          (unless (raft-wal-header-character-p character i) (error "Invalid Raft WAL frame header"))
          (setf (char header i) character)))
      (unless (raft-wal-valid-header-p header)
        (error "Raft WAL frame header checksum mismatch"))
      (let* ((length (parse-integer header :start 10 :end 26 :radix 16))
             (body-start (file-position input))
             (remaining length)
             (digest (ironclad:make-digest :sha256))
             (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
        (unless (plusp length) (error "Empty Raft WAL frame"))
        (file-position binary body-start)
        (loop while (plusp remaining)
              for n = (read-sequence buffer binary :end (min remaining (length buffer)))
              do (when (zerop n)
                   (when (raft-wal-later-header-p binary body-start)
                     (error "Ambiguous short Raft WAL payload contains a later frame"))
                   (return-from raft-wal-read-record (values :torn start)))
                 (ironclad:update-digest digest buffer :end n)
                 (decf remaining n))
        (unless (string-equal (ironclad:byte-array-to-hex-string (ironclad:produce-digest digest))
                              (subseq header 27 91))
          (error "Raft WAL frame payload checksum mismatch"))
        ;; Parse only after the complete payload has passed byte-level integrity.
        ;; FILE-POSITION on this UTF-8 fd stream is the logical octet position.
        (let* ((*package* (find-package :cl-raft)) (*read-base* 10)
               (form (raft-wal-require-record-form (read-preserving-whitespace input))))
          (unless (and (eql #\Newline (read-char input nil nil))
                       (= (file-position input) (+ body-start length)))
            (error "Raft WAL frame does not contain exactly one record"))
          form)))))

(defgeneric file-raft-tail-sync (storage fd)
  (:documentation "Synchronize a repaired terminal frame before any append or listener startup.")
  (:method ((storage file-raft-storage) fd) (sb-posix:fsync fd)))

(defun raft-wal-repair-tail (storage boundary)
  "Discard a provably short framed tail while holding exclusive WAL ownership."
  (unless (and (file-raft-storage-ownership-fd storage)
               (file-raft-storage-stream storage)
               (open-stream-p (file-raft-storage-stream storage)))
    (error "Raft WAL tail repair requires an owned writable storage"))
  (let ((fd (sb-sys:fd-stream-fd (file-raft-storage-stream storage))))
    (handler-case
        (progn (sb-posix:ftruncate fd boundary) (file-raft-tail-sync storage fd))
      (error (condition)
        (ignore-errors (close (file-raft-storage-stream storage) :abort t))
        (setf (file-raft-storage-stream storage) nil)
        (error condition)))))

(defun raft-index-wal-frame (frames form start end &optional (framed t))
  "Track only current entry frames. Caller publishes this index with its WAL."
  (case (first form)
    (:entry
     (if framed
         (setf (gethash (second form) frames) (vector (third form) start end))
         (remhash (second form) frames)))
    (:truncate
     (loop for index being the hash-keys of frames
           when (>= index (second form)) do (remhash index frames)))
    (:snapshot
     (loop for index being the hash-keys of frames
           when (<= index (second form)) do (remhash index frames)))))

(defgeneric file-raft-append-sync (storage fd)
  (:documentation "Make every frame appended so far durable before acknowledging it.")
  (:method ((storage file-raft-storage) fd) (sb-posix:fsync fd)))

(defun file-raft-write (storage form)
  "Append one checksummed frame and fsync it before acknowledging persistence."
  (file-raft-write-batch storage (list form)))

(defun file-raft-write-batch (storage forms)
  "Append one checksummed frame per form, then fsync once before acknowledging
any of them. Frames are byte-identical to individually written ones, so the
recovery format is unchanged and nothing in the batch is acknowledged before the
sync. As with any multi-page frame written before this change, a crash before
the sync can persist the unsynced pages in any order: a short tail is repaired,
while damage before the tail fails its checksum and recovery refuses to start."
  (let ((stream (file-raft-storage-stream storage)))
    (unless (and stream (open-stream-p stream))
      (error "Raft WAL is closed; recover it before writing"))
    (handler-case
        (let ((spans '()))
          ;; A reopened O_APPEND descriptor initially has position zero. Seek
          ;; through the stream before observing its byte position.
          (file-position stream :end)
          (dolist (form forms)
            (let ((start (file-position stream)))
              (raft-wal-write-frame form stream)
              ;; Flush per frame so FILE-POSITION reports the frame's true end.
              (finish-output stream)
              (push (list form start (file-position stream)) spans)))
          (file-raft-append-sync storage (sb-sys:fd-stream-fd stream))
          ;; Publish frame offsets only after the whole batch is durable.
          (loop for (form start end) in (nreverse spans)
                do (raft-index-wal-frame (file-raft-storage-entry-frames storage) form start end)))
      (error (condition)
        ;; An uncertain partial append must not be followed by another append.
        ;; Retain the ownership lease until explicit close and recovery.
        (ignore-errors (close stream :abort t))
        (setf (file-raft-storage-stream storage) nil)
        (error condition)))))

(defmethod raft-storage-save-hard-state ((storage file-raft-storage) term vote)
  (file-raft-write storage (list :hard term vote)))

(defmethod raft-storage-append-entry ((storage file-raft-storage) entry)
  (file-raft-write storage (list :entry (raft-entry-index entry)
                                 (raft-entry-term entry) (raft-entry-command entry))))

(defmethod raft-storage-append-entries ((storage file-raft-storage) entries)
  (when entries
    (file-raft-write-batch storage
                           (mapcar (lambda (entry)
                                     (list :entry (raft-entry-index entry)
                                           (raft-entry-term entry) (raft-entry-command entry)))
                                   entries))))

(defmethod raft-storage-truncate ((storage file-raft-storage) index)
  (file-raft-write storage (list :truncate index)))

(defmethod raft-storage-save-commit-index ((storage file-raft-storage) index)
  (file-raft-write storage (list :commit index)))

(defmethod raft-storage-save-snapshot ((storage file-raft-storage) index term state)
  (file-raft-write storage (list :snapshot index term state)))

(defgeneric file-raft-checkpoint-sync (storage phase fd)
  (:documentation "Synchronize a checkpoint file or its containing directory.")
  (:method ((storage file-raft-storage) phase fd)
    (declare (ignore storage phase))
    (sb-posix:fsync fd)))

(defun raft-current-entry-frame (storage entry file-size)
  "Borrow scalar offsets only when they describe this entry in the owned WAL."
  (let ((frame (gethash (raft-entry-index entry) (file-raft-storage-entry-frames storage))))
    (when (and frame (= (aref frame 0) (raft-entry-term entry))
               (<= 0 (aref frame 1))
               (< (+ (aref frame 1) +raft-wal-header-length+) (aref frame 2))
               (<= (aref frame 2) file-size))
      frame)))

(defun raft-copy-entry-frame (input output frame buffer)
  "Copy one complete frame through caller-owned bounded scratch space."
  (file-position input (aref frame 1))
  (loop with remaining = (- (aref frame 2) (aref frame 1))
        while (plusp remaining)
        for count = (min remaining (length buffer))
        do (unless (= count (read-sequence buffer input :end count))
             (error "Short read copying a durable WAL frame"))
           (write-sequence buffer output :end count)
           (decf remaining count)))

(defmethod raft-storage-replace-snapshot
    ((storage file-raft-storage) index term state hard-term vote entries &key (commit-index index))
  "Replace the WAL without reading or retaining discarded historical records.
The old WAL remains usable on a pre-rename failure. A post-rename failure closes
both streams: its durable outcome is uncertain and requires recovery before use."
  (when (and (raft-snapshot-envelope-p state) (not (getf (cddr state) :application-present)))
    (error "Cannot discard a durable log without an exact application/configuration snapshot"))
  (unless (and (file-raft-storage-stream storage)
               (open-stream-p (file-raft-storage-stream storage)))
    (error "Raft WAL is closed; recover it before checkpointing"))
  (unless (and (typep commit-index '(integer 0 *))
               (<= index commit-index
                   (reduce #'max entries :key #'raft-entry-index :initial-value index)))
    (error "Checkpoint commit boundary is outside its retained log"))
  (let* ((target (namestring (merge-pathnames (file-raft-storage-path storage))))
         (temporary (format nil "~A.checkpoint-~36R" target (random (expt 36 20))))
         (old (file-raft-storage-stream storage))
         (frames (make-hash-table))
         (stream nil) (binary-output nil) (created nil) (renamed nil) (completed nil))
    (unwind-protect
         (progn
           (let ((fd (sb-posix:open temporary
                                    (logior sb-posix:o-wronly sb-posix:o-creat sb-posix:o-excl) #o600)))
             (setf created t)
             (handler-case
                 (setf stream (sb-sys:make-fd-stream fd :output t :element-type 'character
                                                    :external-format :utf-8 :auto-close t))
               (error (condition) (sb-posix:close fd) (error condition))))
           ;; Independent descriptors let existing frames remain bytes, while
           ;; the normal character writer handles boundary and legacy records.
           ;; Flush before switching and seek the other writer to EOF afterward.
           (let ((fd (sb-posix:open (format nil "/proc/self/fd/~D" (sb-sys:fd-stream-fd stream))
                                    (logior sb-posix:o-wronly #o2000000) 0)))
             (handler-case
                 (setf binary-output (sb-sys:make-fd-stream fd :output t :element-type '(unsigned-byte 8) :auto-close t))
               (error (condition) (sb-posix:close fd) (error condition))))
           (with-open-file (input (format nil "/proc/self/fd/~D" (sb-sys:fd-stream-fd old))
                                  :direction :input :element-type '(unsigned-byte 8))
             (let ((*print-readably* t) (*print-circle* nil) (*print-pretty* nil)
                   (*read-eval* nil)
                   (file-size (sb-posix:stat-size (sb-posix:fstat (sb-sys:fd-stream-fd old))))
                   (buffer (make-array 65536 :element-type '(unsigned-byte 8))))
               (labels ((record (form)
                          (let ((start (file-position stream)))
                            (raft-wal-write-frame form stream)
                            (finish-output stream)
                            (raft-index-wal-frame frames form start (file-position stream))))
                        (entry-record (entry)
                          (let ((frame (raft-current-entry-frame storage entry file-size)))
                            (if frame
                                (progn
                                  (finish-output stream)
                                  (file-position binary-output :end)
                                  (let ((start (file-position binary-output)))
                                    (raft-copy-entry-frame input binary-output frame buffer)
                                    (finish-output binary-output)
                                    (file-position stream :end)
                                    (raft-index-wal-frame frames
                                      (list :entry (raft-entry-index entry) (raft-entry-term entry) nil)
                                      start (file-position stream))))
                                (record (list :entry (raft-entry-index entry)
                                              (raft-entry-term entry) (raft-entry-command entry)))))))
                 (record (list :hard hard-term vote))
                 (record (list :snapshot index term state))
                 (map nil (lambda (entry) (when (> (raft-entry-index entry) index) (entry-record entry))) entries)
                 (record (list :commit commit-index)))))
           (close binary-output)
           (setf binary-output nil)
           (finish-output stream)
           (file-raft-checkpoint-sync storage :file (sb-sys:fd-stream-fd stream))
           ;; RENAME takes exact path strings, including extensionless WAL names.
           (sb-sys:without-interrupts
             (sb-posix:rename temporary target)
             (setf renamed t (file-raft-storage-stream storage) stream
                   (file-raft-storage-entry-frames storage) frames))
           (let ((fd (sb-posix:open
                      (namestring (uiop:pathname-directory-pathname target)) sb-posix:o-rdonly 0)))
             (unwind-protect (file-raft-checkpoint-sync storage :directory fd)
               (sb-posix:close fd)))
           (close old)
           (setf completed t))
      (when binary-output (ignore-errors (close binary-output :abort t)))
      (unless completed
        (when stream (ignore-errors (close stream :abort t)))
        (when renamed
          (ignore-errors (close old :abort t))
          (setf (file-raft-storage-stream storage) nil)))
      (when (and created (probe-file temporary)) (delete-file temporary)))))

(defun close-file-raft-storage (storage)
  "Close the WAL and release its ownership lease. Safe to call repeatedly."
  (raft-storage-clear-catchup storage)
  (when (file-raft-storage-stream storage)
    (ignore-errors (close (file-raft-storage-stream storage))))
  (setf (file-raft-storage-stream storage) nil)
  (clrhash (file-raft-storage-entry-frames storage))
  (when (file-raft-storage-ownership-fd storage)
    (sb-posix:close (file-raft-storage-ownership-fd storage))
    (setf (file-raft-storage-ownership-fd storage) nil)))

;;; ---------------------------------------------------------------------------
;;; Recovery: replay the WAL into the persisted Raft fields.

(defstruct raft-recovered
  (term 0) (vote nil) (commit-index 0) (commit-marker-p nil)
  (entries '())                         ; ordered RAFT-ENTRYs with index > snapshot-index
  (snapshot-index 0) (snapshot-term 0) (snapshot-state nil))

(defun load-raft-log (path &key repair-storage)
  "Replay the WAL at PATH and return a RAFT-RECOVERED with the durable state.
   A missing file recovers empty state. REPAIR-STORAGE is internal: the caller
   must own its writable lease. Read-only calls never truncate incomplete frames."
  (let ((*read-eval* nil)
        (term 0) (vote nil) (commit-index 0) (commit-marker-p nil)
        (by-index (make-hash-table))    ; index -> (term . command)
        (frames (when repair-storage (make-hash-table)))
        (snapshot-index 0) (snapshot-term 0) (snapshot-state nil))
    (when (probe-file path)
      (when repair-storage
        (unless (equal (truename path) (truename (file-raft-storage-path repair-storage)))
          (error "Raft WAL repair storage does not own this path")))
      (with-open-file (in path :direction :input :element-type 'character :external-format :utf-8 :if-does-not-exist nil)
        (with-open-file (binary path :direction :input :element-type '(unsigned-byte 8))
          (loop
            (let* ((framed (eql (peek-char t in nil nil) #\@))
                   (start (file-position in)))
            (multiple-value-bind (form boundary) (raft-wal-read-record in binary)
              (when (eq form :eof) (return))
              (when (eq form :torn)
                (unless repair-storage (error "Incomplete terminal Raft WAL frame"))
                (raft-wal-repair-tail repair-storage boundary)
                (return))
              (ecase (first form)
                     (:hard (setf term (second form) vote (third form)))
                     (:entry
                      (when (<= (second form) commit-index) (error "WAL overwrites a committed entry"))
                      (setf (gethash (second form) by-index) (cons (third form) (fourth form))))
                     (:commit
                      (unless (and (>= (second form) commit-index snapshot-index)
                                   (loop for index from (1+ snapshot-index) to (second form)
                                         always (gethash index by-index)))
                        (error "Invalid or regressing WAL commit boundary"))
                      (setf commit-index (second form) commit-marker-p t))
                     (:truncate
                      (when (<= (second form) commit-index) (error "WAL truncates committed entries"))
                      (loop for k being the hash-keys of by-index
                            when (>= k (second form)) do (remhash k by-index)))
                     (:snapshot
                      (when (< (second form) snapshot-index) (error "WAL snapshot boundary regresses"))
                      (setf snapshot-index (second form) snapshot-term (third form)
                            snapshot-state (fourth form) commit-index (max commit-index snapshot-index))
                      (loop for k being the hash-keys of by-index
                            when (<= k snapshot-index) do (remhash k by-index))))
              (when frames (raft-index-wal-frame frames form start (file-position in) framed))))))))
    (when repair-storage
      (setf (file-raft-storage-entry-frames repair-storage) frames))
    (let ((entries (loop for index in (sort (loop for k being the hash-keys of by-index collect k) #'<)
                         for cell = (gethash index by-index)
                         collect (make-raft-entry :index index :term (car cell) :command (cdr cell)))))
      (make-raft-recovered :term term :vote vote :entries entries
                           :commit-index commit-index :commit-marker-p commit-marker-p
                           :snapshot-index snapshot-index :snapshot-term snapshot-term
                           :snapshot-state snapshot-state))))

(defun recover-raft-node (node path &key validate-fn)
  "Load the WAL at PATH into NODE (currentTerm/votedFor/log/snapshot), restore
   the state machine from the snapshot image via the node's restore-fn, and
   attach an append-only storage for continued operation. VALIDATE-FN, when given,
   must return T for the recovered state before NODE is changed. Returns NODE."
  (when (raft-node-storage node)
    (error "Close and detach existing Raft storage before recovery"))
  (let ((storage (open-file-raft-storage path)) (completed nil))
    (unwind-protect
         (let ((recovered (load-raft-log (file-raft-storage-path storage) :repair-storage storage)))
           (when (and validate-fn (not (eq t (funcall validate-fn recovered))))
             (error "Recovered Raft state validation did not succeed"))
           (when (and (not (raft-recovered-commit-marker-p recovered))
                      (some (lambda (entry)
                              (let ((command (raft-entry-command entry)))
                                (and (consp command) (eq :raft-config (car command)))))
                            (raft-recovered-entries recovered)))
             (error "Legacy configuration tail lacks a proven commit boundary; restore a trusted snapshot"))
           (setf (raft-node-current-term node) (raft-recovered-term recovered)
                 (raft-node-voted-for node) (raft-recovered-vote recovered)
                 (raft-node-snapshot-index node) (raft-recovered-snapshot-index recovered)
                 (raft-node-snapshot-term node) (raft-recovered-snapshot-term recovered)
                 (raft-node-snapshot-state node) (raft-recovered-snapshot-state recovered)
                 (raft-node-last-applied node) (raft-recovered-snapshot-index recovered)
                 (raft-node-commit-index node) (raft-recovered-snapshot-index recovered))
           (when (raft-recovered-snapshot-state recovered)
             (raft-restore-snapshot-contents node (raft-recovered-snapshot-state recovered)))
           (let ((v (raft-node-log node)))
             (setf (fill-pointer v) 0)
             (dolist (entry (raft-recovered-entries recovered)) (vector-push-extend entry v)))
           (setf (raft-node-storage node) storage
                 (raft-node-commit-index node) (raft-recovered-commit-index recovered))
           ;; Proven committed entries, including membership, must be restored
           ;; before campaigning or accepting traffic. Uncommitted entries stay
           ;; in the log without changing application or quorum state.
           (raft-apply-committed node)
           (setf completed t)
           node)
      (unless completed (close-file-raft-storage storage)))))

(defun map-raft-catchup-prefix (node index function)
  "Borrow committed entries only; never retain the caller's log vector."
  (unless (<= (raft-node-snapshot-index node) index (raft-node-commit-index node))
    (error "Catch-up cache requires a committed local prefix"))
  (funcall function (list :snapshot (raft-node-snapshot-index node) (raft-node-snapshot-term node) nil))
  (loop for offset below (- index (raft-node-snapshot-index node))
        for entry = (aref (raft-node-log node) offset)
        do (funcall function (list :entry (raft-entry-index entry) (raft-entry-term entry) (raft-entry-command entry))))
  (funcall function (list :commit index)))

(defun raft-catchup-open (segment &key binary)
  "Independent file descriptions: checksum reads must not move the parser's offset."
  (let ((fd (sb-posix:open (format nil "/proc/self/fd/~D" (raft-catchup-segment-fd segment))
                           (logior sb-posix:o-rdonly #o2000000) 0))) ; Linux O_CLOEXEC
    (handler-case
        (if binary (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8) :auto-close t)
            (sb-sys:make-fd-stream fd :input t :external-format :utf-8 :auto-close t))
      (error (condition) (sb-posix:close fd) (error condition)))))

(defun raft-catchup-framed-prefix-size (storage node index)
  "Return exact archive bytes when every selected entry has a current WAL frame.
Missing/legacy metadata uses the existing serializer. No payload is inspected."
  (unless (<= (raft-node-snapshot-index node) index (raft-node-commit-index node))
    (error "Catch-up cache requires a committed local prefix"))
  (let ((size (+ (* 2 +raft-wal-header-length+)
                 (raft-wal-form-metrics (list :snapshot (raft-node-snapshot-index node)
                                             (raft-node-snapshot-term node) nil))
                 (raft-wal-form-metrics (list :commit index))))
        (file-size (sb-posix:stat-size
                    (sb-posix:fstat (sb-sys:fd-stream-fd (file-raft-storage-stream storage))))))
    (loop for offset below (- index (raft-node-snapshot-index node))
          for entry = (aref (raft-node-log node) offset)
          for frame = (raft-current-entry-frame storage entry file-size)
          do (unless (and frame (= (raft-entry-index entry) (+ 1 offset (raft-node-snapshot-index node))))
               (return-from raft-catchup-framed-prefix-size nil))
             (incf size (- (aref frame 2) (aref frame 1))))
    size))

(defun raft-catchup-copy-framed-prefix (storage node index output)
  "Copy certified entry frames with a fixed buffer; only tiny boundary forms print."
  (flet ((boundary (form)
           (write-sequence
            (trivial-utf-8:string-to-utf-8-bytes
             (with-output-to-string (text) (raft-wal-write-frame form text))) output)))
    (boundary (list :snapshot (raft-node-snapshot-index node) (raft-node-snapshot-term node) nil))
    (with-open-file (input (format nil "/proc/self/fd/~D"
                                  (sb-sys:fd-stream-fd (file-raft-storage-stream storage)))
                           :direction :input :element-type '(unsigned-byte 8))
      (let ((buffer (make-array 65536 :element-type '(unsigned-byte 8))))
        (loop for offset below (- index (raft-node-snapshot-index node))
              for entry = (aref (raft-node-log node) offset)
              for frame = (gethash (raft-entry-index entry) (file-raft-storage-entry-frames storage))
              do (raft-copy-entry-frame input output frame buffer))))
    (boundary (list :commit index))))

(defmethod raft-storage-cache-prefix ((storage file-raft-storage) node index byte-limit)
  "Retain a checksummed, anonymous disk segment; cache failure cannot change WAL truth."
  (handler-case
      (let ((size 0) (fd nil) (kept nil) (framed-size nil)
            (*print-readably* t) (*print-circle* nil) (*print-pretty* nil)
            (*print-base* 10) (*print-radix* nil) (*print-case* :upcase)
            (*package* (find-package :cl-raft)))
        (unless (and (file-raft-storage-ownership-fd storage)
                     (open-stream-p (file-raft-storage-stream storage)))
          (error "Catch-up cache requires owned open WAL"))
        (when (= index (raft-node-snapshot-index node)) (return-from raft-storage-cache-prefix nil))
        (setf framed-size (raft-catchup-framed-prefix-size storage node index))
        (if framed-size
            (setf size framed-size)
            (map-raft-catchup-prefix node index
              (lambda (form) (incf size (+ +raft-wal-header-length+ (raft-wal-form-metrics form))))))
        (when (> size byte-limit) (return-from raft-storage-cache-prefix nil))
        (raft-catchup-evict storage size byte-limit)
        (unwind-protect
             (progn
               ;; Linux headers: O_TMPFILE=(020000000|O_DIRECTORY), O_CLOEXEC=02000000.
               ;; No pathname ever exists, including across SIGKILL or failed publication.
               (setf fd (sb-posix:open
                         (namestring (uiop:pathname-directory-pathname (file-raft-storage-path storage)))
                         (logior sb-posix:o-rdwr #o20200000 #o2000000) #o600))
               (let ((writer-fd (sb-posix:open (format nil "/proc/self/fd/~D" fd)
                                               (logior sb-posix:o-wronly #o2000000) 0))
                     (writer nil))
                 (unwind-protect
                      (progn
                        (setf writer (if framed-size
                                         (sb-sys:make-fd-stream writer-fd :output t :element-type '(unsigned-byte 8) :auto-close t)
                                         (sb-sys:make-fd-stream writer-fd :output t :external-format :utf-8 :auto-close t)))
                        (if framed-size
                            (raft-catchup-copy-framed-prefix storage node index writer)
                            (map-raft-catchup-prefix node index (lambda (form) (raft-wal-write-frame form writer))))
                        (finish-output writer)
                        (sb-posix:fsync writer-fd))
                   (if writer
                       (unwind-protect (close writer)
                         (when (open-stream-p writer) (close writer :abort t)))
                       (sb-posix:close writer-fd))))
               (unless (= size (sb-posix:stat-size (sb-posix:fstat fd)))
                 (error "Catch-up cache size changed while writing"))
               (let ((segment (make-raft-catchup-segment
                               :fd fd :first (1+ (raft-node-snapshot-index node))
                               :last index :previous-term (raft-node-snapshot-term node) :bytes size)))
                 (sb-sys:without-interrupts
                   (push segment (file-raft-storage-catchup-segments storage))
                   (setf kept t (file-raft-storage-catchup-error storage) nil))))
          (when (and fd (not kept)) (sb-posix:close fd))))
    (error (condition)
      (setf (file-raft-storage-catchup-error storage) (princ-to-string condition))
      nil)))

(defun raft-catchup-read-record (input binary)
  ;; Unlike durable recovery, this process-local cache has no legacy format.
  (unless (eql #\@ (peek-char t input nil nil)) (error "Unframed catch-up cache record"))
  (raft-wal-read-record input binary))

(defmethod raft-storage-read-catchup ((storage file-raft-storage) node next)
  "Decode at most one ordinary bounded AppendEntries batch; older forms are discarded."
  (handler-case
      (let ((*read-eval* nil)
            (segment (find-if (lambda (segment)
                               (and (<= (raft-catchup-segment-first segment) next (raft-catchup-segment-last segment))
                                    (<= (raft-catchup-segment-last segment) (raft-node-snapshot-index node))))
                             (file-raft-storage-catchup-segments storage))))
        (when segment
          (with-open-stream (input (raft-catchup-open segment))
            (with-open-stream (binary (raft-catchup-open segment :binary t))
              (unless (equal (raft-catchup-read-record input binary)
                             (list :snapshot (1- (raft-catchup-segment-first segment))
                                   (raft-catchup-segment-previous-term segment) nil))
                (error "Catch-up cache boundary mismatch"))
              (let ((previous (raft-catchup-segment-previous-term segment))
                    (entries nil) (bytes 0) (count 0))
                (loop for expected from (raft-catchup-segment-first segment) to (raft-catchup-segment-last segment)
                      for form = (raft-catchup-read-record input binary)
                      do (unless (and (consp form) (eq :entry (first form)) (= expected (second form)))
                           (error "Catch-up cache is not contiguous"))
                         (if (< expected next)
                             (setf previous (third form))
                             (let ((weight (+ 128 (raft-command-wire-weight (fourth form)))))
                               (when (or (>= count (raft-node-append-entry-limit node))
                                         (and (plusp count) (> (+ bytes weight) (raft-node-append-byte-budget node))))
                                 (return))
                               (push (make-raft-entry :index expected :term (third form) :command (fourth form)) entries)
                               (incf bytes weight) (incf count))))
                (when entries (cons previous (nreverse entries))))))))
    (error (condition)
      (setf (file-raft-storage-catchup-error storage) (princ-to-string condition))
      (raft-storage-clear-catchup storage)
      nil)))
