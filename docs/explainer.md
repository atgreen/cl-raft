# cl-raft: An Explainer

An orientation to this codebase for someone new to it (and new to Raft). It
covers the consensus algorithm in [`src/core.lisp`](../src/core.lisp) first, then
goes deep on the durable write-ahead log in [`src/log.lisp`](../src/log.lisp).
Function references point at real line numbers so you can read the code next to
the prose.

The two systems are deliberately separate:

```
   ┌─────────────────────────────────────────────┐
   │  src/core.lisp   — the Raft algorithm        │
   │  elections, replication, commit/apply,       │
   │  membership. Pure logic: no disk, no network, │
   │  no threads.                                  │
   └───────────────────────┬─────────────────────┘
                           │  the storage protocol
                           │  (raft-storage-save-hard-state, …)
                           ▼
   ┌─────────────────────────────────────────────┐
   │  src/log.lisp    — the write-ahead log (WAL) │
   │  turns "remember this" calls into durable     │
   │  bytes on a Linux disk.                       │
   └─────────────────────────────────────────────┘
```

`core.lisp` never opens a file or a socket. The application supplies callbacks
for sending messages, applying commands, and (optionally) storage; the core
supplies the logic. `raft-tick` is driven by a clock the application owns, and
`raft-handle-message` is fed by a transport the application owns. The library
creates no threads and picks no network.

---

# Part A — Raft, the algorithm

## A1. The problem it solves

Several machines ("nodes") need to agree on an ordered list of commands, and keep
agreeing even when some nodes crash or messages are lost. That ordered list is
**the log**. Every node applies the same commands in the same order and therefore
reaches the same state. Raft is one algorithm for maintaining that log safely.

The canonical reference is Ongaro and Ousterhout, *In Search of an Understandable
Consensus Algorithm (Extended Version)*. This implementation follows it,
including several refinements from Ongaro's dissertation (Pre-Vote, CheckQuorum,
leadership transfer, ReadIndex), each cited in the source where it's used.

## A2. Terms: a logical clock

A **term** is a monotonically increasing integer (`raft-node-current-term`). Each
election begins a new term. Terms let nodes detect stale information: a message
carrying an older term than the receiver's is from a deposed leader or a late
candidate and is rejected; a message carrying a newer term tells the receiver it
has fallen behind, so it steps down to follower and adopts the term
(`raft-become-follower`, core.lisp:489).

Term and vote are the first things that must survive a crash — see Part B.

## A3. Two kinds of "role"

The word "role" means two independent things in this code. Keep them separate.

**Runtime role** (`raft-node-role`): where a node is in the election cycle.

```
                 election timeout (no leader heard)
      ┌────────┐ ───────────────────────────────▶ ┌───────────┐
      │follower│                                   │ candidate │
      └────────┘ ◀─────────────────────────────── └───────────┘
          ▲      discovers a current leader or term     │
          │                                             │ wins a quorum of votes
          │      steps down on a newer term             ▼
          └──────────────────────────────────────  ┌────────┐
                                                    │ leader │
                                                    └────────┘
```

- **Follower** — passive. Answers leaders and candidates. If it hears no leader
  for an election timeout, it starts trying to become leader.
- **Candidate** — campaigning for votes in a new term.
- **Leader** — accepts client commands, replicates them, and sends periodic
  heartbeats to keep followers from timing out.

**Membership role** (`raft-node-roles`, `raft-member-role`, core.lisp:424): what
a node *is allowed to do* in the cluster. This is the "learning mode" axis.

| Membership role | Counts toward quorum? | Can it start an election / lead? | Gets the log? |
|-----------------|:---:|:---:|:---:|
| `:voter`   | yes | yes | yes |
| `:witness` | yes | **no** | yes |
| `:learner` | **no** | **no** | yes |

A **witness** votes and counts for quorum but never campaigns — useful as a
tie-breaking member that you don't want to ever become leader. A **learner** is
the subject of the next section.

## A4. Learning mode: learners and promotion

Adding a fresh node to a running cluster is risky. The newcomer's log is empty,
so until it catches up it can't usefully participate — and if it counted toward
quorum immediately, the cluster could stall waiting on a member that has nothing.

A **learner** solves this. It receives the replicated log exactly like any other
follower, but it does not count toward quorum, never votes, and never campaigns
(`raft-tick` skips the election clock unless `self-role` is `:voter`,
core.lisp:1378). So the cluster streams history to it with no effect on
availability. When the learner has caught up, it is **promoted** to voter.

```
   join as learner        catch up via            promote to voter
   (non-voting)           log replication         (committed config change)
   ┌─────────┐            ┌─────────┐             ┌────────┐
   │ learner │ ─────────▶ │ learner │ ──────────▶ │ voter  │
   └─────────┘  streaming └─────────┘  caught up  └────────┘
                 the log               to the leader
```

The intent to promote is itself durable. A learner that asked to become a voter
is recorded as a **promotion intent** (`promotion-intents`, core.lisp:61) so the
decision survives leader changes and crashes. `raft-propose-config`
(core.lisp:974) is the entry point for membership changes — adding a member in a
given role, removing one, or promoting a learner — and
`raft-validate-promotion-intents` (core.lisp:908) enforces that a promotion names
a genuinely committed learner. Membership itself is replicated as an ordinary log
entry (a `(:raft-config …)` command), so every member learns the new roster the
same way it learns any other committed command.

Two related bookkeeping sets round this out: **pending members**
(`pending-members`) are nodes being added but not yet counted for quorum because
the config entry hasn't committed; **departing members** (`departing-members`)
are nodes a committed config just removed, which the leader keeps updating until
they apply the change and learn they're out, so they stop disrupting the new
quorum.

## A5. The tick: a logical clock the application drives

Raft has no timers of its own. The application calls `raft-tick` (core.lisp:1370)
on whatever cadence it chooses, and one tick advances every logical timer by one:

```
   raft-tick(node):
     follower / candidate (only if self-role = :voter):
        leader-contact-elapsed++         ; time since a leader heartbeat
        election-elapsed++  →  on timeout: raft-begin-pre-election
     leader:
        heartbeat-elapsed++ →  on timeout: raft-broadcast-append   (heartbeats)
        transfer-elapsed++  →  on timeout: abandon a stalled leadership transfer
        check-quorum-elapsed++ → on timeout: raft-check-quorum     (step down if isolated)
```

Because time is just counted ticks, the whole algorithm is deterministic and the
test suite can drive it with no real clock.

## A6. Elections, with Pre-Vote

When a voter follower's election timer fires, it does **not** immediately bump
the term. It first runs a **pre-election** (`raft-begin-pre-election`,
core.lisp:518, Raft dissertation §9.6): it asks peers "at a hypothetical next
term, *would* you vote for me?" without incrementing its own term. Only if a
quorum answers yes does it become a real candidate (`raft-become-candidate`,
core.lisp:542): increment term, vote for itself, and request votes for real.

Pre-Vote exists to stop a partitioned or rejoining node — whose term has drifted
high — from bumping everyone's term and deposing a healthy leader. A follower
still hearing the leader refuses to grant a pre-vote, which protects the leader.

```
   follower times out
        │
        ▼
   PRE-VOTE round  ── quorum says "yes, maybe" ──▶  become CANDIDATE (term++)
        │                                                │
        │ quorum refuses (leader still alive,            │ request real votes
        │ or log too short)                              ▼
        ▼                                          quorum of votes?
   stay follower, reset timer                       │           │
                                                   yes          no
                                                    │           │
                                                    ▼           ▼
                                             become LEADER   step down / retry
```

A vote is granted only to a candidate whose log is at least as up to date as the
voter's (`raft-handle-request-vote`, core.lisp:1046) — this is what guarantees a
new leader already holds every committed entry. Election timeouts are randomized,
drawn per node from `[base, 2*base)` ticks (core.lisp:109), so members rarely time
out together and split the vote. Each node seeds its own random state from OS
entropy, because every fresh SBCL process otherwise shares an identical
`*random-state*`.

On winning, a new leader appends a no-op entry for its term (the "election
barrier," `raft-become-leader`, core.lisp:573) so that entries inherited from
earlier terms commit without waiting for the first client write.

## A7. Log replication, commit, and apply

The leader turns a client command into a log entry — `(index, term, command)` —
and replicates it:

```
   client ──▶ raft-propose(leader, command)      [core.lisp:776]
                    │  append entry to local log, persist it
                    ▼
            AppendEntries  ──▶ follower: append + persist, ack   [core.lisp:1134]
                    ◀── ack (matchIndex advances)
                    │
                    ▼
            quorum holds the entry?  ──▶ advance commit-index    [raft-advance-commit, core.lisp:696]
                    │
                    ▼
            apply committed entries to the state machine via apply-fn
                                                                 [raft-apply-committed, core.lisp:710]
```

- **`commit-index`** is the highest log index known to be stored on a quorum.
  Once an entry is committed it will never be lost and never change.
- **`last-applied`** is the highest index handed to the application's `apply-fn`.
  `raft-apply-committed` walks `last-applied` forward to `commit-index`, applying
  each command in order.

Entries that aren't yet committed can still be replaced: if a new leader has a
different entry at the same index, the follower truncates its conflicting tail
(`raft-log-truncate`, which persists a `(:truncate …)` record). Committed entries
are never truncated — recovery and the live path both refuse it.

## A8. Availability features (where to look)

These are documented for users in the [README](../README.md); in the source each
is small and cites its origin:

- **CheckQuorum** (core.lisp:1393, dissertation §6.2) — a leader that hasn't heard
  from a quorum within an election timeout steps down, so clients on the minority
  side of a partition get a fast "not leader" instead of hanging.
- **Leadership transfer** (`raft-transfer-leadership`, core.lisp:865,
  dissertation §3.10) — hand leadership to a named target before, say, restarting
  the current leader.
- **ReadIndex** (`raft-read-index`, core.lisp:822, dissertation §6.4) — a
  linearizable read that confirms leadership with a heartbeat round and waits for
  apply to catch up before reading local state.
- **Batched durability** (`raft-propose-batch`, core.lisp:786) — many commands,
  one `fsync`.

---

# Part B — The write-ahead log (durability)

Everything in Part A assumes a node can **remember** things across a crash. That
is this layer's job.

## B1. Why Raft needs durable storage

Raft stays correct only if a node never forgets certain facts once it has acted on
them. The classic failure:

> A node votes for candidate A in term 5, then crashes and reboots. If it forgets
> it already voted, it might vote again — for candidate B — in the same term. Now
> two leaders believe they won. Split brain, and divergent state.

So before a node acts on a fact, it writes that fact to disk and makes sure the
disk really has it. That is the "**write-ahead**" rule: record it before you rely
on it. Three facts must be durable, and two more records manage the log's size:

| Record form | Meaning |
|-------------|---------|
| `(:hard term vote)` | current term + who we voted for — prevents a reboot double-vote |
| `(:entry index term command)` | one log entry |
| `(:commit index)` | the proven-durable commit watermark |
| `(:truncate index)` | drop entries at or after `index` |
| `(:snapshot index term state)` | fold everything up to `index` into one state image |

Those five forms are the **entire vocabulary** of the log file. A WAL is just a
sequence of them, in the order they happened.

## B2. Append-only on disk

The WAL is **append-only**: existing bytes are never edited, only added to at the
end. A crash can therefore only ever damage the very end of the file — a property
the recovery code leans on heavily.

```
   WAL file grows this way ──────────────────────────────▶

   ┌─────────┬─────────┬─────────┬─────────┬─────────┐
   │ record0 │ record1 │ record2 │ record3 │ record4 │
   └─────────┴─────────┴─────────┴─────────┴─────────┘
    (:hard     (:entry    (:entry    (:commit   (:entry
     5 "A")     1 5 …)     2 5 …)     2)         3 5 …)
```

Recovery (`load-raft-log`, log.lisp:493) reads the file top to bottom and replays
each record into memory: the last `(:hard …)` is the term and vote, the
`(:entry …)` records rebuild the log, the last valid `(:commit …)` sets how far
it's safe to apply. Each node owns one WAL file (e.g. `node-a`, no extension),
beside a lock file (`node-a.lock`) explained in B7.

## B3. One record, wrapped in a frame

Writing a bare record is unsafe: a crash midway through `(:entry 3 5 "set x=5")`
leaves `(:entry 3 5 "se`, and recovery can't reliably tell a complete record from
a truncated one. So each record is wrapped in a **frame** — a fixed-size header
describing the record, followed by the record itself.

```
  ┌──────────────────── HEADER (exactly 157 bytes) ───────────────────┐
  @CLRAFTW1 000000000000001F <64-hex SHA-256 of the payload> <64-hex SHA-256 of the header-so-far>\n
  └────┬────┘└──────┬───────┘└──────────────┬──────────────┘└───────────────┬──────────────────┘
    magic        length          payload checksum                   header checksum
   (10 bytes)   (16 hex =        (the record bytes                 (the header's own
                 payload byte     are intact)                      first 92 bytes are intact)
                 count)

  ┌──────────────────── PAYLOAD (length bytes) ───────────────────────┐
  (:ENTRY 3 5 "set x=5")\n
  └───────────────────────────────────────────────────────────────────┘
```

Byte offsets within the header (`raft-wal-write-frame`, log.lisp:166;
`+raft-wal-header-length+` = 157):

```
  offset:  0        10              26 27                        91 92                       156
           ▼        ▼               ▼  ▼                          ▼ ▼                         ▼
          "@CLRAFTW1 " <16 hex len> ' ' <64 hex payload-sha256>  ' ' <64 hex header-sha256> '\n'
```

There are two checksums because the length field lives in the header: trusting a
corrupted length would make the reader consume the wrong number of payload bytes.
So the header carries a SHA-256 of its own first 92 bytes
(`raft-wal-valid-header-p`, log.lisp:187), and the payload carries a separate
SHA-256 of the record. `@CLRAFTW1 ` is a fixed magic marker that identifies a
frame boundary (`W1` = WAL format version 1).

The digest is computed in one streaming pass as the record is printed, by a
custom output stream (`raft-wal-digest-stream`, log.lisp:108). The record is then
printed a second time into the file and the length and digest are checked against
the first pass (log.lisp:176), which guarantees the bytes that were checksummed
are exactly the bytes stored.

## B4. fsync: making a write real

A successful write to a file does not mean the data is on the storage device; the
OS holds it in a buffer and flushes "later." `fsync(fd)` blocks until the bytes
are physically durable. It is the only thing that makes a write survive power
loss, and the WAL's central rule is: **never acknowledge a write until after
`fsync`.**

```
  core.lisp: "save this entry"
        │
        ▼
  append the framed bytes            ← in the OS buffer, not durable
        │
        ▼
  fsync(fd)                          ← blocks until physically on disk
        │
        ▼
  return to core.lisp                ← only now may it act on the fact
```

See `file-raft-write` (log.lisp:299) → `file-raft-write-batch` (log.lisp:303) →
`file-raft-append-sync` (log.lisp:295).

`fsync` is expensive, so when several records must be persisted together — a
leader accepting a burst of commands, or a follower storing a replicated batch —
`file-raft-write-batch` appends all the frames and then calls `fsync` **once**:

```
  append frame │ append frame │ append frame │ ONE fsync │ ack all three
```

The frames are byte-identical to individually written ones, so recovery is
unaffected, and nothing in the batch is acknowledged until the single `fsync`
returns. This backs `raft-propose-batch`.

## B5. Recovery and torn tails

`load-raft-log` (log.lisp:493) replays the file through a per-record state machine
(log.lisp:517):

```
  for each record:
     (:hard  t v)     → term = t, vote = v
     (:entry i t cmd) → stage entry i in a table
     (:commit i)      → advance the commit watermark to i (validated)
     (:truncate i)    → forget every staged entry with index >= i
     (:snapshot i t s) → set compaction floor to i, keep image s,
                         forget every staged entry with index <= i
  → sort survivors, return a RAFT-RECOVERED
```

The checks are suspicious of the file: a `(:commit …)` beyond the entries we
actually hold is rejected (log.lisp:522), and an `:entry` overwriting an
already-committed index is rejected (log.lisp:520). A file that fails these checks
refuses to load rather than bring up a corrupt node.

`raft-wal-read-record` (log.lisp:221) returns one of three things:

- a **valid record** — complete, both checksums pass;
- `:eof` — a clean end of file;
- `:torn` — the file ends *inside* a frame: the header promised N payload bytes
  but fewer arrived.

A torn tail is the signature of a crash during the final write. Because the log is
append-only, this can only be the last record, and that record was never
`fsync`'d, so it was never acknowledged to Raft and is safe to discard:

```
   ┌─────────┬─────────┬─────────┬────────────────┐
   │ record0 │ record1 │ record2 │ header says 31, │  ← crash: only 12 payload
   └─────────┴─────────┴─────────┴────────────────┘     bytes actually landed
                                  └──────┬───────┘
                                     torn tail
   raft-wal-repair-tail: ftruncate back to here ──┘  (then fsync, continue)
```

`raft-wal-repair-tail` (log.lisp:267) does this only while holding the exclusive
write lease (B7). A read-only load never truncates — it raises "Incomplete
terminal Raft WAL frame" instead, so an inspection tool can't mutate a WAL.

Repair is reserved for a cleanly *short* tail. Bytes that are present but
*corrupted* (checksum mismatch) are a different case: recovery fails loudly rather
than guess. A guard (`raft-wal-later-header-p`, log.lisp:193) covers the ambiguous
case where a short payload happens to contain what looks like another valid frame
header, and refuses it.

## B6. Snapshots: bounding the log's size

A log that only grows will fill the disk. Once commands are committed and applied,
replaying them one by one is unnecessary — the WAL can record a **snapshot**: a
single image of the state machine as of index `i`, plus "everything up to `i` is
folded into this image."

```
   Before, committed through e5:              After snapshot at index 5:
   ┌───┬───┬───┬───┬───┬───┬───┬───┐          ┌──────────────────┬───┬───┬───┐
   │e1 │e2 │e3 │e4 │e5 │e6 │e7 │e8 │    ──▶    │ :snapshot (e1–e5) │e6 │e7 │e8 │
   └───┴───┴───┴───┴───┴───┴───┴───┘          └──────────────────┴───┴───┴───┘
```

Rather than rewrite the live file in place, `raft-storage-replace-snapshot`
(log.lisp:385) writes a new file beside the old one, `fsync`s it, and atomically
renames it over the old name:

```
   1. write  node-a.checkpoint-<random>   (snapshot + surviving entries)
   2. fsync it                            (fully on disk)
   3. rename → node-a                     (atomic: readers see old OR new, never half)
   4. fsync the directory                 (make the rename itself durable)
```

`rename` is atomic on Linux, so at every instant a reader sees the complete old
file or the complete new file. A crash before step 3 leaves the old file intact.
As an optimization, entries that survive compaction are copied as raw frame bytes
from the old file to the new, using the byte-offset index in
`file-raft-storage-entry-frames`, instead of being re-serialized
(`raft-current-entry-frame`, `raft-copy-entry-frame`) — a speed detail, not a
correctness one.

## B7. The ownership lease: one writer only

Two processes writing one WAL would corrupt it. Before opening the file for
writing, `open-file-raft-storage` (log.lisp:55) takes an exclusive lock on a
sidecar file via `flock(LOCK_EX | LOCK_NB)` (log.lisp:77):

```
   process A ──▶ open node-a.lock ──▶ flock(EXCLUSIVE) ──▶ ✓ opens the WAL
   process B ──▶ open node-a.lock ──▶ flock(EXCLUSIVE) ──▶ ✗ "Another owner holds Raft WAL"
```

The lock file is never deleted — deleting it would let a second owner slip in —
and it is a separate file from the WAL so the lease survives the snapshot
`rename` of B6 (the data file is replaced; the lock file stays and keeps holding
the lock). `fstat` checks confirm the lock and data files are regular,
single-linked, and owned by us, and `O_NOFOLLOW` resists symlink substitution
(log.lisp:72).

## B8. The catch-up cache

When a leader must re-send a run of already-committed entries to a follower that
fell behind, it can keep a small, checksummed, **anonymous** disk file (Linux
`O_TMPFILE`, so it has no name and vanishes on crash) holding those entries
pre-framed. It is a performance cache only:

- built by `raft-storage-cache-prefix` (log.lisp:644),
- read back by `raft-storage-read-catchup` (log.lisp:704),
- and discarded silently if anything about it looks wrong — "cache failure cannot
  change WAL truth" (log.lisp:645). The WAL is always the source of truth.

---

# Part C — The seam between them

`core.lisp` knows only a handful of generic functions (around core.lisp:152);
`log.lisp` provides the file-backed method for each. `NIL` storage is legal and
means volatile — the deterministic simulator uses it.

```
   core.lisp calls …                          log.lisp writes …
   ─────────────────────────────────────────────────────────────
   raft-storage-save-hard-state  term vote  →  (:hard term vote)
   raft-storage-append-entry     entry      →  (:entry i t cmd)
   raft-storage-append-entries   entries    →  many (:entry …), one fsync
   raft-storage-save-commit-index index     →  (:commit index)
   raft-storage-truncate         index      →  (:truncate index)
   raft-storage-save-snapshot    i t state  →  (:snapshot i t state)
   raft-storage-replace-snapshot …          →  new file + atomic rename
```

Because that contract is so small, a different backend — in-memory for tests, or
a different OS — can be written without touching the algorithm. The `cl-raft`
system has no disk dependencies; `cl-raft/wal` adds the Linux/SBCL durable backend
on top.

---

## A suggested reading path

1. `src/core.lisp` node struct and its comments (core.lisp:24–145) — every
   mechanism named in one place.
2. `raft-tick` (core.lisp:1370) — the whole logical clock in one function.
3. `raft-begin-pre-election` → `raft-become-candidate` → `raft-become-leader`
   (core.lisp:518, 542, 573) — an election end to end.
4. `raft-propose` → `raft-handle-append-entries` → `raft-advance-commit` →
   `raft-apply-committed` (core.lisp:776, 1134, 696, 710) — replication to apply.
5. `src/log.lisp` top comment (log.lisp:1–19) — the record vocabulary.
6. `raft-wal-write-frame` (log.lisp:166) and `load-raft-log` (log.lisp:493) — a
   record out to disk and back, including the `:torn` case.

Then run the single-member example end to end:

```sh
/usr/bin/sbcl --script examples/single-node.lisp
```

and skim `tests/wal-hardening-test.lisp`, which deliberately corrupts and
truncates WAL files to exercise the recovery cases in B5.

---

### One-paragraph summary

`core.lisp` is pure Raft — terms, elections (with Pre-Vote), log replication,
commit and apply, and a membership model with voters, witnesses, and non-voting
learners that catch up before promotion. It depends on nothing but a few
callbacks and a small storage protocol. `log.lisp` implements that protocol as an
append-only file of five record types, each in a 157-byte checksummed frame,
`fsync`'d before anything is acknowledged. A crash can only damage the last,
unacknowledged frame, which recovery truncates; snapshots bound the file via
write-new-then-rename; a file lock guarantees a single writer.
