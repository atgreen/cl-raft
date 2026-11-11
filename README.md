# cl-raft

`cl-raft` is a transport-independent implementation of the Raft consensus
algorithm in Common Lisp. Applications supply callbacks for message delivery,
state-machine application, snapshots, restoration, timeouts, and durable state.

The portable `cl-raft` ASDF system has no networking or storage dependencies.
The optional `cl-raft/wal` system provides a crash-safe, framed write-ahead log
for SBCL on Linux, including exclusive process ownership, checkpoints, snapshot
replacement, corruption detection, and torn-tail recovery.

## Minimal node

```lisp
(cl-raft:make-raft-node
 :id "node-a"
 :peers '("node-b" "node-c")
 :send-fn (lambda (peer message) (send-to-peer peer message))
 :apply-fn (lambda (command) (apply-command command)))
```

Call `cl-raft:raft-tick` from an application-controlled clock and deliver
received messages with `cl-raft:raft-handle-message`. The library creates no
threads and chooses no transport.

For durable storage, load `cl-raft/wal` and pass an
`open-file-raft-storage` result as `:storage`. The caller owns and must close the
storage object.

## Availability features

- **Election barrier.** A new leader appends a no-op entry for its term, so
  entries inherited from earlier terms commit without waiting for a client write.
- **CheckQuorum.** A leader that has not heard from a quorum within an election
  timeout steps down, so clients on the minority side of a partition get
  `:not-leader` instead of hanging. Disable with `(setf (raft-node-check-quorum node) nil)`.
- **Randomized election timeouts.** Unless `:timeout-fn` is given, each election
  timeout is drawn from `[base, 2*base)` ticks (`:election-timeout-base`,
  default 10) using a per-node random state seeded from OS entropy.
- **Leadership transfer.** `(raft-transfer-leadership leader target-id)` pauses
  proposals (`:transferring`), catches the target up, and has it start an election
  immediately. Use it before restarting the leader. It is abandoned after one
  election timeout if the target does not take over.
- **Batched durability.** `(raft-propose-batch leader commands callbacks)` appends
  many commands with one fsync, and followers persist each replicated batch with
  one fsync. Custom storage can specialize `raft-storage-append-entries`.
- **Linearizable reads.** `(raft-read-index leader callback)` confirms leadership
  with a quorum heartbeat round and waits for apply to catch up, then calls
  `callback` with the read index. Reading local state inside the callback is
  linearizable. The callback receives `nil` if leadership is lost first.

## Scope

The core implements pre-vote, leader election, log replication, commit/apply,
membership configuration, compaction, and snapshot restoration. Network
transport, TLS, service discovery, and application serialization are deliberately
outside the core. TLS integrations should use `pure-tls`.

The algorithm follows Diego Ongaro and John Ousterhout, *In Search of an
Understandable Consensus Algorithm (Extended Version)*.

## Tests

```sh
/usr/bin/sbcl --noinform --non-interactive \
  --eval '(require :asdf)' \
  --eval '(push #P"/path/to/cl-raft/" asdf:*central-registry*)' \
  --eval '(asdf:test-system :cl-raft)'
```

Run the self-contained single-member example with:

```sh
/usr/bin/sbcl --script examples/single-node.lisp
```

## Author

Anthony Green <green@moxielogic.com>

## License

MIT. See `LICENSE`.
