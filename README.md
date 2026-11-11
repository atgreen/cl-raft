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
