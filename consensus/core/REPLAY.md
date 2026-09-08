# Full-epoch consumers

A consumer that reconstructs all derived state on restart can use
`CommitConsumerArgs::new_with_full_replay()` instead of supplying a durable
execution watermark. Consensus owns the database and delivers both historical
and live commits over the same output stream.

The consumer must run before awaiting `ConsensusAuthority::start`, and call
`set_highest_handled_commit` only after applying a commit. Consensus waits for
the consumer between recovery batches of 250 commits, bounding the finalized
history queued across the finalizer and output channels. Existing live input
and commit-sync flow control is unchanged.

`CommitConsumerMonitor::subscribe_progress()` reports:

- The fixed startup replay target. `None` means storage has not been inspected;
  `Some(0)` means there is no finalized work to replay.
- The highest commit applied by the consumer.
- The committed head's index and leader round, independently of consumer lag.

With transaction voting disabled, the target is the last stored commit. With
voting enabled, it is the last finalized commit. An unfinalized tail continues
through normal finalization without an application-acknowledgement wait at
startup, because it may need live network progress. Missing finalized records
inside the target fail startup rather than creating an unresolvable wait.

`replay_to_consumer_last_processed_commit_complete()` waits for a known target
and its application. Applications should also wait for consensus startup before
releasing work that needs a running transaction client. Receive-only consumers
must keep draining while replay is running to avoid circular startup waits.

The original `CommitConsumerArgs::new` keeps its supplied-watermark semantics
and does not opt into replay pacing.

This branch is based on upstream `mainnet-v1.77.2` at
`51d177ad7d65102fc368b582408f466d97b31548`. The crate's Sui dependencies explicitly
retain that upstream source so Ika can patch this crate alone without creating
duplicate companion types from a second git source. Rebase the implementation
and update those companion tags together when upgrading Sui.
