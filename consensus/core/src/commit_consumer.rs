// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use consensus_types::block::Round;
use mysten_metrics::monitored_mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tracing::debug;

use crate::{CommitIndex, CommittedSubDag};

/// Arguments from commit consumer to this consensus instance.
#[derive(Clone)]
pub struct CommitConsumerArgs {
    /// Replay starts at this index plus one; zero requests the entire epoch.
    pub(crate) replay_after_commit_index: CommitIndex,
    /// The legacy consumer-supplied recovery target. Full replay discovers its
    /// target from consensus storage instead.
    pub(crate) consumer_last_processed_commit_index: CommitIndex,
    pub(crate) commit_sender: UnboundedSender<CommittedSubDag>,
    /// Full-epoch consumers acknowledge each recovery batch before storage
    /// reads the next one. Live consensus retains its existing flow control.
    pub(crate) pace_replay: bool,
    monitor: Arc<CommitConsumerMonitor>,
}

impl CommitConsumerArgs {
    pub fn new(
        replay_after_commit_index: CommitIndex,
        consumer_last_processed_commit_index: CommitIndex,
    ) -> (Self, UnboundedReceiver<CommittedSubDag>) {
        let (commit_sender, commit_receiver) = unbounded_channel("consensus_commit_output");
        let monitor = Arc::new(CommitConsumerMonitor::new(
            replay_after_commit_index,
            consumer_last_processed_commit_index,
        ));
        (
            Self {
                replay_after_commit_index,
                consumer_last_processed_commit_index,
                commit_sender,
                pace_replay: false,
                monitor,
            },
            commit_receiver,
        )
    }

    /// Rebuild a consumer with no durable execution watermark. Consensus
    /// discovers the replay target from its own store before delivering commits.
    /// The consumer must run immediately and acknowledge only after applying
    /// each commit; waiting for startup before consuming would deadlock replay.
    pub fn new_with_full_replay() -> (Self, UnboundedReceiver<CommittedSubDag>) {
        let (mut args, receiver) = Self::new(0, 0);
        args.pace_replay = true;
        args.monitor
            .progress
            .send_modify(|progress| progress.replay_target = None);
        (args, receiver)
    }

    pub fn monitor(&self) -> Arc<CommitConsumerMonitor> {
        self.monitor.clone()
    }
}

/// Process-local observations produced by consensus and its commit consumer.
/// Consumers can observe backlog without opening consensus storage or advancing
/// their handled cursor before the actual fold completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitConsumerProgress {
    /// None until consensus has inspected storage for a full replay. With
    /// transaction voting enabled, this is the last finalized commit; the
    /// unfinalized tail still goes through normal consensus finalization.
    pub replay_target: Option<CommitIndex>,
    pub highest_handled_commit: CommitIndex,
    pub highest_committed_index: CommitIndex,
    pub highest_committed_round: Round,
}

pub struct CommitConsumerMonitor {
    progress: watch::Sender<CommitConsumerProgress>,
}

impl CommitConsumerMonitor {
    pub(crate) fn new(
        replay_after_commit_index: CommitIndex,
        consumer_last_processed_commit_index: CommitIndex,
    ) -> Self {
        Self {
            progress: watch::Sender::new(CommitConsumerProgress {
                replay_target: Some(consumer_last_processed_commit_index),
                highest_handled_commit: replay_after_commit_index,
                highest_committed_index: 0,
                highest_committed_round: 0,
            }),
        }
    }

    pub fn progress(&self) -> CommitConsumerProgress {
        *self.progress.borrow()
    }

    pub fn subscribe_progress(&self) -> watch::Receiver<CommitConsumerProgress> {
        self.progress.subscribe()
    }

    pub(crate) fn set_replay_target(&self, target: CommitIndex) {
        self.progress
            .send_modify(|progress| progress.replay_target = Some(target));
    }

    pub(crate) fn report_committed(&self, index: CommitIndex, round: Round) {
        self.progress.send_if_modified(|progress| {
            if index <= progress.highest_committed_index {
                return false;
            }
            progress.highest_committed_index = index;
            progress.highest_committed_round = round;
            true
        });
    }

    pub fn highest_handled_commit(&self) -> CommitIndex {
        self.progress.borrow().highest_handled_commit
    }

    /// Acknowledge application, after the consumer has finished processing the commit.
    pub fn set_highest_handled_commit(&self, highest_handled_commit: CommitIndex) {
        debug!("Highest handled commit set to {}", highest_handled_commit);
        self.progress.send_modify(|progress| {
            progress.highest_handled_commit = highest_handled_commit;
        });
    }

    pub(crate) async fn wait_for_handled(&self, target: CommitIndex) {
        let mut progress = self.subscribe_progress();
        loop {
            if progress.borrow_and_update().highest_handled_commit >= target {
                return;
            }
            progress.changed().await.unwrap();
        }
    }

    /// Wait for the actual fold, including discovery of the target for a full
    /// replay. An undiscovered target is not an empty database.
    pub async fn replay_to_consumer_last_processed_commit_complete(&self) {
        let mut progress = self.subscribe_progress();
        loop {
            let current = *progress.borrow_and_update();
            if current
                .replay_target
                .is_some_and(|target| current.highest_handled_commit >= target)
            {
                return;
            }
            progress.changed().await.unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_commit_consumer_monitor() {
        let monitor = CommitConsumerMonitor::new(0, 10);
        assert_eq!(monitor.highest_handled_commit(), 0);
        monitor.set_highest_handled_commit(100);
        assert_eq!(monitor.highest_handled_commit(), 100);
        monitor
            .replay_to_consumer_last_processed_commit_complete()
            .await;
    }

    #[tokio::test]
    async fn full_replay_waits_for_storage_and_for_the_consumer() {
        let (args, _receiver) = CommitConsumerArgs::new_with_full_replay();
        let monitor = args.monitor();
        assert!(
            timeout(
                Duration::from_millis(20),
                monitor.replay_to_consumer_last_processed_commit_complete()
            )
            .await
            .is_err()
        );
        monitor.set_replay_target(5);
        monitor.set_highest_handled_commit(4);
        assert!(
            timeout(
                Duration::from_millis(20),
                monitor.replay_to_consumer_last_processed_commit_complete()
            )
            .await
            .is_err()
        );
        monitor.set_highest_handled_commit(5);
        timeout(
            Duration::from_secs(1),
            monitor.replay_to_consumer_last_processed_commit_complete(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn empty_full_replay_finishes_only_after_storage_is_inspected() {
        let (args, _receiver) = CommitConsumerArgs::new_with_full_replay();
        let monitor = args.monitor();
        assert_eq!(monitor.progress().replay_target, None);
        monitor.set_replay_target(0);
        timeout(
            Duration::from_secs(1),
            monitor.replay_to_consumer_last_processed_commit_complete(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn committed_head_advances_independently_of_a_stalled_consumer() {
        let monitor = CommitConsumerMonitor::new(0, 0);
        monitor.report_committed(20, 50);
        monitor.set_highest_handled_commit(2);
        monitor.report_committed(21, 52);
        monitor.report_committed(19, 49);
        assert_eq!(monitor.highest_handled_commit(), 2);
        assert_eq!(monitor.progress().highest_committed_index, 21);
        assert_eq!(monitor.progress().highest_committed_round, 52);
    }
}
