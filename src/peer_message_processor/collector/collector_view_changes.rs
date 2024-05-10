//! Defines the collector of messages of type ViewChange.
//! After a sufficient amount (t + 1) of ViewChanges are received and collected,
//! the next leader broadcasts the
//! [NewView](crate::peer_message::usig_message::new_view::NewView) message.

use tracing::trace;

use crate::{peer_message::usig_message::view_change::ViewChange, Config, View};

use super::CollectorMessages;

pub(crate) type CollectorViewChanges<P, Sig> = CollectorMessages<View, ViewChange<P, Sig>>;

impl<P: Clone, Sig: Clone> CollectorViewChanges<P, Sig> {
    /// # Arguments
    ///
    /// * `msg` - The ViewChange message to be collected.
    ///
    /// # Return Value
    ///
    /// The amount of so far collected ViewChanges for the same next [View] as
    /// the given message.
    pub(crate) fn collect_view_change(&mut self, msg: ViewChange<P, Sig>) -> u64 {
        let origin = msg.origin;
        let next_view = msg.next_view;
        trace!(
            "Collecting ViewChange (origin: {:?}, next_view: {:?}) ...",
            origin,
            next_view
        );
        let amount_collected = self.collect(msg, origin, next_view);
        trace!(
            "Successfully collected ViewChange (origin: {:?}, next view: {:?}).",
            origin,
            next_view
        );
        amount_collected
    }

    /// Retrieves a collection of at least `t + 1` ViewChanges if they are valid
    /// and if already at least `t + 1` ViewChanges have been received for the
    /// same next [View].
    /// Is this the case, then the collection only retains ViewChanges which are
    /// for a higher next [View].
    ///
    /// # Arguments
    ///
    /// * `msg` - The [ViewChange] message to be collected.
    /// * `config` - The [Config] of the replica.
    ///
    /// # Return Value
    ///
    /// If at least `t + 1` ViewChanges have been collected that share the same
    /// next [View], a [Vec] containing said ViewChanges is returned.
    /// Otherwise, [None] is returned.
    pub(crate) fn retrieve_collected_view_changes(
        &mut self,
        msg: &ViewChange<P, Sig>,
        config: &Config,
    ) -> Option<Vec<ViewChange<P, Sig>>> {
        trace!(
            "Retrieving ViewChanges (next view: {:?}) from collector ...",
            msg.next_view,
        );
        let mut retrieved = self.retrieve(msg.next_view, config)?;
        let mut retrieved_all = Vec::new();
        retrieved_all.push(retrieved.0);
        retrieved_all.append(&mut retrieved.1);
        trace!(
            "Successfully retrieved ViewChanges (next view: {:?}).",
            msg.next_view
        );
        Some(retrieved_all)
    }
}

#[cfg(test)]

mod test {
    use rand::thread_rng;
    use rstest::rstest;

    use super::CollectorViewChanges;
    use std::num::NonZeroU64;

    use rand::Rng;
    use usig::ReplicaId;

    use crate::{
        peer_message::usig_message::view_change::test::{
            create_message_log, create_view_change, setup_view_change_tests,
        },
        tests::{
            create_attested_usigs_for_replicas, create_default_configs_for_replicas,
            get_random_included_index, get_random_view_with_max, get_shuffled_remaining_replicas,
        },
        View,
    };

    /// Tests if the collection of a single ViewChange succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_vc_single(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let mut vc_setup = setup_view_change_tests(n);

        let message_log = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            None,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );

        let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

        let view_change = create_view_change(
            vc_setup.origin,
            vc_setup.next_view,
            None,
            message_log,
            usig_origin,
        );

        let mut collector = CollectorViewChanges::new();
        collector.collect_view_change(view_change.clone());

        assert_eq!(collector.0.len(), 1);

        let key = view_change.next_view;

        assert!(collector.0.get(&key).is_some());
        let collected_view_changes = collector.0.get(&key).unwrap();
        assert!(collected_view_changes.get(&view_change.origin).is_some());
        let collected_vc = collected_view_changes.get(&view_change.origin).unwrap();
        assert_eq!(collected_vc.origin, view_change.origin);
        assert_eq!(collected_vc.next_view, view_change.next_view);
        assert!(collected_vc.checkpoint_cert.is_none());
        assert_eq!(
            collected_vc.data.variant.message_log.len(),
            view_change.data.variant.message_log.len()
        );
    }

    /// Tests if the retrieval of a collected ViewChange succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn retrieve_view_change(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let t = n / 2;

        let mut rng = rand::thread_rng();
        let next_view = get_random_view_with_max(View(2 * n + 1)) + 1;

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let shuffled_replicas = get_shuffled_remaining_replicas(n_parsed, None, &mut rng);
        let shuffled_iter = shuffled_replicas.iter().take((t + 1).try_into().unwrap());
        let shuffled_set: Vec<ReplicaId> = shuffled_iter.clone().cloned().collect();

        let origin_index = get_random_included_index(shuffled_iter.len(), None, &mut rng);
        let origin = shuffled_set[origin_index];
        let config_origin = configs.get(&origin).unwrap();

        let mut collector = CollectorViewChanges::new();
        let mut view_changes_to_collect = Vec::new();

        let mut last_collected_view_change = None;

        let mut counter_collected = 0;
        for rep_id in shuffled_iter {
            let amount_messages: u64 = rng.gen_range(5..10);

            let message_log = create_message_log(
                *rep_id,
                amount_messages,
                None,
                &mut rng,
                &configs,
                &mut usigs,
            );

            let usig_origin = usigs.get_mut(rep_id).unwrap();

            let view_change =
                create_view_change(*rep_id, next_view, None, message_log, usig_origin);

            view_changes_to_collect.push(view_change.clone());

            collector.collect_view_change(view_change.clone());
            counter_collected += 1;
            last_collected_view_change = Some(view_change.clone());

            if counter_collected <= t.try_into().unwrap() {
                let collected_vcs = collector.retrieve_collected_view_changes(
                    &last_collected_view_change.clone().unwrap(),
                    config_origin,
                );
                assert!(collected_vcs.is_none());
            }
        }

        assert!(last_collected_view_change.is_some());

        let collected_vcs = collector
            .retrieve_collected_view_changes(&last_collected_view_change.unwrap(), config_origin);
        assert!(collected_vcs.is_some());
        let collected_vcs = collected_vcs.unwrap();

        assert_eq!(collected_vcs.len(), view_changes_to_collect.len());

        assert_eq!(
            collected_vcs
                .iter()
                .filter(|vc| vc.next_view == next_view)
                .count(),
            view_changes_to_collect.len()
        );
    }

    /// Tests if the collection of different ViewChanges behaves as expected.
    /// That is, if they are grouped together when sharing the same next view.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_diff_view_changes(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let t = n / 2;

        let next_view_1 = get_random_view_with_max(View(2 * n + 1));
        let mut next_view_2 = get_random_view_with_max(View(2 * n + 1));
        while next_view_1 == next_view_2 {
            next_view_2 = get_random_view_with_max(View(2 * n + 1));
        }

        let shuffled_replicas = get_shuffled_remaining_replicas(n_parsed, None, &mut rng);
        let mut shuffled_set = shuffled_replicas.iter().take(2);

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let mut collector = CollectorViewChanges::new();

        // Create first ViewChange message and collect it.
        let first_rep_id = shuffled_set.next().unwrap();
        let amount_messages: u64 = rng.gen_range(5..10);
        let message_log = create_message_log(
            *first_rep_id,
            amount_messages,
            None,
            &mut rng,
            &configs,
            &mut usigs,
        );
        let usig_origin = usigs.get_mut(first_rep_id).unwrap();
        let first_view_change =
            create_view_change(*first_rep_id, next_view_1, None, message_log, usig_origin);
        collector.collect_view_change(first_view_change.clone());

        assert_eq!(collector.0.len(), 1);

        // Create second ViewChange message and collect it.
        let second_rep_id = shuffled_set.next().unwrap();
        let amount_messages: u64 = rng.gen_range(5..10);
        let message_log = create_message_log(
            *second_rep_id,
            amount_messages,
            None,
            &mut rng,
            &configs,
            &mut usigs,
        );
        let usig_origin = usigs.get_mut(second_rep_id).unwrap();
        let second_view_change =
            create_view_change(*second_rep_id, next_view_2, None, message_log, usig_origin);
        collector.collect_view_change(second_view_change.clone());

        assert_eq!(collector.0.len(), 2);

        // Check if first created view change was collected successfully.
        let first_key = first_view_change.next_view;
        assert!(collector.0.get(&first_key).is_some());
        let collected_view_changes = collector.0.get(&first_key).unwrap();
        assert!(collected_view_changes
            .get(&first_view_change.origin)
            .is_some());
        let collected_vc = collected_view_changes
            .get(&first_view_change.origin)
            .unwrap();
        assert_eq!(collected_vc.origin, first_view_change.origin);
        assert_eq!(collected_vc.next_view, first_view_change.next_view);
        assert!(collected_vc.checkpoint_cert.is_none());
        assert_eq!(
            collected_vc.data.variant.message_log.len(),
            first_view_change.data.variant.message_log.len()
        );

        // Check if second created ViewChange was collected successfully.
        let second_key = second_view_change.next_view;
        assert!(collector.0.get(&second_key).is_some());
        let collected_view_changes = collector.0.get(&second_key).unwrap();
        assert!(collected_view_changes
            .get(&second_view_change.origin)
            .is_some());
        let collected_vc = collected_view_changes
            .get(&second_view_change.origin)
            .unwrap();
        assert_eq!(collected_vc.origin, second_view_change.origin);
        assert_eq!(collected_vc.next_view, second_view_change.next_view);
        assert!(collected_vc.checkpoint_cert.is_none());
        assert_eq!(
            collected_vc.data.variant.message_log.len(),
            second_view_change.data.variant.message_log.len()
        );
    }
}
