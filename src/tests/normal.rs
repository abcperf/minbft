use crate::peer_message::usig_message::{view_peer_message::ViewPeerMessage, UsigMessage};

use super::*;

/// Tests if a ClientRequest is accepted successfully by a Replica with id = 0.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2.
#[test]
fn client_n_1() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup(1, 0, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 3);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);

    assert_eq!(output.responses.len(), 1);
    let responses = Vec::from(output.responses); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = responses.into_iter();
    let (client, request) = iter.next().unwrap();
    assert_eq!(client, ClientId::from_u64(23));
    assert_eq!(request.id(), RequestId::from_u64(56));
}

/// Tests if a timeout for a ClientRequest is set by a Replica with id = 0.
/// The Replica with id = 0 is the primary and
/// has a setup of n = 3, t = 1, checkpoint_period = 2.
#[test]
fn client_n_3_primary() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup(3, 1, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
}

/// Tests if a timeout for a ClientRequest is set by a Replica with id = 1.
/// The Replica with id = 1 is not the primary and
/// has a setup of n = 3, t = 1, checkpoint_period = 2.
#[test]
fn client_n_3_secondary() {
    let id = ReplicaId::from_u64(1);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup(3, 1, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 1);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
}

/// Tests if a Replica with id = 0 broadcast its message of type Hello.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2 while batching is on.
#[test]
fn hello_batching() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, output), mut _timeout_handler) = minimal_setup_batching(1, 0, id, 2);

    assert_eq!(output.responses.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 0);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(message, ValidatedPeerMessage::Hello(_)));
}

/// Tests if a ClientRequest is successfully accepted by a Replica with id = 0.
/// Furthermore, it is checked if an additional timeout for the batch is set, too.
/// The timeout is triggered in order to force the batch to be accepted.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_1_batching() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(1, 0, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 1);
    let responses = Vec::from(output.responses); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = responses.into_iter();
    let (client, request) = iter.next().unwrap();
    assert_eq!(client, ClientId::from_u64(23));
    assert_eq!(request.id(), RequestId::from_u64(56));

    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);
}

/// Tests if the timeout generation of the timeout set
/// for the batch containing a second ClientRequest is the next expected timeout generation.
/// A Replica with id = 0 accepts a first ClientRequest, and then receives a second ClientRequest from the same ClientId.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_1_batching_again() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(1, 0, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 1);
    let responses = Vec::from(output.responses); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = responses.into_iter();
    let (client, request) = iter.next().unwrap();
    assert_eq!(client, ClientId::from_u64(23));
    assert_eq!(request.id(), RequestId::from_u64(56));

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(57, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));
}

/// Tests if two ClientRequests are successfully accepted at the same time (beloging to the same batch) by a Replica with id = 0.
/// The timeout is triggered in order to force the batch to be accepted.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_1_batching_two() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(1, 0, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));

    let output = minbft.handle_client_message(ClientId::from_u64(11), DummyPayload(55, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(11)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 2);
    assert_eq!(output.errors.len(), 0);
    let responses = Vec::from(output.responses); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = responses.into_iter();
    let (client, request) = iter.next().unwrap();
    assert_eq!(client, ClientId::from_u64(23));
    assert_eq!(request.id(), RequestId::from_u64(56));

    let (client, request) = iter.next().unwrap();
    assert_eq!(client, ClientId::from_u64(11));
    assert_eq!(request.id(), RequestId::from_u64(55));

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 4);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert_eq!(timeout.stop_class, StopClass(23));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert_eq!(timeout.stop_class, StopClass(11));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert_eq!(timeout.stop_class, StopClass(11));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);
}

/// Tests if multiple (11) ClientRequests are successfully accepted at the same time (beloging to the same batch) by a Replica with id = 0.
/// The timeout is triggered in order to force the batch to be accepted.
/// The Replica with id = 0 has a setup of n = 1, t = 0, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_1_batching_multi() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(1, 0, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));

    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));

    for id in 0..10 {
        let output = minbft.handle_client_message(ClientId::from_u64(id), DummyPayload(55, true));

        assert_eq!(output.broadcasts.len(), 0);
        assert_eq!(output.responses.len(), 0);
        assert_eq!(output.errors.len(), 0);

        let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
        assert_eq!(timeout_requests.len(), 2);
        let mut iter = timeout_requests.clone().into_iter();
        let timeout_request = iter.next().unwrap();
        assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
        if let TimeoutRequest::Start(timeout) = timeout_request {
            assert!(matches!(timeout.timeout_type, TimeoutType::Client));
            assert!(matches!(timeout.stop_class, StopClass(_id)));
        } else {
            unreachable!();
        }
        let timeout_request = iter.next().unwrap();
        assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
        if let TimeoutRequest::Start(timeout) = timeout_request {
            assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
            assert_eq!(timeout.stop_class, StopClass::default());
        } else {
            unreachable!();
        }

        timeout_handler.handle_timeout_requests(timeout_requests);
        let timeout_types = timeout_handler.retrieve_timeouts_ordered();
        assert_eq!(timeout_types.len(), 0);
    }

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 11);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 22); // 2 (Timeouts Client and Batch) * 11 (client-requests)
    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);
}

/// Tests if handling the timeout of a batch containing a ClientRequest
/// does not result in accepting the ClientRequest if not yet sufficient (t + 1) Commits have been received.
/// Furthermore, test if the primary (the Replica with id = 0) has a timeout set for the batch
/// (and does indeed use batching).
/// The Replica with id = 0 has a setup of n = 3, t = 1, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_3_primary_batching() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(3, 1, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 2);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Batch));

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();

    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 0);
}

/// Tests if handling the timeout of a batch containing a ClientRequest
/// does not result in accepting the ClientRequest if not yet sufficient (t + 1) Commits have been received.
/// Furthermore, test if a non primary (the Replica with id = 1) does not have a timeout set for the batch
/// (and does not indeed use batching).
/// The Replica with id = 1 has a setup of n = 3, t = 1, checkpoint_period = 2 while batching is on.
#[test]
fn client_n_3_secondary_batching() {
    let id = ReplicaId::from_u64(1);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup_batching(3, 1, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(0), DummyPayload(0, false));
    assert_eq!(output.errors.len(), 1);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 1);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));
}
