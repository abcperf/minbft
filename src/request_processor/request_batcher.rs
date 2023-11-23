use std::{mem, num::NonZeroUsize, time::Duration};

use tracing::debug;

use crate::{
    client_request::{ClientRequest, RequestBatch},
    output::TimeoutRequest,
    RequestPayload,
};

/// The purpose of this struct is to batch Requests.
#[derive(Debug)]
pub(crate) struct RequestBatcher<P> {
    /// The next batch of ClientRequests.
    next_batch: Vec<ClientRequest<P>>,
    /// The timeout for the batch of ClientRequests.
    timeout_duration: Duration,
    /// The maximum size for the batch of ClientRequests.
    max_size: Option<NonZeroUsize>,
}

impl<P: RequestPayload> RequestBatcher<P> {
    /// Creates a new RequestBatcher.
    pub(super) fn new(timeout: Duration, max_size: Option<NonZeroUsize>) -> Self {
        Self {
            next_batch: Vec::new(),
            timeout_duration: timeout,
            max_size,
        }
    }

    /// Returns the next batch of ClientRequests if ready.
    pub(super) fn batch(
        &mut self,
        request: ClientRequest<P>,
    ) -> (Option<RequestBatch<P>>, TimeoutRequest) {
        self.next_batch.push(request);

        match self.max_size {
            Some(max_size) if self.next_batch.len() >= max_size.get() => {
                let batch = mem::take(&mut self.next_batch);
                debug!("Reached the maximum size of batched client requests.");
                (
                    Some(RequestBatch::new(batch.into_boxed_slice())),
                    TimeoutRequest::new_stop_batch_req(),
                )
            }
            _ => (
                None,
                TimeoutRequest::new_start_batch_req(self.timeout_duration),
            ),
        }
    }

    /// Triggers the timeout for the next RequestBatch.
    /// Returns the next RequestBatch.
    pub(crate) fn timeout(&mut self) -> (Option<RequestBatch<P>>, TimeoutRequest) {
        (
            if self.next_batch.is_empty() {
                None
            } else {
                let batch = mem::take(&mut self.next_batch);
                Some(RequestBatch::new(batch.into_boxed_slice()))
            },
            TimeoutRequest::new_stop_batch_req(),
        )
    }
}
