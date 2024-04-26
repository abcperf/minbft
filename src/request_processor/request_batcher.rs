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
    ///
    /// # Arguments
    ///
    /// * `timeout` - The timeout duration before the batch of requests
    ///               should be handled.
    /// * `max_size` - The maximum size of requests that a batch should hold.
    ///                If the maximum size is reached before the duration,
    ///                the batch should be handled right away.
    ///
    /// # Return Value
    ///
    /// The created [RequestBatcher].
    pub(super) fn new(timeout: Duration, max_size: Option<NonZeroUsize>) -> Self {
        Self {
            next_batch: Vec::new(),
            timeout_duration: timeout,
            max_size,
        }
    }

    /// Batches the provided [ClientRequest].
    ///
    /// # Arguments
    ///
    /// * `request` - The [ClientRequest] to batch.
    ///
    /// # Return Value
    ///
    /// If the next batch of [ClientRequest]s is ready, it is returned.
    /// Additionally, the timeout request to stop the batch timeout is returned.
    /// Otherwise, only a new timeout for the batch request is sent.
    ///
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
    ///
    /// # Return Value
    ///
    /// Returns the next batch of requests and a request to stop the current
    /// batch timeout.
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
