use std::ops::{Deref, DerefMut};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared_ids::ClientId;
use tracing::debug;

use crate::RequestPayload;

/// Defines a ClientRequest.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientRequest<P> {
    pub(crate) client: ClientId,
    pub(crate) payload: P,
}

impl<P> Deref for ClientRequest<P> {
    type Target = P;

    /// Returns a reference to the payload of the ClientRequest.
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<P> DerefMut for ClientRequest<P> {
    /// Returns a mutable reference to the payload of the ClientRequest.
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        &mut self.payload
    }
}

impl<P: RequestPayload> ClientRequest<P> {
    /// Validates the RequestPayload.
    pub(crate) fn validate(&self) -> Result<()> {
        self.verify(self.client)
    }
}

/// Defines a RequestBatch.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct RequestBatch<P> {
    /// The batch of ClientRequests.
    pub(crate) batch: Box<[ClientRequest<P>]>,
}

impl<P: RequestPayload> RequestBatch<P> {
    /// Creates a new RequestBatch with the given batch of ClientRequests.
    pub(crate) fn new(batch: Box<[ClientRequest<P>]>) -> Self {
        Self { batch }
    }

    /// Validates the RequestBatch.
    pub(crate) fn validate(&self) -> Result<()> {
        debug!("Validating batch of requests ...");
        for request in self.batch.iter() {
            debug!(
                "Validating client request (ID: {:?}, client ID: {:?}) contained in batch ...",
                request.id(),
                request.client
            );
            request.validate()?;
            debug!("Successfully validated client request (ID: {:?}, client ID: {:?}) contained in batch.", request.id(), request.client);
        }
        debug!("Successfully validated batch of requests.");
        Ok(())
    }
}

impl<P> IntoIterator for RequestBatch<P> {
    type Item = ClientRequest<P>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    /// Converts the RequestBatch into an Iterator.
    fn into_iter(self) -> Self::IntoIter {
        let batch = Vec::from(self.batch); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
        batch.into_iter()
    }
}

#[cfg(test)]

pub(crate) mod test {
    use rand::{rngs::ThreadRng, Rng};
    use shared_ids::ClientId;

    use crate::tests::DummyPayload;

    use super::{ClientRequest, RequestBatch};

    pub(crate) fn create_invalid_client_req(rng: &mut ThreadRng) -> ClientRequest<DummyPayload> {
        let rand_client_id = rng.gen::<u64>();
        let rand_req_id = rng.gen::<u64>();
        ClientRequest {
            client: ClientId::from_u64(rand_client_id),
            payload: DummyPayload(rand_req_id, false),
        }
    }

    pub(crate) fn create_batch() -> RequestBatch<DummyPayload> {
        let mut batch = Vec::new();
        for i in 0..10 {
            let client_req = ClientRequest {
                client: ClientId::from_u64(i),
                payload: DummyPayload(i, true),
            };
            batch.push(client_req);
        }
        let batch: [ClientRequest<DummyPayload>; 10] = batch.try_into().unwrap();
        let batch = Box::new(batch);
        RequestBatch { batch }
    }
}
