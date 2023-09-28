use std::ops::{Deref, DerefMut};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared_ids::ClientId;

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
        for request in self.batch.iter() {
            request.validate()?;
        }
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
