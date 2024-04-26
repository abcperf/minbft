//! Defines how messages are signed by the USIG, and wraps them in a
//! respective struct ([UsigSigned]).

use std::ops::{Deref, DerefMut};

use std::fmt::Debug;

use anyhow::Result;
use blake2::digest::Update;
use blake2::{Blake2b512, Digest};
use serde::{Deserialize, Serialize};
use usig::{Count, Counter, Usig, UsigError};

use crate::ReplicaId;

/// Defines a signed UsigMessage.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct UsigSigned<T, Sig> {
    /// The data of the signed [crate::UsigMessage].
    pub(crate) data: T,
    /// The signature of the signed [crate::UsigMessage].
    signature: Sig,
}

impl<T, Sig: Clone> UsigSigned<T, Sig> {
    /// Clones the signature of the [UsigSigned].
    pub(super) fn clone_signature<D>(&self, data: D) -> UsigSigned<D, Sig> {
        UsigSigned {
            data,
            signature: self.signature.clone(),
        }
    }
}

impl<T, Sig> Deref for UsigSigned<T, Sig> {
    type Target = T;

    /// Dereferencing a [UsigSigned] returns a reference to the data of the
    /// [UsigSigned].
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T, Sig> DerefMut for UsigSigned<T, Sig> {
    /// Mutably dereferencing [UsigSigned] returns a mutable reference to the
    /// data of the [UsigSigned].
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T, Sig: Counter> Counter for UsigSigned<T, Sig> {
    /// Returns the counter of the [UsigSigned] (the counter of the signature).
    fn counter(&self) -> Count {
        self.signature.counter()
    }
}

pub(crate) trait UsigSignable: AsRef<ReplicaId> {
    /// Hashes the [UsigSignable].
    /// Required for signing and verifying a [UsigSignable].
    fn hash_content<H: Update>(&self, hasher: &mut H);
}

impl<T: UsigSignable, Sig> UsigSigned<T, Sig> {
    /// Signs the [UsigSignable].
    pub(crate) fn sign(data: T, usig: &mut impl Usig<Signature = Sig>) -> Result<Self, UsigError> {
        let mut hasher = Blake2b512::new();
        data.hash_content(&mut hasher);
        let signature = usig.sign(hasher.finalize())?;
        Ok(Self { data, signature })
    }

    /// Verifies the [UsigSignable].
    pub(crate) fn verify(&self, usig: &mut impl Usig<Signature = Sig>) -> Result<(), UsigError> {
        let mut hasher = Blake2b512::new();
        self.data.hash_content(&mut hasher);
        usig.verify(*self.as_ref(), hasher.finalize(), &self.signature)
    }
}
