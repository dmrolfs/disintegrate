//! Event store bound to a transaction owned by the caller.
//!
//! [`PgEventStore`] opens, validates and commits its own transaction in
//! [`EventStore::append`]. An application that must commit other writes together with the events
//! (for example an authorization check, a command result and an audit row) needs the events to be
//! read and appended on its own transaction instead. [`PgTxEventStore`] does that and never
//! begins, commits or rolls back.
//!
//! # Requirements on the caller
//!
//! * Begin the transaction with `BEGIN ISOLATION LEVEL SERIALIZABLE` (for example
//!   `pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")`). The append still issues
//!   `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`; that is a no-op on a SERIALIZABLE transaction
//!   and an error on any other transaction that has already run a query, so a weaker transaction
//!   fails instead of silently losing the concurrency check.
//! * Commit or roll back the transaction after the decision. A serialization failure may also be
//!   reported by the commit itself (SQLSTATE 40001); the caller decides how to retry.
//!
//! # Behavior to know
//!
//! * Events appended on the transaction are not returned by a later read on the same transaction,
//!   because the epoch that bounds reads does not count the transaction's own events. Use the events
//!   returned by the append.
//! * Validation relies on the transaction's snapshot. [`EventStore::append_without_validation`]
//!   inserts without reading, so a writer that uses it can commit after a concurrent validated
//!   decision without a conflict. Do not use it for event types that a validated decision reads.
//! * Snapshots are not supported on this path: the snapshotter uses its own connection pool.
use std::error::Error as StdError;

use async_stream::stream;
use async_trait::async_trait;
use disintegrate::{
    DecisionMaker, Event, EventSourcedStateStore, EventStore, NoSnapshot, PersistedEvent,
    StreamItem, StreamQuery,
};
use disintegrate_serde::Serde;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::Mutex;

use super::PgEventStore;
use crate::{Error, PgEventId};

/// A transaction owned by the caller, as returned by `Pool::begin` and `Pool::begin_with`.
pub type PgTx = sqlx::Transaction<'static, sqlx::Postgres>;

/// An [`EventStore`] whose reads and appends run on a transaction owned by the caller.
///
/// Created with [`PgEventStore::in_transaction`]. See the [module documentation](self) for the
/// requirements on the caller.
///
/// The mutex only provides `&self` access, which the [`EventStore`] trait requires. It is not
/// contended while a decision runs, because decisions are sequential. After the decision, take the
/// transaction back with [`Mutex::into_inner`] to do other writes and to commit.
pub struct PgTxEventStore<'t, E, S>
where
    S: Serde<E> + Send + Sync,
{
    store: &'t PgEventStore<E, S>,
    tx: &'t Mutex<PgTx>,
}

impl<E, S> Clone for PgTxEventStore<'_, E, S>
where
    S: Serde<E> + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            tx: self.tx,
        }
    }
}

impl<E, S> PgEventStore<E, S>
where
    E: Event + Send + Sync,
    S: Serde<E> + Send + Sync,
{
    /// Binds this store to a transaction owned by the caller.
    ///
    /// The transaction must have been begun as SERIALIZABLE. See the
    /// [module documentation](self).
    pub fn in_transaction<'t>(&'t self, tx: &'t Mutex<PgTx>) -> PgTxEventStore<'t, E, S> {
        PgTxEventStore { store: self, tx }
    }
}

#[async_trait]
impl<E, S> EventStore<PgEventId, E> for PgTxEventStore<'_, E, S>
where
    E: Event + Send + Sync,
    S: Serde<E> + Send + Sync,
{
    type Error = Error;

    fn stream<'a, QE>(
        &'a self,
        query: &'a StreamQuery<PgEventId, QE>,
    ) -> BoxStream<'a, Result<StreamItem<PgEventId, QE>, Self::Error>>
    where
        QE: TryFrom<E> + Event + 'static + Clone + Send + Sync,
        <QE as TryFrom<E>>::Error: StdError + 'static + Send + Sync,
    {
        stream! {
            let mut tx = self.tx.lock().await;
            let mut items = self.store.stream_with(&mut **tx, query);
            while let Some(item) = items.next().await {
                yield item;
            }
        }
        .boxed()
    }

    async fn append<QE>(
        &self,
        events: Vec<E>,
        query: StreamQuery<PgEventId, QE>,
        version: PgEventId,
    ) -> Result<Vec<PersistedEvent<PgEventId, E>>, Self::Error>
    where
        E: Clone + 'async_trait,
        QE: Event + Clone + Send + Sync,
    {
        let mut tx = self.tx.lock().await;
        self.store
            .append_in_tx(&mut tx, events, query, version)
            .await
    }

    async fn append_without_validation(
        &self,
        events: Vec<E>,
    ) -> Result<Vec<PersistedEvent<PgEventId, E>>, Self::Error>
    where
        E: Clone + 'async_trait,
    {
        let mut tx = self.tx.lock().await;
        self.store
            .append_without_validation_in_tx(&mut tx, events)
            .await
    }
}

/// A decision maker that loads and persists on a transaction owned by the caller.
///
/// Snapshots are off by construction.
pub type PgTxDecisionMaker<'t, E, S> =
    DecisionMaker<EventSourcedStateStore<PgEventId, E, PgTxEventStore<'t, E, S>, NoSnapshot>>;

/// Creates a [`PgTxDecisionMaker`] over the given transaction-bound store.
pub fn tx_decision_maker<'t, E, S>(store: PgTxEventStore<'t, E, S>) -> PgTxDecisionMaker<'t, E, S>
where
    E: Event + Clone + Send + Sync + 'static,
    S: Serde<E> + Send + Sync,
{
    DecisionMaker::new(EventSourcedStateStore::new(store, NoSnapshot))
}
