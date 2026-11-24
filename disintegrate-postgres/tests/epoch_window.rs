//! A reader must not be told an epoch that covers a writer whose commit its scan cannot see.
//!
//! The epoch function is parked behind an advisory lock so the window between the reader's snapshot and the
//! function's own reads is wide enough to hit on purpose. The window exists without the lock; it is just short.
use disintegrate::{query, Event, EventStore, StateMutate, StateQuery, StreamItem};
use disintegrate_postgres::PgEventStore;
use disintegrate_serde::serde::json::Json;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Event, Serialize, Deserialize)]
#[stream(BalanceEvent, [Deposited])]
enum DomainEvent {
    Deposited {
        #[id]
        account_id: i64,
        amount: i32,
    },
}

#[derive(Default, StateQuery, Clone, Debug, Serialize, Deserialize)]
#[state_query(BalanceEvent)]
struct Balance {
    #[id]
    account_id: i64,
    total: i32,
}

impl StateMutate for Balance {
    fn mutate(&mut self, event: Self::Event) {
        match event {
            BalanceEvent::Deposited { amount, .. } => self.total += amount,
        }
    }
}

const GATE_KEY: i64 = 4242;

/// Re-creates `event_store_current_epoch()` with one extra line that waits on the gate key.
async fn park_epoch_function(pool: &PgPool) {
    let body: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('event_store_current_epoch()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let gated = body.replacen(
        "BEGIN\n",
        &format!("BEGIN\n    PERFORM pg_advisory_xact_lock({GATE_KEY});\n"),
        1,
    );
    assert_ne!(gated, body, "the gate line must have been inserted");
    sqlx::raw_sql(sqlx::AssertSqlSafe(gated)).execute(pool).await.unwrap();
}

async fn wait_until_parked(pool: &PgPool) {
    for _ in 0..200 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND objid = $1",
        )
        .bind(GATE_KEY as i32)
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the reader never reached the epoch function");
}

#[sqlx::test]
async fn a_writer_that_commits_while_the_epoch_is_computed_is_delivered(pool: PgPool) {
    let store = PgEventStore::<DomainEvent, Json<DomainEvent>>::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    park_epoch_function(&pool).await;

    // Writer: marks an in-flight append and inserts event 1, not yet committed.
    let mut writer = pool.begin().await.unwrap();
    sqlx::query("SELECT event_store_begin_epoch()").execute(&mut *writer).await.unwrap();
    sqlx::query(
        "INSERT INTO event (event_type, payload, account_id) VALUES ('Deposited', $1, 1)",
    )
    .bind(br#"{"Deposited":{"account_id":1,"amount":1}}"#.to_vec())
    .execute(&mut *writer)
    .await
    .unwrap();

    // The test holds the gate, so the reader parks inside the epoch function.
    let mut gate = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)").bind(GATE_KEY).execute(&mut *gate).await.unwrap();
    let reader = tokio::spawn({
        let store = store.clone();
        async move {
            let query = query!(DomainEvent);
            let mut items = store.stream(&query);
            let mut seen = Vec::new();
            let mut end = None;
            while let Some(item) = items.next().await {
                match item.unwrap() {
                    StreamItem::Event(event) => seen.push(event.id()),
                    StreamItem::End(epoch) => end = Some(epoch),
                }
            }
            (seen, end)
        }
    });
    wait_until_parked(&pool).await;

    writer.commit().await.unwrap();
    sqlx::query("SELECT pg_advisory_unlock($1)").bind(GATE_KEY).execute(&mut *gate).await.unwrap();

    let (seen, end) = reader.await.unwrap();
    assert_eq!(
        (seen, end),
        (vec![1], Some(1)),
        "an epoch that covers event 1 must come with event 1, or a listener checkpoints past it"
    );
}

/// A listener-shaped cursor: read everything above `cursor`, return the ids and the epoch the read ended at.
async fn read_from(store: &PgEventStore<DomainEvent, Json<DomainEvent>>, cursor: i64) -> (Vec<i64>, i64) {
    let query = query!(DomainEvent).change_origin(cursor);
    let mut items = store.stream(&query);
    let (mut seen, mut end) = (Vec::new(), cursor);
    while let Some(item) = items.next().await {
        match item.unwrap() {
            StreamItem::Event(event) => seen.push(event.id()),
            StreamItem::End(epoch) => end = epoch,
        }
    }
    (seen, end)
}

async fn append_open(pool: &PgPool) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT event_store_begin_epoch()").execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO event (event_type, payload, account_id) VALUES ('Deposited', $1, 1)")
        .bind(br#"{"Deposited":{"account_id":1,"amount":1}}"#.to_vec())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx
}

/// On a sequence that has never been used the first writer's id equals the sequence's `last_value`, so its marker must
/// be 0, not `last_value`. Otherwise a second writer that commits id 2 lets a listener checkpoint 1 and skip event 1.
#[sqlx::test]
async fn the_first_event_of_a_new_database_is_not_skipped_when_a_second_writer_commits_first(pool: PgPool) {
    let store = PgEventStore::<DomainEvent, Json<DomainEvent>>::try_new(pool.clone(), Json::default())
        .await
        .unwrap();

    let first = append_open(&pool).await; // draws id 1 and stays open
    let second = append_open(&pool).await; // draws id 2
    second.commit().await.unwrap();

    let (seen, cursor) = read_from(&store, 0).await;
    first.commit().await.unwrap();
    let (later, _) = read_from(&store, cursor).await;

    let mut delivered = seen;
    delivered.extend(later);
    assert_eq!(delivered, vec![1, 2], "event 1 committed and must be delivered (cursor was {cursor})");
}
