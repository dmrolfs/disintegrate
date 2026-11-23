use super::append::InsertEventsBuilder;
use crate::{Error, PgEventId, PgEventStore, PgTx};
use disintegrate::{
    domain_ids, ident, query, DomainIdInfo, DomainIdSet, Event, EventInfo, EventSchema, EventStore,
    IdentifierType,
};
use disintegrate_serde::serde::json::Json;
use disintegrate_serde::Deserializer;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum ShoppingCartEvent {
    Added { product_id: String, cart_id: String },
    Removed { product_id: String, cart_id: String },
}
fn added_event(product_id: &str, cart_id: &str) -> ShoppingCartEvent {
    ShoppingCartEvent::Added {
        product_id: product_id.to_string(),
        cart_id: cart_id.to_string(),
    }
}
fn removed_event(product_id: &str, cart_id: &str) -> ShoppingCartEvent {
    ShoppingCartEvent::Removed {
        product_id: product_id.to_string(),
        cart_id: cart_id.to_string(),
    }
}

impl Event for ShoppingCartEvent {
    const SCHEMA: EventSchema = EventSchema {
        events: &["ShoppingCartAdded", "ShoppingCartRemoved"],
        events_info: &[
            &EventInfo {
                name: "ShoppingCartAdded",
                domain_ids: &[&ident!(#product_id), &ident!(#cart_id)],
            },
            &EventInfo {
                name: "ShoppingCartRemoved",
                domain_ids: &[&ident!(#product_id), &ident!(#cart_id)],
            },
        ],
        domain_ids: &[
            &DomainIdInfo {
                ident: ident!(#cart_id),
                type_info: IdentifierType::String,
            },
            &DomainIdInfo {
                ident: ident!(#product_id),
                type_info: IdentifierType::String,
            },
        ],
    };
    fn name(&self) -> &'static str {
        match self {
            ShoppingCartEvent::Added { .. } => "ShoppingCartAdded",
            ShoppingCartEvent::Removed { .. } => "ShoppingCartRemoved",
        }
    }
    fn domain_ids(&self) -> DomainIdSet {
        match self {
            ShoppingCartEvent::Added {
                product_id,
                cart_id,
                ..
            } => domain_ids! {product_id: product_id, cart_id: cart_id},
            ShoppingCartEvent::Removed {
                product_id,
                cart_id,
                ..
            } => domain_ids! {product_id: product_id, cart_id: cart_id},
        }
    }
}

#[sqlx::test]
async fn it_queries_events(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    let events = vec![
        added_event("product_1", "cart_1"),
        removed_event("product_1", "cart_1"),
        added_event("product_2", "cart_1"),
        added_event("product_2", "cart_1"),
    ];
    insert_events(&pool, &events).await;

    // Test the stream function
    let query = query!(ShoppingCartEvent; product_id == "product_1");
    let result = event_store.stream(&query).collect::<Vec<_>>().await;

    assert_eq!(result.len(), 3);
}

#[sqlx::test]
async fn it_appends_events(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();
    let events: Vec<ShoppingCartEvent> = vec![
        added_event("product_1", "cart_1"),
        removed_event("product_2", "cart_1"),
    ];

    let query = query!(ShoppingCartEvent; cart_id == "cart_1");

    event_store.append(events, query.clone(), 0).await.unwrap();

    let stored_events = sqlx::query("SELECT event_id, event_type, payload FROM event")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(stored_events.len(), 2);
    assert_event_row(
        stored_events.first().unwrap(),
        1,
        "ShoppingCartAdded",
        added_event("product_1", "cart_1"),
    );
    assert_event_row(
        stored_events.get(1).unwrap(),
        2,
        "ShoppingCartRemoved",
        removed_event("product_2", "cart_1"),
    );
}

#[sqlx::test]
async fn it_appends_unchecked(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();
    let events: Vec<ShoppingCartEvent> = vec![
        added_event("product_1", "cart_1"),
        removed_event("product_2", "cart_1"),
    ];

    event_store.append_without_validation(events).await.unwrap();

    let stored_events = sqlx::query("SELECT event_id, event_type, payload FROM event")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(stored_events.len(), 2);
    assert_event_row(
        stored_events.first().unwrap(),
        1,
        "ShoppingCartAdded",
        added_event("product_1", "cart_1"),
    );
    assert_event_row(
        stored_events.get(1).unwrap(),
        2,
        "ShoppingCartRemoved",
        removed_event("product_2", "cart_1"),
    );
}

#[track_caller]
fn assert_event_row(
    row: &PgRow,
    event_id: PgEventId,
    event_type: &str,
    payload: ShoppingCartEvent,
) {
    let stored_event_id: PgEventId = row.get(0);
    assert_eq!(stored_event_id, event_id);
    let stored_event_type: String = row.get(1);
    assert_eq!(stored_event_type, event_type);
    let stored_payload: Vec<u8> = row.get(2);
    assert_eq!(
        Json::<ShoppingCartEvent>::default()
            .deserialize(stored_payload)
            .unwrap(),
        payload
    );
}

#[sqlx::test]
async fn it_returns_a_concurrency_error_when_it_appends_events_of_a_query_which_its_events_have_been_changed_and_event_store_is_empty(
    pool: PgPool,
) {
    let event_store =
        PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(pool, Json::default())
            .await
            .unwrap();

    let query = query!(ShoppingCartEvent; product_id == "product_1", cart_id == "cart_1");
    event_store
        .append(vec![added_event("product_1", "cart_1")], query, 0)
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; product_id == "product_1", cart_id == "cart_1");
    let result = event_store
        .append(vec![removed_event("product_1", "cart_1")], query, 0)
        .await;
    assert!(matches!(result, Err(Error::Concurrency)));
}

#[sqlx::test]
async fn it_returns_a_concurrency_error_when_it_appends_events_of_a_query_which_its_events_have_been_changed(
    pool: PgPool,
) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();
    let events = vec![
        added_event("product_1", "cart_1"),
        removed_event("product_1", "cart_1"),
    ];
    insert_events(&pool, &events).await;

    let query_1 = query!(ShoppingCartEvent; cart_id == "cart_1");
    let query_1_result = event_store
        .stream(&query_1)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let query_2 = query!(ShoppingCartEvent; product_id == "product_1", cart_id == "cart_1");
    let query_2_result = event_store
        .stream(&query_2)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let _result = event_store
        .append(
            vec![removed_event("product_1", "cart_1")],
            query_1,
            query_1_result.last().unwrap().id(),
        )
        .await
        .unwrap();

    let result = event_store
        .append(
            vec![removed_event("product_1", "cart_1")],
            query_2,
            query_2_result.last().unwrap().id(),
        )
        .await;

    assert!(matches!(result, Err(Error::Concurrency)));
}

/// Tests that a reader does not skip events when two writers commit out of sequence order.
///
/// Scenario:
/// 1. Insert an initial event (event_id = 1)
/// 2. Writer A starts a transaction and appends an event (gets event_id = 2), but does not commit
/// 3. Writer B starts a transaction, appends an event (gets event_id = 3), and commits
/// 4. A reader streams events — event 3 is committed but event 2 is not yet visible
/// 5. Writer A commits
/// 6. A second reader streams all events
/// 7. We verify that all events from the first read appear in the second read,
///    and any new events in the second read have IDs greater than the max from the first read
#[sqlx::test]
async fn stream_does_not_skip_events_when_writers_commit_out_of_order(pool: PgPool) {
    use disintegrate::StreamItem;
    use std::sync::Arc;

    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    // Insert an initial event so we have a baseline
    let init_query = query!(ShoppingCartEvent; cart_id == "cart_1");
    event_store
        .append(vec![added_event("product_0", "cart_1")], init_query, 0)
        .await
        .unwrap();

    let writer_a_inserted = Arc::new(tokio::sync::Notify::new());
    let reader_done = Arc::new(tokio::sync::Notify::new());

    // Writer A: append in tx but delay commit until reader is done
    let writer_a = tokio::spawn({
        let event_store = event_store.clone();
        let pool = pool.clone();
        let writer_a_inserted = writer_a_inserted.clone();
        let reader_done = reader_done.clone();
        async move {
            let mut tx = pool.begin().await.unwrap();
            let query_a = query!(ShoppingCartEvent; cart_id == "cart_a");
            event_store
                .append_in_tx(
                    &mut tx,
                    vec![added_event("product_a", "cart_a")],
                    query_a,
                    0,
                )
                .await
                .unwrap();

            // Signal: Writer A has appended (but not committed)
            writer_a_inserted.notify_one();

            // Wait until the reader has finished reading
            reader_done.notified().await;

            // Now commit
            tx.commit().await.unwrap();
        }
    });

    // Writer B: wait for Writer A to insert, then append and commit immediately
    let writer_b = tokio::spawn({
        let event_store = event_store.clone();
        let writer_a_inserted = writer_a_inserted.clone();
        async move {
            // Wait for Writer A to insert (but not commit)
            writer_a_inserted.notified().await;

            let query_b = query!(ShoppingCartEvent; cart_id == "cart_b");
            event_store
                .append(vec![added_event("product_b", "cart_b")], query_b, 0)
                .await
                .unwrap();
        }
    });

    // Wait for Writer B to commit (Writer A is still uncommitted)
    writer_b.await.unwrap();

    // Reader: stream events now. Writer B's event is committed, Writer A's is not.
    let read_query = query!(ShoppingCartEvent);
    let first_stream_events: Vec<_> = event_store
        .stream(&read_query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|item| match item {
            Ok(StreamItem::Event(e)) => Some(Ok(e.id())),
            Err(err) => Some(Err(err)),
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let max_event_id_first_stream = first_stream_events.iter().max().unwrap();

    // Signal Writer A to commit
    reader_done.notify_one();
    writer_a.await.unwrap();

    // Second read: full read to see all events
    let second_stream_events: Vec<_> = event_store
        .stream(&read_query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|item| match item {
            Ok(StreamItem::Event(e)) => Some(Ok(e.id())),
            Err(err) => Some(Err(err)),
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let second_stream_new_events: Vec<i64> = second_stream_events
        .iter()
        .filter(|id| !first_stream_events.contains(id))
        .copied()
        .collect();

    assert!(first_stream_events
        .iter()
        .all(|e| second_stream_events.contains(e)));
    assert!(second_stream_new_events
        .iter()
        .all(|id| id > max_event_id_first_stream));
}

/// Tests that `event_store_current_epoch()` never exceeds the max committed event_id.
///
/// Scenario:
/// 1. Insert one event (event_id = 1) — the only committed event
/// 2. Writer A starts a transaction and appends (gets event_id = 2), does NOT commit
/// 3. Writer B starts a transaction and appends (gets event_id = 3), does NOT commit
/// 4. Writer A rolls back — its advisory lock is released
/// 5. A reader streams events
/// 6. We assert epoch (the End item) <= 1 (the known max committed event_id).
#[sqlx::test]
async fn epoch_does_not_exceed_max_committed_event_id(pool: PgPool) {
    use disintegrate::StreamItem;
    use std::sync::Arc;

    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    // Insert one committed event (event_id = 1)
    let init_query = query!(ShoppingCartEvent; cart_id == "cart_1");
    event_store
        .append(vec![added_event("product_0", "cart_1")], init_query, 0)
        .await
        .unwrap();

    const MAX_COMMITTED: PgEventId = 1;

    let writer_a_inserted = Arc::new(tokio::sync::Notify::new());
    let writer_b_inserted = Arc::new(tokio::sync::Notify::new());
    let writer_a_rolled_back = Arc::new(tokio::sync::Notify::new());
    let reader_done = Arc::new(tokio::sync::Notify::new());

    // Writer A: append in tx, then rollback after Writer B inserts
    let writer_a = tokio::spawn({
        let event_store = event_store.clone();
        let pool = pool.clone();
        let writer_a_inserted = writer_a_inserted.clone();
        let writer_b_inserted = writer_b_inserted.clone();
        let writer_a_rolled_back = writer_a_rolled_back.clone();
        async move {
            let mut tx = pool.begin().await.unwrap();
            let query_a = query!(ShoppingCartEvent; cart_id == "cart_a");
            event_store
                .append_in_tx(
                    &mut tx,
                    vec![added_event("product_a", "cart_a")],
                    query_a,
                    0,
                )
                .await
                .unwrap();

            // Signal: Writer A has appended (but not committed)
            writer_a_inserted.notify_one();

            // Wait for Writer B to insert
            writer_b_inserted.notified().await;

            // Rollback — releases Writer A's advisory lock
            tx.rollback().await.unwrap();
            writer_a_rolled_back.notify_one();
        }
    });

    // Writer B: wait for Writer A, then append in tx and hold open
    let writer_b = tokio::spawn({
        let event_store = event_store.clone();
        let pool = pool.clone();
        let writer_a_inserted = writer_a_inserted.clone();
        let writer_b_inserted = writer_b_inserted.clone();
        let reader_done = reader_done.clone();
        async move {
            writer_a_inserted.notified().await;

            let mut tx = pool.begin().await.unwrap();
            let query_b = query!(ShoppingCartEvent; cart_id == "cart_b");
            event_store
                .append_in_tx(
                    &mut tx,
                    vec![added_event("product_b", "cart_b")],
                    query_b,
                    0,
                )
                .await
                .unwrap();

            // Signal: Writer B has appended
            writer_b_inserted.notify_one();

            // Hold transaction open until reader is done
            reader_done.notified().await;
            tx.rollback().await.unwrap();
        }
    });

    // Wait for Writer A to rollback
    writer_a_rolled_back.notified().await;
    writer_a.await.unwrap();

    // Reader: stream events. Writer A rolled back, Writer B still in-flight.
    let read_query = query!(ShoppingCartEvent);
    let stream_results: Vec<_> = event_store
        .stream(&read_query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let epoch = stream_results
        .iter()
        .find_map(|item| match item {
            StreamItem::End(id) => Some(*id),
            _ => None,
        })
        .expect("stream should yield an End item");

    assert!(
        epoch <= MAX_COMMITTED,
        "epoch {epoch} exceeds max committed event_id {MAX_COMMITTED}"
    );

    // Cleanup
    reader_done.notify_one();
    writer_b.await.unwrap();
}

pub async fn insert_events<E: Event + Clone + Serialize + DeserializeOwned>(
    pool: &PgPool,
    events: &[E],
) {
    let serde = disintegrate_serde::serde::json::Json::default();
    InsertEventsBuilder::new(events, &serde)
        .build()
        .execute(pool)
        .await
        .unwrap();
}

type CartStore = PgEventStore<ShoppingCartEvent, Json<ShoppingCartEvent>>;

async fn serializable_tx(pool: &PgPool) -> tokio::sync::Mutex<PgTx> {
    tokio::sync::Mutex::new(
        pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap(),
    )
}

async fn event_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM event")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test]
async fn tx_store_appends_on_the_callers_transaction_and_never_commits(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; cart_id == "cart_1");

    // Rolled back by the caller: nothing is stored, and nothing is visible before the commit.
    let tx = serializable_tx(&pool).await;
    let tx_store = store.in_transaction(&tx);
    let persisted = tx_store
        .append(vec![added_event("product_1", "cart_1")], query.clone(), 0)
        .await
        .unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(event_count(&pool).await, 0);
    tx.into_inner().rollback().await.unwrap();
    assert_eq!(event_count(&pool).await, 0);

    // Committed by the caller: the event is stored.
    let tx = serializable_tx(&pool).await;
    store
        .in_transaction(&tx)
        .append(vec![added_event("product_1", "cart_1")], query, 0)
        .await
        .unwrap();
    tx.into_inner().commit().await.unwrap();
    assert_eq!(event_count(&pool).await, 1);
}

#[sqlx::test]
async fn tx_store_shares_the_transaction_with_other_caller_writes(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE audit (note TEXT)")
        .execute(&pool)
        .await
        .unwrap();

    let tx = serializable_tx(&pool).await;
    store
        .in_transaction(&tx)
        .append(
            vec![added_event("product_1", "cart_1")],
            query!(ShoppingCartEvent; cart_id == "cart_1"),
            0,
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO audit VALUES ('appended')")
        .execute(&mut **tx.lock().await)
        .await
        .unwrap();
    tx.into_inner().rollback().await.unwrap();

    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM audit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((event_count(&pool).await, audit), (0, 0));
}

#[sqlx::test]
async fn tx_store_refuses_a_transaction_that_is_not_serializable(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();

    let tx = tokio::sync::Mutex::new(pool.begin().await.unwrap());
    sqlx::query("SELECT 1")
        .execute(&mut **tx.lock().await)
        .await
        .unwrap();
    let result = store
        .in_transaction(&tx)
        .append(
            vec![added_event("product_1", "cart_1")],
            query!(ShoppingCartEvent; cart_id == "cart_1"),
            0,
        )
        .await;

    assert!(matches!(result, Err(Error::Database(_))));
    tx.into_inner().rollback().await.unwrap();
    assert_eq!(event_count(&pool).await, 0);
}

#[sqlx::test]
async fn tx_store_rejects_a_decision_made_before_a_matching_event_committed(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; cart_id == "cart_1");

    let tx = serializable_tx(&pool).await;
    let tx_store = store.in_transaction(&tx);
    let seen = tx_store
        .stream(&query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let version = seen.last().map_or(0, |item| item.id());

    // A validated rival commits a matching event after the caller's snapshot was taken.
    store
        .append(vec![added_event("product_9", "cart_1")], query.clone(), 0)
        .await
        .unwrap();

    // The rival's event is invisible to the caller's snapshot, so the conflict is reported by
    // PostgreSQL as a serialization failure (SQLSTATE 40001), here or at commit, not as `Concurrency`.
    let appended = tx_store
        .append(vec![added_event("product_1", "cart_1")], query, version)
        .await;
    let committed = match appended {
        Ok(_) => tx.into_inner().commit().await.is_ok(),
        Err(_) => false,
    };

    assert!(!committed, "the stale decision must not commit");
    assert_eq!(event_count(&pool).await, 1);
}

#[sqlx::test]
async fn tx_store_lets_exactly_one_of_two_overlapping_decisions_commit(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; cart_id == "cart_1");

    let (a, b) = (serializable_tx(&pool).await, serializable_tx(&pool).await);
    store
        .in_transaction(&a)
        .append(vec![added_event("product_1", "cart_1")], query.clone(), 0)
        .await
        .unwrap();
    store
        .in_transaction(&b)
        .append(vec![added_event("product_2", "cart_1")], query, 0)
        .await
        .unwrap();
    let committed = [
        a.into_inner().commit().await.is_ok(),
        b.into_inner().commit().await.is_ok(),
    ];

    assert_eq!(committed.iter().filter(|ok| **ok).count(), 1);
    assert_eq!(event_count(&pool).await, 1);
}

#[sqlx::test]
async fn tx_store_does_not_return_its_own_events_to_a_later_read(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; cart_id == "cart_1");
    // An earlier committed event, so the new event's id is above the epoch marker. In an empty store
    // the first event has the same id as the marker and a re-read does return it; callers must not
    // rely on either behavior and should use the events returned by the append.
    insert_events(&pool, &[added_event("product_0", "cart_1")]).await;

    let tx = serializable_tx(&pool).await;
    let tx_store = store.in_transaction(&tx);
    tx_store
        .append(vec![added_event("product_1", "cart_1")], query.clone(), 1)
        .await
        .unwrap();
    let events = tx_store
        .stream(&query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .filter(|item| matches!(item, disintegrate::StreamItem::Event(_)))
        .count();

    assert_eq!(
        events, 1,
        "only the earlier committed event, not the new one"
    );
    tx.into_inner().rollback().await.unwrap();
}

#[sqlx::test]
async fn tx_store_does_not_detect_an_unvalidated_rival_append(pool: PgPool) {
    let store = CartStore::try_new(pool.clone(), Json::default())
        .await
        .unwrap();
    let query = query!(ShoppingCartEvent; cart_id == "cart_1");

    let tx = serializable_tx(&pool).await;
    let tx_store = store.in_transaction(&tx);
    let seen = tx_store
        .stream(&query)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let version = seen.last().map_or(0, |item| item.id());

    // A rival inserts a matching event without validation (and without reading) and commits.
    store
        .append_without_validation(vec![added_event("product_9", "cart_1")])
        .await
        .unwrap();

    // Documented behavior: the caller's snapshot does not contain the rival's event and the rival
    // read nothing, so PostgreSQL finds no conflict and the stale decision commits. The store's own
    // `append`, which validates in a fresh transaction, returns `Error::Concurrency` here.
    tx_store
        .append(
            vec![added_event("product_1", "cart_1")],
            query.clone(),
            version,
        )
        .await
        .unwrap();
    tx.into_inner().commit().await.unwrap();
    assert_eq!(event_count(&pool).await, 2);

    let own_path = store
        .append(vec![added_event("product_2", "cart_1")], query, version)
        .await;
    assert!(matches!(own_path, Err(Error::Concurrency)));
}
