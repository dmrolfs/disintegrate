use super::*;

use async_trait::async_trait;
use disintegrate::{
    domain_ids, ident, query, DomainIdInfo, DomainIdSet, EventInfo, EventSchema, EventStore,
    IdentifierType, PersistedEvent, StreamQuery,
};
use disintegrate_serde::serde::json::Json;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum ShoppingCartEvent {
    Added(CartEventPayload),
    Removed(CartEventPayload),
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
            ShoppingCartEvent::Added(payload) | ShoppingCartEvent::Removed(payload) => {
                domain_ids! {product_id: payload.product_id, cart_id: payload.cart_id}
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CartEventPayload {
    cart_id: String,
    product_id: String,
    quantity: i64,
}

#[derive(FromRow, Debug)]
struct Cart {
    cart_id: String,
    product_id: String,
    quantity: i32,
}

impl Cart {
    async fn carts(pool: &PgPool) -> Result<Vec<Cart>, sqlx::Error> {
        sqlx::query_as::<_, Cart>("SELECT cart_id, product_id, quantity FROM carts")
            .fetch_all(pool)
            .await
    }
}

struct CartEventHandler {
    query: StreamQuery<PgEventId, ShoppingCartEvent>,
    pool: PgPool,
}

impl CartEventHandler {
    async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS carts (
           product_id TEXT,
           cart_id TEXT,
           quantity INT 
        )"#,
        )
        .execute(&pool)
        .await?;
        Ok(Self {
            query: query!(ShoppingCartEvent),
            pool,
        })
    }
}

#[async_trait]
impl EventListener<PgEventId, ShoppingCartEvent> for CartEventHandler {
    type Error = sqlx::Error;
    fn id(&self) -> &'static str {
        "carts"
    }

    fn query(&self) -> &StreamQuery<PgEventId, ShoppingCartEvent> {
        &self.query
    }

    async fn handle(
        &self,
        persisted_event: PersistedEvent<PgEventId, ShoppingCartEvent>,
    ) -> Result<(), Self::Error> {
        match persisted_event.into_inner() {
            ShoppingCartEvent::Added(payload) => {
                sqlx::query("INSERT INTO carts (cart_id, product_id, quantity) VALUES($1, $2, $3)")
                    .bind(payload.cart_id.clone())
                    .bind(payload.product_id.clone())
                    .bind(payload.quantity)
                    .execute(&self.pool)
                    .await
                    .unwrap();
            }
            ShoppingCartEvent::Removed(_) => unimplemented!(),
        }
        Ok(())
    }
}

#[sqlx::test]
async fn it_handles_events(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    let event_handler_executor = PgEventListerExecutor::new(
        event_store.clone(),
        CartEventHandler::new(pool.clone()).await.unwrap(),
        CancellationToken::new(),
        PgEventListenerConfig::poller(Duration::from_secs(1)),
    );

    let cart_id = "cart_1".to_string();
    let product_id = "product_1".to_string();
    let query = query!(ShoppingCartEvent; cart_id == cart_id, product_id == product_id);
    let _result = event_store
        .append(
            vec![ShoppingCartEvent::Added(CartEventPayload {
                cart_id,
                product_id,
                quantity: 1,
            })],
            query,
            0,
        )
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    event_handler_executor
        .handle_events_from(0, &mut tx)
        .await
        .unwrap();

    let carts = Cart::carts(&pool).await.unwrap();
    assert_eq!(carts.len(), 1);
    let first_row = carts.first().unwrap();
    assert_eq!("cart_1", &first_row.cart_id);
    assert_eq!("product_1", &first_row.product_id);
    assert_eq!(1, first_row.quantity);
}

#[sqlx::test]
async fn it_runs_event_listeners(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    let cart_id = "cart_1".to_string();
    let product_id = "product_1".to_string();
    let query = query!(ShoppingCartEvent; cart_id == cart_id, product_id == product_id);
    let append_result = event_store
        .append(
            vec![ShoppingCartEvent::Added(CartEventPayload {
                cart_id,
                product_id,
                quantity: 1,
            })],
            query,
            0,
        )
        .await;

    PgEventListener::builder(event_store.clone())
        .register_listener(
            CartEventHandler::new(pool.clone()).await.unwrap(),
            PgEventListenerConfig::poller(Duration::from_millis(10)),
        )
        .start_with_shutdown(async {
            tokio::time::sleep(Duration::from_millis(400)).await;
        })
        .await
        .unwrap();

    assert!(append_result.is_ok());
    let carts = Cart::carts(&pool).await.unwrap();
    assert_eq!(carts.len(), 1);
    let first_row = carts.first().unwrap();
    assert_eq!("cart_1", &first_row.cart_id);
    assert_eq!("product_1", &first_row.product_id);
    assert_eq!(1, first_row.quantity);
}

#[sqlx::test]
async fn it_runs_event_listener_with_db_listener(pool: PgPool) {
    let event_store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();

    let cart_id = "cart_1".to_string();
    let product_id = "product_1".to_string();
    let query = query!(ShoppingCartEvent; cart_id == cart_id, product_id == product_id);
    let append_result = event_store
        .append(
            vec![ShoppingCartEvent::Added(CartEventPayload {
                cart_id,
                product_id,
                quantity: 1,
            })],
            query,
            0,
        )
        .await;

    PgEventListener::builder(event_store.clone())
        .register_listener(
            CartEventHandler::new(pool.clone()).await.unwrap(),
            PgEventListenerConfig::poller(Duration::from_millis(5000)).with_notifier(),
        )
        .start_with_shutdown(async {
            tokio::time::sleep(Duration::from_millis(400)).await;
        })
        .await
        .unwrap();

    assert!(append_result.is_ok());
    let carts = Cart::carts(&pool).await.unwrap();
    assert_eq!(carts.len(), 1);
    let first_row = carts.first().unwrap();
    assert_eq!("cart_1", &first_row.cart_id);
    assert_eq!("product_1", &first_row.product_id);
    assert_eq!(1, first_row.quantity);
}

const EPOCH_GATE: i64 = 4343;

/// Re-creates `event_store_current_epoch()` with one extra line that waits on a gate, so a reader can be parked
/// after its transaction's snapshot exists and before it computes the epoch.
async fn park_epoch_function(pool: &PgPool) {
    let body: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('event_store_current_epoch()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let gated = body.replacen(
        "BEGIN\n",
        &format!("BEGIN\n    PERFORM pg_advisory_xact_lock({EPOCH_GATE});\n"),
        1,
    );
    assert_ne!(gated, body, "the gate line must have been inserted");
    sqlx::raw_sql(sqlx::AssertSqlSafe(gated))
        .execute(pool)
        .await
        .unwrap();
}

async fn wait_until_epoch_parked(pool: &PgPool) {
    for _ in 0..400 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND objid = $1",
        )
        .bind(EPOCH_GATE as i32)
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the listener never reached the epoch function");
}

/// A writer that has marked its append and inserted one cart event, and has not committed. It uses plain SQL rather
/// than the library's append, so two overlapping writers do not abort each other with serialization failures.
async fn open_writer(pool: &PgPool, cart: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT event_store_begin_epoch()")
        .execute(&mut *tx)
        .await
        .unwrap();
    let payload = serde_json::to_vec(&ShoppingCartEvent::Added(CartEventPayload {
        cart_id: cart.to_string(),
        product_id: "product_1".to_string(),
        quantity: 1,
    }))
    .unwrap();
    sqlx::query(
        "INSERT INTO event (event_type, payload, cart_id, product_id) VALUES ('ShoppingCartAdded', $1, $2, 'product_1')",
    )
    .bind(payload)
    .bind(cart)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx
}

/// Writer A draws id 1 and stays open; writer B draws id 2 and commits. The listener's transaction then starts and
/// takes its snapshot (it sees event 2 only), parks before it computes the epoch, A commits, and the listener goes
/// on. The epoch covers event 1 (A's marker is gone) and the snapshot's own maximum is 2. Returns the checkpoint and
/// the number of cart rows the handler wrote.
async fn pass_with_earlier_writer_committing_after_the_snapshot(
    pool: &PgPool,
    isolation: &str,
) -> (i64, i64) {
    let options = pool
        .connect_options()
        .as_ref()
        .clone()
        .options([("default_transaction_isolation", isolation)]);
    let listener_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let store = PgEventStore::<ShoppingCartEvent, Json<ShoppingCartEvent>>::try_new(
        listener_pool.clone(),
        Json::default(),
    )
    .await
    .unwrap();
    let executor = PgEventListerExecutor::new(
        store.clone(),
        CartEventHandler::new(pool.clone()).await.unwrap(),
        CancellationToken::new(),
        PgEventListenerConfig::poller(Duration::from_secs(1)),
    );
    Migrator::new(store.clone()).init_listener().await.unwrap();
    executor.init().await.unwrap();

    let writer_a = open_writer(pool, "cart_1").await;
    let writer_b = open_writer(pool, "cart_2").await;
    writer_b.commit().await.unwrap();
    // Last, because creating a store re-runs the migrations that define the epoch function.
    park_epoch_function(pool).await;

    let mut gate = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(EPOCH_GATE)
        .execute(&mut *gate)
        .await
        .unwrap();
    let pass = tokio::spawn(async move { executor.try_execute().await.unwrap() });
    for _ in 0..40 {
        assert!(
            !pass.is_finished(),
            "the listener pass ended before it reached the epoch function"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    wait_until_epoch_parked(pool).await;

    writer_a.commit().await.unwrap();
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(EPOCH_GATE)
        .execute(&mut *gate)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), pass)
        .await
        .expect("the listener pass did not finish")
        .unwrap();

    let checkpoint: i64 =
        sqlx::query_scalar("SELECT last_processed_event_id FROM event_listener WHERE id = 'carts'")
            .fetch_one(pool)
            .await
            .unwrap();
    let rows = Cart::carts(pool).await.unwrap().len() as i64;
    eprintln!("{isolation}: checkpoint {checkpoint}, rows {rows}");
    (checkpoint, rows)
}

/// Every event up to the checkpoint has been handled. Event ids here have no gaps, so that is rows == checkpoint.
fn assert_no_skip(isolation: &str, (checkpoint, rows): (i64, i64)) {
    assert_eq!(
        rows, checkpoint,
        "{isolation}: the checkpoint {checkpoint} covers events whose handler never ran (rows {rows})"
    );
}

#[sqlx::test]
async fn an_earlier_writer_committing_after_the_snapshot_is_not_skipped_at_read_committed(pool: PgPool) {
    let r = pass_with_earlier_writer_committing_after_the_snapshot(&pool, "read committed").await;
    assert_no_skip("read committed", r);
}

#[sqlx::test]
async fn an_earlier_writer_committing_after_the_snapshot_is_not_skipped_at_repeatable_read(pool: PgPool) {
    let r = pass_with_earlier_writer_committing_after_the_snapshot(&pool, "repeatable read").await;
    assert_no_skip("repeatable read", r);
}

#[sqlx::test]
async fn an_earlier_writer_committing_after_the_snapshot_is_not_skipped_at_serializable(pool: PgPool) {
    let r = pass_with_earlier_writer_committing_after_the_snapshot(&pool, "serializable").await;
    assert_no_skip("serializable", r);
}
