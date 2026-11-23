//! Decisions made through `tx_decision_maker` on a transaction owned by the caller.
use disintegrate::{Decision, Event, StateMutate, StateQuery};
use disintegrate_postgres::{tx_decision_maker, PgDecisionMaker, PgEventStore, PgTx};
use disintegrate_serde::serde::json::Json;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Mutex;

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

struct Deposit {
    account_id: i64,
    amount: i32,
    /// Runs on its own connection after the state is loaded and before the events are appended.
    rival: Option<Rival>,
}

#[derive(Clone)]
struct Rival {
    options: sqlx::postgres::PgConnectOptions,
    validates: bool,
}

impl Deposit {
    fn new(account_id: i64, amount: i32) -> Self {
        Self {
            account_id,
            amount,
            rival: None,
        }
    }
}

impl Decision for Deposit {
    type Event = DomainEvent;
    type StateQuery = Balance;
    type Error = std::convert::Infallible;

    fn state_query(&self) -> Balance {
        Balance {
            account_id: self.account_id,
            total: 0,
        }
    }

    fn process(&self, _state: &Balance) -> Result<Vec<DomainEvent>, Self::Error> {
        if let Some(rival) = self.rival.clone() {
            let account_id = self.account_id;
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(rival_commit(rival, account_id));
            })
            .join()
            .unwrap();
        }
        Ok(vec![DomainEvent::Deposited {
            account_id: self.account_id,
            amount: self.amount,
        }])
    }
}

async fn rival_commit(rival: Rival, account_id: i64) {
    use disintegrate::EventStore;
    let pool = PgPool::connect_with(rival.options).await.unwrap();
    let store = Store::new_uninitialized(pool, Json::default());
    if rival.validates {
        let decision_maker: PgDecisionMaker<
            DomainEvent,
            Json<DomainEvent>,
            disintegrate::NoSnapshot,
        > = disintegrate_postgres::decision_maker(store, disintegrate::NoSnapshot);
        decision_maker
            .make(Deposit::new(account_id, 1))
            .await
            .unwrap();
    } else {
        store
            .append_without_validation(vec![DomainEvent::Deposited {
                account_id,
                amount: 1,
            }])
            .await
            .unwrap();
    }
}

type Store = PgEventStore<DomainEvent, Json<DomainEvent>>;

async fn store(pool: &PgPool) -> Store {
    Store::try_new(pool.clone(), Json::default()).await.unwrap()
}

async fn begin(pool: &PgPool) -> Mutex<PgTx> {
    Mutex::new(
        pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap(),
    )
}

async fn count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM event")
        .fetch_one(pool)
        .await
        .unwrap()
}

fn assert_send<T: Send>(_: &T) {}

#[sqlx::test]
async fn a_decision_on_the_callers_transaction_is_send_and_commits_with_the_caller(pool: PgPool) {
    let store = store(&pool).await;

    let tx = begin(&pool).await;
    let decision_maker = tx_decision_maker(store.in_transaction(&tx));
    let decision = decision_maker.make(Deposit::new(1, 5));
    assert_send(&decision);
    assert_eq!(decision.await.unwrap().len(), 1);
    assert_eq!(
        count(&pool).await,
        0,
        "nothing is visible before the caller commits"
    );
    tx.into_inner().rollback().await.unwrap();
    assert_eq!(count(&pool).await, 0);

    let tx = begin(&pool).await;
    tx_decision_maker(store.in_transaction(&tx))
        .make(Deposit::new(1, 5))
        .await
        .unwrap();
    tx.into_inner().commit().await.unwrap();
    assert_eq!(count(&pool).await, 1);
}

#[sqlx::test]
async fn a_decision_fails_when_the_caller_transaction_is_not_serializable(pool: PgPool) {
    let store = store(&pool).await;

    let weak = Mutex::new(pool.begin().await.unwrap());
    sqlx::query("SELECT 1")
        .execute(&mut **weak.lock().await)
        .await
        .unwrap();
    let result = tx_decision_maker(store.in_transaction(&weak))
        .make(Deposit::new(1, 5))
        .await;

    assert!(result.is_err());
    weak.into_inner().rollback().await.unwrap();
    assert_eq!(count(&pool).await, 0);
}

#[sqlx::test]
async fn a_validated_rival_after_the_load_stops_the_decision(pool: PgPool) {
    let store = store(&pool).await;
    let options = (*pool.connect_options()).clone();

    let tx = begin(&pool).await;
    let mut deposit = Deposit::new(1, 5);
    deposit.rival = Some(Rival {
        options,
        validates: true,
    });
    let result = tx_decision_maker(store.in_transaction(&tx))
        .make(deposit)
        .await;

    assert!(result.is_err(), "the stale decision must not append");
    tx.into_inner().rollback().await.unwrap();
    assert_eq!(count(&pool).await, 1, "only the rival's event");
}

#[sqlx::test]
async fn an_unvalidated_rival_is_not_detected_on_the_callers_transaction(pool: PgPool) {
    let store = store(&pool).await;
    let options = (*pool.connect_options()).clone();

    let tx = begin(&pool).await;
    let mut deposit = Deposit::new(1, 5);
    deposit.rival = Some(Rival {
        options: options.clone(),
        validates: false,
    });
    let result = tx_decision_maker(store.in_transaction(&tx))
        .make(deposit)
        .await;
    assert!(result.is_ok());
    tx.into_inner().commit().await.unwrap();
    assert_eq!(count(&pool).await, 2, "documented: both events commit");

    // The library's own path returns a concurrency error on the same schedule.
    let own: PgDecisionMaker<DomainEvent, Json<DomainEvent>, disintegrate::NoSnapshot> =
        disintegrate_postgres::decision_maker(store, disintegrate::NoSnapshot);
    let mut deposit = Deposit::new(2, 5);
    deposit.rival = Some(Rival {
        options,
        validates: false,
    });
    let error = own.make(deposit).await.unwrap_err();
    assert!(
        error.to_string().contains("concurrent modification"),
        "{error}"
    );
}
