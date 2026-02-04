use anyhow::Result;
use axum::{Router, extract::State, routing::get};
use bitcoin::{Script, Transaction, hashes::Hash, opcodes::all::OP_IF, script::Instruction};
use bitcoind_async_client::{Client, traits::*};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_rusqlite::{
    Connection, params,
    rusqlite::{self, OptionalExtension},
};
use tower_http::services::ServeFile;
use tracing::{error, info, warn};

use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Deserialize)]
struct CoinbaseResponse {
    data: CoinbaseData,
}

#[derive(Deserialize)]
struct CoinbaseData {
    amount: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FeesResponse {
    btc: f64,
    usd: f64,
    block_height: u64,
    total_vbytes: f64,
}

const CITREA_START_HEIGHT: u64 = 925165;
const REVEAL_PREFIX: &[u8] = &[2, 2];
const PRICE_CACHE_SECS: u64 = 30;
const BATCH_SIZE: u64 = 50;

struct RollupRule {
    id: u32,
    matcher: fn(&Transaction) -> bool,
    get_related_txids: Option<fn(&Transaction) -> Vec<bitcoin::Txid>>,
}

const RULES: &[RollupRule] = &[RollupRule {
    id: 1, // Citrea
    matcher: |tx| {
        tx.compute_wtxid()
            .to_raw_hash()
            .to_byte_array()
            .starts_with(REVEAL_PREFIX)
            && tx.input.iter().any(|i| {
                i.witness.nth(1).map_or(false, |b| {
                    Script::from_bytes(b)
                        .instructions()
                        .any(|ins| matches!(ins, Ok(Instruction::Op(op)) if op == OP_IF))
                })
            })
    },
    get_related_txids: Some(|tx| tx.input.iter().map(|i| i.previous_output.txid).collect()),
}];

struct CachedPrice {
    usd: f64,
    fetched_at: Instant,
}

#[derive(Clone)]
struct AppState {
    db: Connection,
    price: Arc<RwLock<Option<CachedPrice>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    fs::create_dir_all("rollup_db")?;

    let writer = Connection::open("rollup_db/rollups.sqlite").await?;
    let reader = Connection::open("rollup_db/rollups.sqlite").await?;

    writer
        .call(|conn| {
            setup_db(conn)?;
            Ok::<_, rusqlite::Error>(())
        })
        .await?;

    reader
        .call(|conn| {
            conn.busy_timeout(Duration::from_secs(5))?;
            Ok::<_, rusqlite::Error>(())
        })
        .await?;

    let state = AppState {
        db: reader,
        price: Arc::new(RwLock::new(None)),
    };

    tokio::spawn(async move {
        if let Err(e) = sync_loop(writer).await {
            error!("Sync loop failed: {e}");
        }
    });

    let app = Router::new()
        .route("/api/fees", get(get_fees))
        .route_service("/", ServeFile::new("index.html"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Listening on 0.0.0.0:3000");
    axum::serve(listener, app).await?;

    Ok(())
}

async fn get_fees(
    State(state): State<AppState>,
) -> Result<axum::Json<FeesResponse>, axum::http::StatusCode> {
    let (sats, block_height, total_vbytes) = state
        .db
        .call(|conn| {
            let total = get_cached_total(conn)?;
            let height = get_latest_height(conn)?;
            let vbytes: f64 = conn.query_row(
                "SELECT IFNULL(SUM(fee_sats / fee_rate), 0) FROM tx_fees",
                [],
                |r| r.get(0),
            )?;
            Ok::<_, rusqlite::Error>((total, height, vbytes))
        })
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let price = get_btc_price(&state).await?;

    let btc = sats as f64 / 100_000_000.0;
    let usd = btc * price;

    Ok(axum::Json(FeesResponse {
        btc,
        usd,
        block_height,
        total_vbytes,
    }))
}

async fn sync_loop(conn: Connection) -> Result<()> {
    info!("Sync loop starting...");

    let client = Client::new(
        env::var("RPC_URL").expect("RPC_URL not set"),
        env::var("RPC_USERNAME").expect("RPC_USERNAME not set"),
        env::var("RPC_PASSWORD").expect("RPC_PASSWORD not set"),
        None,
        None,
    )
    .map_err(|e| {
        error!("Failed to create client: {e}");
        anyhow::anyhow!("Failed to create client: {e}")
    })?;

    info!("Connected to Bitcoin RPC");

    let start_height = CITREA_START_HEIGHT;
    let mut current_height: u64 = conn
        .call(move |conn| {
            conn.query_row(
                "SELECT IFNULL(MAX(height), ?1) FROM blocks",
                params![start_height - 1],
                |r| r.get::<_, u64>(0).map(|h| h + 1),
            )
        })
        .await?;

    loop {
        match client.get_block_count().await {
            Ok(chain_tip) => {
                let total_blocks = chain_tip - CITREA_START_HEIGHT;
                let synced_blocks = current_height.saturating_sub(CITREA_START_HEIGHT);

                if current_height > chain_tip {
                    info!("Synced ({total_blocks} blocks), waiting for new blocks...");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }

                let progress = (synced_blocks as f64 / total_blocks as f64) * 100.0;
                info!(
                    "Syncing: {:.2}% ({synced_blocks}/{total_blocks} blocks) - current: {current_height}, tip: {chain_tip}",
                    progress
                );

                // Check for reorg
                if current_height > CITREA_START_HEIGHT {
                    let prev_height = current_height - 1;
                    let local_prev_hash: String = conn
                        .call(move |conn| {
                            conn.query_row(
                                "SELECT hash FROM blocks WHERE height = ?1",
                                params![prev_height],
                                |r| r.get(0),
                            )
                        })
                        .await?;

                    let remote_prev_hash = client.get_block_hash(prev_height).await?.to_string();

                    if local_prev_hash != remote_prev_hash {
                        warn!("Reorg at {prev_height}! Rolling back...");

                        let new_total = conn
                            .call(move |conn| {
                                conn.execute(
                                    "DELETE FROM blocks WHERE height >= ?1",
                                    params![prev_height],
                                )?;
                                recompute_total(conn)
                            })
                            .await?;

                        warn!("Reorg complete. New total: {new_total} sats");
                        current_height = prev_height;
                        continue;
                    }
                }

                let end_height = std::cmp::min(current_height + BATCH_SIZE - 1, chain_tip);
                info!(
                    "Processing batch of {} blocks ({} -> {})",
                    end_height - current_height + 1,
                    current_height,
                    end_height
                );

                let mut tasks = JoinSet::new();
                for height in current_height..=end_height {
                    let client = client.clone();
                    tasks.spawn(async move {
                        let block_hash = client.get_block_hash(height).await?;
                        let block = client.get_block(&block_hash).await?;
                        let txs = find_all_rollup_txs(&client, &block).await?;
                        Ok::<_, anyhow::Error>((height, block_hash.to_string(), txs))
                    });
                }

                let mut results: Vec<(u64, String, Vec<(String, u32, u64, f64)>)> = Vec::new();
                while let Some(res) = tasks.join_next().await {
                    match res {
                        Ok(Ok(data)) => results.push(data),
                        Ok(Err(e)) => {
                            error!("Block processing error: {e}");
                            return Err(e);
                        }
                        Err(e) => {
                            error!("Task join error: {e}");
                            return Err(e.into());
                        }
                    }
                }

                results.sort_by_key(|(h, _, _)| *h);

                let batch_end = end_height;
                let (total_tx_count, batch_delta, new_total) = conn
                    .call(move |conn| {
                        let tx = conn.transaction()?;
                        let mut total_delta: i64 = 0;
                        let mut total_tx_count: usize = 0;

                        for (height, block_hash, found_txs) in results {
                            tx.execute(
                                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                                params![height, block_hash],
                            )?;

                            for (txid, rid, sats, rate) in &found_txs {
                                let old_fee: Option<i64> = tx
                                    .query_row(
                                        "SELECT fee_sats FROM tx_fees WHERE txid = ?1",
                                        params![txid],
                                        |r| r.get(0),
                                    )
                                    .optional()?;

                                tx.execute(
                    "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(txid) DO UPDATE SET
                    rollup_id = excluded.rollup_id,
                    fee_sats = excluded.fee_sats,
                    fee_rate = excluded.fee_rate,
                    block_height = excluded.block_height",
                    params![txid, rid, sats, rate, height],
                )?;

                                total_delta += *sats as i64 - old_fee.unwrap_or(0);
                            }
                            total_tx_count += found_txs.len();
                        }

                        tx.execute(
                            "UPDATE totals SET total_fee_sats = total_fee_sats + ?1 WHERE id = 1",
                            params![total_delta],
                        )?;

                        tx.commit()?;

                        let total = get_cached_total(conn)?;
                        Ok::<_, rusqlite::Error>((total_tx_count, total_delta, total))
                    })
                    .await?;

                info!(
                    "Batch complete ({} -> {}): {} txs, delta={} sats. Total: {} sats",
                    current_height, batch_end, total_tx_count, batch_delta, new_total
                );

                current_height = end_height + 1;
            }
            Err(e) => {
                error!("Error fetching block count: {e}");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        }
    }
}

async fn find_all_rollup_txs(
    client: &Client,
    block: &bitcoin::Block,
) -> Result<Vec<(String, u32, u64, f64)>> {
    let block_txs: HashMap<bitcoin::Txid, &Transaction> = block
        .txdata
        .iter()
        .map(|tx| (tx.compute_txid(), tx))
        .collect();

    // Collect all txs we need to process
    let mut work: Vec<(Transaction, u32)> = Vec::new();

    for tx in &block.txdata {
        if let Some(rule) = RULES.iter().find(|r| (r.matcher)(tx)) {
            work.push((tx.clone(), rule.id));

            if let Some(get_related) = rule.get_related_txids {
                for txid in get_related(tx) {
                    if let Some(related_tx) = block_txs.get(&txid) {
                        work.push(((*related_tx).clone(), rule.id));
                    }
                }
            }
        }
    }

    if work.is_empty() {
        return Ok(Vec::new());
    }

    // Collect all unique prevouts we need to fetch
    let mut prevouts_needed: HashSet<bitcoin::Txid> = HashSet::new();
    for (tx, _) in &work {
        for input in &tx.input {
            prevouts_needed.insert(input.previous_output.txid);
        }
    }

    // Fetch all prevouts in parallel
    let prevout_map = fetch_prevouts_parallel(client, prevouts_needed).await?;

    // Calculate fees
    let results: Vec<(String, u32, u64, f64)> = work
        .into_iter()
        .filter_map(|(tx, rule_id)| {
            calculate_fee_from_cache(&tx, &prevout_map)
                .map(|(sats, rate)| (tx.compute_txid().to_string(), rule_id, sats, rate))
        })
        .collect();

    Ok(results)
}

async fn fetch_prevouts_parallel(
    client: &Client,
    txids: HashSet<bitcoin::Txid>,
) -> Result<HashMap<bitcoin::Txid, Transaction>> {
    let mut set = JoinSet::new();

    for txid in txids {
        let client = client.clone();

        set.spawn(async move {
            let result = client.get_raw_transaction_verbosity_one(&txid).await;
            (txid, result)
        });
    }

    let mut map = HashMap::new();
    while let Some(res) = set.join_next().await {
        if let Ok((txid, Ok(tx_info))) = res {
            map.insert(txid, tx_info.transaction);
        }
    }

    Ok(map)
}

fn calculate_fee_from_cache(
    tx: &Transaction,
    prevout_map: &HashMap<bitcoin::Txid, Transaction>,
) -> Option<(u64, f64)> {
    let mut total_in: u64 = 0;

    for input in &tx.input {
        let prev_tx = prevout_map.get(&input.previous_output.txid)?;
        total_in += prev_tx.output[input.previous_output.vout as usize]
            .value
            .to_sat();
    }

    let total_out: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
    let fee_sats = total_in.saturating_sub(total_out);
    let fee_rate = fee_sats as f64 / tx.vsize() as f64;

    Some((fee_sats, fee_rate))
}

fn get_cached_total(conn: &rusqlite::Connection) -> Result<u64, rusqlite::Error> {
    conn.query_row("SELECT total_fee_sats FROM totals WHERE id = 1", [], |r| {
        r.get::<_, i64>(0).map(|v| v as u64)
    })
}

fn get_latest_height(conn: &rusqlite::Connection) -> Result<u64, rusqlite::Error> {
    conn.query_row(
        "SELECT IFNULL(MAX(height), ?1) FROM blocks",
        params![CITREA_START_HEIGHT - 1],
        |r| r.get(0),
    )
}

fn recompute_total(conn: &rusqlite::Connection) -> Result<u64, rusqlite::Error> {
    let total: i64 = conn.query_row("SELECT IFNULL(SUM(fee_sats), 0) FROM tx_fees", [], |r| {
        r.get(0)
    })?;
    conn.execute(
        "UPDATE totals SET total_fee_sats = ?1 WHERE id = 1",
        params![total],
    )?;
    Ok(total as u64)
}

fn setup_db(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;

         CREATE TABLE IF NOT EXISTS blocks (
             height INTEGER PRIMARY KEY,
             hash TEXT NOT NULL
         );

         CREATE TABLE IF NOT EXISTS tx_fees (
             txid TEXT PRIMARY KEY,
             rollup_id INTEGER NOT NULL,
             fee_sats INTEGER NOT NULL,
             fee_rate REAL NOT NULL,
             block_height INTEGER NOT NULL,
             FOREIGN KEY(block_height) REFERENCES blocks(height) ON DELETE CASCADE
         );

         CREATE TABLE IF NOT EXISTS totals (
             id INTEGER PRIMARY KEY CHECK(id=1),
             total_fee_sats INTEGER NOT NULL
         );
         INSERT OR IGNORE INTO totals(id, total_fee_sats) VALUES (1, 0);

         CREATE INDEX IF NOT EXISTS idx_tx_fees_block ON tx_fees (block_height);
         CREATE INDEX IF NOT EXISTS idx_tx_fees_rollup ON tx_fees (rollup_id, block_height);",
    )?;
    Ok(())
}

async fn get_btc_price(state: &AppState) -> Result<f64, axum::http::StatusCode> {
    {
        let cache = state.price.read().await;
        if let Some(cached) = &*cache {
            if cached.fetched_at.elapsed().as_secs() < PRICE_CACHE_SECS {
                return Ok(cached.usd);
            }
        }
    }

    let price: f64 = reqwest::get("https://api.coinbase.com/v2/prices/BTC-USD/spot")
        .await
        .map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?
        .json::<CoinbaseResponse>()
        .await
        .map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?
        .data
        .amount
        .parse()
        .map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?;

    {
        let mut cache = state.price.write().await;
        *cache = Some(CachedPrice {
            usd: price,
            fetched_at: Instant::now(),
        });
    }

    Ok(price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    async fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().await.unwrap();
        conn.call(|conn| {
            setup_db(conn)?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();
        conn
    }

    #[tokio::test]
    async fn test_fees_endpoint() {
        let conn = Connection::open_in_memory().await.unwrap();
        conn.call(|conn| {
            setup_db(conn)?;
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![100, "abc123"],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
             VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx1", 1, 123456789_i64, 100.0, 100],
            )?;
            conn.execute(
                "UPDATE totals SET total_fee_sats = 123456789 WHERE id = 1",
                [],
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        let state = AppState {
            db: conn,
            price: Arc::new(RwLock::new(Some(CachedPrice {
                usd: 100_000.0,
                fetched_at: Instant::now(),
            }))),
        };

        let app = Router::new()
            .route("/api/fees", get(get_fees))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/fees")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: FeesResponse = serde_json::from_slice(&body).unwrap();

        assert!((json.btc - 1.23456789).abs() < 0.0000001);
        assert!((json.usd - 123456.789).abs() < 0.001);
        assert_eq!(json.block_height, 100);
        // 123456789 sats / 100.0 sat/vB = 1234567.89 vbytes
        assert!((json.total_vbytes - 1234567.89).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_setup_db_creates_tables() {
        let conn = setup_test_db().await;

        let tables: Vec<String> = conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?;
                let rows = stmt.query_map([], |r| r.get(0))?;
                Ok::<_, rusqlite::Error>(rows.filter_map(|r| r.ok()).collect())
            })
            .await
            .unwrap();

        assert!(tables.contains(&"blocks".to_string()));
        assert!(tables.contains(&"tx_fees".to_string()));
        assert!(tables.contains(&"totals".to_string()));
    }

    #[tokio::test]
    async fn test_initial_total_is_zero() {
        let conn = setup_test_db().await;
        let total = conn.call(|conn| get_cached_total(conn)).await.unwrap();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_insert_and_update_total() {
        let conn = setup_test_db().await;

        conn.call(|conn| {
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![100, "abc123"],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx1", 1, 5000_i64, 10.0, 100],
            )?;
            conn.execute(
                "UPDATE totals SET total_fee_sats = total_fee_sats + ?1 WHERE id = 1",
                params![5000_i64],
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        let total = conn.call(|conn| get_cached_total(conn)).await.unwrap();
        assert_eq!(total, 5000);
    }

    #[tokio::test]
    async fn test_upsert_with_delta() {
        let conn = setup_test_db().await;

        conn.call(|conn| {
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![100, "hash"],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx1", 1, 5000_i64, 10.0, 100],
            )?;
            conn.execute("UPDATE totals SET total_fee_sats = 5000 WHERE id = 1", [])?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        conn.call(|conn| {
            let old_fee: Option<i64> = conn
                .query_row(
                    "SELECT fee_sats FROM tx_fees WHERE txid = ?1",
                    params!["tx1"],
                    |r| r.get(0),
                )
                .optional()?;

            let new_fee: i64 = 7000;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(txid) DO UPDATE SET
                   fee_sats = excluded.fee_sats,
                   fee_rate = excluded.fee_rate",
                params!["tx1", 1, new_fee, 14.0, 100],
            )?;

            let delta = new_fee - old_fee.unwrap_or(0);
            conn.execute(
                "UPDATE totals SET total_fee_sats = total_fee_sats + ?1 WHERE id = 1",
                params![delta],
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        let total = conn.call(|conn| get_cached_total(conn)).await.unwrap();
        assert_eq!(total, 7000);
    }

    #[tokio::test]
    async fn test_recompute_after_reorg() {
        let conn = setup_test_db().await;

        conn.call(|conn| {
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![100, "h100"],
            )?;
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![101, "h101"],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx1", 1, 5000_i64, 10.0, 100],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx2", 1, 3000_i64, 6.0, 101],
            )?;
            conn.execute("UPDATE totals SET total_fee_sats = 8000 WHERE id = 1", [])?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        let new_total = conn
            .call(|conn| {
                conn.execute("DELETE FROM blocks WHERE height >= ?1", params![101])?;
                recompute_total(conn)
            })
            .await
            .unwrap();

        assert_eq!(new_total, 5000);
    }

    #[tokio::test]
    async fn test_cascade_delete() {
        let conn = setup_test_db().await;

        conn.call(|conn| {
            conn.execute(
                "INSERT INTO blocks (height, hash) VALUES (?1, ?2)",
                params![100, "hash"],
            )?;
            conn.execute(
                "INSERT INTO tx_fees (txid, rollup_id, fee_sats, fee_rate, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["tx1", 1, 5000_i64, 10.0, 100],
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        conn.call(|conn| {
            conn.execute("DELETE FROM blocks WHERE height = ?1", params![100])?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .unwrap();

        let count: i64 = conn
            .call(|conn| conn.query_row("SELECT COUNT(*) FROM tx_fees", [], |r| r.get(0)))
            .await
            .unwrap();

        assert_eq!(count, 0);
    }
}
