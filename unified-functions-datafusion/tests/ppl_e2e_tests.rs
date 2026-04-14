//! PPL E2E tests — SQL queries (representing PPL-transpiled SQL) executed against
//! DataFusion with all UDFs registered and realistic table data from test_tables.yaml.

use datafusion::assert_batches_eq;
use unified_functions_datafusion::schema_loader::create_context_with_tables;

const YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../api/src/main/resources/unified-functions/test_tables.yaml"
);

async fn ctx() -> datafusion::prelude::SessionContext {
    create_context_with_tables(YAML_PATH).await.unwrap()
}

// ── IP functions ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cidrmatch_filters_192_168_subnet() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, ip FROM logs WHERE cidrmatch(ip, '192.168.0.0/16') ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+--------------+",
         "| id | ip           |",
         "+----+--------------+",
         "| 1  | 192.168.1.10 |",
         "| 6  | 192.168.1.10 |",
         "+----+--------------+"],
        &batches
    );
}

#[tokio::test]
async fn test_is_ipv4_and_is_ipv6() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, is_ipv4(ip) AS v4, is_ipv6(ip) AS v6 FROM logs ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    // rows 1-3,6 are IPv4; row 4 (::1) and 5 are IPv6; row 7 has zone ID so neither parses
    assert_batches_eq!(
        ["+----+-------+-------+",
         "| id | v4    | v6    |",
         "+----+-------+-------+",
         "| 1  | true  | false |",
         "| 2  | true  | false |",
         "| 3  | true  | false |",
         "| 4  | false | true  |",
         "| 5  | false | true  |",
         "| 6  | true  | false |",
         "| 7  | false | false |",
         "+----+-------+-------+"],
        &batches
    );
}

// ── JSON functions ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_extract_action_from_logs() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, json_extract(payload, '$.action') AS action FROM logs WHERE id = 1")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+---------+",
         "| id | action  |",
         "+----+---------+",
         "| 1  | \"login\" |",
         "+----+---------+"],
        &batches
    );
}

#[tokio::test]
async fn test_json_keys_on_payload() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, json_keys(payload) AS keys FROM logs WHERE id = 1")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+-------------+",
         "| id | keys        |",
         "+----+-------------+",
         "| 1  | action,user |",
         "+----+-------------+"],
        &batches
    );
}

#[tokio::test]
async fn test_json_valid() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT json_valid('{\"a\":1}') AS valid, json_valid('not json') AS invalid")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+-------+---------+",
         "| valid | invalid |",
         "+-------+---------+",
         "| true  | false   |",
         "+-------+---------+"],
        &batches
    );
}

// ── Datetime functions ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_dayname_on_hire_date() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, dayname(hire_date) AS day FROM employees ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    // 2019-03-15=Friday, 2020-07-01=Wednesday, 2017-11-20=Monday,
    // 2021-01-10=Sunday, 2018-06-30=Saturday, 2023-09-05=Tuesday
    assert_batches_eq!(
        ["+----+-----------+",
         "| id | day       |",
         "+----+-----------+",
         "| 1  | Friday    |",
         "| 2  | Wednesday |",
         "| 3  | Monday    |",
         "| 4  | Sunday    |",
         "| 5  | Saturday  |",
         "| 6  | Tuesday   |",
         "+----+-----------+"],
        &batches
    );
}

#[tokio::test]
async fn test_monthname_on_hire_date() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, monthname(hire_date) AS mon FROM employees WHERE id <= 3 ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+----------+",
         "| id | mon      |",
         "+----+----------+",
         "| 1  | March    |",
         "| 2  | July     |",
         "| 3  | November |",
         "+----+----------+"],
        &batches
    );
}

// ── Math functions ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cbrt_on_salary() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, cbrt(salary) AS cube_root FROM employees WHERE id = 1")
        .await.unwrap().collect().await.unwrap();
    // cbrt(105000.0) ≈ 47.1...
    let val = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 47.1).abs() < 1.0, "cbrt(105000) should be ~47.1, got {val}");
}

#[tokio::test]
async fn test_signum_on_latency() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, signum(latency) AS s FROM logs WHERE id <= 3 ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+---+",
         "| id | s |",
         "+----+---+",
         "| 1  | 1 |",
         "| 2  | 1 |",
         "| 3  | 1 |",
         "+----+---+"],
        &batches
    );
}

// ── Collection functions ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_array_length() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT array_length('[10,20,30]') AS len")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+-----+",
         "| len |",
         "+-----+",
         "| 3   |",
         "+-----+"],
        &batches
    );
}

// ── Type conversion functions ─────────────────────────────────────────────────

#[tokio::test]
async fn test_typeof_on_columns() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT typeof(salary) AS t FROM employees WHERE id = 1")
        .await.unwrap().collect().await.unwrap();
    // typeof() casts input to string internally, so always returns "STRING"
    assert_batches_eq!(
        ["+--------+",
         "| t      |",
         "+--------+",
         "| STRING |",
         "+--------+"],
        &batches
    );
}

#[tokio::test]
async fn test_cast_to_string_on_status_code() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, cast_to_string(status_code) AS sc FROM logs WHERE id <= 2 ORDER BY id")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----+-----+",
         "| id | sc  |",
         "+----+-----+",
         "| 1  | 200 |",
         "| 2  | 504 |",
         "+----+-----+"],
        &batches
    );
}

// ── Condition functions ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_ifnull_with_literal() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT ifnull(NULL, 'fallback') AS val")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+----------+",
         "| val      |",
         "+----------+",
         "| fallback |",
         "+----------+"],
        &batches
    );
}

#[tokio::test]
async fn test_nullif_returns_null_on_equal() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT nullif('same', 'same') AS val")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+-----+",
         "| val |",
         "+-----+",
         "|     |",
         "+-----+"],
        &batches
    );
}

// ── Crypto functions ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_md5_on_message() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT id, md5(message) AS hash FROM logs WHERE id = 2")
        .await.unwrap().collect().await.unwrap();
    // md5("Connection timeout to upstream")
    let hash = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(hash.len(), 32, "MD5 hash should be 32 hex chars");
}

// ── Parse functions ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grok_extracts_ip_pattern() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT grok(ip, '%{IP:addr}') AS addr FROM logs WHERE id = 1")
        .await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        ["+-------------------------+",
         "| addr                    |",
         "+-------------------------+",
         "| {\"addr\":\"192.168.1.10\"} |",
         "+-------------------------+"],
        &batches
    );
}
