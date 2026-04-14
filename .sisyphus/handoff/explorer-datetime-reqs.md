# Datetime Function Implementation Requirements

## 1. Current State of datetime.rs
File: `unified-functions-datafusion/src/datetime.rs` — **EMPTY stub**, only contains:
```rust
use datafusion::prelude::SessionContext;
pub fn register_datetime_udfs(_ctx: &SessionContext) {}
```

## 2. Functions to Implement (33 total: 29 deterministic + 4 non-deterministic)

### Test Cases Per Function (from unified_datetime_functions.test)

| # | Function | Tests | Args (YAML types) | Return | DataFusion Return | Notes |
|---|----------|-------|--------------------|--------|-------------------|-------|
| 1 | adddate | 12 | (date, i64) | date | Date32 | chrono: add days to NaiveDate |
| 2 | addtime | 11 | (timestamp, string) | timestamp | Timestamp | parse time str, add to ts |
| 3 | convert_tz | 12 | (timestamp, string, string) | timestamp | Timestamp | needs chrono-tz crate |
| 4 | date | 11 | (string) | date | Date32 | parse string to NaiveDate |
| 5 | date_format | 11 | (timestamp, string) | string | Utf8 | strftime-style format |
| 6 | dayname | 12 | (date) | string | Utf8 | weekday name |
| 7 | from_days | 11 | (i64) | date | Date32 | day 1 = 0000-01-01 |
| 8 | from_unixtime | 11 | (fp64) | timestamp | Timestamp | epoch seconds to ts |
| 9 | get_format | 18 | (string, string) | string | Utf8 | pure lookup table |
| 10 | makedate | 11 | (fp64, fp64) | date | Date32 | year + day-of-year |
| 11 | maketime | 11 | (fp64, fp64, fp64) | string | Utf8 | HH:MM:SS string |
| 12 | microsecond | 11 | (string) | i32 | Int32 | extract µs from time str |
| 13 | minute_of_day | 11 | (string) | i32 | Int32 | h*60+m |
| 14 | monthname | 11 | (date) | string | Utf8 | month name |
| 15 | period_add | 11 | (i32, i32) | i32 | Int32 | YYYYMM arithmetic |
| 16 | period_diff | 11 | (i32, i32) | i32 | Int32 | YYYYMM diff |
| 17 | sec_to_time | 11 | (i64) | string | Utf8 | seconds to HH:MM:SS |
| 18 | str_to_date | 11 | (string, string) | timestamp | Timestamp | parse with format |
| 19 | strftime | 11 | (timestamp, string) | string | Utf8 | format timestamp |
| 20 | subdate | 11 | (date, i64) | date | Date32 | subtract days |
| 21 | subtime | 11 | (timestamp, string) | timestamp | Timestamp | subtract time |
| 22 | sysdate | 0* | () | timestamp | Timestamp | non-deterministic, Volatile |
| 23 | time | 11 | (string) | string | Utf8 | extract time portion |
| 24 | timediff | 11 | (string, string) | string | Utf8 | time difference |
| 25 | time_to_sec | 11 | (string) | i64 | Int64 | time to seconds |
| 26 | timestamp | 11 | (string) | timestamp | Timestamp | parse to timestamp |
| 27 | to_days | 11 | (date) | i64 | Int64 | date to day number |
| 28 | unix_timestamp | 11 | (timestamp) | fp64 | Float64 | ts to epoch seconds |
| 29 | utc_date | 0* | () | date | Date32 | non-deterministic, Volatile |
| 30 | utc_time | 0* | () | string | Utf8 | non-deterministic, Volatile |
| 31 | utc_timestamp | 0* | () | timestamp | Timestamp | non-deterministic, Volatile |
| 32 | weekday | 12 | (date) | i32 | Int32 | 0=Mon, 6=Sun |
| 33 | yearweek | 11 | (date) | i32 | Int32 | YYYYWW ISO week |

**Total test assertions: 329** (29 deterministic functions × ~11 each + get_format with 18)
*Non-deterministic functions (sysdate, utc_date, utc_time, utc_timestamp) have 0 assertable tests.

## 3. DataFusion Type Mapping (from test_harness.rs)

| DSL Type | DataFusion Type | SQL Literal Format |
|----------|----------------|--------------------|
| str | Utf8 (VARCHAR) | `'value'` |
| i32 | Int32 (INT) | `123` |
| i64 | Int64 (BIGINT) | `123` |
| fp64 | Float64 (DOUBLE) | `123.0` |
| date | Date32 (DATE) | `DATE 'YYYY-MM-DD'` |
| timestamp | Timestamp (TIMESTAMP) | `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` |

## 4. Implementation Pattern (from ip.rs and json.rs)

Each UDF follows this pattern:
```rust
#[derive(Debug)]
struct FuncNameUdf { signature: Signature }
impl FuncNameUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![/* arg DataTypes */], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for FuncNameUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "func_name" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Xxx) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        // 1. Expand args to arrays via to_array() helper
        // 2. Downcast to typed arrays (StringArray, Int32Array, etc.)
        // 3. Iterate rows, handle nulls, compute result
        // 4. Collect into result array, wrap in ColumnarValue::Array
    }
}
```

Registration:
```rust
pub fn register_datetime_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(FuncNameUdf::new()));
    // ... one line per function
}
```

Key helpers used in ip.rs/json.rs:
- `to_array(arg, num_rows)` / `to_string_array(arg, num_rows)` — expand ColumnarValue to ArrayRef
- Null check: `if arr.is_null(i) { None } else { Some(...) }`
- Return `None` from iterator → null in output array

## 5. Required Crates (from YAML ai_hints)

- `chrono` — used by most functions (date/time parsing, formatting, arithmetic)
- `chrono-tz` — needed specifically for `convert_tz` timezone conversion

## 6. Function Complexity Groups

**Simple (pure arithmetic, no chrono):** get_format, period_add, period_diff, sec_to_time, minute_of_day, time_to_sec, microsecond, maketime
**Medium (chrono NaiveDate/NaiveDateTime):** adddate, subdate, date, dayname, monthname, weekday, yearweek, from_days, to_days, makedate, from_unixtime, unix_timestamp, timestamp, time, timediff
**Format-based (chrono strftime):** date_format, strftime, str_to_date
**Time arithmetic (parse time strings):** addtime, subtime
**Timezone (chrono-tz):** convert_tz
**Non-deterministic (Utc::now):** sysdate, utc_date, utc_time, utc_timestamp

## 7. Key Test Edge Cases to Watch

- `from_days(1) = '0000-01-01'` — day 1 maps to year 0000 (not 0001!)
- `from_days(0) = null` — zero/negative → null
- `period_add(9901, 1) = 199902` — 2-digit year: 99 → 1999
- `yearweek('2024-12-31') = 202501` — ISO week at year boundary
- `yearweek('2000-01-01') = 199952` — ISO week at year boundary
- `adddate` / `subdate` with negative days (reverses direction)
- `from_unixtime(0.5) = '1970-01-01 00:00:00'` — fractional seconds truncated
- `date('') = null`, `time('') = null`, `timestamp('') = null` — empty string → null
- `convert_tz` supports both named TZ ('US/Eastern') and offset ('+05:30')
