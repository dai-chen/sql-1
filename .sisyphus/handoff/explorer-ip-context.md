# IP UDF Implementation Context

## Crate Structure

- **Crate**: `unified-functions-datafusion/` (edition 2021)
- **Entry**: `src/lib.rs` — declares modules, exports `register_all_udfs(ctx: &SessionContext)`
- **IP stub**: `src/ip.rs` — empty `register_ip_udfs(_ctx: &SessionContext) {}`
- **All modules are stubs** — no existing ScalarUDF implementations anywhere in the project

## Registration Pattern

```rust
// lib.rs calls each module's register function:
pub fn register_all_udfs(ctx: &SessionContext) {
    ip::register_ip_udfs(ctx);
    // ... other modules
}

// Each module must implement:
pub fn register_ip_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MyUdf::new()));
    // ...
}
```

## DataFusion 45 ScalarUDFImpl Pattern

Since no existing implementations exist, use the standard DataFusion 45 `ScalarUDFImpl` trait:

```rust
use datafusion::logical_expr::{ScalarUDFImpl, ScalarUDF, Signature, Volatility};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
struct CidrMatchUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CidrMatchUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cidrmatch" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> { ... }
}
```

## 8 Functions to Implement

| Function | Args | Return | Null Handling | Key Logic |
|----------|------|--------|---------------|-----------|
| `cidrmatch` | (str, str) | bool | null→null, invalid→false | `IpNetwork::contains()` |
| `is_ipv4` | (str) | bool | null→null, invalid→false | `Ipv4Addr::from_str()` |
| `is_ipv6` | (str) | bool | null→null, invalid→false | `Ipv6Addr::from_str()` |
| `ip_to_int` | (str) | i64 | null→null, invalid→null | Parse Ipv4Addr → u32 → i64 |
| `int_to_ip` | (i64) | str | null→null, out-of-range→null | i64 → u32 → Ipv4Addr |
| `ip_version` | (str) | i32 | null→null, invalid→null | Try v4 then v6 parse |
| `ip_prefix` | (str, i32) | str | null→null, invalid/OOB→null | `IpNetwork::new()` |
| `geoip` | (str, str) | str | null→null, all edge→null | Needs maxminddb (not in deps) |

## Test Harness (test_harness.rs)

- **`parse_test_file(path)`** — parses `.test` DSL files into `Vec<TestCase>`
- **`test_case_to_sql(tc)`** — converts a TestCase to `SELECT func(args...)` SQL
- **`run_test_file(ctx, path)`** — executes all cases against a SessionContext, returns `Vec<TestResult>`
- **DSL types**: `DslType::Str`, `I32`, `I64`, `Fp64`, `Bool`, `Date`, `Timestamp`
- **TypedLiteral**: `{ value: Option<String>, dsl_type: DslType }`
- **Comparison**: null expected → checks `col.is_null(0)`, otherwise string equality

## Spec Tests (tests/spec_tests.rs)

- Tests only parse `.test` files and verify SQL generation — no execution tests yet
- Test file path: `../api/src/main/resources/unified-functions/tests/unified_ip_functions.test`
- Execution tests would use `run_test_file()` with a SessionContext that has UDFs registered

## Test File Summary (unified_ip_functions.test)

- **Total**: ~80 test cases across 8 functions
- **Groups per function**: basic, null_handling, edge_cases
- **Format**: `func(arg::type, ...) = expected::type`
- **geoip tests**: Require external GeoIP database (placeholder expected values)

## Available Dependencies (Cargo.toml)

| Crate | Version | Use |
|-------|---------|-----|
| `datafusion` | 45 | UDF framework, SessionContext |
| `arrow` | 54 | Array types, ColumnarValue |
| `ipnetwork` | 0.20 | CIDR parsing, `IpNetwork::contains()`, `IpNetwork::new()` |
| `chrono` | 0.4 | (datetime, not needed for IP) |
| `regex` | 1 | (not needed for IP) |
| `serde_json` | 1 | (not needed for IP) |

**Not in deps**: `maxminddb` — needed for `geoip`. Either add to Cargo.toml or stub geoip.

## Key Implementation Notes

1. All 7 non-geoip functions use only `std::net` + `ipnetwork` (already in deps)
2. `geoip` requires `maxminddb` crate + a GeoLite2 database file — likely needs to be stubbed or deferred
3. Null handling: check for null arrays first, propagate nulls through
4. Use `arrow::array::StringArray`, `Int32Array`, `Int64Array`, `BooleanArray` for columnar ops
5. `ColumnarValue` can be `Array` or `Scalar` — handle both or convert scalar to array
6. Functions should use `Signature::exact()` or `Signature::uniform()` with `Volatility::Immutable`
