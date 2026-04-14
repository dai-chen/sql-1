# Test Harness Analysis

## 1. DslType Enum (test_harness.rs:18-25)

```rust
pub enum DslType {
    Str, I32, I64, Fp64, Bool, Date, Timestamp,
}
```

All 7 types are supported. Date and Timestamp are already present.

## 2. SQL Generation — `literal_to_sql()` (test_harness.rs:193-205)

Key type-to-SQL mappings:
```rust
DslType::Str       => format!("'{}'", val)           // 'hello'
DslType::Bool      => val.to_string()                 // true
DslType::I32/I64/Fp64 => val.to_string()              // 42, 3.14
DslType::Date      => format!("DATE '{}'", val)       // DATE '2024-01-15'
DslType::Timestamp => format!("TIMESTAMP '{}'", val)  // TIMESTAMP '2024-01-15 10:30:00'
```

For nulls: `CAST(NULL AS <SQL_TYPE>)` where SQL types map as:
- `Date` → `DATE`, `Timestamp` → `TIMESTAMP`, `Str` → `VARCHAR`, etc.

## 3. `test_case_to_sql()` (test_harness.rs:218-221)

Simply joins args with `literal_to_sql()`:
```rust
pub fn test_case_to_sql(tc: &TestCase) -> String {
    let args_sql: Vec<String> = tc.args.iter().map(literal_to_sql).collect();
    format!("SELECT {}({})", tc.function_name, args_sql.join(", "))
}
```

Example output: `SELECT adddate(DATE '2024-01-15', 10)`

## 4. `run_test_file()` (test_harness.rs:224-272)

Flow:
1. `parse_test_file(path)` → `Vec<TestCase>`
2. For each case: generate SQL via `test_case_to_sql()`, execute via `ctx.sql(&sql).await`
3. Extract first column of first row using `arrow::util::display::array_value_to_string(col, 0)`
4. Compare: if expected is null → check `col.is_null(0)`, else string equality `actual == expected_str`
5. Returns `Vec<TestResult>` with `passed`, `actual`, `error` fields

## 5. .test File DSL Format (parsed in `parse_test_file()`)

```text
### SUBSTRAIT_SCALAR_TEST: v1.0
### SUBSTRAIT_INCLUDE: extension:org.opensearch:unified_datetime_functions

# function_name
# group: group_name
func_name('2024-01-15'::date, 10::i32) = '2024-01-25'::date
func_name('2024-01-15 10:30:00'::timestamp, '01:00:00'::str) = '2024-01-15 11:30:00'::timestamp
func_name(null::date, 10::i32) = null::date
```

- `# comment` with just a name → sets current function name
- `# group: name` → sets current group
- Date values: `'YYYY-MM-DD'::date`
- Timestamp values: `'YYYY-MM-DD HH:MM:SS'::timestamp`

## 6. Spec Test Pattern (spec_tests.rs)

### Adding a new spec test runner:

```rust
// 1. Define the test file path constant (already exists for datetime):
const DATETIME_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_datetime_functions.test";

// 2. Add an async test function following this pattern:
#[tokio::test]
async fn test_run_datetime_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, DATETIME_TEST_FILE).await.expect("Failed to run datetime spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(),
        "{} datetime spec tests failed", failures.len());
}
```

### Existing patterns for filtering expected failures (from IP tests):
```rust
// Filter out known failures (e.g., geoip needs external DB)
let non_geoip_failures: Vec<_> = failures.iter()
    .filter(|r| !(r.test_case.function_name == "geoip" && r.test_case.group_name == "basic"))
    .collect();
```

## 7. Key Observations

- **DATETIME_TEST_FILE is already defined** in spec_tests.rs but has NO `test_run_datetime_spec_tests()` runner yet
- **Parse test exists** (`test_parse_datetime_test_file`) — validates parsing works
- **Result comparison is string-based** — `arrow::util::display::array_value_to_string` converts the result to string, then compares with expected value string
- **Date/Timestamp SQL literals** use standard SQL syntax: `DATE '...'` and `TIMESTAMP '...'`
- **No tolerance for floating point** — comparison is exact string match
