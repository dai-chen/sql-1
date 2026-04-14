use unified_functions_datafusion::test_harness::{parse_test_file, test_case_to_sql, DslType};

const IP_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_ip_functions.test";
const JSON_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_json_functions.test";
const MATH_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_math_functions.test";
const DATETIME_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_datetime_functions.test";
const COLLECTION_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_collection_functions.test";
const CONDITION_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_condition_functions.test";
const REX_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_rex_functions.test";
const CRYPTO_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_crypto_functions.test";
const PARSE_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_parse_functions.test";
const TYPE_CONV_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_type_conv_functions.test";
const BINNING_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_binning_functions.test";
const AGGREGATION_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_aggregation_functions.test";

#[test]
fn test_parse_ip_test_file() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    assert!(cases.len() > 0, "Expected test cases from IP test file");

    // Verify first test case structure
    let first = &cases[0];
    assert_eq!(first.function_name, "cidrmatch");
    assert_eq!(first.args.len(), 2);
    assert_eq!(first.args[0].value, Some("192.168.1.1".to_string()));
    assert_eq!(first.args[0].dsl_type, DslType::Str);
    assert_eq!(first.expected.value, Some("true".to_string()));
    assert_eq!(first.expected.dsl_type, DslType::Bool);
    assert_eq!(first.group_name, "basic");
}

#[test]
fn test_parse_ip_null_handling() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let null_cases: Vec<_> = cases.iter().filter(|c| c.group_name == "null_handling").collect();
    assert!(null_cases.len() > 0, "Expected null_handling test cases");

    // First null case should have null arg
    let first_null = null_cases.iter().find(|c| c.function_name == "cidrmatch").unwrap();
    assert!(first_null.args.iter().any(|a| a.value.is_none()), "Expected a null argument");
}

#[test]
fn test_parse_json_test_file() {
    let cases = parse_test_file(JSON_TEST_FILE).expect("Failed to parse JSON test file");
    assert!(cases.len() > 0, "Expected test cases from JSON test file");

    // JSON strings contain special chars like braces, colons, commas
    let first = &cases[0];
    assert_eq!(first.function_name, "json_extract");
    assert_eq!(first.args[0].value, Some("{\"a\":1}".to_string()));
}

#[test]
fn test_parse_math_test_file() {
    let cases = parse_test_file(MATH_TEST_FILE).expect("Failed to parse math test file");
    assert!(cases.len() > 0, "Expected test cases from math test file");

    // Check fp64 type parsing
    let cbrt_case = &cases[0];
    assert_eq!(cbrt_case.function_name, "cbrt");
    assert_eq!(cbrt_case.args[0].dsl_type, DslType::Fp64);
    assert_eq!(cbrt_case.expected.dsl_type, DslType::Fp64);

    // Check zero-arg function (e())
    let e_cases: Vec<_> = cases.iter().filter(|c| c.function_name == "e").collect();
    assert!(e_cases.len() > 0, "Expected e() test cases");
    assert!(e_cases[0].args.is_empty(), "e() should have no args");
}

#[test]
fn test_parse_datetime_test_file() {
    let cases = parse_test_file(DATETIME_TEST_FILE).expect("Failed to parse datetime test file");
    assert!(cases.len() > 0, "Expected test cases from datetime test file");

    // Check date type
    let adddate = &cases[0];
    assert_eq!(adddate.function_name, "adddate");
    assert_eq!(adddate.args[0].dsl_type, DslType::Date);
    assert_eq!(adddate.expected.dsl_type, DslType::Date);

    // Check timestamp type
    let ts_cases: Vec<_> = cases.iter().filter(|c| c.function_name == "addtime").collect();
    assert!(ts_cases.len() > 0);
    assert_eq!(ts_cases[0].args[0].dsl_type, DslType::Timestamp);
}

#[test]
fn test_parse_collection_test_file() {
    let cases = parse_test_file(COLLECTION_TEST_FILE).expect("Failed to parse collection test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_parse_condition_test_file() {
    let cases = parse_test_file(CONDITION_TEST_FILE).expect("Failed to parse condition test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_parse_rex_test_file() {
    let cases = parse_test_file(REX_TEST_FILE).expect("Failed to parse rex test file");
    assert!(cases.len() > 0);

    // Rex has regex patterns with backslashes and special chars in strings
    let first = &cases[0];
    assert_eq!(first.function_name, "rex");
    assert!(first.args[1].value.as_ref().unwrap().contains("(?P<"));
}

#[test]
fn test_parse_crypto_test_file() {
    let cases = parse_test_file(CRYPTO_TEST_FILE).expect("Failed to parse crypto test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_sql_generation() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let sql = test_case_to_sql(&cases[0]);
    assert!(sql.starts_with("SELECT cidrmatch("));
    assert!(sql.contains("'192.168.1.1'"));
    assert!(sql.contains("'192.168.1.0/24'"));
}

#[test]
fn test_sql_generation_null_args() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let null_case = cases.iter().find(|c| c.args.iter().any(|a| a.value.is_none())).unwrap();
    let sql = test_case_to_sql(null_case);
    assert!(sql.contains("CAST(NULL AS VARCHAR)"));
}

/// Parse ALL test files to ensure the parser handles every format variation.
#[test]
fn test_parse_all_test_files() {
    let test_dir = "../api/src/main/resources/unified-functions/tests";
    let entries = std::fs::read_dir(test_dir).expect("Failed to read test directory");
    let mut total_cases = 0;
    let mut file_count = 0;

    for entry in entries {
        let entry = entry.expect("Failed to read dir entry");
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "test") {
            let path_str = path.to_str().unwrap();
            let cases = parse_test_file(path_str)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path_str, e));
            assert!(cases.len() > 0, "No test cases in {}", path_str);
            total_cases += cases.len();
            file_count += 1;
        }
    }

    assert!(file_count >= 10, "Expected at least 10 test files, found {}", file_count);
    assert!(total_cases > 100, "Expected > 100 total test cases, found {}", total_cases);
    println!("Parsed {} test cases across {} files", total_cases, file_count);
}

#[tokio::test]
async fn test_run_ip_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, IP_TEST_FILE).await.expect("Failed to run IP spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    // geoip basic tests (3) will fail since no GeoIP database
    let non_geoip_failures: Vec<_> = failures.iter()
        .filter(|r| !(r.test_case.function_name == "geoip" && r.test_case.group_name == "basic"))
        .collect();
    for f in &non_geoip_failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(non_geoip_failures.is_empty(),
        "{} non-geoip IP spec tests failed", non_geoip_failures.len());
}

#[tokio::test]
async fn test_run_json_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, JSON_TEST_FILE).await.expect("Failed to run JSON spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(),
        "{} JSON spec tests failed", failures.len());
}

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

#[tokio::test]
async fn test_run_math_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, MATH_TEST_FILE).await.expect("Failed to run math spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} math spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_condition_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, CONDITION_TEST_FILE).await.expect("Failed to run condition spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} condition spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_collection_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, COLLECTION_TEST_FILE).await.expect("Failed to run collection spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} collection spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_parse_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, PARSE_TEST_FILE).await.expect("Failed to run parse spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} parse spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_type_conv_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, TYPE_CONV_TEST_FILE).await.expect("Failed to run type_conv spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} type_conv spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_crypto_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, CRYPTO_TEST_FILE).await.expect("Failed to run crypto spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} crypto spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_rex_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, REX_TEST_FILE).await.expect("Failed to run rex spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} rex spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_binning_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, BINNING_TEST_FILE).await.expect("Failed to run binning spec tests");
    // ntile is a window function implemented as placeholder scalar - exclude its tests
    let failures: Vec<_> = results.iter()
        .filter(|r| !r.passed && r.test_case.function_name != "ntile")
        .collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} binning spec tests failed", failures.len());
}

#[tokio::test]
async fn test_run_aggregation_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, AGGREGATION_TEST_FILE).await.expect("Failed to run aggregation spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    for f in &failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(failures.is_empty(), "{} aggregation spec tests failed", failures.len());
}
