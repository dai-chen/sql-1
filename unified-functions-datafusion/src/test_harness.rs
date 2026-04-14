//! Test harness for parsing and executing Substrait-style .test spec files against DataFusion.
//!
//! The DSL format:
//! ```text
//! ### SUBSTRAIT_SCALAR_TEST: v1.0
//! ### SUBSTRAIT_INCLUDE: extension:org.opensearch:unified_ip_functions
//!
//! # function_name
//! # group: group_name
//! func(arg1::type, arg2::type) = expected::type
//! ```

use datafusion::prelude::SessionContext;
use std::fmt;
use std::fs;

/// Supported types in the test DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum DslType {
    Str,
    I32,
    I64,
    Fp64,
    Bool,
    Date,
    Timestamp,
}

impl fmt::Display for DslType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DslType::Str => write!(f, "str"),
            DslType::I32 => write!(f, "i32"),
            DslType::I64 => write!(f, "i64"),
            DslType::Fp64 => write!(f, "fp64"),
            DslType::Bool => write!(f, "bool"),
            DslType::Date => write!(f, "date"),
            DslType::Timestamp => write!(f, "timestamp"),
        }
    }
}

/// A literal value with its DSL type.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedLiteral {
    pub value: Option<String>, // None means null
    pub dsl_type: DslType,
}

/// A single parsed test case.
#[derive(Debug, Clone)]
pub struct TestCase {
    pub function_name: String,
    pub args: Vec<TypedLiteral>,
    pub expected: TypedLiteral,
    pub line_number: usize,
    pub group_name: String,
}

/// Result of executing a single test case.
#[derive(Debug)]
pub struct TestResult {
    pub test_case: TestCase,
    pub passed: bool,
    pub actual: String,
    pub error: Option<String>,
}

/// Parse a DSL type string into a DslType.
fn parse_dsl_type(s: &str) -> Result<DslType, String> {
    match s {
        "str" => Ok(DslType::Str),
        "i32" => Ok(DslType::I32),
        "i64" => Ok(DslType::I64),
        "fp64" => Ok(DslType::Fp64),
        "bool" => Ok(DslType::Bool),
        "date" => Ok(DslType::Date),
        "timestamp" => Ok(DslType::Timestamp),
        _ => Err(format!("Unknown type: {}", s)),
    }
}

/// Parse a single typed literal like `'hello'::str`, `123::i32`, `null::bool`, etc.
/// Returns the TypedLiteral and the number of bytes consumed from `input`.
fn parse_typed_literal(input: &str) -> Result<(TypedLiteral, usize), String> {
    let s = input.trim_start();
    let leading_ws = input.len() - s.len();

    if s.starts_with("null::") {
        // null::type
        let rest = &s[6..];
        let type_end = rest.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(rest.len());
        let dsl_type = parse_dsl_type(rest[..type_end].trim())?;
        Ok((TypedLiteral { value: None, dsl_type }, leading_ws + 6 + type_end))
    } else if s.starts_with('\'') {
        // Quoted string: find matching closing quote
        // Strings can contain any characters inside quotes
        let mut i = 1;
        let bytes = s.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                // Check for escaped quote ('')
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                break;
            }
            i += 1;
        }
        if i >= bytes.len() {
            return Err(format!("Unterminated string literal in: {}", s));
        }
        let str_val = s[1..i].to_string();
        // After closing quote, expect ::type
        let after_quote = &s[i + 1..];
        if !after_quote.starts_with("::") {
            return Err(format!("Expected :: after string literal, got: {}", after_quote));
        }
        let type_str = &after_quote[2..];
        let type_end = type_str.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(type_str.len());
        let dsl_type = parse_dsl_type(type_str[..type_end].trim())?;
        let consumed = leading_ws + (i + 1) + 2 + type_end;
        Ok((TypedLiteral { value: Some(str_val), dsl_type }, consumed))
    } else {
        // Unquoted literal: number, bool, negative number
        // Find the :: separator
        let sep = s.find("::").ok_or_else(|| format!("Expected :: in literal: {}", s))?;
        let raw_val = s[..sep].trim().to_string();
        let type_str = &s[sep + 2..];
        let type_end = type_str.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(type_str.len());
        let dsl_type = parse_dsl_type(type_str[..type_end].trim())?;
        let consumed = leading_ws + sep + 2 + type_end;
        Ok((TypedLiteral { value: Some(raw_val), dsl_type }, consumed))
    }
}

/// Parse the argument list between parentheses. Handles nested quotes with commas inside.
/// `input` should start right after the opening '('.
/// Returns the list of args and the position after the closing ')'.
fn parse_args(input: &str) -> Result<(Vec<TypedLiteral>, usize), String> {
    let mut args = Vec::new();
    let mut pos = 0;

    // Skip whitespace
    while pos < input.len() && input.as_bytes()[pos] == b' ' {
        pos += 1;
    }

    // Empty args: immediate closing paren
    if pos < input.len() && input.as_bytes()[pos] == b')' {
        return Ok((args, pos + 1));
    }

    loop {
        let (lit, consumed) = parse_typed_literal(&input[pos..])?;
        pos += consumed;
        args.push(lit);

        // Skip whitespace
        while pos < input.len() && input.as_bytes()[pos] == b' ' {
            pos += 1;
        }

        if pos >= input.len() {
            return Err("Unexpected end of input while parsing args".to_string());
        }

        match input.as_bytes()[pos] {
            b')' => {
                pos += 1;
                break;
            }
            b',' => {
                pos += 1; // skip comma
            }
            c => return Err(format!("Unexpected char '{}' in arg list at pos {}", c as char, pos)),
        }
    }

    Ok((args, pos))
}

/// Parse a test case line like: `func_name(arg1::type, arg2::type) = expected::type`
fn parse_test_line(line: &str) -> Result<(String, Vec<TypedLiteral>, TypedLiteral), String> {
    let paren_pos = line.find('(')
        .ok_or_else(|| format!("No '(' found in test line: {}", line))?;
    let func_name = line[..paren_pos].trim().to_string();
    if func_name.is_empty() {
        return Err(format!("Empty function name in: {}", line));
    }

    let after_name = &line[paren_pos + 1..];
    let (args, consumed) = parse_args(after_name)?;

    // After closing paren, expect ` = expected::type`
    let rest = after_name[consumed..].trim_start();
    if !rest.starts_with('=') {
        return Err(format!("Expected '=' after args, got: {}", rest));
    }
    let expected_str = rest[1..].trim_start();
    let (expected, _) = parse_typed_literal(expected_str)?;

    Ok((func_name, args, expected))
}

/// Parse a .test file and return all test cases.
pub fn parse_test_file(path: &str) -> Result<Vec<TestCase>, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {}", path, e))?;

    let mut cases = Vec::new();
    let mut current_function = String::new();
    let mut current_group = String::new();

    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = idx + 1;

        // Skip blank lines and header lines
        if trimmed.is_empty() || trimmed.starts_with("### ") {
            continue;
        }

        // Comment lines: extract function name or group
        if trimmed.starts_with('#') {
            let comment = trimmed[1..].trim();
            if let Some(group) = comment.strip_prefix("group:") {
                current_group = group.trim().to_string();
            } else if !comment.starts_with("Note:") && !comment.contains(':') && !comment.is_empty() {
                // Bare comment with just a name = function name marker
                current_function = comment.to_string();
            }
            continue;
        }

        // Test case line
        match parse_test_line(trimmed) {
            Ok((func_name, args, expected)) => {
                // If the parsed function name differs from current_function, update it
                if current_function.is_empty() {
                    current_function = func_name.clone();
                }
                cases.push(TestCase {
                    function_name: func_name,
                    args,
                    expected,
                    line_number,
                    group_name: current_group.clone(),
                });
            }
            Err(e) => {
                return Err(format!("Parse error at line {}: {}", line_number, e));
            }
        }
    }

    Ok(cases)
}

/// Convert a TypedLiteral to a SQL expression string.
fn literal_to_sql(lit: &TypedLiteral) -> String {
    match &lit.value {
        None => {
            let sql_type = dsl_type_to_sql(&lit.dsl_type);
            format!("CAST(NULL AS {})", sql_type)
        }
        Some(val) => match lit.dsl_type {
            DslType::Str => format!("'{}'", val),
            DslType::Bool => val.to_string(),
            DslType::I32 | DslType::I64 | DslType::Fp64 => val.to_string(),
            DslType::Date => format!("DATE '{}'", val),
            DslType::Timestamp => format!("TIMESTAMP '{}'", val),
        },
    }
}

/// Map DSL type to SQL type name for CAST expressions.
fn dsl_type_to_sql(t: &DslType) -> &'static str {
    match t {
        DslType::Str => "VARCHAR",
        DslType::I32 => "INT",
        DslType::I64 => "BIGINT",
        DslType::Fp64 => "DOUBLE",
        DslType::Bool => "BOOLEAN",
        DslType::Date => "DATE",
        DslType::Timestamp => "TIMESTAMP",
    }
}

/// Generate a SQL SELECT statement for a test case.
pub fn test_case_to_sql(tc: &TestCase) -> String {
    let args_sql: Vec<String> = tc.args.iter().map(literal_to_sql).collect();
    format!("SELECT {}({})", tc.function_name, args_sql.join(", "))
}

/// Execute all test cases from a .test file against a DataFusion SessionContext.
pub async fn run_test_file(ctx: &SessionContext, path: &str) -> Result<Vec<TestResult>, String> {
    let cases = parse_test_file(path)?;
    let mut results = Vec::with_capacity(cases.len());

    for tc in cases {
        let sql = test_case_to_sql(&tc);
        let result = match ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() || batches[0].num_rows() == 0 {
                        TestResult {
                            test_case: tc,
                            passed: false,
                            actual: "<no rows>".to_string(),
                            error: None,
                        }
                    } else {
                        let col = batches[0].column(0);
                        let actual = arrow::util::display::array_value_to_string(col, 0)
                            .unwrap_or_else(|e| format!("<display error: {}>", e));
                        let expected_str = tc.expected.value.clone().unwrap_or_else(|| "NULL".to_string());
                        // Normalize comparison: treat empty-looking nulls
                        let passed = if tc.expected.value.is_none() {
                            col.is_null(0)
                        } else {
                            actual == expected_str
                        };
                        TestResult {
                            test_case: tc,
                            passed,
                            actual,
                            error: None,
                        }
                    }
                }
                Err(e) => TestResult {
                    test_case: tc,
                    passed: false,
                    actual: String::new(),
                    error: Some(format!("Execution error: {}", e)),
                },
            },
            Err(e) => TestResult {
                test_case: tc,
                passed: false,
                actual: String::new(),
                error: Some(format!("SQL error: {}", e)),
            },
        };
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_typed_literal_null() {
        let (lit, consumed) = parse_typed_literal("null::bool").unwrap();
        assert_eq!(lit.value, None);
        assert_eq!(lit.dsl_type, DslType::Bool);
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_parse_typed_literal_string() {
        let (lit, _) = parse_typed_literal("'hello'::str").unwrap();
        assert_eq!(lit.value, Some("hello".to_string()));
        assert_eq!(lit.dsl_type, DslType::Str);
    }

    #[test]
    fn test_parse_typed_literal_string_with_special_chars() {
        let (lit, _) = parse_typed_literal("'{\"a\":1,\"b\":2}'::str").unwrap();
        assert_eq!(lit.value, Some("{\"a\":1,\"b\":2}".to_string()));
        assert_eq!(lit.dsl_type, DslType::Str);
    }

    #[test]
    fn test_parse_typed_literal_number() {
        let (lit, _) = parse_typed_literal("3232235777::i64").unwrap();
        assert_eq!(lit.value, Some("3232235777".to_string()));
        assert_eq!(lit.dsl_type, DslType::I64);
    }

    #[test]
    fn test_parse_typed_literal_negative() {
        let (lit, _) = parse_typed_literal("-1::i64").unwrap();
        assert_eq!(lit.value, Some("-1".to_string()));
        assert_eq!(lit.dsl_type, DslType::I64);
    }

    #[test]
    fn test_parse_typed_literal_float() {
        let (lit, _) = parse_typed_literal("27.0::fp64").unwrap();
        assert_eq!(lit.value, Some("27.0".to_string()));
        assert_eq!(lit.dsl_type, DslType::Fp64);
    }

    #[test]
    fn test_parse_typed_literal_bool() {
        let (lit, _) = parse_typed_literal("true::bool").unwrap();
        assert_eq!(lit.value, Some("true".to_string()));
        assert_eq!(lit.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_parse_typed_literal_date() {
        let (lit, _) = parse_typed_literal("'2024-01-15'::date").unwrap();
        assert_eq!(lit.value, Some("2024-01-15".to_string()));
        assert_eq!(lit.dsl_type, DslType::Date);
    }

    #[test]
    fn test_parse_typed_literal_timestamp() {
        let (lit, _) = parse_typed_literal("'2024-01-15 10:30:00'::timestamp").unwrap();
        assert_eq!(lit.value, Some("2024-01-15 10:30:00".to_string()));
        assert_eq!(lit.dsl_type, DslType::Timestamp);
    }

    #[test]
    fn test_parse_test_line_basic() {
        let (name, args, expected) = parse_test_line(
            "cidrmatch('192.168.1.1'::str, '192.168.1.0/24'::str) = true::bool"
        ).unwrap();
        assert_eq!(name, "cidrmatch");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].value, Some("192.168.1.1".to_string()));
        assert_eq!(args[1].value, Some("192.168.1.0/24".to_string()));
        assert_eq!(expected.value, Some("true".to_string()));
        assert_eq!(expected.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_parse_test_line_zero_args() {
        let (name, args, expected) = parse_test_line(
            "e() = 2.718281828459045::fp64"
        ).unwrap();
        assert_eq!(name, "e");
        assert!(args.is_empty());
        assert_eq!(expected.value, Some("2.718281828459045".to_string()));
    }

    #[test]
    fn test_parse_test_line_json_string() {
        let (name, args, expected) = parse_test_line(
            "json_extract('{\"a\":1}'::str, '$.a'::str) = '1'::str"
        ).unwrap();
        assert_eq!(name, "json_extract");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].value, Some("{\"a\":1}".to_string()));
        assert_eq!(expected.value, Some("1".to_string()));
    }

    #[test]
    fn test_parse_test_line_null_result() {
        let (_, _, expected) = parse_test_line(
            "cidrmatch(null::str, '192.168.1.0/24'::str) = null::bool"
        ).unwrap();
        assert_eq!(expected.value, None);
        assert_eq!(expected.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_literal_to_sql_string() {
        let lit = TypedLiteral { value: Some("hello".to_string()), dsl_type: DslType::Str };
        assert_eq!(literal_to_sql(&lit), "'hello'");
    }

    #[test]
    fn test_literal_to_sql_null() {
        let lit = TypedLiteral { value: None, dsl_type: DslType::I32 };
        assert_eq!(literal_to_sql(&lit), "CAST(NULL AS INT)");
    }

    #[test]
    fn test_literal_to_sql_date() {
        let lit = TypedLiteral { value: Some("2024-01-15".to_string()), dsl_type: DslType::Date };
        assert_eq!(literal_to_sql(&lit), "DATE '2024-01-15'");
    }

    #[test]
    fn test_literal_to_sql_timestamp() {
        let lit = TypedLiteral { value: Some("2024-01-15 10:30:00".to_string()), dsl_type: DslType::Timestamp };
        assert_eq!(literal_to_sql(&lit), "TIMESTAMP '2024-01-15 10:30:00'");
    }

    #[test]
    fn test_test_case_to_sql() {
        let tc = TestCase {
            function_name: "cidrmatch".to_string(),
            args: vec![
                TypedLiteral { value: Some("192.168.1.1".to_string()), dsl_type: DslType::Str },
                TypedLiteral { value: Some("192.168.1.0/24".to_string()), dsl_type: DslType::Str },
            ],
            expected: TypedLiteral { value: Some("true".to_string()), dsl_type: DslType::Bool },
            line_number: 1,
            group_name: "basic".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT cidrmatch('192.168.1.1', '192.168.1.0/24')");
    }

    #[test]
    fn test_test_case_to_sql_with_null() {
        let tc = TestCase {
            function_name: "cidrmatch".to_string(),
            args: vec![
                TypedLiteral { value: None, dsl_type: DslType::Str },
                TypedLiteral { value: Some("192.168.1.0/24".to_string()), dsl_type: DslType::Str },
            ],
            expected: TypedLiteral { value: None, dsl_type: DslType::Bool },
            line_number: 1,
            group_name: "null_handling".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT cidrmatch(CAST(NULL AS VARCHAR), '192.168.1.0/24')");
    }

    #[test]
    fn test_test_case_to_sql_zero_args() {
        let tc = TestCase {
            function_name: "e".to_string(),
            args: vec![],
            expected: TypedLiteral { value: Some("2.718281828459045".to_string()), dsl_type: DslType::Fp64 },
            line_number: 1,
            group_name: "basic".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT e()");
    }
}
