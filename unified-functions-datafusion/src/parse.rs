use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use regex::Regex;
use serde_json::{Map, Value};

/// Helper: extract an array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

/// Expand basic grok patterns into regex equivalents.
fn expand_grok_pattern(pattern: &str) -> String {
    let grok_patterns: &[(&str, &str)] = &[
        ("IP", r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"),
        ("WORD", r"\w+"),
        ("NUMBER", r"[+-]?\d+\.?\d*"),
        ("URIPATHPARAM", r"\S+"),
        ("DATE", r"\d{4}-\d{2}-\d{2}"),
        ("GREEDYDATA", r".*"),
    ];
    let grok_re = Regex::new(r"%\{(\w+):(\w+)\}").unwrap();
    let mut result = pattern.to_string();
    // Iteratively replace grok tokens with named capture groups
    loop {
        let prev = result.clone();
        result = grok_re
            .replace_all(&prev, |caps: &regex::Captures| {
                let grok_name = &caps[1];
                let capture_name = &caps[2];
                let re_pattern = grok_patterns
                    .iter()
                    .find(|(name, _)| *name == grok_name)
                    .map(|(_, pat)| *pat)
                    .unwrap_or(r"\S+"); // fallback for unknown patterns
                format!("(?P<{}>{})", capture_name, re_pattern)
            })
            .to_string();
        if result == prev {
            break;
        }
    }
    result
}

// ============================================================
// parse(source: str, pattern: str) -> str
// Parse string using regex with named groups, return JSON object
// ============================================================
#[derive(Debug)]
struct ParseUdf {
    signature: Signature,
}
impl ParseUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ParseUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "parse" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_src = to_array(&args[0], num_rows);
        let arr_pat = to_array(&args[1], num_rows);
        let sources = arr_src.as_any().downcast_ref::<StringArray>().unwrap();
        let patterns = arr_pat.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..sources.len())
            .map(|i| {
                if sources.is_null(i) || patterns.is_null(i) {
                    return None;
                }
                let re = Regex::new(patterns.value(i)).ok()?;
                let caps = re.captures(sources.value(i))?;
                let mut map = Map::new();
                for name in re.capture_names().flatten() {
                    if let Some(m) = caps.name(name) {
                        map.insert(name.to_string(), Value::String(m.as_str().to_string()));
                    }
                }
                if map.is_empty() { None } else { Some(serde_json::to_string(&map).unwrap()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// patterns(source: str, pattern_type: str) -> str
// Extract known patterns (email/ip/url) from text, return JSON array
// ============================================================
#[derive(Debug)]
struct PatternsUdf {
    signature: Signature,
}
impl PatternsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for PatternsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "patterns" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_src = to_array(&args[0], num_rows);
        let arr_typ = to_array(&args[1], num_rows);
        let sources = arr_src.as_any().downcast_ref::<StringArray>().unwrap();
        let types = arr_typ.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..sources.len())
            .map(|i| {
                if sources.is_null(i) || types.is_null(i) {
                    return None;
                }
                let re = match types.value(i) {
                    "email" => Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").ok(),
                    "ip" => Regex::new(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}").ok(),
                    "url" => Regex::new(r"https?://[^\s]+").ok(),
                    _ => return None, // unknown pattern type → null
                };
                let re = re?;
                let matches: Vec<Value> = re
                    .find_iter(sources.value(i))
                    .map(|m| Value::String(m.as_str().to_string()))
                    .collect();
                Some(serde_json::to_string(&matches).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// grok(source: str, pattern: str) -> str
// Parse using grok pattern, return JSON object
// ============================================================
#[derive(Debug)]
struct GrokUdf {
    signature: Signature,
}
impl GrokUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for GrokUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "grok" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_src = to_array(&args[0], num_rows);
        let arr_pat = to_array(&args[1], num_rows);
        let sources = arr_src.as_any().downcast_ref::<StringArray>().unwrap();
        let patterns = arr_pat.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..sources.len())
            .map(|i| {
                if sources.is_null(i) || patterns.is_null(i) {
                    return None;
                }
                let expanded = expand_grok_pattern(patterns.value(i));
                let re = Regex::new(&expanded).ok()?;
                let caps = re.captures(sources.value(i))?;
                let mut map = Map::new();
                for name in re.capture_names().flatten() {
                    if let Some(m) = caps.name(name) {
                        map.insert(name.to_string(), Value::String(m.as_str().to_string()));
                    }
                }
                if map.is_empty() { None } else { Some(serde_json::to_string(&map).unwrap()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_parse_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ParseUdf::new()));
    ctx.register_udf(ScalarUDF::from(PatternsUdf::new()));
    ctx.register_udf(ScalarUDF::from(GrokUdf::new()));
}
