use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use regex::Regex;
use serde_json::Map;

/// Helper: extract a string array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_string_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// rex(source: str, pattern: str) -> str
// Extracts named capture groups from regex, returns JSON object.
// ============================================================
#[derive(Debug)]
struct RexUdf { signature: Signature }
impl RexUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for RexUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "rex" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let src_arr = to_string_array(&args[0], num_rows);
        let pat_arr = to_string_array(&args[1], num_rows);
        let srcs = src_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pat_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..srcs.len())
            .map(|i| {
                if srcs.is_null(i) || pats.is_null(i) {
                    return None;
                }
                let re = match Regex::new(pats.value(i)) {
                    Ok(r) => r,
                    Err(_) => return None,
                };
                let caps = match re.captures(srcs.value(i)) {
                    Some(c) => c,
                    None => return None,
                };
                // Build JSON from named capture groups
                let mut map = Map::new();
                for name in re.capture_names().flatten() {
                    if let Some(m) = caps.name(name) {
                        map.insert(name.to_string(), serde_json::Value::String(m.as_str().to_string()));
                    }
                }
                if map.is_empty() { None } else { Some(serde_json::Value::Object(map).to_string()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// replace(source: str, pattern: str, replacement: str) -> str
// Replaces all regex matches in a string.
// ============================================================
#[derive(Debug)]
struct ReplaceUdf { signature: Signature }
impl ReplaceUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for ReplaceUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "replace" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let src_arr = to_string_array(&args[0], num_rows);
        let pat_arr = to_string_array(&args[1], num_rows);
        let rep_arr = to_string_array(&args[2], num_rows);
        let srcs = src_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pat_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let reps = rep_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..srcs.len())
            .map(|i| {
                if srcs.is_null(i) || pats.is_null(i) || reps.is_null(i) {
                    return None;
                }
                let re = match Regex::new(pats.value(i)) {
                    Ok(r) => r,
                    Err(_) => return None,
                };
                Some(re.replace_all(srcs.value(i), reps.value(i)).into_owned())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// dissect(source: str, pattern: str) -> str
// Parses a string using dissect tokenizer (%{fieldname}), returns JSON.
// ============================================================
#[derive(Debug)]
struct DissectUdf { signature: Signature }
impl DissectUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}

/// Parse a dissect pattern into (field_names, separators).
/// Pattern like "%{name} %{age}" → fields=["name","age"], separators=["", " ", ""]
/// separators[0] is prefix before first field, separators[i+1] is between field i and i+1.
fn parse_dissect_pattern(pattern: &str) -> Option<(Vec<String>, Vec<String>)> {
    let mut fields = Vec::new();
    let mut separators = Vec::new();
    let mut rest = pattern;

    loop {
        match rest.find("%{") {
            Some(pos) => {
                separators.push(rest[..pos].to_string());
                rest = &rest[pos + 2..];
                match rest.find('}') {
                    Some(end) => {
                        fields.push(rest[..end].to_string());
                        rest = &rest[end + 1..];
                    }
                    None => return None,
                }
            }
            None => {
                // Trailing separator after last field
                separators.push(rest.to_string());
                break;
            }
        }
    }
    if fields.is_empty() { None } else { Some((fields, separators)) }
}

/// Apply a dissect pattern to an input string, returning field values.
fn apply_dissect(input: &str, fields: &[String], separators: &[String]) -> Option<Vec<String>> {
    let mut pos = 0;

    // Match leading separator
    if !input[pos..].starts_with(&separators[0]) {
        return None;
    }
    pos += separators[0].len();

    let mut values = Vec::with_capacity(fields.len());
    for i in 0..fields.len() {
        let sep = &separators[i + 1];
        if i == fields.len() - 1 && sep.is_empty() {
            // Last field with no trailing separator: consume rest
            values.push(input[pos..].to_string());
        } else if sep.is_empty() {
            // Empty separator between fields — shouldn't normally happen in well-formed patterns
            return None;
        } else {
            match input[pos..].find(sep.as_str()) {
                Some(idx) => {
                    values.push(input[pos..pos + idx].to_string());
                    pos += idx + sep.len();
                }
                None => return None,
            }
        }
    }
    // If there's a trailing separator, verify remaining input matches
    if fields.len() < separators.len() - 1 {
        return None;
    }
    Some(values)
}

impl ScalarUDFImpl for DissectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "dissect" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let src_arr = to_string_array(&args[0], num_rows);
        let pat_arr = to_string_array(&args[1], num_rows);
        let srcs = src_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pat_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..srcs.len())
            .map(|i| {
                if srcs.is_null(i) || pats.is_null(i) {
                    return None;
                }
                let input = srcs.value(i);
                if input.is_empty() {
                    return None;
                }
                let (fields, separators) = parse_dissect_pattern(pats.value(i))?;
                let values = apply_dissect(input, &fields, &separators)?;
                // Build sorted JSON object (BTreeMap for deterministic key order)
                let mut map = std::collections::BTreeMap::new();
                for (f, v) in fields.iter().zip(values.iter()) {
                    map.insert(f.clone(), serde_json::Value::String(v.clone()));
                }
                Some(serde_json::Value::Object(map.into_iter().collect()).to_string())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_rex_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RexUdf::new()));
    ctx.register_udf(ScalarUDF::from(ReplaceUdf::new()));
    ctx.register_udf(ScalarUDF::from(DissectUdf::new()));
}
