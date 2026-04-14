use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use serde_json::{Map, Value};

/// Helper: expand ColumnarValue to an array of `num_rows`.
fn to_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

/// Helper: get string value from StringArray, returning None if null.
fn get_str<'a>(arr: &'a StringArray, i: usize) -> Option<&'a str> {
    if arr.is_null(i) { None } else { Some(arr.value(i)) }
}

/// Parse a simple JSON path expression ($.key, $.key.nested, $.key[0]).
/// Returns the extracted Value or None.
fn json_path_extract(root: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix('$')?;
    let mut current = root;
    if path.is_empty() {
        return Some(current.clone());
    }
    // Split on '.' but handle array indices like [0]
    let path = path.strip_prefix('.')?;
    for segment in split_path_segments(path) {
        match segment {
            PathSegment::Key(key) => {
                current = current.get(key)?;
            }
            PathSegment::Index(idx) => {
                current = current.get(idx)?;
            }
            PathSegment::KeyThenIndex(key, idx) => {
                current = current.get(key)?.get(idx)?;
            }
        }
    }
    Some(current.clone())
}

enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
    KeyThenIndex(&'a str, usize),
}

fn split_path_segments(path: &str) -> Vec<PathSegment<'_>> {
    let mut segments = Vec::new();
    for part in path.split('.') {
        if let Some(bracket_pos) = part.find('[') {
            let key = &part[..bracket_pos];
            let idx_str = &part[bracket_pos + 1..part.len() - 1];
            let idx: usize = idx_str.parse().unwrap_or(0);
            if key.is_empty() {
                segments.push(PathSegment::Index(idx));
            } else {
                segments.push(PathSegment::KeyThenIndex(key, idx));
            }
        } else {
            segments.push(PathSegment::Key(part));
        }
    }
    segments
}

/// Serialize a serde_json::Value to its compact JSON string representation.
/// For the purposes of json_extract, this returns the raw JSON (numbers unquoted, strings with quotes).
fn value_to_json_string(v: &Value) -> String {
    // serde_json::to_string produces compact JSON
    serde_json::to_string(v).unwrap_or_default()
}

// ============================================================
// json_extract(json_string: str, path: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonExtractUdf { signature: Signature }
impl JsonExtractUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonExtractUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_extract" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let json_arr = to_array(&args[0], num_rows);
        let path_arr = to_array(&args[1], num_rows);
        let jsons = json_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let paths = path_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..jsons.len())
            .map(|i| {
                let json_str = get_str(jsons, i)?;
                let path_str = get_str(paths, i)?;
                let parsed: Value = serde_json::from_str(json_str).ok()?;
                let extracted = json_path_extract(&parsed, path_str)?;
                Some(value_to_json_string(&extracted))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_valid(json_string: str) -> bool
// ============================================================
#[derive(Debug)]
struct JsonValidUdf { signature: Signature }
impl JsonValidUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonValidUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_valid" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { Some(serde_json::from_str::<Value>(strs.value(i)).is_ok()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_keys(json_string: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonKeysUdf { signature: Signature }
impl JsonKeysUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                let s = get_str(strs, i)?;
                let parsed: Value = serde_json::from_str(s).ok()?;
                let obj = parsed.as_object()?;
                let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
                keys.sort();
                Some(keys.join(","))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_delete(json_string: str, key: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonDeleteUdf { signature: Signature }
impl JsonDeleteUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonDeleteUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_delete" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let json_arr = to_array(&args[0], num_rows);
        let key_arr = to_array(&args[1], num_rows);
        let jsons = json_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let keys = key_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..jsons.len())
            .map(|i| {
                let s = get_str(jsons, i)?;
                let k = get_str(keys, i)?;
                let mut parsed: Value = serde_json::from_str(s).ok()?;
                let obj = parsed.as_object_mut()?;
                obj.remove(k);
                Some(serde_json::to_string(&parsed).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_set(json_string: str, key: str, value: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonSetUdf { signature: Signature }
impl JsonSetUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonSetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_set" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let json_arr = to_array(&args[0], num_rows);
        let key_arr = to_array(&args[1], num_rows);
        let val_arr = to_array(&args[2], num_rows);
        let jsons = json_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let keys = key_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let vals = val_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..jsons.len())
            .map(|i| {
                let s = get_str(jsons, i)?;
                let k = get_str(keys, i)?;
                let v_str = get_str(vals, i)?;
                let mut parsed: Value = serde_json::from_str(s).ok()?;
                let val: Value = serde_json::from_str(v_str).ok()?;
                let obj = parsed.as_object_mut()?;
                obj.insert(k.to_string(), val);
                Some(serde_json::to_string(&parsed).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_append(json_string: str, key: str, value: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonAppendUdf { signature: Signature }
impl JsonAppendUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonAppendUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_append" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let json_arr = to_array(&args[0], num_rows);
        let key_arr = to_array(&args[1], num_rows);
        let val_arr = to_array(&args[2], num_rows);
        let jsons = json_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let keys = key_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let vals = val_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..jsons.len())
            .map(|i| {
                let s = get_str(jsons, i)?;
                let k = get_str(keys, i)?;
                let v_str = get_str(vals, i)?;
                let mut parsed: Value = serde_json::from_str(s).ok()?;
                let val: Value = serde_json::from_str(v_str).ok()?;
                let arr = parsed.get_mut(k)?.as_array_mut()?;
                arr.push(val);
                Some(serde_json::to_string(&parsed).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_extend(json_string: str, key: str, values: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonExtendUdf { signature: Signature }
impl JsonExtendUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonExtendUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_extend" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let json_arr = to_array(&args[0], num_rows);
        let key_arr = to_array(&args[1], num_rows);
        let val_arr = to_array(&args[2], num_rows);
        let jsons = json_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let keys = key_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let vals = val_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..jsons.len())
            .map(|i| {
                let s = get_str(jsons, i)?;
                let k = get_str(keys, i)?;
                let v_str = get_str(vals, i)?;
                let mut parsed: Value = serde_json::from_str(s).ok()?;
                let extend_val: Value = serde_json::from_str(v_str).ok()?;
                let extend_arr = extend_val.as_array()?;
                let target = parsed.get_mut(k)?.as_array_mut()?;
                target.extend(extend_arr.iter().cloned());
                Some(serde_json::to_string(&parsed).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_array(value1: str, value2: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonArrayUdf { signature: Signature }
impl JsonArrayUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr0 = to_array(&args[0], num_rows);
        let arr1 = to_array(&args[1], num_rows);
        let strs0 = arr0.as_any().downcast_ref::<StringArray>().unwrap();
        let strs1 = arr1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs0.len())
            .map(|i| {
                // null args become JSON null in the array
                let v0 = if strs0.is_null(i) { Value::Null } else { serde_json::from_str(strs0.value(i)).unwrap_or(Value::Null) };
                let v1 = if strs1.is_null(i) { Value::Null } else { serde_json::from_str(strs1.value(i)).unwrap_or(Value::Null) };
                Some(serde_json::to_string(&Value::Array(vec![v0, v1])).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// json_object(key: str, value: str) -> str
// ============================================================
#[derive(Debug)]
struct JsonObjectUdf { signature: Signature }
impl JsonObjectUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for JsonObjectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_object" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let key_arr = to_array(&args[0], num_rows);
        let val_arr = to_array(&args[1], num_rows);
        let keys = key_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let vals = val_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..keys.len())
            .map(|i| {
                let k = get_str(keys, i)?; // null key -> null result
                let v = if vals.is_null(i) { Value::Null } else { serde_json::from_str(vals.value(i)).unwrap_or(Value::Null) };
                let mut map = Map::new();
                map.insert(k.to_string(), v);
                Some(serde_json::to_string(&Value::Object(map)).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_json_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonValidUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonKeysUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonDeleteUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonSetUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonAppendUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonExtendUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonArrayUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonObjectUdf::new()));
}
