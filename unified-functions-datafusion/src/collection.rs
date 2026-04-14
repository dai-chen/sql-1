use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use serde_json::Value;

/// Helper: extract an array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

/// Parse a JSON string as a Vec<Value>, returning None on failure.
fn parse_json_array(s: &str) -> Option<Vec<Value>> {
    serde_json::from_str::<Vec<Value>>(s).ok()
}

/// Compare two serde_json Values for sorting.
/// Numbers are compared numerically, everything else as strings.
fn cmp_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(fa), Some(fb)) => fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// Parse a search value string into a serde_json::Value for comparison.
fn parse_search_value(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or(Value::String(s.to_string()))
}

// ============================================================
// array(value1: str, value2: str, ...) -> str
// Creates a JSON array from arguments (variadic)
// ============================================================
#[derive(Debug)]
struct ArrayUdf {
    signature: Signature,
}
impl ArrayUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        // Expand all args to arrays
        let arrays: Vec<ArrayRef> = args.iter().map(|a| to_array(a, num_rows)).collect();
        let str_arrays: Vec<&StringArray> = arrays
            .iter()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
            .collect();
        let result: StringArray = (0..num_rows)
            .map(|i| {
                let elements: Vec<Value> = str_arrays
                    .iter()
                    .map(|sa| {
                        if sa.is_null(i) {
                            Value::Null
                        } else {
                            Value::String(sa.value(i).to_string())
                        }
                    })
                    .collect();
                Some(serde_json::to_string(&elements).unwrap())
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_length(json_array: str) -> i32
// ============================================================
#[derive(Debug)]
struct ArrayLengthUdf {
    signature: Signature,
}
impl ArrayLengthUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayLengthUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_length" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    parse_json_array(strs.value(i)).map(|v| v.len() as i32)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_flatten(json_array: str) -> str
// Flattens nested JSON arrays one level deep
// ============================================================
#[derive(Debug)]
struct ArrayFlattenUdf {
    signature: Signature,
}
impl ArrayFlattenUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayFlattenUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_flatten" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    parse_json_array(strs.value(i)).map(|v| {
                        let flat: Vec<Value> = v
                            .into_iter()
                            .flat_map(|el| {
                                // Flatten one level: if element is array, expand it
                                if let Value::Array(inner) = el {
                                    inner
                                } else {
                                    vec![el]
                                }
                            })
                            .collect();
                        serde_json::to_string(&flat).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_distinct(json_array: str) -> str
// Remove duplicates, preserving order
// ============================================================
#[derive(Debug)]
struct ArrayDistinctUdf {
    signature: Signature,
}
impl ArrayDistinctUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayDistinctUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_distinct" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    parse_json_array(strs.value(i)).map(|v| {
                        let mut seen = Vec::new();
                        let mut unique = Vec::new();
                        for el in v {
                            let key = el.to_string();
                            if !seen.contains(&key) {
                                seen.push(key);
                                unique.push(el);
                            }
                        }
                        serde_json::to_string(&unique).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_sort(json_array: str) -> str
// Sort JSON array elements (numeric-aware)
// ============================================================
#[derive(Debug)]
struct ArraySortUdf {
    signature: Signature,
}
impl ArraySortUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArraySortUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_sort" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    parse_json_array(strs.value(i)).map(|mut v| {
                        v.sort_by(cmp_json_values);
                        serde_json::to_string(&v).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_contains(json_array: str, value: str) -> boolean
// Check if JSON array contains value
// ============================================================
#[derive(Debug)]
struct ArrayContainsUdf {
    signature: Signature,
}
impl ArrayContainsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayContainsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_contains" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_a = to_array(&args[0], num_rows);
        let arr_b = to_array(&args[1], num_rows);
        let arrays = arr_a.as_any().downcast_ref::<StringArray>().unwrap();
        let values = arr_b.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..arrays.len())
            .map(|i| {
                if arrays.is_null(i) || values.is_null(i) {
                    None
                } else {
                    let search = parse_search_value(values.value(i));
                    Some(
                        parse_json_array(arrays.value(i))
                            .map(|v| v.contains(&search))
                            .unwrap_or(false),
                    )
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_position(json_array: str, value: str) -> i32
// 1-based index, 0 if not found
// ============================================================
#[derive(Debug)]
struct ArrayPositionUdf {
    signature: Signature,
}
impl ArrayPositionUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayPositionUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_position" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_a = to_array(&args[0], num_rows);
        let arr_b = to_array(&args[1], num_rows);
        let arrays = arr_a.as_any().downcast_ref::<StringArray>().unwrap();
        let values = arr_b.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..arrays.len())
            .map(|i| {
                if arrays.is_null(i) || values.is_null(i) {
                    None
                } else {
                    let search = parse_search_value(values.value(i));
                    Some(
                        parse_json_array(arrays.value(i))
                            .and_then(|v| v.iter().position(|el| *el == search).map(|p| p as i32 + 1))
                            .unwrap_or(0),
                    )
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_join(json_array: str, separator: str) -> str
// Join JSON array elements with separator
// ============================================================
#[derive(Debug)]
struct ArrayJoinUdf {
    signature: Signature,
}
impl ArrayJoinUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayJoinUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_join" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_a = to_array(&args[0], num_rows);
        let arr_b = to_array(&args[1], num_rows);
        let arrays = arr_a.as_any().downcast_ref::<StringArray>().unwrap();
        let seps = arr_b.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..arrays.len())
            .map(|i| {
                if arrays.is_null(i) || seps.is_null(i) {
                    None
                } else {
                    parse_json_array(arrays.value(i)).map(|v| {
                        let parts: Vec<String> = v
                            .iter()
                            .map(|el| match el {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .collect();
                        parts.join(seps.value(i))
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_remove(json_array: str, value: str) -> str
// Remove all occurrences of value from JSON array
// ============================================================
#[derive(Debug)]
struct ArrayRemoveUdf {
    signature: Signature,
}
impl ArrayRemoveUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayRemoveUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_remove" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_a = to_array(&args[0], num_rows);
        let arr_b = to_array(&args[1], num_rows);
        let arrays = arr_a.as_any().downcast_ref::<StringArray>().unwrap();
        let values = arr_b.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..arrays.len())
            .map(|i| {
                if arrays.is_null(i) || values.is_null(i) {
                    None
                } else {
                    let search = parse_search_value(values.value(i));
                    parse_json_array(arrays.value(i)).map(|v| {
                        let filtered: Vec<Value> = v.into_iter().filter(|el| *el != search).collect();
                        serde_json::to_string(&filtered).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_reverse(json_array: str) -> str
// ============================================================
#[derive(Debug)]
struct ArrayReverseUdf {
    signature: Signature,
}
impl ArrayReverseUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for ArrayReverseUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_reverse" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    parse_json_array(strs.value(i)).map(|mut v| {
                        v.reverse();
                        serde_json::to_string(&v).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// array_slice(json_array: str, start: i32, end: i32) -> str
// Inclusive range [start, end] (0-based indices)
// ============================================================
#[derive(Debug)]
struct ArraySliceUdf {
    signature: Signature,
}
impl ArraySliceUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int32, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for ArraySliceUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_slice" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_a = to_array(&args[0], num_rows);
        let arr_s = to_array(&args[1], num_rows);
        let arr_e = to_array(&args[2], num_rows);
        let arrays = arr_a.as_any().downcast_ref::<StringArray>().unwrap();
        let starts = arr_s.as_any().downcast_ref::<Int32Array>().unwrap();
        let ends = arr_e.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: StringArray = (0..arrays.len())
            .map(|i| {
                if arrays.is_null(i) || starts.is_null(i) || ends.is_null(i) {
                    None
                } else {
                    parse_json_array(arrays.value(i)).map(|v| {
                        let start = (starts.value(i) as usize).min(v.len());
                        let end = ((ends.value(i) as usize) + 1).min(v.len());
                        let sliced: Vec<Value> = if start <= end {
                            v[start..end].to_vec()
                        } else {
                            vec![]
                        };
                        serde_json::to_string(&sliced).unwrap()
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_collection_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ArrayUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayLengthUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayFlattenUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayDistinctUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArraySortUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayContainsUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayPositionUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayJoinUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayRemoveUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayReverseUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArraySliceUdf::new()));
}
