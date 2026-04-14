use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

/// Helper: extract a string array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_string_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// ifnull(value1: str, value2: str) -> str
// Returns first arg if not null, else second arg.
// ============================================================
#[derive(Debug)]
struct IfNullUdf { signature: Signature }
impl IfNullUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for IfNullUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ifnull" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let a1 = to_string_array(&args[0], num_rows);
        let a2 = to_string_array(&args[1], num_rows);
        let s1 = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let s2 = a2.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..s1.len())
            .map(|i| {
                if !s1.is_null(i) {
                    Some(s1.value(i).to_string())
                } else if !s2.is_null(i) {
                    Some(s2.value(i).to_string())
                } else {
                    None
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// nullif(value1: str, value2: str) -> str
// Returns null if args are equal, else first arg.
// ============================================================
#[derive(Debug)]
struct NullIfUdf { signature: Signature }
impl NullIfUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for NullIfUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "nullif" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let a1 = to_string_array(&args[0], num_rows);
        let a2 = to_string_array(&args[1], num_rows);
        let s1 = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let s2 = a2.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..s1.len())
            .map(|i| {
                if s1.is_null(i) {
                    None
                } else if s2.is_null(i) {
                    // first is non-null, second is null → not equal, return first
                    Some(s1.value(i).to_string())
                } else if s1.value(i) == s2.value(i) {
                    None
                } else {
                    Some(s1.value(i).to_string())
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_condition_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(IfNullUdf::new()));
    ctx.register_udf(ScalarUDF::from(NullIfUdf::new()));
}
