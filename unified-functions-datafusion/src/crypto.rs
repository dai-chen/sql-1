use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use md5::{Md5, Digest};

/// Helper: extract a string array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_string_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// md5(value: str) -> str
// Computes MD5 hash, returns lowercase hex string.
// ============================================================
#[derive(Debug)]
struct Md5Udf {
    signature: Signature,
}
impl Md5Udf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for Md5Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "md5" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    let hash = Md5::digest(strs.value(i).as_bytes());
                    Some(format!("{:x}", hash))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_crypto_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Md5Udf::new()));
}
