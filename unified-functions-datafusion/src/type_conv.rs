use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

/// Helper: extract an array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// typeof(value: str) -> str
// Returns the type name of the value (always 'STRING' for string input)
// ============================================================
#[derive(Debug)]
struct TypeofUdf {
    signature: Signature,
}
impl TypeofUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for TypeofUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "typeof" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| if strs.is_null(i) { None } else { Some("STRING") })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_string(value: str) -> str
// Identity function for string input
// ============================================================
#[derive(Debug)]
struct CastToStringUdf {
    signature: Signature,
}
impl CastToStringUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToStringUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_string" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len())
            .map(|i| if strs.is_null(i) { None } else { Some(strs.value(i)) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_int(value: str) -> i32
// Parse string to i32, null on failure
// ============================================================
#[derive(Debug)]
struct CastToIntUdf {
    signature: Signature,
}
impl CastToIntUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToIntUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_int" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { strs.value(i).trim().parse::<i32>().ok() }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_long(value: str) -> i64
// Parse string to i64, null on failure
// ============================================================
#[derive(Debug)]
struct CastToLongUdf {
    signature: Signature,
}
impl CastToLongUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToLongUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_long" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int64Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { strs.value(i).trim().parse::<i64>().ok() }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_float(value: str) -> fp64
// Parse string to f64, null on failure
// ============================================================
#[derive(Debug)]
struct CastToFloatUdf {
    signature: Signature,
}
impl CastToFloatUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToFloatUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_float" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Float64Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { strs.value(i).trim().parse::<f64>().ok() }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_boolean(value: str) -> boolean
// Parse 'true'/'false'/'1'/'0'/'yes'/'no' (case-insensitive)
// ============================================================
#[derive(Debug)]
struct CastToBooleanUdf {
    signature: Signature,
}
impl CastToBooleanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToBooleanUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_boolean" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    match strs.value(i).trim().to_lowercase().as_str() {
                        "true" | "1" | "yes" => Some(true),
                        "false" | "0" | "no" => Some(false),
                        _ => None,
                    }
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_date(value: str) -> date
// Parse ISO date string (YYYY-MM-DD) to Date32
// ============================================================
#[derive(Debug)]
struct CastToDateUdf {
    signature: Signature,
}
impl CastToDateUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToDateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_date" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Date32Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    NaiveDate::parse_from_str(strs.value(i).trim(), "%Y-%m-%d")
                        .ok()
                        .map(|d| (d - epoch).num_days() as i32)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// cast_to_timestamp(value: str) -> timestamp
// Parse ISO timestamp string to Timestamp(Microsecond, None)
// ============================================================
#[derive(Debug)]
struct CastToTimestampUdf {
    signature: Signature,
}
impl CastToTimestampUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for CastToTimestampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cast_to_timestamp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: TimestampMicrosecondArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    chrono::NaiveDateTime::parse_from_str(strs.value(i).trim(), "%Y-%m-%d %H:%M:%S")
                        .ok()
                        .map(|dt| dt.and_utc().timestamp_micros())
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_type_conv_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TypeofUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToStringUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToIntUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToLongUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToFloatUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToBooleanUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToDateUdf::new()));
    ctx.register_udf(ScalarUDF::from(CastToTimestampUdf::new()));
}
