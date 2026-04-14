use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

/// Helper: extract a float64 array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_f64_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

/// Helper: extract a string array from a ColumnarValue.
fn to_string_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// cbrt(value: fp64) -> fp64
// ============================================================
#[derive(Debug)]
struct CbrtUdf { signature: Signature }
impl CbrtUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for CbrtUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cbrt" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(vals.value(i).cbrt()) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// crc32(value: str) -> i64
// ============================================================
#[derive(Debug)]
struct Crc32Udf { signature: Signature }
impl Crc32Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for Crc32Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "crc32" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int64Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { Some(crc32fast::hash(strs.value(i).as_bytes()) as i64) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// e() -> fp64
// Returns Euler's number.
// ============================================================
#[derive(Debug)]
struct EUdf { signature: Signature }
impl EUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for EUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "e" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, _args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let result: Float64Array = (0..num_rows).map(|_| Some(std::f64::consts::E)).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// expm1(value: fp64) -> fp64
// Returns e^x - 1.
// ============================================================
#[derive(Debug)]
struct Expm1Udf { signature: Signature }
impl Expm1Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for Expm1Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "expm1" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(vals.value(i).exp_m1()) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// log2(value: fp64) -> fp64
// ============================================================
#[derive(Debug)]
struct Log2Udf { signature: Signature }
impl Log2Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for Log2Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "log2" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(vals.value(i).log2()) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// log10(value: fp64) -> fp64
// ============================================================
#[derive(Debug)]
struct Log10Udf { signature: Signature }
impl Log10Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for Log10Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "log10" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(vals.value(i).log10()) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// rint(value: fp64) -> fp64
// Banker's rounding: round to nearest even on tie.
// ============================================================
#[derive(Debug)]
struct RintUdf { signature: Signature }
impl RintUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}

/// Banker's rounding: if exactly halfway, round to nearest even integer.
fn rint(v: f64) -> f64 {
    let rounded = v.round();
    // Check if exactly halfway: fractional part is 0.5
    if (v - (v.floor() + 0.5)).abs() < f64::EPSILON {
        // Round to even
        if rounded % 2.0 != 0.0 {
            rounded - v.signum()
        } else {
            rounded
        }
    } else {
        rounded
    }
}

impl ScalarUDFImpl for RintUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "rint" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(rint(vals.value(i))) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// signum(value: fp64) -> i32
// Returns -1, 0, or 1.
// ============================================================
#[derive(Debug)]
struct SignumUdf { signature: Signature }
impl SignumUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for SignumUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "signum" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Int32Array = (0..vals.len())
            .map(|i| {
                if vals.is_null(i) {
                    None
                } else {
                    let v = vals.value(i);
                    // Map to -1, 0, or 1; treat -0.0 as 0
                    Some(if v == 0.0 { 0 } else if v > 0.0 { 1 } else { -1 })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_math_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CbrtUdf::new()));
    ctx.register_udf(ScalarUDF::from(Crc32Udf::new()));
    ctx.register_udf(ScalarUDF::from(EUdf::new()));
    ctx.register_udf(ScalarUDF::from(Expm1Udf::new()));
    ctx.register_udf(ScalarUDF::from(Log2Udf::new()));
    ctx.register_udf(ScalarUDF::from(Log10Udf::new()));
    ctx.register_udf(ScalarUDF::from(RintUdf::new()));
    ctx.register_udf(ScalarUDF::from(SignumUdf::new()));
}
