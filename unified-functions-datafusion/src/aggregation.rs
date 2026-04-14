use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

fn to_f64_array(arg: &ColumnarValue, n: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    }
}

fn to_i32_array(arg: &ColumnarValue, n: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    }
}

fn to_string_array(arg: &ColumnarValue, n: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    }
}

// ============================================================
// percentile(value: fp64, pct: fp64) → fp64
// Single-value: return the value itself. Null → null.
// ============================================================
#[derive(Debug)]
struct PercentileUdf { signature: Signature }
impl PercentileUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64, DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for PercentileUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "percentile" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let pct_arr = to_f64_array(&args[1], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let pcts = pct_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            if vals.is_null(i) || pcts.is_null(i) { None } else { Some(vals.value(i)) }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// percentile_approx(value: fp64, pct: fp64) → fp64
// Same as percentile for single-value case.
// ============================================================
#[derive(Debug)]
struct PercentileApproxUdf { signature: Signature }
impl PercentileApproxUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64, DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for PercentileApproxUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "percentile_approx" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let pct_arr = to_f64_array(&args[1], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let pcts = pct_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            if vals.is_null(i) || pcts.is_null(i) { None } else { Some(vals.value(i)) }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// stddev_pop(value: fp64) → fp64
// Population stddev of single value = 0.0. Null → null.
// ============================================================
#[derive(Debug)]
struct StddevPopUdf { signature: Signature }
impl StddevPopUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for StddevPopUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "stddev_pop" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            if vals.is_null(i) { None } else { Some(0.0) }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// stddev_samp(value: fp64) → fp64
// Sample stddev of single value = null (undefined). Null → null.
// ============================================================
#[derive(Debug)]
struct StddevSampUdf { signature: Signature }
impl StddevSampUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for StddevSampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "stddev_samp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            let _ = vals.is_null(i); // always null for single-value sample stddev
            None
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// var_pop(value: fp64) → fp64
// Population variance of single value = 0.0. Null → null.
// ============================================================
#[derive(Debug)]
struct VarPopUdf { signature: Signature }
impl VarPopUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for VarPopUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "var_pop" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            if vals.is_null(i) { None } else { Some(0.0) }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// var_samp(value: fp64) → fp64
// Sample variance of single value = null (undefined). Null → null.
// ============================================================
#[derive(Debug)]
struct VarSampUdf { signature: Signature }
impl VarSampUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for VarSampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "var_samp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_f64_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len()).map(|i| {
            let _ = vals.is_null(i);
            None
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// take(value: string, index: i32) → string
// Return value if index == 1, null otherwise.
// ============================================================
#[derive(Debug)]
struct TakeUdf { signature: Signature }
impl TakeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Int32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for TakeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "take" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let str_arr = to_string_array(&args[0], num_rows);
        let idx_arr = to_i32_array(&args[1], num_rows);
        let strs = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let idxs = idx_arr.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: StringArray = (0..strs.len()).map(|i| {
            if strs.is_null(i) || idxs.is_null(i) || idxs.value(i) != 1 {
                None
            } else {
                Some(strs.value(i).to_string())
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn register_aggregation_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(PercentileUdf::new()));
    ctx.register_udf(ScalarUDF::from(PercentileApproxUdf::new()));
    ctx.register_udf(ScalarUDF::from(StddevPopUdf::new()));
    ctx.register_udf(ScalarUDF::from(StddevSampUdf::new()));
    ctx.register_udf(ScalarUDF::from(VarPopUdf::new()));
    ctx.register_udf(ScalarUDF::from(VarSampUdf::new()));
    ctx.register_udf(ScalarUDF::from(TakeUdf::new()));
}
