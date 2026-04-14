use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
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
// span(value: fp64, interval: fp64) -> fp64
// Bucket value: floor(value / interval) * interval
// ============================================================
#[derive(Debug)]
struct SpanUdf {
    signature: Signature,
}
impl SpanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64, DataType::Float64], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for SpanUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "span" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_v = to_array(&args[0], num_rows);
        let arr_i = to_array(&args[1], num_rows);
        let vals = arr_v.as_any().downcast_ref::<Float64Array>().unwrap();
        let intervals = arr_i.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Float64Array = (0..vals.len())
            .map(|i| {
                if vals.is_null(i) || intervals.is_null(i) {
                    None
                } else {
                    let interval = intervals.value(i);
                    if interval <= 0.0 { None } else { Some((vals.value(i) / interval).floor() * interval) }
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// width_bucket(value: fp64, min: fp64, max: fp64, num_buckets: i32) -> i32
// Standard SQL width_bucket: 0 if below min, num_buckets+1 if >= max
// ============================================================
#[derive(Debug)]
struct WidthBucketUdf {
    signature: Signature,
}
impl WidthBucketUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for WidthBucketUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "width_bucket" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_v = to_array(&args[0], num_rows);
        let arr_min = to_array(&args[1], num_rows);
        let arr_max = to_array(&args[2], num_rows);
        let arr_n = to_array(&args[3], num_rows);
        let vals = arr_v.as_any().downcast_ref::<Float64Array>().unwrap();
        let mins = arr_min.as_any().downcast_ref::<Float64Array>().unwrap();
        let maxs = arr_max.as_any().downcast_ref::<Float64Array>().unwrap();
        let buckets = arr_n.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: Int32Array = (0..vals.len())
            .map(|i| {
                if vals.is_null(i) || mins.is_null(i) || maxs.is_null(i) || buckets.is_null(i) {
                    None
                } else {
                    let v = vals.value(i);
                    let lo = mins.value(i);
                    let hi = maxs.value(i);
                    let n = buckets.value(i);
                    Some(if v < lo {
                        0
                    } else if v >= hi {
                        n + 1
                    } else {
                        // 1-based bucket assignment
                        ((v - lo) / (hi - lo) * n as f64).floor() as i32 + 1
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// ntile(num_buckets: i32) -> i32
// Placeholder scalar UDF — always returns 1 (true ntile needs window context)
// ============================================================
#[derive(Debug)]
struct NtileUdf {
    signature: Signature,
}
impl NtileUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for NtileUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ntile" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_array(&args[0], num_rows);
        let vals = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: Int32Array = (0..vals.len())
            .map(|i| if vals.is_null(i) { None } else { Some(1) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// histogram(value: fp64, bucket_size: i32) -> str
// Returns JSON: {"bucket": floor(value/bucket_size), "count": 1}
// ============================================================
#[derive(Debug)]
struct HistogramUdf {
    signature: Signature,
}
impl HistogramUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for HistogramUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "histogram" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr_v = to_array(&args[0], num_rows);
        let arr_b = to_array(&args[1], num_rows);
        let vals = arr_v.as_any().downcast_ref::<Float64Array>().unwrap();
        let sizes = arr_b.as_any().downcast_ref::<Int32Array>().unwrap();
        let result: StringArray = (0..vals.len())
            .map(|i| {
                if vals.is_null(i) || sizes.is_null(i) {
                    None
                } else {
                    let bucket = (vals.value(i) / sizes.value(i) as f64).floor() as i64;
                    Some(format!("{{\"bucket\":{},\"count\":1}}", bucket))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_binning_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SpanUdf::new()));
    ctx.register_udf(ScalarUDF::from(WidthBucketUdf::new()));
    ctx.register_udf(ScalarUDF::from(NtileUdf::new()));
    ctx.register_udf(ScalarUDF::from(HistogramUdf::new()));
}
