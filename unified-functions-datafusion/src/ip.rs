use std::any::Any;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use ipnetwork::IpNetwork;

/// Helper: extract a string array from a ColumnarValue, expanding scalars to `num_rows`.
fn to_string_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

/// Helper: extract an i64 array from a ColumnarValue.
fn to_i64_array(arg: &ColumnarValue, num_rows: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
    }
}

// ============================================================
// cidrmatch(ip: str, cidr: str) -> bool
// ============================================================
#[derive(Debug)]
struct CidrMatchUdf {
    signature: Signature,
}
impl CidrMatchUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for CidrMatchUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cidrmatch" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let ip_arr = to_string_array(&args[0], num_rows);
        let cidr_arr = to_string_array(&args[1], num_rows);
        let ips = ip_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let cidrs = cidr_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..ips.len())
            .map(|i| {
                if ips.is_null(i) || cidrs.is_null(i) {
                    None
                } else {
                    let ok = IpAddr::from_str(ips.value(i))
                        .ok()
                        .and_then(|ip| IpNetwork::from_str(cidrs.value(i)).ok().map(|net| net.contains(ip)))
                        .unwrap_or(false);
                    Some(ok)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// is_ipv4(ip: str) -> bool
// ============================================================
#[derive(Debug)]
struct IsIpv4Udf {
    signature: Signature,
}
impl IsIpv4Udf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for IsIpv4Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "is_ipv4" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { Some(Ipv4Addr::from_str(strs.value(i)).is_ok()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// is_ipv6(ip: str) -> bool
// ============================================================
#[derive(Debug)]
struct IsIpv6Udf {
    signature: Signature,
}
impl IsIpv6Udf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for IsIpv6Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "is_ipv6" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) { None } else { Some(Ipv6Addr::from_str(strs.value(i)).is_ok()) }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// ip_to_int(ip: str) -> i64
// ============================================================
#[derive(Debug)]
struct IpToIntUdf {
    signature: Signature,
}
impl IpToIntUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for IpToIntUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ip_to_int" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int64Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    Ipv4Addr::from_str(strs.value(i))
                        .ok()
                        .map(|ip| u32::from(ip) as i64)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// int_to_ip(n: i64) -> str
// ============================================================
#[derive(Debug)]
struct IntToIpUdf {
    signature: Signature,
}
impl IntToIpUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for IntToIpUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "int_to_ip" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_i64_array(&args[0], num_rows);
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: StringArray = (0..ints.len())
            .map(|i| {
                if ints.is_null(i) {
                    None
                } else {
                    let v = ints.value(i);
                    if v >= 0 && v <= u32::MAX as i64 {
                        Some(Ipv4Addr::from(v as u32).to_string())
                    } else {
                        None
                    }
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// ip_version(ip: str) -> i32
// ============================================================
#[derive(Debug)]
struct IpVersionUdf {
    signature: Signature,
}
impl IpVersionUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for IpVersionUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ip_version" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let arr = to_string_array(&args[0], num_rows);
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..strs.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    IpAddr::from_str(strs.value(i)).ok().map(|ip| match ip {
                        IpAddr::V4(_) => 4,
                        IpAddr::V6(_) => 6,
                    })
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// ip_prefix(ip: str, prefix_len: i32) -> str
// ============================================================
#[derive(Debug)]
struct IpPrefixUdf {
    signature: Signature,
}
impl IpPrefixUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for IpPrefixUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ip_prefix" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        let ip_arr = to_string_array(&args[0], num_rows);
        let prefix_arr = to_i64_array(&args[1], num_rows);
        let ips = ip_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let prefixes = prefix_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: StringArray = (0..ips.len())
            .map(|i| {
                if ips.is_null(i) || prefixes.is_null(i) {
                    None
                } else {
                    let prefix_len = prefixes.value(i);
                    if prefix_len < 0 || prefix_len > 128 {
                        return None;
                    }
                    IpAddr::from_str(ips.value(i))
                        .ok()
                        .and_then(|ip| {
                            IpNetwork::new(ip, prefix_len as u8).ok()
                        })
                        .map(|net| format!("{}/{}", net.network(), net.prefix()))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// geoip(ip: str, field: str) -> str
// Always returns null (no MaxMind database available)
// ============================================================
#[derive(Debug)]
struct GeoIpUdf {
    signature: Signature,
}
impl GeoIpUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for GeoIpUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "geoip" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, _args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
        // No GeoIP database available — always return null
        let result: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_ip_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CidrMatchUdf::new()));
    ctx.register_udf(ScalarUDF::from(IsIpv4Udf::new()));
    ctx.register_udf(ScalarUDF::from(IsIpv6Udf::new()));
    ctx.register_udf(ScalarUDF::from(IpToIntUdf::new()));
    ctx.register_udf(ScalarUDF::from(IntToIpUdf::new()));
    ctx.register_udf(ScalarUDF::from(IpVersionUdf::new()));
    ctx.register_udf(ScalarUDF::from(IpPrefixUdf::new()));
    ctx.register_udf(ScalarUDF::from(GeoIpUdf::new()));
}
