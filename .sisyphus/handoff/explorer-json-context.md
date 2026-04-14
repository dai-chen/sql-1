# JSON UDF Implementation Context

## 1. Reference Implementation: unified-functions-datafusion/src/ip.rs

```rust
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
```

## 2. Test Cases: api/src/main/resources/unified-functions/tests/unified_json_functions.test

```
### SUBSTRAIT_SCALAR_TEST: v1.0
### SUBSTRAIT_INCLUDE: extension:org.opensearch:unified_json_functions

# json_extract
# group: basic
json_extract('{"a":1}'::str, '$.a'::str) = '1'::str
json_extract('{"a":"hello"}'::str, '$.a'::str) = '"hello"'::str
json_extract('{"a":true}'::str, '$.a'::str) = 'true'::str
json_extract('{"a":{"b":2}}'::str, '$.a.b'::str) = '2'::str
json_extract('{"a":{"b":{"c":3}}}'::str, '$.a.b.c'::str) = '3'::str

# group: null_handling
json_extract(null::str, '$.a'::str) = null::str
json_extract('{"a":1}'::str, null::str) = null::str
json_extract(null::str, null::str) = null::str

# group: edge_cases
json_extract('{"a":1}'::str, '$.b'::str) = null::str
json_extract('not json'::str, '$.a'::str) = null::str
json_extract(''::str, '$.a'::str) = null::str
json_extract('{"arr":[1,2,3]}'::str, '$.arr[0]'::str) = '1'::str
json_extract('{"arr":[1,2,3]}'::str, '$.arr[2]'::str) = '3'::str
json_extract('{"a":null}'::str, '$.a'::str) = 'null'::str
json_extract('{"a":{"b":[10,20]}}'::str, '$.a.b[1]'::str) = '20'::str

# json_valid
# group: basic
json_valid('{"a":1}'::str) = true::bool
json_valid('[1,2,3]'::str) = true::bool
json_valid('"hello"'::str) = true::bool
json_valid('123'::str) = true::bool
json_valid('true'::str) = true::bool
json_valid('null'::str) = true::bool

# group: null_handling
json_valid(null::str) = null::bool

# group: edge_cases
json_valid(''::str) = false::bool
json_valid('not json'::str) = false::bool
json_valid('{missing_quotes: 1}'::str) = false::bool
json_valid('{"a":}'::str) = false::bool
json_valid('{"a":1,}'::str) = false::bool

# json_keys
# group: basic
json_keys('{"a":1,"b":2}'::str) = 'a,b'::str
json_keys('{"x":1}'::str) = 'x'::str
json_keys('{"a":1,"b":2,"c":3}'::str) = 'a,b,c'::str

# group: null_handling
json_keys(null::str) = null::str

# group: edge_cases
json_keys('[1,2,3]'::str) = null::str
json_keys('"hello"'::str) = null::str
json_keys('123'::str) = null::str
json_keys('not json'::str) = null::str
json_keys(''::str) = null::str
json_keys('{}'::str) = ''::str
json_keys('{"z":1,"a":2,"m":3}'::str) = 'a,m,z'::str

# json_delete
# group: basic
json_delete('{"a":1,"b":2}'::str, 'a'::str) = '{"b":2}'::str
json_delete('{"a":1,"b":2,"c":3}'::str, 'b'::str) = '{"a":1,"c":3}'::str
json_delete('{"x":10}'::str, 'x'::str) = '{}'::str

# group: null_handling
json_delete(null::str, 'a'::str) = null::str
json_delete('{"a":1}'::str, null::str) = null::str
json_delete(null::str, null::str) = null::str

# group: edge_cases
json_delete('{"a":1,"b":2}'::str, 'z'::str) = '{"a":1,"b":2}'::str
json_delete('not json'::str, 'a'::str) = null::str
json_delete(''::str, 'a'::str) = null::str
json_delete('{"a":{"b":1},"c":2}'::str, 'a'::str) = '{"c":2}'::str
json_delete('{}'::str, 'a'::str) = '{}'::str

# json_set
# group: basic
json_set('{"a":1}'::str, 'b'::str, '2'::str) = '{"a":1,"b":2}'::str
json_set('{"a":1}'::str, 'a'::str, '99'::str) = '{"a":99}'::str
json_set('{}'::str, 'x'::str, '"hello"'::str) = '{"x":"hello"}'::str

# group: null_handling
json_set(null::str, 'a'::str, '1'::str) = null::str
json_set('{"a":1}'::str, null::str, '1'::str) = null::str
json_set('{"a":1}'::str, 'a'::str, null::str) = null::str
json_set(null::str, null::str, null::str) = null::str

# group: edge_cases
json_set('not json'::str, 'a'::str, '1'::str) = null::str
json_set(''::str, 'a'::str, '1'::str) = null::str
json_set('{"a":1,"b":2}'::str, 'c'::str, '[1,2]'::str) = '{"a":1,"b":2,"c":[1,2]}'::str
json_set('{"a":1}'::str, 'b'::str, '{"nested":true}'::str) = '{"a":1,"b":{"nested":true}}'::str

# json_append
# group: basic
json_append('{"a":[1,2]}'::str, 'a'::str, '3'::str) = '{"a":[1,2,3]}'::str
json_append('{"a":[]}'::str, 'a'::str, '1'::str) = '{"a":[1]}'::str
json_append('{"a":[1],"b":"x"}'::str, 'a'::str, '2'::str) = '{"a":[1,2],"b":"x"}'::str

# group: null_handling
json_append(null::str, 'a'::str, '1'::str) = null::str
json_append('{"a":[1]}'::str, null::str, '1'::str) = null::str
json_append('{"a":[1]}'::str, 'a'::str, null::str) = null::str
json_append(null::str, null::str, null::str) = null::str

# group: edge_cases
json_append('{"a":"not_array"}'::str, 'a'::str, '1'::str) = null::str
json_append('{"a":1}'::str, 'a'::str, '2'::str) = null::str
json_append('not json'::str, 'a'::str, '1'::str) = null::str
json_append(''::str, 'a'::str, '1'::str) = null::str
json_append('{"a":[1,2]}'::str, 'b'::str, '1'::str) = null::str
json_append('{"a":["x"]}'::str, 'a'::str, '"y"'::str) = '{"a":["x","y"]}'::str

# json_extend
# group: basic
json_extend('{"a":[1,2]}'::str, 'a'::str, '[3,4]'::str) = '{"a":[1,2,3,4]}'::str
json_extend('{"a":[]}'::str, 'a'::str, '[1,2]'::str) = '{"a":[1,2]}'::str
json_extend('{"a":[1],"b":"x"}'::str, 'a'::str, '[2,3]'::str) = '{"a":[1,2,3],"b":"x"}'::str

# group: null_handling
json_extend(null::str, 'a'::str, '[1]'::str) = null::str
json_extend('{"a":[1]}'::str, null::str, '[1]'::str) = null::str
json_extend('{"a":[1]}'::str, 'a'::str, null::str) = null::str
json_extend(null::str, null::str, null::str) = null::str

# group: edge_cases
json_extend('{"a":[1]}'::str, 'a'::str, 'not_array'::str) = null::str
json_extend('{"a":"not_array"}'::str, 'a'::str, '[1]'::str) = null::str
json_extend('not json'::str, 'a'::str, '[1]'::str) = null::str
json_extend(''::str, 'a'::str, '[1]'::str) = null::str
json_extend('{"a":[1]}'::str, 'b'::str, '[1]'::str) = null::str
json_extend('{"a":[1]}'::str, 'a'::str, '42'::str) = null::str

# json_array
# group: basic
json_array('1'::str, '2'::str) = '[1,2]'::str
json_array('"a"'::str, '"b"'::str) = '["a","b"]'::str
json_array('true'::str, 'false'::str) = '[true,false]'::str
json_array('"hello"'::str, '123'::str) = '["hello",123]'::str

# group: null_handling
json_array(null::str, '1'::str) = '[null,1]'::str
json_array('1'::str, null::str) = '[1,null]'::str
json_array(null::str, null::str) = '[null,null]'::str

# group: edge_cases
json_array('{"a":1}'::str, '[1,2]'::str) = '[{"a":1},[1,2]]'::str
json_array('null'::str, 'null'::str) = '[null,null]'::str
json_array('"x"'::str, '"y"'::str) = '["x","y"]'::str

# json_object
# group: basic
json_object('a'::str, '1'::str) = '{"a":1}'::str
json_object('name'::str, '"Alice"'::str) = '{"name":"Alice"}'::str
json_object('flag'::str, 'true'::str) = '{"flag":true}'::str
json_object('data'::str, '[1,2]'::str) = '{"data":[1,2]}'::str

# group: null_handling
json_object(null::str, '1'::str) = null::str
json_object('a'::str, null::str) = '{"a":null}'::str
json_object(null::str, null::str) = null::str

# group: edge_cases
json_object('k'::str, '{"nested":true}'::str) = '{"k":{"nested":true}}'::str
json_object('k'::str, 'null'::str) = '{"k":null}'::str
json_object(''::str, '1'::str) = '{"":1}'::str
```

## 3. Function Specs: api/src/main/resources/unified-functions/extensions/unified_json_functions.yaml

```yaml
%YAML 1.2
---
urn: "extension:org.opensearch:unified_json_functions"
scalar_functions:
  - name: json_extract
    description: >-
      Extracts a value from a JSON string using a path expression. Supports nested paths.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args -> null"
        - "invalid JSON -> null"
        - "missing path -> null"
        - "nested paths supported"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json", "jsonpath_lib"]
            notes: "Use jsonpath_lib::select() for path extraction, return as JSON string"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON string to extract from"
          - value: "string"
            name: "path"
            description: "The path expression to evaluate"
        return: "string"

  - name: json_valid
    description: >-
      Returns true if the string is valid JSON.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null -> null"
        - "empty string -> false"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Use serde_json::from_str::<Value>() to validate"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The string to validate as JSON"
        return: "boolean"

  - name: json_keys
    description: >-
      Returns the keys of a JSON object as a comma-separated string.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null -> null"
        - "invalid JSON -> null"
        - "non-object (array/scalar) -> null"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Parse as Value, match Object variant, collect keys"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON object string to extract keys from"
        return: "string"

  - name: json_delete
    description: >-
      Deletes a key from a JSON object and returns the modified JSON string.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args -> null"
        - "invalid JSON -> null"
        - "missing key -> returns original JSON unchanged"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Parse as Value, remove key from Object map, serialize back"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON object string to modify"
          - value: "string"
            name: "key"
            description: "The key to delete"
        return: "string"

  - name: json_set
    description: >-
      Sets a value at a key in a JSON object and returns the modified JSON string.
      Creates the key if it does not exist, overwrites if it exists.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args -> null"
        - "invalid JSON -> null"
        - "creates key if not exists"
        - "overwrites if exists"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Parse as Value, insert into Object map, serialize back"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON object string to modify"
          - value: "string"
            name: "key"
            description: "The key to set"
          - value: "string"
            name: "value"
            description: "The value to set (as JSON string)"
        return: "string"

  - name: json_append
    description: >-
      Appends a value to a JSON array at the given key in a JSON object.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args -> null"
        - "invalid JSON -> null"
        - "key not an array -> null"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Parse as Value, locate array at key, push value, serialize back"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON object string containing the array"
          - value: "string"
            name: "key"
            description: "The key of the array to append to"
          - value: "string"
            name: "value"
            description: "The value to append (as JSON string)"
        return: "string"

  - name: json_extend
    description: >-
      Extends a JSON array at the given key with elements from another array.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args -> null"
        - "invalid JSON -> null"
        - "values not an array -> null"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Parse both values, extend target array with source array elements, serialize back"
    impls:
      - args:
          - value: "string"
            name: "json_string"
            description: "The JSON object string containing the array"
          - value: "string"
            name: "key"
            description: "The key of the array to extend"
          - value: "string"
            name: "values"
            description: "A JSON array string whose elements will be appended"
        return: "string"

  - name: json_array
    description: >-
      Creates a JSON array string from the given arguments. Variadic in practice;
      spec shows a 2-argument overload.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null args included as JSON null in array"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Variadic function. Collect args into Vec<Value>, serialize as JSON array."
    impls:
      - args:
          - value: "string"
            name: "value1"
            description: "The first value for the array"
          - value: "string"
            name: "value2"
            description: "The second value for the array"
        return: "string"

  - name: json_object
    description: >-
      Creates a JSON object string from key-value pairs. Variadic in practice
      with alternating key/value arguments; spec shows a 1-pair overload.
    metadata:
      null_handling: "ACCEPT_NULLS"
      edge_cases:
        - "null key -> null"
        - "null value -> JSON null for that key"
      ai_hints:
        platform_hints:
          datafusion:
            crates: ["serde_json"]
            notes: "Variadic function. Collect alternating key/value pairs into Map, serialize as JSON object."
    impls:
      - args:
          - value: "string"
            name: "key"
            description: "The key for the JSON object entry"
          - value: "string"
            name: "value"
            description: "The value for the JSON object entry"
        return: "string"
```

## 4. Current Stub: unified-functions-datafusion/src/json.rs

```rust
use datafusion::prelude::SessionContext;

pub fn register_json_udfs(_ctx: &SessionContext) {}
```

## 5. Test Harness: unified-functions-datafusion/tests/spec_tests.rs

```rust
use unified_functions_datafusion::test_harness::{parse_test_file, test_case_to_sql, DslType};

const IP_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_ip_functions.test";
const JSON_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_json_functions.test";
const MATH_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_math_functions.test";
const DATETIME_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_datetime_functions.test";
const COLLECTION_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_collection_functions.test";
const CONDITION_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_condition_functions.test";
const REX_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_rex_functions.test";
const CRYPTO_TEST_FILE: &str = "../api/src/main/resources/unified-functions/tests/unified_crypto_functions.test";

#[test]
fn test_parse_ip_test_file() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    assert!(cases.len() > 0, "Expected test cases from IP test file");

    // Verify first test case structure
    let first = &cases[0];
    assert_eq!(first.function_name, "cidrmatch");
    assert_eq!(first.args.len(), 2);
    assert_eq!(first.args[0].value, Some("192.168.1.1".to_string()));
    assert_eq!(first.args[0].dsl_type, DslType::Str);
    assert_eq!(first.expected.value, Some("true".to_string()));
    assert_eq!(first.expected.dsl_type, DslType::Bool);
    assert_eq!(first.group_name, "basic");
}

#[test]
fn test_parse_ip_null_handling() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let null_cases: Vec<_> = cases.iter().filter(|c| c.group_name == "null_handling").collect();
    assert!(null_cases.len() > 0, "Expected null_handling test cases");

    // First null case should have null arg
    let first_null = null_cases.iter().find(|c| c.function_name == "cidrmatch").unwrap();
    assert!(first_null.args.iter().any(|a| a.value.is_none()), "Expected a null argument");
}

#[test]
fn test_parse_json_test_file() {
    let cases = parse_test_file(JSON_TEST_FILE).expect("Failed to parse JSON test file");
    assert!(cases.len() > 0, "Expected test cases from JSON test file");

    // JSON strings contain special chars like braces, colons, commas
    let first = &cases[0];
    assert_eq!(first.function_name, "json_extract");
    assert_eq!(first.args[0].value, Some("{\"a\":1}".to_string()));
}

#[test]
fn test_parse_math_test_file() {
    let cases = parse_test_file(MATH_TEST_FILE).expect("Failed to parse math test file");
    assert!(cases.len() > 0, "Expected test cases from math test file");

    // Check fp64 type parsing
    let cbrt_case = &cases[0];
    assert_eq!(cbrt_case.function_name, "cbrt");
    assert_eq!(cbrt_case.args[0].dsl_type, DslType::Fp64);
    assert_eq!(cbrt_case.expected.dsl_type, DslType::Fp64);

    // Check zero-arg function (e())
    let e_cases: Vec<_> = cases.iter().filter(|c| c.function_name == "e").collect();
    assert!(e_cases.len() > 0, "Expected e() test cases");
    assert!(e_cases[0].args.is_empty(), "e() should have no args");
}

#[test]
fn test_parse_datetime_test_file() {
    let cases = parse_test_file(DATETIME_TEST_FILE).expect("Failed to parse datetime test file");
    assert!(cases.len() > 0, "Expected test cases from datetime test file");

    // Check date type
    let adddate = &cases[0];
    assert_eq!(adddate.function_name, "adddate");
    assert_eq!(adddate.args[0].dsl_type, DslType::Date);
    assert_eq!(adddate.expected.dsl_type, DslType::Date);

    // Check timestamp type
    let ts_cases: Vec<_> = cases.iter().filter(|c| c.function_name == "addtime").collect();
    assert!(ts_cases.len() > 0);
    assert_eq!(ts_cases[0].args[0].dsl_type, DslType::Timestamp);
}

#[test]
fn test_parse_collection_test_file() {
    let cases = parse_test_file(COLLECTION_TEST_FILE).expect("Failed to parse collection test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_parse_condition_test_file() {
    let cases = parse_test_file(CONDITION_TEST_FILE).expect("Failed to parse condition test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_parse_rex_test_file() {
    let cases = parse_test_file(REX_TEST_FILE).expect("Failed to parse rex test file");
    assert!(cases.len() > 0);

    // Rex has regex patterns with backslashes and special chars in strings
    let first = &cases[0];
    assert_eq!(first.function_name, "rex");
    assert!(first.args[1].value.as_ref().unwrap().contains("(?P<"));
}

#[test]
fn test_parse_crypto_test_file() {
    let cases = parse_test_file(CRYPTO_TEST_FILE).expect("Failed to parse crypto test file");
    assert!(cases.len() > 0);
}

#[test]
fn test_sql_generation() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let sql = test_case_to_sql(&cases[0]);
    assert!(sql.starts_with("SELECT cidrmatch("));
    assert!(sql.contains("'192.168.1.1'"));
    assert!(sql.contains("'192.168.1.0/24'"));
}

#[test]
fn test_sql_generation_null_args() {
    let cases = parse_test_file(IP_TEST_FILE).expect("Failed to parse IP test file");
    let null_case = cases.iter().find(|c| c.args.iter().any(|a| a.value.is_none())).unwrap();
    let sql = test_case_to_sql(null_case);
    assert!(sql.contains("CAST(NULL AS VARCHAR)"));
}

/// Parse ALL test files to ensure the parser handles every format variation.
#[test]
fn test_parse_all_test_files() {
    let test_dir = "../api/src/main/resources/unified-functions/tests";
    let entries = std::fs::read_dir(test_dir).expect("Failed to read test directory");
    let mut total_cases = 0;
    let mut file_count = 0;

    for entry in entries {
        let entry = entry.expect("Failed to read dir entry");
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "test") {
            let path_str = path.to_str().unwrap();
            let cases = parse_test_file(path_str)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path_str, e));
            assert!(cases.len() > 0, "No test cases in {}", path_str);
            total_cases += cases.len();
            file_count += 1;
        }
    }

    assert!(file_count >= 10, "Expected at least 10 test files, found {}", file_count);
    assert!(total_cases > 100, "Expected > 100 total test cases, found {}", total_cases);
    println!("Parsed {} test cases across {} files", total_cases, file_count);
}

#[tokio::test]
async fn test_run_ip_spec_tests() {
    use unified_functions_datafusion::test_harness::run_test_file;
    use datafusion::prelude::SessionContext;
    use unified_functions_datafusion::register_all_udfs;

    let ctx = SessionContext::new();
    register_all_udfs(&ctx);
    let results = run_test_file(&ctx, IP_TEST_FILE).await.expect("Failed to run IP spec tests");
    let failures: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    // geoip basic tests (3) will fail since no GeoIP database
    let non_geoip_failures: Vec<_> = failures.iter()
        .filter(|r| !(r.test_case.function_name == "geoip" && r.test_case.group_name == "basic"))
        .collect();
    for f in &non_geoip_failures {
        eprintln!("FAIL: {}({}) [{}] expected={:?} actual={} error={:?}",
            f.test_case.function_name, f.test_case.line_number,
            f.test_case.group_name, f.test_case.expected.value, f.actual,
            f.error);
    }
    assert!(non_geoip_failures.is_empty(),
        "{} non-geoip IP spec tests failed", non_geoip_failures.len());
}```

## 6. Parser Code: unified-functions-datafusion/src/test_harness.rs

```rust
//! Test harness for parsing and executing Substrait-style .test spec files against DataFusion.
//!
//! The DSL format:
//! ```text
//! ### SUBSTRAIT_SCALAR_TEST: v1.0
//! ### SUBSTRAIT_INCLUDE: extension:org.opensearch:unified_ip_functions
//!
//! # function_name
//! # group: group_name
//! func(arg1::type, arg2::type) = expected::type
//! ```

use datafusion::prelude::SessionContext;
use std::fmt;
use std::fs;

/// Supported types in the test DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum DslType {
    Str,
    I32,
    I64,
    Fp64,
    Bool,
    Date,
    Timestamp,
}

impl fmt::Display for DslType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DslType::Str => write!(f, "str"),
            DslType::I32 => write!(f, "i32"),
            DslType::I64 => write!(f, "i64"),
            DslType::Fp64 => write!(f, "fp64"),
            DslType::Bool => write!(f, "bool"),
            DslType::Date => write!(f, "date"),
            DslType::Timestamp => write!(f, "timestamp"),
        }
    }
}

/// A literal value with its DSL type.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedLiteral {
    pub value: Option<String>, // None means null
    pub dsl_type: DslType,
}

/// A single parsed test case.
#[derive(Debug, Clone)]
pub struct TestCase {
    pub function_name: String,
    pub args: Vec<TypedLiteral>,
    pub expected: TypedLiteral,
    pub line_number: usize,
    pub group_name: String,
}

/// Result of executing a single test case.
#[derive(Debug)]
pub struct TestResult {
    pub test_case: TestCase,
    pub passed: bool,
    pub actual: String,
    pub error: Option<String>,
}

/// Parse a DSL type string into a DslType.
fn parse_dsl_type(s: &str) -> Result<DslType, String> {
    match s {
        "str" => Ok(DslType::Str),
        "i32" => Ok(DslType::I32),
        "i64" => Ok(DslType::I64),
        "fp64" => Ok(DslType::Fp64),
        "bool" => Ok(DslType::Bool),
        "date" => Ok(DslType::Date),
        "timestamp" => Ok(DslType::Timestamp),
        _ => Err(format!("Unknown type: {}", s)),
    }
}

/// Parse a single typed literal like `'hello'::str`, `123::i32`, `null::bool`, etc.
/// Returns the TypedLiteral and the number of bytes consumed from `input`.
fn parse_typed_literal(input: &str) -> Result<(TypedLiteral, usize), String> {
    let s = input.trim_start();
    let leading_ws = input.len() - s.len();

    if s.starts_with("null::") {
        // null::type
        let rest = &s[6..];
        let type_end = rest.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(rest.len());
        let dsl_type = parse_dsl_type(rest[..type_end].trim())?;
        Ok((TypedLiteral { value: None, dsl_type }, leading_ws + 6 + type_end))
    } else if s.starts_with('\'') {
        // Quoted string: find matching closing quote
        // Strings can contain any characters inside quotes
        let mut i = 1;
        let bytes = s.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                // Check for escaped quote ('')
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                break;
            }
            i += 1;
        }
        if i >= bytes.len() {
            return Err(format!("Unterminated string literal in: {}", s));
        }
        let str_val = s[1..i].to_string();
        // After closing quote, expect ::type
        let after_quote = &s[i + 1..];
        if !after_quote.starts_with("::") {
            return Err(format!("Expected :: after string literal, got: {}", after_quote));
        }
        let type_str = &after_quote[2..];
        let type_end = type_str.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(type_str.len());
        let dsl_type = parse_dsl_type(type_str[..type_end].trim())?;
        let consumed = leading_ws + (i + 1) + 2 + type_end;
        Ok((TypedLiteral { value: Some(str_val), dsl_type }, consumed))
    } else {
        // Unquoted literal: number, bool, negative number
        // Find the :: separator
        let sep = s.find("::").ok_or_else(|| format!("Expected :: in literal: {}", s))?;
        let raw_val = s[..sep].trim().to_string();
        let type_str = &s[sep + 2..];
        let type_end = type_str.find(|c: char| c == ',' || c == ')' || c == '\n' || c == '\r')
            .unwrap_or(type_str.len());
        let dsl_type = parse_dsl_type(type_str[..type_end].trim())?;
        let consumed = leading_ws + sep + 2 + type_end;
        Ok((TypedLiteral { value: Some(raw_val), dsl_type }, consumed))
    }
}

/// Parse the argument list between parentheses. Handles nested quotes with commas inside.
/// `input` should start right after the opening '('.
/// Returns the list of args and the position after the closing ')'.
fn parse_args(input: &str) -> Result<(Vec<TypedLiteral>, usize), String> {
    let mut args = Vec::new();
    let mut pos = 0;

    // Skip whitespace
    while pos < input.len() && input.as_bytes()[pos] == b' ' {
        pos += 1;
    }

    // Empty args: immediate closing paren
    if pos < input.len() && input.as_bytes()[pos] == b')' {
        return Ok((args, pos + 1));
    }

    loop {
        let (lit, consumed) = parse_typed_literal(&input[pos..])?;
        pos += consumed;
        args.push(lit);

        // Skip whitespace
        while pos < input.len() && input.as_bytes()[pos] == b' ' {
            pos += 1;
        }

        if pos >= input.len() {
            return Err("Unexpected end of input while parsing args".to_string());
        }

        match input.as_bytes()[pos] {
            b')' => {
                pos += 1;
                break;
            }
            b',' => {
                pos += 1; // skip comma
            }
            c => return Err(format!("Unexpected char '{}' in arg list at pos {}", c as char, pos)),
        }
    }

    Ok((args, pos))
}

/// Parse a test case line like: `func_name(arg1::type, arg2::type) = expected::type`
fn parse_test_line(line: &str) -> Result<(String, Vec<TypedLiteral>, TypedLiteral), String> {
    let paren_pos = line.find('(')
        .ok_or_else(|| format!("No '(' found in test line: {}", line))?;
    let func_name = line[..paren_pos].trim().to_string();
    if func_name.is_empty() {
        return Err(format!("Empty function name in: {}", line));
    }

    let after_name = &line[paren_pos + 1..];
    let (args, consumed) = parse_args(after_name)?;

    // After closing paren, expect ` = expected::type`
    let rest = after_name[consumed..].trim_start();
    if !rest.starts_with('=') {
        return Err(format!("Expected '=' after args, got: {}", rest));
    }
    let expected_str = rest[1..].trim_start();
    let (expected, _) = parse_typed_literal(expected_str)?;

    Ok((func_name, args, expected))
}

/// Parse a .test file and return all test cases.
pub fn parse_test_file(path: &str) -> Result<Vec<TestCase>, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {}", path, e))?;

    let mut cases = Vec::new();
    let mut current_function = String::new();
    let mut current_group = String::new();

    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = idx + 1;

        // Skip blank lines and header lines
        if trimmed.is_empty() || trimmed.starts_with("### ") {
            continue;
        }

        // Comment lines: extract function name or group
        if trimmed.starts_with('#') {
            let comment = trimmed[1..].trim();
            if let Some(group) = comment.strip_prefix("group:") {
                current_group = group.trim().to_string();
            } else if !comment.starts_with("Note:") && !comment.contains(':') && !comment.is_empty() {
                // Bare comment with just a name = function name marker
                current_function = comment.to_string();
            }
            continue;
        }

        // Test case line
        match parse_test_line(trimmed) {
            Ok((func_name, args, expected)) => {
                // If the parsed function name differs from current_function, update it
                if current_function.is_empty() {
                    current_function = func_name.clone();
                }
                cases.push(TestCase {
                    function_name: func_name,
                    args,
                    expected,
                    line_number,
                    group_name: current_group.clone(),
                });
            }
            Err(e) => {
                return Err(format!("Parse error at line {}: {}", line_number, e));
            }
        }
    }

    Ok(cases)
}

/// Convert a TypedLiteral to a SQL expression string.
fn literal_to_sql(lit: &TypedLiteral) -> String {
    match &lit.value {
        None => {
            let sql_type = dsl_type_to_sql(&lit.dsl_type);
            format!("CAST(NULL AS {})", sql_type)
        }
        Some(val) => match lit.dsl_type {
            DslType::Str => format!("'{}'", val),
            DslType::Bool => val.to_string(),
            DslType::I32 | DslType::I64 | DslType::Fp64 => val.to_string(),
            DslType::Date => format!("DATE '{}'", val),
            DslType::Timestamp => format!("TIMESTAMP '{}'", val),
        },
    }
}

/// Map DSL type to SQL type name for CAST expressions.
fn dsl_type_to_sql(t: &DslType) -> &'static str {
    match t {
        DslType::Str => "VARCHAR",
        DslType::I32 => "INT",
        DslType::I64 => "BIGINT",
        DslType::Fp64 => "DOUBLE",
        DslType::Bool => "BOOLEAN",
        DslType::Date => "DATE",
        DslType::Timestamp => "TIMESTAMP",
    }
}

/// Generate a SQL SELECT statement for a test case.
pub fn test_case_to_sql(tc: &TestCase) -> String {
    let args_sql: Vec<String> = tc.args.iter().map(literal_to_sql).collect();
    format!("SELECT {}({})", tc.function_name, args_sql.join(", "))
}

/// Execute all test cases from a .test file against a DataFusion SessionContext.
pub async fn run_test_file(ctx: &SessionContext, path: &str) -> Result<Vec<TestResult>, String> {
    let cases = parse_test_file(path)?;
    let mut results = Vec::with_capacity(cases.len());

    for tc in cases {
        let sql = test_case_to_sql(&tc);
        let result = match ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    if batches.is_empty() || batches[0].num_rows() == 0 {
                        TestResult {
                            test_case: tc,
                            passed: false,
                            actual: "<no rows>".to_string(),
                            error: None,
                        }
                    } else {
                        let col = batches[0].column(0);
                        let actual = arrow::util::display::array_value_to_string(col, 0)
                            .unwrap_or_else(|e| format!("<display error: {}>", e));
                        let expected_str = tc.expected.value.clone().unwrap_or_else(|| "NULL".to_string());
                        // Normalize comparison: treat empty-looking nulls
                        let passed = if tc.expected.value.is_none() {
                            col.is_null(0)
                        } else {
                            actual == expected_str
                        };
                        TestResult {
                            test_case: tc,
                            passed,
                            actual,
                            error: None,
                        }
                    }
                }
                Err(e) => TestResult {
                    test_case: tc,
                    passed: false,
                    actual: String::new(),
                    error: Some(format!("Execution error: {}", e)),
                },
            },
            Err(e) => TestResult {
                test_case: tc,
                passed: false,
                actual: String::new(),
                error: Some(format!("SQL error: {}", e)),
            },
        };
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_typed_literal_null() {
        let (lit, consumed) = parse_typed_literal("null::bool").unwrap();
        assert_eq!(lit.value, None);
        assert_eq!(lit.dsl_type, DslType::Bool);
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_parse_typed_literal_string() {
        let (lit, _) = parse_typed_literal("'hello'::str").unwrap();
        assert_eq!(lit.value, Some("hello".to_string()));
        assert_eq!(lit.dsl_type, DslType::Str);
    }

    #[test]
    fn test_parse_typed_literal_string_with_special_chars() {
        let (lit, _) = parse_typed_literal("'{\"a\":1,\"b\":2}'::str").unwrap();
        assert_eq!(lit.value, Some("{\"a\":1,\"b\":2}".to_string()));
        assert_eq!(lit.dsl_type, DslType::Str);
    }

    #[test]
    fn test_parse_typed_literal_number() {
        let (lit, _) = parse_typed_literal("3232235777::i64").unwrap();
        assert_eq!(lit.value, Some("3232235777".to_string()));
        assert_eq!(lit.dsl_type, DslType::I64);
    }

    #[test]
    fn test_parse_typed_literal_negative() {
        let (lit, _) = parse_typed_literal("-1::i64").unwrap();
        assert_eq!(lit.value, Some("-1".to_string()));
        assert_eq!(lit.dsl_type, DslType::I64);
    }

    #[test]
    fn test_parse_typed_literal_float() {
        let (lit, _) = parse_typed_literal("27.0::fp64").unwrap();
        assert_eq!(lit.value, Some("27.0".to_string()));
        assert_eq!(lit.dsl_type, DslType::Fp64);
    }

    #[test]
    fn test_parse_typed_literal_bool() {
        let (lit, _) = parse_typed_literal("true::bool").unwrap();
        assert_eq!(lit.value, Some("true".to_string()));
        assert_eq!(lit.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_parse_typed_literal_date() {
        let (lit, _) = parse_typed_literal("'2024-01-15'::date").unwrap();
        assert_eq!(lit.value, Some("2024-01-15".to_string()));
        assert_eq!(lit.dsl_type, DslType::Date);
    }

    #[test]
    fn test_parse_typed_literal_timestamp() {
        let (lit, _) = parse_typed_literal("'2024-01-15 10:30:00'::timestamp").unwrap();
        assert_eq!(lit.value, Some("2024-01-15 10:30:00".to_string()));
        assert_eq!(lit.dsl_type, DslType::Timestamp);
    }

    #[test]
    fn test_parse_test_line_basic() {
        let (name, args, expected) = parse_test_line(
            "cidrmatch('192.168.1.1'::str, '192.168.1.0/24'::str) = true::bool"
        ).unwrap();
        assert_eq!(name, "cidrmatch");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].value, Some("192.168.1.1".to_string()));
        assert_eq!(args[1].value, Some("192.168.1.0/24".to_string()));
        assert_eq!(expected.value, Some("true".to_string()));
        assert_eq!(expected.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_parse_test_line_zero_args() {
        let (name, args, expected) = parse_test_line(
            "e() = 2.718281828459045::fp64"
        ).unwrap();
        assert_eq!(name, "e");
        assert!(args.is_empty());
        assert_eq!(expected.value, Some("2.718281828459045".to_string()));
    }

    #[test]
    fn test_parse_test_line_json_string() {
        let (name, args, expected) = parse_test_line(
            "json_extract('{\"a\":1}'::str, '$.a'::str) = '1'::str"
        ).unwrap();
        assert_eq!(name, "json_extract");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].value, Some("{\"a\":1}".to_string()));
        assert_eq!(expected.value, Some("1".to_string()));
    }

    #[test]
    fn test_parse_test_line_null_result() {
        let (_, _, expected) = parse_test_line(
            "cidrmatch(null::str, '192.168.1.0/24'::str) = null::bool"
        ).unwrap();
        assert_eq!(expected.value, None);
        assert_eq!(expected.dsl_type, DslType::Bool);
    }

    #[test]
    fn test_literal_to_sql_string() {
        let lit = TypedLiteral { value: Some("hello".to_string()), dsl_type: DslType::Str };
        assert_eq!(literal_to_sql(&lit), "'hello'");
    }

    #[test]
    fn test_literal_to_sql_null() {
        let lit = TypedLiteral { value: None, dsl_type: DslType::I32 };
        assert_eq!(literal_to_sql(&lit), "CAST(NULL AS INT)");
    }

    #[test]
    fn test_literal_to_sql_date() {
        let lit = TypedLiteral { value: Some("2024-01-15".to_string()), dsl_type: DslType::Date };
        assert_eq!(literal_to_sql(&lit), "DATE '2024-01-15'");
    }

    #[test]
    fn test_literal_to_sql_timestamp() {
        let lit = TypedLiteral { value: Some("2024-01-15 10:30:00".to_string()), dsl_type: DslType::Timestamp };
        assert_eq!(literal_to_sql(&lit), "TIMESTAMP '2024-01-15 10:30:00'");
    }

    #[test]
    fn test_test_case_to_sql() {
        let tc = TestCase {
            function_name: "cidrmatch".to_string(),
            args: vec![
                TypedLiteral { value: Some("192.168.1.1".to_string()), dsl_type: DslType::Str },
                TypedLiteral { value: Some("192.168.1.0/24".to_string()), dsl_type: DslType::Str },
            ],
            expected: TypedLiteral { value: Some("true".to_string()), dsl_type: DslType::Bool },
            line_number: 1,
            group_name: "basic".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT cidrmatch('192.168.1.1', '192.168.1.0/24')");
    }

    #[test]
    fn test_test_case_to_sql_with_null() {
        let tc = TestCase {
            function_name: "cidrmatch".to_string(),
            args: vec![
                TypedLiteral { value: None, dsl_type: DslType::Str },
                TypedLiteral { value: Some("192.168.1.0/24".to_string()), dsl_type: DslType::Str },
            ],
            expected: TypedLiteral { value: None, dsl_type: DslType::Bool },
            line_number: 1,
            group_name: "null_handling".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT cidrmatch(CAST(NULL AS VARCHAR), '192.168.1.0/24')");
    }

    #[test]
    fn test_test_case_to_sql_zero_args() {
        let tc = TestCase {
            function_name: "e".to_string(),
            args: vec![],
            expected: TypedLiteral { value: Some("2.718281828459045".to_string()), dsl_type: DslType::Fp64 },
            line_number: 1,
            group_name: "basic".to_string(),
        };
        assert_eq!(test_case_to_sql(&tc), "SELECT e()");
    }
}
```

## 7. Dependencies: unified-functions-datafusion/Cargo.toml

```toml
[package]
name = "unified-functions-datafusion"
version = "0.1.0"
edition = "2021"

[dependencies]
datafusion = "45"
arrow = "54"
serde_yaml = "0.9"
serde_json = "1"
chrono = "0.4"
ipnetwork = "0.20"
regex = "1"

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }```

## 8. Module Registration: unified-functions-datafusion/src/lib.rs

```rust
pub mod ip;
pub mod json;
pub mod datetime;
pub mod collection;
pub mod math;
pub mod parse;
pub mod condition;
pub mod aggregation;
pub mod type_conv;
pub mod crypto;
pub mod rex;
pub mod binning;
pub mod relevance;
pub mod test_harness;

use datafusion::prelude::SessionContext;

pub fn register_all_udfs(ctx: &SessionContext) {
    ip::register_ip_udfs(ctx);
    json::register_json_udfs(ctx);
    datetime::register_datetime_udfs(ctx);
    collection::register_collection_udfs(ctx);
    math::register_math_udfs(ctx);
    parse::register_parse_udfs(ctx);
    condition::register_condition_udfs(ctx);
    aggregation::register_aggregation_udfs(ctx);
    type_conv::register_type_conv_udfs(ctx);
    crypto::register_crypto_udfs(ctx);
    rex::register_rex_udfs(ctx);
    binning::register_binning_udfs(ctx);
    relevance::register_relevance_udfs(ctx);
}
```

**Note:** `json::register_json_udfs(ctx)` is already wired into `register_all_udfs`. The json.rs stub just needs the UDF implementations added. Cargo.toml already has `serde_json = "1"` as a dependency. The YAML spec recommends also adding `jsonpath_lib` for `json_extract` path evaluation.
