use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

// ============================================================
// Helpers
// ============================================================

fn to_array(arg: &ColumnarValue, n: usize) -> ArrayRef {
    match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    }
}

fn date32_to_naive(d: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1).and_then(|e| e.checked_add_signed(Duration::days(d as i64)))
}

fn naive_to_date32(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (d - epoch).num_days() as i32
}

/// MySQL-compatible day number calculation (matches MySQL's calc_daynr).
/// Day 1 = 0000-01-01. Uses MySQL's calendar conventions.
fn calc_daynr(year: i32, month: i32, day: i32) -> i64 {
    if year == 0 && month == 0 { return 0; }
    let mut y = year as i64;
    let mut delsum = 365 * y + 31 * (month as i64 - 1) + day as i64;
    if month <= 2 {
        y -= 1;
    } else {
        delsum -= (month as i64 * 4 + 23) / 10;
    }
    if y > 0 {
        let temp = ((y / 100 + 1) * 3 + 1) / 4;
        delsum + y / 4 - temp
    } else if y == 0 {
        if year == 0 && month > 2 { delsum + 1 } else { delsum }
    } else {
        delsum
    }
}

/// Inverse of calc_daynr: convert MySQL day number to (year, month, day).
fn from_daynr(daynr: i64) -> Option<(i32, u32, u32)> {
    if daynr <= 0 { return None; }
    // Year 0: days 1-365
    if daynr <= 365 {
        let mut remaining = daynr as u32;
        let days_in_months: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for (i, &dm) in days_in_months.iter().enumerate() {
            if remaining <= dm {
                return Some((0, i as u32 + 1, remaining));
            }
            remaining -= dm;
        }
        return Some((0, 12, 31));
    }
    // Estimate year
    let mut y = (daynr * 100 / 36525) as i32;
    while calc_daynr(y + 1, 1, 1) <= daynr { y += 1; }
    while calc_daynr(y, 1, 1) > daynr { y -= 1; }
    let mut remaining = (daynr - calc_daynr(y, 1, 1) + 1) as u32;
    let is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    let days_in_months: [u32; 12] = [31, if is_leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for (i, &dm) in days_in_months.iter().enumerate() {
        if remaining <= dm {
            return Some((y, i as u32 + 1, remaining));
        }
        remaining -= dm;
    }
    Some((y, 12, 31))
}

/// Convert Date32 to MySQL day number
fn date32_to_daynr(d: i32) -> i64 {
    if let Some(nd) = date32_to_naive(d) {
        calc_daynr(nd.year(), nd.month() as i32, nd.day() as i32)
    } else {
        0
    }
}

/// Convert MySQL day number to Date32
fn daynr_to_date32(daynr: i64) -> Option<i32> {
    from_daynr(daynr).and_then(|(y, m, d)| {
        NaiveDate::from_ymd_opt(y, m, d).map(naive_to_date32)
    })
}

fn ts_nanos_to_naive(ns: i64) -> Option<NaiveDateTime> {
    let secs = ns.div_euclid(1_000_000_000);
    let nsecs = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
}

fn naive_to_ts_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

/// Extract NaiveDateTime from a timestamp ColumnarValue (handles both Nano and Micro)
fn get_ts_array_value(arr: &dyn Array, i: usize) -> Option<NaiveDateTime> {
    if arr.is_null(i) { return None; }
    if let Some(ns_arr) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        ts_nanos_to_naive(ns_arr.value(i))
    } else if let Some(us_arr) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let us = us_arr.value(i);
        let secs = us.div_euclid(1_000_000);
        let nsecs = (us.rem_euclid(1_000_000) * 1000) as u32;
        DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
    } else {
        None
    }
}

fn parse_time_str(s: &str) -> Option<(i64, i64, i64)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 { return None; }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    // Handle fractional seconds - take only integer part
    let s_part = parts[2].split('.').next().unwrap_or(parts[2]);
    let s: i64 = s_part.parse().ok()?;
    Some((h, m, s))
}

/// Convert MySQL format specifiers to chrono format specifiers
fn mysql_to_chrono_fmt(mysql_fmt: &str) -> String {
    let mut result = String::with_capacity(mysql_fmt.len());
    let chars: Vec<char> = mysql_fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '%' && i + 1 < chars.len() {
            match chars[i + 1] {
                'i' => { result.push_str("%M"); i += 2; }  // MySQL %i = minutes -> chrono %M
                'h' => { result.push_str("%I"); i += 2; }  // MySQL %h = 12-hour -> chrono %I
                's' => { result.push_str("%S"); i += 2; }  // MySQL %s = seconds -> chrono %S
                c => { result.push('%'); result.push(c); i += 2; }
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

/// Parse a YYYYMM or YYMM period into (year, month)
fn parse_period(p: i32) -> (i32, i32) {
    if p <= 0 { return (0, 0); }
    let year_part = p / 100;
    let month_part = p % 100;
    let year = if year_part < 100 {
        if year_part <= 69 { 2000 + year_part } else { 1900 + year_part }
    } else {
        year_part
    };
    (year, month_part)
}

/// Parse a timezone string - either named ("US/Eastern") or offset ("+05:30")
fn parse_tz_offset(tz_str: &str) -> Option<FixedOffset> {
    // Try offset format first: +HH:MM or -HH:MM
    if (tz_str.starts_with('+') || tz_str.starts_with('-')) && tz_str.len() >= 5 {
        let sign = if tz_str.starts_with('+') { 1 } else { -1 };
        let parts: Vec<&str> = tz_str[1..].split(':').collect();
        if parts.len() == 2 {
            let h: i32 = parts[0].parse().ok()?;
            let m: i32 = parts[1].parse().ok()?;
            let total_secs = sign * (h * 3600 + m * 60);
            return FixedOffset::east_opt(total_secs);
        }
    }
    None
}


// ============================================================
// adddate(date: Date32, days: i64) -> Date32
// ============================================================
#[derive(Debug)]
struct AdddateUdf { signature: Signature }
impl AdddateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32, DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for AdddateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "adddate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let days = a1.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: Date32Array = (0..dates.len()).map(|i| {
            if dates.is_null(i) || days.is_null(i) { return None; }
            let dn = date32_to_daynr(dates.value(i));
            daynr_to_date32(dn + days.value(i))
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// addtime(ts: Timestamp, time_str: str) -> Timestamp
// ============================================================
#[derive(Debug)]
struct AddtimeUdf { signature: Signature }
impl AddtimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for AddtimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "addtime" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let strs = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let len = a0.len();
        let result: TimestampMicrosecondArray = (0..len).map(|i| {
            if a0.is_null(i) || strs.is_null(i) { return None; }
            let dt = get_ts_array_value(a0.as_ref(), i)?;
            let (h, m, s) = parse_time_str(strs.value(i))?;
            dt.checked_add_signed(Duration::seconds(h * 3600 + m * 60 + s))
                .map(naive_to_ts_micros)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// convert_tz(ts: Timestamp, from_tz: str, to_tz: str) -> Timestamp
// ============================================================
#[derive(Debug)]
struct ConvertTzUdf { signature: Signature }
impl ConvertTzUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for ConvertTzUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "convert_tz" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let a2 = to_array(&args[2], n);
        let ts = &a0;
        let from_strs = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let to_strs = a2.as_any().downcast_ref::<StringArray>().unwrap();
        let result: TimestampMicrosecondArray = (0..ts.len()).map(|i| {
            if ts.is_null(i) || from_strs.is_null(i) || to_strs.is_null(i) { return None; }
            let ndt = get_ts_array_value(ts.as_ref(), i)?;
            let from_tz_str = from_strs.value(i);
            let to_tz_str = to_strs.value(i);

            // Try named timezone first, then offset
            if let (Ok(from_tz), Ok(to_tz)) = (Tz::from_str(from_tz_str), Tz::from_str(to_tz_str)) {
                let src = from_tz.from_local_datetime(&ndt).single()?;
                let dst = src.with_timezone(&to_tz);
                Some(naive_to_ts_micros(dst.naive_local()))
            } else if let (Some(from_off), Some(to_off)) = (parse_tz_offset(from_tz_str), parse_tz_offset(to_tz_str)) {
                let src = from_off.from_local_datetime(&ndt).single()?;
                let dst = src.with_timezone(&to_off);
                Some(naive_to_ts_micros(dst.naive_local()))
            } else if let (Ok(from_tz), Some(to_off)) = (Tz::from_str(from_tz_str), parse_tz_offset(to_tz_str)) {
                let src = from_tz.from_local_datetime(&ndt).single()?;
                let dst = src.with_timezone(&to_off);
                Some(naive_to_ts_micros(dst.naive_local()))
            } else if let (Some(from_off), Ok(to_tz)) = (parse_tz_offset(from_tz_str), Tz::from_str(to_tz_str)) {
                let src = from_off.from_local_datetime(&ndt).single()?;
                let dst = src.with_timezone(&to_tz);
                Some(naive_to_ts_micros(dst.naive_local()))
            } else {
                None
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// date(str) -> Date32
// ============================================================
#[derive(Debug)]
struct DateUdf { signature: Signature }
impl DateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for DateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "date" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Date32Array = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let s = strs.value(i);
            if s.is_empty() { return None; }
            // Try YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
            let date_part = if s.len() >= 10 { &s[..10] } else { s };
            NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok().map(naive_to_date32)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// date_format(ts: Timestamp, fmt: str) -> str
// ============================================================
#[derive(Debug)]
struct DateFormatUdf { signature: Signature }
impl DateFormatUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for DateFormatUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "date_format" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let ts = a0.as_ref();
        let fmts = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..ts.len()).map(|i| {
            if ts.is_null(i) || fmts.is_null(i) { return None; }
            let fmt = fmts.value(i);
            if fmt.is_empty() { return Some(String::new()); }
            let dt = get_ts_array_value(ts, i)?;
            let chrono_fmt = mysql_to_chrono_fmt(fmt);
            Some(dt.format(&chrono_fmt).to_string())
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// dayname(date: Date32) -> str
// ============================================================
#[derive(Debug)]
struct DaynameUdf { signature: Signature }
impl DaynameUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for DaynameUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "dayname" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let result: StringArray = (0..dates.len()).map(|i| {
            if dates.is_null(i) { return None; }
            date32_to_naive(dates.value(i)).map(|d| d.format("%A").to_string())
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// from_days(days: i64) -> Date32
// ============================================================
#[derive(Debug)]
struct FromDaysUdf { signature: Signature }
impl FromDaysUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for FromDaysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "from_days" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let vals = a0.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: Date32Array = (0..vals.len()).map(|i| {
            if vals.is_null(i) { return None; }
            let day_num = vals.value(i);
            if day_num <= 0 { return None; }
            daynr_to_date32(day_num)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// from_unixtime(secs: fp64) -> Timestamp
// ============================================================
#[derive(Debug)]
struct FromUnixtimeUdf { signature: Signature }
impl FromUnixtimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for FromUnixtimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "from_unixtime" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let vals = a0.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: TimestampMicrosecondArray = (0..vals.len()).map(|i| {
            if vals.is_null(i) { return None; }
            let secs = vals.value(i);
            let whole_secs = secs.trunc() as i64;
            // Truncate fractional seconds, output in microseconds
            whole_secs.checked_mul(1_000_000)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// get_format(type: str, locale: str) -> str
// ============================================================
#[derive(Debug)]
struct GetFormatUdf { signature: Signature }
impl GetFormatUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for GetFormatUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "get_format" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let types = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let locales = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..types.len()).map(|i| {
            if types.is_null(i) || locales.is_null(i) { return None; }
            let fmt = match (types.value(i), locales.value(i)) {
                ("DATE", "USA") => "%m.%d.%Y",
                ("DATE", "EUR") => "%d.%m.%Y",
                ("DATE", "ISO") | ("DATE", "JIS") => "%Y-%m-%d",
                ("DATE", "INTERNAL") => "%Y%m%d",
                ("TIME", "USA") => "%h:%i:%s %p",
                ("TIME", "EUR") => "%H.%i.%s",
                ("TIME", "ISO") | ("TIME", "JIS") => "%H:%i:%s",
                ("TIME", "INTERNAL") => "%H%i%s",
                ("DATETIME", "USA") => "%Y-%m-%d %H.%i.%s",
                ("DATETIME", "EUR") => "%Y-%m-%d %H.%i.%s",
                ("DATETIME", "ISO") | ("DATETIME", "JIS") => "%Y-%m-%d %H:%i:%s",
                ("DATETIME", "INTERNAL") => "%Y%m%d%H%i%s",
                _ => return None,
            };
            Some(fmt.to_string())
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// makedate(year: fp64, day_of_year: fp64) -> Date32
// ============================================================
#[derive(Debug)]
struct MakedateUdf { signature: Signature }
impl MakedateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64, DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for MakedateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "makedate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let years = a0.as_any().downcast_ref::<Float64Array>().unwrap();
        let days = a1.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: Date32Array = (0..years.len()).map(|i| {
            if years.is_null(i) || days.is_null(i) { return None; }
            let year = years.value(i) as i32;
            let day = days.value(i) as i64;
            if day <= 0 { return None; }
            let jan1_dn = calc_daynr(year, 1, 1);
            daynr_to_date32(jan1_dn + day - 1)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// maketime(h: fp64, m: fp64, s: fp64) -> str
// ============================================================
#[derive(Debug)]
struct MaketimeUdf { signature: Signature }
impl MaketimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64, DataType::Float64, DataType::Float64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for MaketimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "maketime" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let a2 = to_array(&args[2], n);
        let hs = a0.as_any().downcast_ref::<Float64Array>().unwrap();
        let ms = a1.as_any().downcast_ref::<Float64Array>().unwrap();
        let ss = a2.as_any().downcast_ref::<Float64Array>().unwrap();
        let result: StringArray = (0..hs.len()).map(|i| {
            if hs.is_null(i) || ms.is_null(i) || ss.is_null(i) { return None; }
            Some(format!("{:02}:{:02}:{:02}", hs.value(i) as i64, ms.value(i) as i64, ss.value(i) as i64))
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// microsecond(time_str: str) -> i32
// ============================================================
#[derive(Debug)]
struct MicrosecondUdf { signature: Signature }
impl MicrosecondUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for MicrosecondUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "microsecond" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let s = strs.value(i);
            // Find fractional part after last '.'
            if let Some(dot_pos) = s.rfind('.') {
                let frac = &s[dot_pos + 1..];
                // Pad or truncate to 6 digits
                let padded = format!("{:0<6}", frac);
                padded[..6].parse::<i32>().ok()
            } else {
                Some(0)
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// minute_of_day(time_str: str) -> i32
// ============================================================
#[derive(Debug)]
struct MinuteOfDayUdf { signature: Signature }
impl MinuteOfDayUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for MinuteOfDayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "minute_of_day" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int32Array = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let (h, m, _) = parse_time_str(strs.value(i))?;
            Some((h * 60 + m) as i32)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// monthname(date: Date32) -> str
// ============================================================
#[derive(Debug)]
struct MonthnameUdf { signature: Signature }
impl MonthnameUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for MonthnameUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "monthname" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let result: StringArray = (0..dates.len()).map(|i| {
            if dates.is_null(i) { return None; }
            date32_to_naive(dates.value(i)).map(|d| d.format("%B").to_string())
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// period_add(period: i32, months: i32) -> i32
// ============================================================
#[derive(Debug)]
struct PeriodAddUdf { signature: Signature }
impl PeriodAddUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Int64, DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for PeriodAddUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "period_add" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let periods = a0.as_any().downcast_ref::<Int64Array>().unwrap();
        let months = a1.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: Int32Array = (0..periods.len()).map(|i| {
            if periods.is_null(i) || months.is_null(i) { return None; }
            let (year, month) = parse_period(periods.value(i) as i32);
            let total_months = year * 12 + month + months.value(i) as i32;
            let new_year = (total_months - 1) / 12;
            let new_month = (total_months - 1) % 12 + 1;
            Some(new_year * 100 + new_month)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// period_diff(p1: i32, p2: i32) -> i32
// ============================================================
#[derive(Debug)]
struct PeriodDiffUdf { signature: Signature }
impl PeriodDiffUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Int64, DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for PeriodDiffUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "period_diff" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let p1s = a0.as_any().downcast_ref::<Int64Array>().unwrap();
        let p2s = a1.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: Int32Array = (0..p1s.len()).map(|i| {
            if p1s.is_null(i) || p2s.is_null(i) { return None; }
            let (y1, m1) = parse_period(p1s.value(i) as i32);
            let (y2, m2) = parse_period(p2s.value(i) as i32);
            Some((y1 * 12 + m1) - (y2 * 12 + m2))
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// sec_to_time(secs: i64) -> str
// ============================================================
#[derive(Debug)]
struct SecToTimeUdf { signature: Signature }
impl SecToTimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for SecToTimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "sec_to_time" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let vals = a0.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: StringArray = (0..vals.len()).map(|i| {
            if vals.is_null(i) { return None; }
            let total = vals.value(i);
            let neg = total < 0;
            let abs = total.unsigned_abs();
            let h = abs / 3600;
            let m = (abs % 3600) / 60;
            let s = abs % 60;
            Some(if neg {
                format!("-{:02}:{:02}:{:02}", h, m, s)
            } else {
                format!("{:02}:{:02}:{:02}", h, m, s)
            })
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// str_to_date(str, fmt: str) -> Timestamp
// ============================================================
#[derive(Debug)]
struct StrToDateUdf { signature: Signature }
impl StrToDateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for StrToDateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "str_to_date" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let fmts = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: TimestampMicrosecondArray = (0..strs.len()).map(|i| {
            if strs.is_null(i) || fmts.is_null(i) { return None; }
            let s = strs.value(i);
            if s.is_empty() { return None; }
            let chrono_fmt = mysql_to_chrono_fmt(fmts.value(i));
            // Try parsing as datetime first, then as date
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, &chrono_fmt) {
                Some(naive_to_ts_micros(dt))
            } else if let Ok(d) = NaiveDate::parse_from_str(s, &chrono_fmt) {
                Some(naive_to_ts_micros(d.and_hms_opt(0, 0, 0).unwrap()))
            } else {
                None
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// strftime(ts: Timestamp, fmt: str) -> str  (alias for date_format)
// ============================================================
#[derive(Debug)]
struct StrftimeUdf { signature: Signature }
impl StrftimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for StrftimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "strftime" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let ts = a0.as_ref();
        let fmts = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..ts.len()).map(|i| {
            if ts.is_null(i) || fmts.is_null(i) { return None; }
            let fmt = fmts.value(i);
            if fmt.is_empty() { return Some(String::new()); }
            let dt = get_ts_array_value(ts, i)?;
            let chrono_fmt = mysql_to_chrono_fmt(fmt);
            Some(dt.format(&chrono_fmt).to_string())
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// subdate(date: Date32, days: i64) -> Date32
// ============================================================
#[derive(Debug)]
struct SubdateUdf { signature: Signature }
impl SubdateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32, DataType::Int64], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for SubdateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "subdate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let days = a1.as_any().downcast_ref::<Int64Array>().unwrap();
        let result: Date32Array = (0..dates.len()).map(|i| {
            if dates.is_null(i) || days.is_null(i) { return None; }
            let dn = date32_to_daynr(dates.value(i));
            daynr_to_date32(dn - days.value(i))
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// subtime(ts: Timestamp, time_str: str) -> Timestamp
// ============================================================
#[derive(Debug)]
struct SubtimeUdf { signature: Signature }
impl SubtimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for SubtimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "subtime" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let strs = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let len = a0.len();
        let result: TimestampMicrosecondArray = (0..len).map(|i| {
            if a0.is_null(i) || strs.is_null(i) { return None; }
            let dt = get_ts_array_value(a0.as_ref(), i)?;
            let (h, m, s) = parse_time_str(strs.value(i))?;
            dt.checked_sub_signed(Duration::seconds(h * 3600 + m * 60 + s))
                .map(naive_to_ts_micros)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// sysdate() -> Timestamp (Volatile)
// ============================================================
#[derive(Debug)]
struct SysdateUdf { signature: Signature }
impl SysdateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Volatile) }
    }
}
impl ScalarUDFImpl for SysdateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "sysdate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, _args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let now = chrono::Utc::now().naive_utc();
        let us = naive_to_ts_micros(now);
        let result: TimestampMicrosecondArray = (0..n).map(|_| Some(us)).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// time(str) -> str
// ============================================================
#[derive(Debug)]
struct TimeUdf { signature: Signature }
impl TimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for TimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "time" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let s = strs.value(i);
            if s.is_empty() { return None; }
            // If contains space, take part after last space (datetime format)
            if let Some(space_pos) = s.rfind(' ') {
                let time_part = &s[space_pos + 1..];
                if time_part.contains(':') {
                    return Some(time_part.to_string());
                }
            }
            // Otherwise it's already a time string
            if s.contains(':') {
                Some(s.to_string())
            } else {
                None
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// timediff(t1: str, t2: str) -> str
// ============================================================
#[derive(Debug)]
struct TimediffUdf { signature: Signature }
impl TimediffUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for TimediffUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "timediff" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let a1 = to_array(&args[1], n);
        let s1 = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let s2 = a1.as_any().downcast_ref::<StringArray>().unwrap();
        let result: StringArray = (0..s1.len()).map(|i| {
            if s1.is_null(i) || s2.is_null(i) { return None; }
            let (h1, m1, sec1) = parse_time_str(s1.value(i))?;
            let (h2, m2, sec2) = parse_time_str(s2.value(i))?;
            let total1 = h1 * 3600 + m1 * 60 + sec1;
            let total2 = h2 * 3600 + m2 * 60 + sec2;
            let diff = total1 - total2;
            let neg = diff < 0;
            let abs = diff.unsigned_abs();
            let h = abs / 3600;
            let m = (abs % 3600) / 60;
            let s = abs % 60;
            Some(if neg {
                format!("-{:02}:{:02}:{:02}", h, m, s)
            } else {
                format!("{:02}:{:02}:{:02}", h, m, s)
            })
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// time_to_sec(time_str: str) -> i64
// ============================================================
#[derive(Debug)]
struct TimeToSecUdf { signature: Signature }
impl TimeToSecUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for TimeToSecUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "time_to_sec" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: Int64Array = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let (h, m, s) = parse_time_str(strs.value(i))?;
            Some(h * 3600 + m * 60 + s)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// timestamp(str) -> Timestamp
// ============================================================
#[derive(Debug)]
struct TimestampUdf { signature: Signature }
impl TimestampUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for TimestampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "timestamp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let strs = a0.as_any().downcast_ref::<StringArray>().unwrap();
        let result: TimestampMicrosecondArray = (0..strs.len()).map(|i| {
            if strs.is_null(i) { return None; }
            let s = strs.value(i);
            if s.is_empty() { return None; }
            // Try datetime first, then date-only
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Some(naive_to_ts_micros(dt))
            } else if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Some(naive_to_ts_micros(d.and_hms_opt(0, 0, 0).unwrap()))
            } else {
                None
            }
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// to_days(date: Date32) -> i64
// ============================================================
#[derive(Debug)]
struct ToDaysUdf { signature: Signature }
impl ToDaysUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for ToDaysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "to_days" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let result: Int64Array = (0..dates.len()).map(|i| {
            if dates.is_null(i) { return None; }
            Some(date32_to_daynr(dates.value(i)))
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// unix_timestamp(ts: Timestamp) -> fp64
// ============================================================
#[derive(Debug)]
struct UnixTimestampUdf { signature: Signature }
impl UnixTimestampUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for UnixTimestampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "unix_timestamp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let len = a0.len();
        let result: Float64Array = (0..len).map(|i| {
            if a0.is_null(i) { return None; }
            let dt = get_ts_array_value(a0.as_ref(), i)?;
            Some(dt.and_utc().timestamp() as f64)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// utc_date() -> Date32 (Volatile)
// ============================================================
#[derive(Debug)]
struct UtcDateUdf { signature: Signature }
impl UtcDateUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Volatile) }
    }
}
impl ScalarUDFImpl for UtcDateUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "utc_date" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn invoke_batch(&self, _args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let today = chrono::Utc::now().naive_utc().date();
        let d32 = naive_to_date32(today);
        let result: Date32Array = (0..n).map(|_| Some(d32)).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// utc_time() -> str (Volatile)
// ============================================================
#[derive(Debug)]
struct UtcTimeUdf { signature: Signature }
impl UtcTimeUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Volatile) }
    }
}
impl ScalarUDFImpl for UtcTimeUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "utc_time" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_batch(&self, _args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let now = chrono::Utc::now().naive_utc();
        let time_str = now.format("%H:%M:%S").to_string();
        let result: StringArray = (0..n).map(|_| Some(time_str.clone())).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// utc_timestamp() -> Timestamp (Volatile)
// ============================================================
#[derive(Debug)]
struct UtcTimestampUdf { signature: Signature }
impl UtcTimestampUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Volatile) }
    }
}
impl ScalarUDFImpl for UtcTimestampUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "utc_timestamp" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn invoke_batch(&self, _args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let now = chrono::Utc::now().naive_utc();
        let us = naive_to_ts_micros(now);
        let result: TimestampMicrosecondArray = (0..n).map(|_| Some(us)).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// weekday(date: Date32) -> i32
// ============================================================
#[derive(Debug)]
struct WeekdayUdf { signature: Signature }
impl WeekdayUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for WeekdayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "weekday" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let result: Int32Array = (0..dates.len()).map(|i| {
            if dates.is_null(i) { return None; }
            date32_to_naive(dates.value(i)).map(|d| d.weekday().num_days_from_monday() as i32)
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// yearweek(date: Date32) -> i32
// ============================================================
#[derive(Debug)]
struct YearweekUdf { signature: Signature }
impl YearweekUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable) }
    }
}
impl ScalarUDFImpl for YearweekUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "yearweek" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> Result<ColumnarValue> {
        let a0 = to_array(&args[0], n);
        let dates = a0.as_any().downcast_ref::<Date32Array>().unwrap();
        let result: Int32Array = (0..dates.len()).map(|i| {
            if dates.is_null(i) { return None; }
            date32_to_naive(dates.value(i)).map(|d| {
                let iso = d.iso_week();
                iso.year() * 100 + iso.week() as i32
            })
        }).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ============================================================
// Registration
// ============================================================
pub fn register_datetime_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(AdddateUdf::new()));
    ctx.register_udf(ScalarUDF::from(AddtimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(ConvertTzUdf::new()));
    ctx.register_udf(ScalarUDF::from(DateUdf::new()));
    ctx.register_udf(ScalarUDF::from(DateFormatUdf::new()));
    ctx.register_udf(ScalarUDF::from(DaynameUdf::new()));
    ctx.register_udf(ScalarUDF::from(FromDaysUdf::new()));
    ctx.register_udf(ScalarUDF::from(FromUnixtimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(GetFormatUdf::new()));
    ctx.register_udf(ScalarUDF::from(MakedateUdf::new()));
    ctx.register_udf(ScalarUDF::from(MaketimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(MicrosecondUdf::new()));
    ctx.register_udf(ScalarUDF::from(MinuteOfDayUdf::new()));
    ctx.register_udf(ScalarUDF::from(MonthnameUdf::new()));
    ctx.register_udf(ScalarUDF::from(PeriodAddUdf::new()));
    ctx.register_udf(ScalarUDF::from(PeriodDiffUdf::new()));
    ctx.register_udf(ScalarUDF::from(SecToTimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(StrToDateUdf::new()));
    ctx.register_udf(ScalarUDF::from(StrftimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(SubdateUdf::new()));
    ctx.register_udf(ScalarUDF::from(SubtimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(SysdateUdf::new()));
    ctx.register_udf(ScalarUDF::from(TimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(TimediffUdf::new()));
    ctx.register_udf(ScalarUDF::from(TimeToSecUdf::new()));
    ctx.register_udf(ScalarUDF::from(TimestampUdf::new()));
    ctx.register_udf(ScalarUDF::from(ToDaysUdf::new()));
    ctx.register_udf(ScalarUDF::from(UnixTimestampUdf::new()));
    ctx.register_udf(ScalarUDF::from(UtcDateUdf::new()));
    ctx.register_udf(ScalarUDF::from(UtcTimeUdf::new()));
    ctx.register_udf(ScalarUDF::from(UtcTimestampUdf::new()));
    ctx.register_udf(ScalarUDF::from(WeekdayUdf::new()));
    ctx.register_udf(ScalarUDF::from(YearweekUdf::new()));
}
