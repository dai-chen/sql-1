//! Schema loader that reads test_tables.yaml and creates DataFusion MemTables with sample data.

use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDate;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize)]
struct YamlRoot {
    tables: HashMap<String, YamlTable>,
}

#[derive(Deserialize)]
struct YamlTable {
    columns: Vec<YamlColumn>,
    sample_data: Vec<Vec<serde_yaml::Value>>,
}

#[derive(Deserialize)]
struct YamlColumn {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

/// Maps a YAML type string to an Arrow DataType.
fn map_type(t: &str) -> Result<DataType, Box<dyn std::error::Error>> {
    match t {
        "STRING" => Ok(DataType::Utf8),
        "INTEGER" => Ok(DataType::Int32),
        "LONG" => Ok(DataType::Int64),
        "DOUBLE" => Ok(DataType::Float64),
        "BOOLEAN" => Ok(DataType::Boolean),
        "TIMESTAMP" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        "DATE" => Ok(DataType::Date32),
        _ => Err(format!("Unknown type: {t}").into()),
    }
}

/// Parse a YAML value into the appropriate Arrow column and push to builders.
fn build_column(
    values: &[serde_yaml::Value],
    dt: &DataType,
) -> Result<Arc<dyn arrow::array::Array>, Box<dyn std::error::Error>> {
    match dt {
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| v.as_i64().map(|n| n as i32))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr: Int64Array = values.iter().map(|v| v.as_i64()).collect();
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr: Float64Array = values.iter().map(|v| v.as_f64()).collect();
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr: BooleanArray = values.iter().map(|v| v.as_bool()).collect();
            Ok(Arc::new(arr))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let arr: TimestampNanosecondArray = values
                .iter()
                .map(|v| {
                    v.as_str().and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                            .ok()
                            .map(|dt| dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let arr: Date32Array = values
                .iter()
                .map(|v| {
                    v.as_str().and_then(|s| {
                        NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| (d - epoch).num_days() as i32)
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        _ => Err(format!("Unsupported DataType: {dt:?}").into()),
    }
}

/// Loads tables from a YAML file and registers them (plus all UDFs) on a SessionContext.
pub async fn create_context_with_tables(
    yaml_path: &str,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(yaml_path)?;
    let root: YamlRoot = serde_yaml::from_str(&content)?;
    let ctx = SessionContext::new();

    for (table_name, table_def) in &root.tables {
        let fields: Vec<Field> = table_def
            .columns
            .iter()
            .map(|c| {
                let dt = map_type(&c.col_type)?;
                Ok(Field::new(&c.name, dt, true))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let schema = Arc::new(Schema::new(fields));
        let num_cols = table_def.columns.len();

        // Transpose row-major sample_data into column-major
        let num_rows = table_def.sample_data.len();
        let mut col_values: Vec<Vec<serde_yaml::Value>> = (0..num_cols).map(|_| Vec::with_capacity(num_rows)).collect();
        for row in &table_def.sample_data {
            for (ci, val) in row.iter().enumerate() {
                col_values[ci].push(val.clone());
            }
        }

        let columns: Vec<Arc<dyn arrow::array::Array>> = col_values
            .iter()
            .enumerate()
            .map(|(i, vals)| build_column(vals, schema.field(i).data_type()))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table(table_name.as_str(), Arc::new(mem_table))?;
    }

    crate::register_all_udfs(&ctx);
    Ok(ctx)
}
