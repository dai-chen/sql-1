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
