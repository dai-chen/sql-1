/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;

public class CalciteExecAggregationBuilder
    extends AbstractAggregationBuilder<CalciteExecAggregationBuilder> {

  public static final String NAME = "calcite_exec";

  private List<String> fields;
  private boolean probe;
  private String plan;
  private List<String> schema;

  private static final ObjectParser<CalciteExecAggregationBuilder, Void> INTERNAL_PARSER =
      new ObjectParser<>(NAME);

  static {
    INTERNAL_PARSER.declareStringArray(
        CalciteExecAggregationBuilder::setFields, new ParseField("fields"));
    INTERNAL_PARSER.declareBoolean(
        CalciteExecAggregationBuilder::setProbe, new ParseField("probe"));
    INTERNAL_PARSER.declareString(CalciteExecAggregationBuilder::setPlan, new ParseField("plan"));
    INTERNAL_PARSER.declareStringArray(
        CalciteExecAggregationBuilder::setSchema, new ParseField("schema"));
  }

  public static final ContextParser<String, CalciteExecAggregationBuilder> PARSER =
      (parser, aggName) -> {
        CalciteExecAggregationBuilder builder = new CalciteExecAggregationBuilder(aggName);
        INTERNAL_PARSER.parse(parser, builder, null);
        return builder;
      };

  public CalciteExecAggregationBuilder(String name) {
    super(name);
    this.fields = List.of();
    this.probe = true;
    this.plan = null;
    this.schema = List.of();
  }

  public CalciteExecAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    this.fields = in.readStringList();
    this.probe = in.readBoolean();
    this.plan = in.readOptionalString();
    this.schema = in.readStringList();
  }

  protected CalciteExecAggregationBuilder(
      CalciteExecAggregationBuilder clone,
      AggregatorFactories.Builder factoriesBuilder,
      Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
    this.fields = clone.fields;
    this.probe = clone.probe;
    this.plan = clone.plan;
    this.schema = clone.schema;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    return new CalciteExecAggregationBuilder(this, factoriesBuilder, metadata);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeStringCollection(fields);
    out.writeBoolean(probe);
    out.writeOptionalString(plan);
    out.writeStringCollection(schema);
  }

  @Override
  protected AggregatorFactory doBuild(
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subfactoriesBuilder)
      throws IOException {
    return new CalciteExecAggregatorFactory(
        name,
        fields,
        probe,
        plan,
        schema,
        queryShardContext,
        parent,
        subfactoriesBuilder,
        metadata);
  }

  @Override
  protected XContentBuilder internalXContent(XContentBuilder builder, Params params)
      throws IOException {
    builder.startObject();
    builder.field("fields", fields);
    builder.field("probe", probe);
    if (plan != null) {
      builder.field("plan", plan);
    }
    if (!schema.isEmpty()) {
      builder.field("schema", schema);
    }
    builder.endObject();
    return builder;
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  public BucketCardinality bucketCardinality() {
    return BucketCardinality.NONE;
  }

  public void setFields(List<String> fields) {
    this.fields = new ArrayList<>(fields);
  }

  public void setProbe(boolean probe) {
    this.probe = probe;
  }

  public List<String> getFields() {
    return fields;
  }

  public boolean isProbe() {
    return probe;
  }

  public void setPlan(String plan) {
    this.plan = plan;
  }

  public String getPlan() {
    return plan;
  }

  public void setSchema(List<String> schema) {
    this.schema = new ArrayList<>(schema);
  }

  public List<String> getSchema() {
    return schema;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), fields, probe, plan, schema);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    if (!super.equals(obj)) return false;
    CalciteExecAggregationBuilder other = (CalciteExecAggregationBuilder) obj;
    return Objects.equals(fields, other.fields)
        && probe == other.probe
        && Objects.equals(plan, other.plan)
        && Objects.equals(schema, other.schema);
  }
}
