/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;

/**
 * AggregationBuilder for the staged Calcite execution custom aggregation. Carries the serialized
 * shard fragment plan, output field descriptors, combine descriptor, and row budget.
 */
public class CalciteExecAggregationBuilder
    extends AbstractAggregationBuilder<CalciteExecAggregationBuilder> {

  public static final String NAME = "calcite_exec";

  // Wire names follow the design doc verbatim: snake_case for top-level XContent keys
  // (row_budget), camelCase within the combine descriptor. Do not "fix" this naming.
  private String plan;
  private List<FieldDescriptor> fields;
  private CombineDescriptor combine;
  private int rowBudget;

  /** A name+type pair describing a single output field. */
  public static class FieldDescriptor implements Writeable, ToXContentObject {
    private final String name;
    private final String type;

    public FieldDescriptor(String name, String type) {
      this.name = Objects.requireNonNull(name);
      this.type = Objects.requireNonNull(type);
    }

    public FieldDescriptor(StreamInput in) throws IOException {
      this.name = in.readString();
      this.type = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeString(name);
      out.writeString(type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      builder.startObject();
      builder.field("name", name);
      builder.field("type", type);
      builder.endObject();
      return builder;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldDescriptor that = (FieldDescriptor) o;
      return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }

  public CalciteExecAggregationBuilder(String name) {
    super(name);
    this.plan = "";
    this.fields = List.of();
    this.combine = CombineDescriptor.concat();
    this.rowBudget = 200000;
  }

  public CalciteExecAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    this.plan = in.readString();
    this.fields = in.readList(FieldDescriptor::new);
    this.combine = new CombineDescriptor(in);
    this.rowBudget = in.readVInt();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(plan);
    out.writeList(fields);
    combine.writeTo(out);
    out.writeVInt(rowBudget);
  }

  @Override
  protected XContentBuilder internalXContent(XContentBuilder builder, Params params)
      throws IOException {
    builder.startObject();
    builder.field("plan", plan);
    builder.startArray("fields");
    for (FieldDescriptor fd : fields) {
      fd.toXContent(builder, params);
    }
    builder.endArray();
    builder.field("combine");
    combine.toXContent(builder, params);
    // Design doc wire name is "row_budget" (snake_case).
    builder.field("row_budget", rowBudget);
    builder.endObject();
    return builder;
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    CalciteExecAggregationBuilder copy = new CalciteExecAggregationBuilder(name);
    copy.plan = this.plan;
    copy.fields = this.fields;
    copy.combine = this.combine;
    copy.rowBudget = this.rowBudget;
    copy.factoriesBuilder = factoriesBuilder;
    copy.metadata = metadata;
    return copy;
  }

  @Override
  protected AggregatorFactory doBuild(
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subfactoriesBuilder)
      throws IOException {
    return new CalciteExecAggregatorFactory(
        name,
        plan,
        fields,
        combine,
        rowBudget,
        queryShardContext,
        parent,
        subfactoriesBuilder,
        metadata);
  }

  @Override
  public BucketCardinality bucketCardinality() {
    return BucketCardinality.NONE;
  }

  // --- Setters for builder pattern ---

  public CalciteExecAggregationBuilder plan(String plan) {
    this.plan = plan;
    return this;
  }

  public CalciteExecAggregationBuilder fields(List<FieldDescriptor> fields) {
    this.fields = List.copyOf(fields);
    return this;
  }

  public CalciteExecAggregationBuilder combine(CombineDescriptor combine) {
    this.combine = combine;
    return this;
  }

  public CalciteExecAggregationBuilder rowBudget(int rowBudget) {
    this.rowBudget = rowBudget;
    return this;
  }

  // --- Getters ---

  public String getPlan() {
    return plan;
  }

  public List<FieldDescriptor> getFields() {
    return fields;
  }

  public CombineDescriptor getCombine() {
    return combine;
  }

  public int getRowBudget() {
    return rowBudget;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), plan, fields, combine, rowBudget);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    if (!super.equals(obj)) return false;
    CalciteExecAggregationBuilder other = (CalciteExecAggregationBuilder) obj;
    return Objects.equals(plan, other.plan)
        && Objects.equals(fields, other.fields)
        && Objects.equals(combine, other.combine)
        && rowBudget == other.rowBudget;
  }

  /**
   * Parse a CalciteExecAggregationBuilder from XContent. Used as the ContextParser in the
   * AggregationSpec registration.
   */
  public static CalciteExecAggregationBuilder parse(XContentParser parser, String aggName)
      throws IOException {
    CalciteExecAggregationBuilder builder = new CalciteExecAggregationBuilder(aggName);
    String fieldName = null;
    XContentParser.Token token;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        fieldName = parser.currentName();
      } else if (token == XContentParser.Token.VALUE_STRING) {
        if ("plan".equals(fieldName)) {
          builder.plan = parser.text();
        }
      } else if (token == XContentParser.Token.VALUE_NUMBER) {
        if ("row_budget".equals(fieldName)) {
          builder.rowBudget = parser.intValue();
        }
      } else if (token == XContentParser.Token.START_ARRAY && "fields".equals(fieldName)) {
        List<FieldDescriptor> fds = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
          fds.add(parseFieldDescriptor(parser));
        }
        builder.fields = List.copyOf(fds);
      } else if (token == XContentParser.Token.START_OBJECT && "combine".equals(fieldName)) {
        builder.combine = CombineDescriptor.fromXContent(parser);
      }
    }
    return builder;
  }

  private static FieldDescriptor parseFieldDescriptor(XContentParser parser) throws IOException {
    String name = null;
    String type = null;
    String fieldName = null;
    XContentParser.Token token;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        fieldName = parser.currentName();
      } else if (token == XContentParser.Token.VALUE_STRING) {
        if ("name".equals(fieldName)) {
          name = parser.text();
        } else if ("type".equals(fieldName)) {
          type = parser.text();
        }
      }
    }
    return new FieldDescriptor(name != null ? name : "", type != null ? type : "");
  }
}
