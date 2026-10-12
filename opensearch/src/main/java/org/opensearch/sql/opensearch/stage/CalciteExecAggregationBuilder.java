/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.common.ParsingException;
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

/** Internal transport builder for shard-local Calcite fragment execution. */
public final class CalciteExecAggregationBuilder
    extends AbstractAggregationBuilder<CalciteExecAggregationBuilder> {

  public static final String NAME = "calcite_exec";

  /** Field name and OpenSearch mapping type used by the Lucene row reader. */
  public record FieldDescriptor(String name, String mappingType)
      implements Writeable, ToXContentObject {

    public FieldDescriptor {
      Objects.requireNonNull(name);
      Objects.requireNonNull(mappingType);
    }

    public FieldDescriptor(StreamInput in) throws IOException {
      this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeString(name);
      out.writeString(mappingType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      return builder.startObject().field("name", name).field("type", mappingType).endObject();
    }
  }

  private List<FieldDescriptor> fields = List.of();
  private String fragmentTableName = "";
  private String fragmentJson = "";
  private String inputRowTypeJson = "";
  private long currentTimeNanos = -1L;

  public CalciteExecAggregationBuilder(String name) {
    super(name);
  }

  public CalciteExecAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    fields = in.readList(FieldDescriptor::new);
    fragmentTableName = in.readString();
    fragmentJson = in.readString();
    inputRowTypeJson = in.readString();
    currentTimeNanos = in.readLong();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeList(fields);
    out.writeString(fragmentTableName);
    out.writeString(fragmentJson);
    out.writeString(inputRowTypeJson);
    out.writeLong(currentTimeNanos);
  }

  @Override
  protected XContentBuilder internalXContent(XContentBuilder builder, Params params)
      throws IOException {
    builder.startObject().startArray("fields");
    for (FieldDescriptor field : fields) {
      field.toXContent(builder, params);
    }
    return builder
        .endArray()
        .field("internal", true)
        .field("fragment_bytes", fragmentJson.length())
        .endObject();
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    CalciteExecAggregationBuilder copy =
        new CalciteExecAggregationBuilder(name)
            .fields(fields)
            .fragmentTableName(fragmentTableName)
            .fragmentJson(fragmentJson)
            .inputRowTypeJson(inputRowTypeJson)
            .currentTimeNanos(currentTimeNanos);
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
        fields,
        fragmentTableName,
        fragmentJson,
        inputRowTypeJson,
        currentTimeNanos,
        queryShardContext,
        parent,
        subfactoriesBuilder,
        metadata);
  }

  @Override
  public BucketCardinality bucketCardinality() {
    return BucketCardinality.NONE;
  }

  public CalciteExecAggregationBuilder fields(List<FieldDescriptor> fields) {
    this.fields = List.copyOf(fields);
    return this;
  }

  public CalciteExecAggregationBuilder fragmentTableName(String fragmentTableName) {
    this.fragmentTableName = Objects.requireNonNull(fragmentTableName);
    return this;
  }

  public CalciteExecAggregationBuilder fragmentJson(String fragmentJson) {
    this.fragmentJson = Objects.requireNonNull(fragmentJson);
    return this;
  }

  public CalciteExecAggregationBuilder inputRowTypeJson(String inputRowTypeJson) {
    this.inputRowTypeJson = Objects.requireNonNull(inputRowTypeJson);
    return this;
  }

  public CalciteExecAggregationBuilder currentTimeNanos(long currentTimeNanos) {
    if (currentTimeNanos < 0) {
      throw new IllegalArgumentException("currentTimeNanos must be non-negative");
    }
    this.currentTimeNanos = currentTimeNanos;
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        fields,
        fragmentTableName,
        fragmentJson,
        inputRowTypeJson,
        currentTimeNanos);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CalciteExecAggregationBuilder that) || !super.equals(other)) {
      return false;
    }
    return fields.equals(that.fields)
        && fragmentTableName.equals(that.fragmentTableName)
        && fragmentJson.equals(that.fragmentJson)
        && inputRowTypeJson.equals(that.inputRowTypeJson)
        && currentTimeNanos == that.currentTimeNanos;
  }

  /**
   * Deliberately rejects REST requests. The descriptor is generated by the SQL coordinator and
   * transported as a typed builder; callers cannot submit an executable program.
   */
  public static CalciteExecAggregationBuilder parse(XContentParser parser, String name)
      throws IOException {
    throw new ParsingException(
        parser.getTokenLocation(), "[" + NAME + "] is an internal aggregation");
  }
}
