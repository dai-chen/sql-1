/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Wire round-trip tests for {@link CalciteExecAggregationBuilder}. Verifies that all fields
 * (including forcing_operator) survive StreamInput/StreamOutput and XContent serialize/parse.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CalciteExecAggregationBuilderWireTest {

  @Test
  void round_trip_through_stream_with_forcing_operator() throws IOException {
    CalciteExecAggregationBuilder original = buildFullBuilder("LogicalWindow");

    // Serialize
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    // Deserialize
    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
  }

  @Test
  void round_trip_through_stream_with_null_forcing_operator() throws IOException {
    CalciteExecAggregationBuilder original = buildFullBuilder(null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
  }

  @Test
  void round_trip_through_xcontent_with_forcing_operator() throws IOException {
    CalciteExecAggregationBuilder original = buildFullBuilder("LogicalAggregate");

    // Serialize to XContent — toXContent on AggregationBuilder expects to be inside an object
    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    // Parse back: the toXContent writes { "calcite_stage": { "calcite_exec": { ... } } }
    // The parse() method expects the parser positioned at the first token inside the
    // calcite_exec body object.
    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    // Advance: START_OBJECT (root) → FIELD_NAME ("calcite_stage") → START_OBJECT →
    //          FIELD_NAME ("calcite_exec") → START_OBJECT (body)
    parser.nextToken(); // START_OBJECT (root)
    parser.nextToken(); // FIELD_NAME "calcite_stage"
    parser.nextToken(); // START_OBJECT
    parser.nextToken(); // FIELD_NAME "calcite_exec"
    parser.nextToken(); // START_OBJECT (body)

    CalciteExecAggregationBuilder parsed =
        CalciteExecAggregationBuilder.parse(parser, original.getName());

    assertEquals(original, parsed);
  }

  @Test
  void round_trip_through_xcontent_with_null_forcing_operator() throws IOException {
    CalciteExecAggregationBuilder original = buildFullBuilder(null);

    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    parser.nextToken(); // START_OBJECT (root)
    parser.nextToken(); // FIELD_NAME "calcite_stage"
    parser.nextToken(); // START_OBJECT
    parser.nextToken(); // FIELD_NAME "calcite_exec"
    parser.nextToken(); // START_OBJECT (body)

    CalciteExecAggregationBuilder parsed =
        CalciteExecAggregationBuilder.parse(parser, original.getName());

    assertEquals(original, parsed);
  }

  private CalciteExecAggregationBuilder buildFullBuilder(String forcingOperator) {
    return new CalciteExecAggregationBuilder("calcite_stage")
        .plan("dGVzdA==")
        .fields(
            List.of(
                new CalciteExecAggregationBuilder.FieldDescriptor("account_number", "long"),
                new CalciteExecAggregationBuilder.FieldDescriptor("gender", "keyword")))
        .combine(CombineDescriptor.concat())
        .rowBudget(200000)
        .forcingOperator(forcingOperator);
  }
}
