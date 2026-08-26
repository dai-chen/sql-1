/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

  @Test
  void round_trip_through_stream_with_early_termination_limit() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort").earlyTerminationLimit(42);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
    assertEquals(42, deserialized.getEarlyTerminationLimit());
  }

  @Test
  void round_trip_through_stream_with_null_early_termination_limit() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort").earlyTerminationLimit(null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
    assertEquals(null, deserialized.getEarlyTerminationLimit());
  }

  @Test
  void round_trip_through_xcontent_with_early_termination_limit() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort").earlyTerminationLimit(100);

    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();

    CalciteExecAggregationBuilder parsed =
        CalciteExecAggregationBuilder.parse(parser, original.getName());

    assertEquals(original, parsed);
    assertEquals(100, parsed.getEarlyTerminationLimit());
  }

  @Test
  void round_trip_through_xcontent_with_null_early_termination_limit() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort").earlyTerminationLimit(null);

    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();

    CalciteExecAggregationBuilder parsed =
        CalciteExecAggregationBuilder.parse(parser, original.getName());

    assertEquals(original, parsed);
    assertEquals(null, parsed.getEarlyTerminationLimit());
  }

  @Test
  void round_trip_through_stream_with_top_n_combine() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort")
            .combine(
                CombineDescriptor.topN(
                    List.of(1, 0), List.of("DESCENDING:LAST", "ASCENDING:FIRST"), 5));

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
  }

  @Test
  void round_trip_through_xcontent_with_top_n_combine() throws IOException {
    // TOP_N is the first mode carrying BOTH an int list (keys) and a string list (dirs), so the
    // XContent parser's field-name-driven array typing is exercised here for real: "keys" must
    // parse as integers and "dirs" as strings, or the descriptor silently changes meaning.
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort")
            .combine(
                CombineDescriptor.topN(
                    List.of(1, 0), List.of("DESCENDING:LAST", "ASCENDING:FIRST"), 5));

    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();

    CalciteExecAggregationBuilder parsed =
        CalciteExecAggregationBuilder.parse(parser, original.getName());

    assertEquals(original, parsed);
    assertEquals(List.of(1, 0), parsed.getCombine().getIntListParam());
    assertEquals(
        List.of("DESCENDING:LAST", "ASCENDING:FIRST"), parsed.getCombine().getStringListParam());
    assertEquals(5, parsed.getCombine().getIntParam());
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

  @Test
  void round_trip_through_stream_with_rank_limit_combine() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalProject[window]")
            .combine(
                CombineDescriptor.rankLimit(
                    List.of(2), List.of("1:ASCENDING:LAST"), 3, "DENSE_RANK"));

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    CalciteExecAggregationBuilder deserialized = new CalciteExecAggregationBuilder(in);

    assertEquals(original, deserialized);
  }

  /**
   * The rank function is the first thing to travel in CombineDescriptor's single-string slot, and
   * it is the ONLY field distinguishing RANK from DENSE_RANK on the reduce side. A descriptor whose
   * function is lost in the parser would silently change the answer on ties, so both the non-null
   * and the null case are asserted for whole-object equality.
   */
  @Test
  void round_trip_through_xcontent_with_rank_limit_combine() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalProject[window]")
            .combine(
                CombineDescriptor.rankLimit(
                    List.of(2), List.of("1:ASCENDING:LAST"), 3, "DENSE_RANK"));

    CalciteExecAggregationBuilder parsed = roundTripThroughXContent(original);

    assertEquals(original, parsed);
    assertEquals(List.of(2), parsed.getCombine().getIntListParam());
    assertEquals(List.of("1:ASCENDING:LAST"), parsed.getCombine().getStringListParam());
    assertEquals(3, parsed.getCombine().getIntParam());
    assertEquals("DENSE_RANK", parsed.getCombine().getRankFunction());
  }

  @Test
  void round_trip_through_xcontent_with_unordered_rank_limit_combine() throws IOException {
    // The dedup shape: no order keys, default ranking function.
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalProject[window]")
            .combine(CombineDescriptor.rankLimit(List.of(1), List.of(), 1, "ROW_NUMBER"));

    CalciteExecAggregationBuilder parsed = roundTripThroughXContent(original);

    assertEquals(original, parsed);
    assertEquals(List.of(), parsed.getCombine().getStringListParam());
    assertEquals("ROW_NUMBER", parsed.getCombine().getRankFunction());
  }

  /** A descriptor with no explicit rank function must survive both wires as an absent field. */
  @Test
  void round_trip_preserves_an_absent_rank_function() throws IOException {
    CalciteExecAggregationBuilder original =
        buildFullBuilder("LogicalSort").combine(CombineDescriptor.limit(7));

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    CalciteExecAggregationBuilder deserialized =
        new CalciteExecAggregationBuilder(out.bytes().streamInput());

    assertEquals(original, deserialized);
    assertNull(deserialized.getCombine().getStringParam());
    assertEquals(original, roundTripThroughXContent(original));
  }

  private static CalciteExecAggregationBuilder roundTripThroughXContent(
      CalciteExecAggregationBuilder original) throws IOException {
    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    xContentBuilder.startObject();
    original.toXContent(xContentBuilder, null);
    xContentBuilder.endObject();
    xContentBuilder.flush();
    String json = xContentBuilder.getOutputStream().toString();

    XContentParser parser = XContentType.JSON.xContent().createParser(null, null, json);
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    parser.nextToken();
    return CalciteExecAggregationBuilder.parse(parser, original.getName());
  }
}
