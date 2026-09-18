/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;

class CalciteExecAggregationBuilderTest {

  @Test
  void typedTransportRoundTrips() throws IOException {
    CalciteExecAggregationBuilder original =
        new CalciteExecAggregationBuilder("stage")
            .fields(
                List.of(
                    new CalciteExecAggregationBuilder.FieldDescriptor("service", "keyword"),
                    new CalciteExecAggregationBuilder.FieldDescriptor("bytes", "long")))
            .fragmentTableName("_matching_rows_0")
            .fragmentJson("{\"rels\":[]}")
            .inputRowTypeJson("{\"fields\":[]}")
            .currentTimeNanos(123L);

    BytesStreamOutput output = new BytesStreamOutput();
    original.writeTo(output);
    try (StreamInput input = output.bytes().streamInput()) {
      assertEquals(original, new CalciteExecAggregationBuilder(input));
    }
  }

  @Test
  void restParsingIsRejected() throws IOException {
    try (XContentParser parser =
        XContentType.JSON.xContent().createParser(null, null, "{\"plan\":\"untrusted\"}")) {
      parser.nextToken();
      ParsingException error =
          assertThrows(
              ParsingException.class, () -> CalciteExecAggregationBuilder.parse(parser, "stage"));
      assertEquals("[calcite_exec] is an internal aggregation", error.getMessage());
    }
  }
}
