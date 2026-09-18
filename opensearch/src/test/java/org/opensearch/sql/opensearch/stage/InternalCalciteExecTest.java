/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIpValue;

class InternalCalciteExecTest {

  private static final String NAME = "calcite_stage";
  private static final Map<String, Object> METADATA = Map.of();

  @Test
  void rowGatherIsAssociative() {
    InternalCalciteExec first = partial(List.of(List.of("api", 4L), List.of("web", 2L)));
    InternalCalciteExec second = partial(List.of(List.of("api", 6L), List.of("web", 3L)));
    InternalCalciteExec third = partial(List.of(List.of("api", 1L), List.of("web", 5L)));

    InternalCalciteExec firstPair = reduce(first, first, second);
    InternalCalciteExec leftGrouped = reduce(firstPair, firstPair, third);
    InternalCalciteExec secondPair = reduce(second, second, third);
    InternalCalciteExec rightGrouped = reduce(first, first, secondPair);

    assertEquals(leftGrouped.getRows(), rightGrouped.getRows());
    assertEquals(
        List.of(
            List.of("api", 4L),
            List.of("web", 2L),
            List.of("api", 6L),
            List.of("web", 3L),
            List.of("api", 1L),
            List.of("web", 5L)),
        leftGrouped.getRows());
  }

  @Test
  void rowGatherPreservesNullCells() {
    InternalCalciteExec result =
        reduce(
            partial(List.of(Arrays.asList("api", null))),
            partial(List.of(Arrays.asList("api", null))),
            partial(List.of(List.of("web", 3L))));

    assertEquals(List.of(Arrays.asList("api", null), List.of("web", 3L)), result.getRows());
  }

  @Test
  void ipCellsUseTheirTransportRepresentation() {
    InternalCalciteExec result = partial(List.of(List.of(new ExprIpValue("192.168.0.1"))));

    assertEquals(List.of(List.of("192.168.0.1")), result.getRows());
  }

  private static InternalCalciteExec partial(List<List<Object>> rows) {
    return new InternalCalciteExec(NAME, rows, METADATA);
  }

  private static InternalCalciteExec reduce(
      InternalCalciteExec receiver, InternalCalciteExec... values) {
    return (InternalCalciteExec) receiver.reduce(List.of(values), null);
  }
}
