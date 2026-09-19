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
    InternalCalciteExec first = partial(row("api", 4L), row("web", 2L));
    InternalCalciteExec second = partial(row("api", 6L), row("web", 3L));
    InternalCalciteExec third = partial(row("api", 1L), row("web", 5L));

    InternalCalciteExec firstPair = reduce(first, first, second);
    InternalCalciteExec leftGrouped = reduce(firstPair, firstPair, third);
    InternalCalciteExec secondPair = reduce(second, second, third);
    InternalCalciteExec rightGrouped = reduce(first, first, secondPair);

    assertEquals(rows(leftGrouped), rows(rightGrouped));
    assertEquals(
        List.of(
            List.of("api", 4L),
            List.of("web", 2L),
            List.of("api", 6L),
            List.of("web", 3L),
            List.of("api", 1L),
            List.of("web", 5L)),
        rows(leftGrouped));
  }

  @Test
  void rowGatherPreservesNullCells() {
    InternalCalciteExec result =
        reduce(partial(row("api", null)), partial(row("api", null)), partial(row("web", 3L)));

    assertEquals(List.of(Arrays.asList("api", null), List.of("web", 3L)), rows(result));
  }

  @Test
  void ipCellsUseTheirTransportRepresentation() {
    Object[] input = row(new ExprIpValue("192.168.0.1"));
    InternalCalciteExec result = partial(input);

    assertEquals(List.of(List.of("192.168.0.1")), rows(result));
    assertEquals(new ExprIpValue("192.168.0.1"), input[0]);
  }

  private static InternalCalciteExec partial(Object[]... rows) {
    return new InternalCalciteExec(NAME, List.of(rows), METADATA);
  }

  private static InternalCalciteExec reduce(
      InternalCalciteExec receiver, InternalCalciteExec... values) {
    return (InternalCalciteExec) receiver.reduce(List.of(values), null);
  }

  private static Object[] row(Object... cells) {
    return cells;
  }

  private static List<List<Object>> rows(InternalCalciteExec result) {
    return result.rows().stream().map(Arrays::asList).toList();
  }
}
