/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

class AggregateOutputScopeTest {

  private final UnresolvedExpression key = new QualifiedName("age");
  private AggregateOutputScope scope;
  private RelBuilder relBuilder;
  private RexInputRef column;

  @BeforeEach
  void setUp() {
    scope = new AggregateOutputScope();
    relBuilder = mock(RelBuilder.class);
    column = mock(RexInputRef.class);
    RelNode top = mock(RelNode.class);
    RelDataType rowType = mock(RelDataType.class);
    when(relBuilder.peek()).thenReturn(top);
    when(top.getRowType()).thenReturn(rowType);
    when(rowType.getFieldCount()).thenReturn(2);
  }

  @Test
  void unregisteredKeyFallsThrough() {
    assertNull(scope.lookupGroupKey(key, relBuilder));
  }

  @Test
  void registeredKeyResolvesToItsColumn() {
    when(relBuilder.field(1)).thenReturn(column);
    scope.registerGroupKey(key, 1);
    assertEquals(column, scope.lookupGroupKey(key, relBuilder));
  }

  @Test
  void enterDiscardsThePreviousScope() {
    scope.registerGroupKey(key, 1);
    scope.enter();
    assertNull(scope.lookupGroupKey(key, relBuilder));
  }

  /** An ordinal that outlived its Aggregate must be reported, never used as a column. */
  @Test
  void ordinalPastTheCurrentRowTypeIsRejected() {
    scope.registerGroupKey(key, 5);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> scope.lookupGroupKey(key, relBuilder));
    assertEquals(
        "Group-key scope outlived its Aggregate: age registered at output column 5 but the current"
            + " row type has only 2 column(s)",
        e.getMessage());
  }
}
