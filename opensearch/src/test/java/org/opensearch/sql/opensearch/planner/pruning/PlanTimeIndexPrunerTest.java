/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.pruning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class PlanTimeIndexPrunerTest {

  private static final long LOW = 1_600_000_000_000L;

  @Mock private OpenSearchClient client;
  @Mock private Settings settings;

  private PlanTimeIndexPruner pruner;

  @BeforeEach
  void setUp() {
    pruner = new PlanTimeIndexPruner(client, settings);
  }

  @Test
  void testSettingDisabledReturnsInputUnchanged() {
    when(settings.getSettingValue(Settings.Key.CALCITE_PRUNING_ENABLED)).thenReturn(false);
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
    verify(client, never()).fieldCaps(any());
  }

  @Test
  void testNoExtractableTimeFilterReturnsInputUnchanged() {
    enabled();
    UnresolvedPlan plan = relation("logs-*");
    assertSame(plan, pruner.rewrite(plan));
    verify(client, never()).fieldCaps(any());
  }

  @Test
  void testCrossClusterNameReturnsInputUnchanged() {
    enabled();
    UnresolvedPlan plan = timeFilterOver("c:logs-*");
    assertSame(plan, pruner.rewrite(plan));
    verify(client, never()).fieldCaps(any());
  }

  @Test
  void testNoWildcardReturnsInputUnchanged() {
    enabled();
    UnresolvedPlan plan = timeFilterOver("logs-2026.04.01");
    assertSame(plan, pruner.rewrite(plan));
    verify(client, never()).fieldCaps(any());
  }

  @Test
  void testDatasourceQualifiedNameReturnsInputUnchanged() {
    enabled();
    UnresolvedPlan plan =
        new Filter(bound()).attach(new Relation(QualifiedName.of("myds", "logs-*")));
    assertSame(plan, pruner.rewrite(plan));
    verify(client, never()).fieldCaps(any());
  }

  @Test
  void testMatchedBelowMinIndicesReturnsInputUnchanged() {
    enabled();
    when(client.fieldCaps(any())).thenReturn(response(indices("a", "b", "c"), dateTypes()));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
  }

  @Test
  void testNonDateTimeTypeReturnsInputUnchanged() {
    enabled();
    when(client.fieldCaps(any()))
        .thenReturn(response(indices("a", "b", "c", "d", "e"), keywordTypes()));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
  }

  @Test
  void testEmptySurvivorsReturnsInputUnchanged() {
    enabled();
    when(client.fieldCaps(any()))
        .thenReturn(
            response(indices("a", "b", "c", "d", "e"), dateTypes()),
            response(indices(), dateTypes()));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
  }

  @Test
  void testAllSurviveReturnsInputUnchanged() {
    enabled();
    String[] all = indices("a", "b", "c", "d", "e");
    when(client.fieldCaps(any()))
        .thenReturn(response(all, dateTypes()), response(all, dateTypes()));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
  }

  @Test
  void testHappyPathExcludesPrunedIndicesByName() {
    enabled();
    when(client.fieldCaps(any()))
        .thenReturn(
            response(
                indices("logs-1", "logs-2", "logs-3", "logs-4", "logs-5", "logs-6"), dateTypes()),
            response(indices("logs-1", "logs-2"), dateTypes()));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertEquals(
        "logs-*,-logs-3,-logs-4,-logs-5,-logs-6",
        relationOf(pruner.rewrite(plan)).getTableQualifiedName().toString());
  }

  @Test
  void testFieldCapsFailureFailsOpen() {
    enabled();
    when(client.fieldCaps(any())).thenThrow(new IllegalStateException("boom"));
    UnresolvedPlan plan = timeFilterOver("logs-*");
    assertSame(plan, pruner.rewrite(plan));
  }

  private void enabled() {
    when(settings.getSettingValue(Settings.Key.CALCITE_PRUNING_ENABLED)).thenReturn(true);
  }

  private static UnresolvedPlan timeFilterOver(String index) {
    return new Filter(bound()).attach(relation(index));
  }

  private static Relation relation(String index) {
    return new Relation(new QualifiedName(index));
  }

  private static Compare bound() {
    return new Compare(
        ">=",
        new Field(QualifiedName.of(TimeRangeFilterExtractor.TIME_FIELD)),
        new Literal(LOW, DataType.LONG));
  }

  private static Relation relationOf(UnresolvedPlan plan) {
    Node node = plan;
    while (!(node instanceof Relation)) {
      node = node.getChild().get(0);
    }
    return (Relation) node;
  }

  private static String[] indices(String... names) {
    return names;
  }

  private static Map<String, FieldCapabilities> dateTypes() {
    return Map.of("date", mock(FieldCapabilities.class));
  }

  private static Map<String, FieldCapabilities> keywordTypes() {
    return Map.of("keyword", mock(FieldCapabilities.class));
  }

  private static FieldCapabilitiesResponse response(
      String[] indices, Map<String, FieldCapabilities> field) {
    return new FieldCapabilitiesResponse(
        indices, Map.of(TimeRangeFilterExtractor.TIME_FIELD, field));
  }
}
