/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.pruning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.opensearch.planner.pruning.TimeRangeFilterExtractor.SLOP_MILLIS;
import static org.opensearch.sql.opensearch.planner.pruning.TimeRangeFilterExtractor.TIME_FIELD;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Union;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

class TimeRangeFilterExtractorTest {

  private static final long LOW = 1_600_000_000_000L;
  private static final long HIGH = 1_700_000_000_000L;

  private final TimeRangeFilterExtractor extractor = new TimeRangeFilterExtractor();

  @Test
  void testSingleLowerBound() {
    UnresolvedPlan plan = filter(compare(">=", timeField(), longLit(LOW)), relation());
    assertEquals(boolFilter(range().gte(LOW - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testTwoStackedFilters() {
    UnresolvedPlan plan =
        filter(
            compare(">=", timeField(), longLit(LOW)),
            filter(compare("<", timeField(), longLit(HIGH)), relation()));
    assertEquals(
        boolFilter(range().gte(LOW - SLOP_MILLIS), range().lt(HIGH + SLOP_MILLIS)),
        extractor.extract(plan));
  }

  @Test
  void testAndDropsNonTimePredicate() {
    UnresolvedPlan plan =
        filter(
            new And(
                compare(">=", timeField(), longLit(LOW)),
                compare(">", field("status"), longLit(5))),
            relation());
    assertEquals(boolFilter(range().gte(LOW - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testSqlFunctionComparison() {
    UnresolvedPlan plan = filter(new Function(">", List.of(timeField(), longLit(LOW))), relation());
    assertEquals(boolFilter(range().gt(LOW - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testFilterBelowAggregation() {
    UnresolvedPlan plan = aggregation(filter(compare(">=", timeField(), longLit(LOW)), relation()));
    assertEquals(boolFilter(range().gte(LOW - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testIsoTimestampLiteral() {
    UnresolvedPlan plan =
        filter(compare(">=", timeField(), stringLit("2026-04-03T08:00:00Z")), relation());
    long millis = Instant.parse("2026-04-03T08:00:00Z").toEpochMilli();
    assertEquals(boolFilter(range().gte(millis - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testWidenLowerBoundBySlop() {
    UnresolvedPlan plan = filter(compare(">=", timeField(), longLit(LOW)), relation());
    assertEquals(boolFilter(range().gte(LOW - SLOP_MILLIS)), extractor.extract(plan));
  }

  @Test
  void testBoundUnderOr() {
    UnresolvedPlan plan =
        filter(
            new Or(
                compare(">=", timeField(), longLit(LOW)),
                compare("<", field("status"), longLit(5))),
            relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testBoundUnderNot() {
    UnresolvedPlan plan = filter(new Not(compare(">=", timeField(), longLit(LOW))), relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testNonTimestampField() {
    UnresolvedPlan plan = filter(compare(">=", field("created"), longLit(LOW)), relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testFilterAboveAggregation() {
    UnresolvedPlan plan = filter(compare(">=", timeField(), longLit(LOW)), aggregation(relation()));
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testFunctionWrappedLhs() {
    UnresolvedPlan plan =
        filter(compare(">=", new Function("abs", List.of(timeField())), longLit(LOW)), relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testNonIsoStringLiteral() {
    UnresolvedPlan plan = filter(compare(">=", timeField(), stringLit("03/04/2026")), relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testFieldToFieldComparison() {
    UnresolvedPlan plan = filter(compare(">=", timeField(), field("ingested_at")), relation());
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testMultiRelationPlan() {
    UnresolvedPlan plan =
        filter(
            compare(">=", timeField(), longLit(LOW)), new Union(List.of(relation(), relation())));
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testTwoStackedBoundariesClearAllFilters() {
    UnresolvedPlan plan =
        filter(
            compare(">=", timeField(), longLit(HIGH)),
            eval(filter(compare(">=", timeField(), longLit(LOW)), eval(relation()))));
    assertEquals(Optional.empty(), extractor.extract(plan));
  }

  @Test
  void testUpperBoundOverflowSaturatesToMax() {
    UnresolvedPlan plan = filter(compare("<=", timeField(), longLit(Long.MAX_VALUE)), relation());
    assertEquals(boolFilter(range().lte(Long.MAX_VALUE)), extractor.extract(plan));
  }

  private static Relation relation() {
    return new Relation(QualifiedName.of("events"));
  }

  private static UnresolvedPlan filter(UnresolvedExpression condition, UnresolvedPlan child) {
    return new Filter(condition).attach(child);
  }

  private static UnresolvedPlan eval(UnresolvedPlan child) {
    return new Eval(List.of()).attach(child);
  }

  private static UnresolvedPlan aggregation(UnresolvedPlan child) {
    return new Aggregation(List.of(), List.of(), List.of()).attach(child);
  }

  private static Compare compare(String op, UnresolvedExpression left, UnresolvedExpression right) {
    return new Compare(op, left, right);
  }

  private static Field timeField() {
    return field(TIME_FIELD);
  }

  private static Field field(String name) {
    return new Field(QualifiedName.of(name));
  }

  private static Literal longLit(long value) {
    return new Literal(value, DataType.LONG);
  }

  private static Literal stringLit(String value) {
    return new Literal(value, DataType.STRING);
  }

  private static RangeQueryBuilder range() {
    return QueryBuilders.rangeQuery(TIME_FIELD);
  }

  private static Optional<QueryBuilder> boolFilter(RangeQueryBuilder... ranges) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    for (RangeQueryBuilder range : ranges) {
      bool.filter(range);
    }
    return Optional.of(bool);
  }
}
