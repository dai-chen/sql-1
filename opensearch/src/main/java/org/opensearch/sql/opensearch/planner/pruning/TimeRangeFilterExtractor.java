/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.pruning;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.Convert;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Union;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;

/**
 * Derives a deliberately loose superset of a query's {@code @timestamp} range from an unresolved
 * plan, for use as an {@code index_filter} on {@code _field_caps}. A wider filter is always safe; a
 * narrower one silently drops data, so anything uncertain contributes nothing.
 */
public class TimeRangeFilterExtractor extends AbstractNodeVisitor<List<RangeQueryBuilder>, Void> {

  public static final String TIME_FIELD = "@timestamp";
  public static final long SLOP_MILLIS = Duration.ofDays(1).toMillis();

  private static final Map<String, String> OPERATORS =
      Map.of(">", "gt", ">=", "gte", "<", "lt", "<=", "lte");

  /** Returns empty when no safe filter can be derived. */
  public Optional<QueryBuilder> extract(UnresolvedPlan plan) {
    List<Filter> filters = new ArrayList<>();
    if (!collectFilters(plan, filters)) {
      return Optional.empty();
    }
    List<RangeQueryBuilder> fragments = new ArrayList<>();
    for (Filter filter : filters) {
      fragments.addAll(filter.getCondition().accept(this, null));
    }
    if (fragments.isEmpty()) {
      return Optional.empty();
    }
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    fragments.forEach(bool::filter);
    return Optional.of(bool);
  }

  private boolean collectFilters(UnresolvedPlan root, List<Filter> filters) {
    Node node = root;
    boolean relationSeen = false;
    while (node != null) {
      if (isMultiRelation(node)) {
        return false;
      }
      List<? extends Node> children = node.getChild();
      int childCount = children == null ? 0 : children.size();
      if (childCount > 1) {
        return false;
      }
      if (node instanceof Relation) {
        relationSeen = true;
      } else if (node instanceof Filter) {
        filters.add((Filter) node);
      } else if (isBoundary(node)) {
        // Only filters below the lowest boundary act on raw source fields; a filter above any
        // boundary may reference a re-derived field, so every boundary discards what came higher.
        filters.clear();
      }
      node = childCount == 1 ? children.get(0) : null;
    }
    return relationSeen;
  }

  private boolean isMultiRelation(Node node) {
    return node instanceof Join
        || node instanceof Union
        || node instanceof Append
        || node instanceof AppendCol
        || node instanceof Lookup
        || node instanceof SubqueryAlias;
  }

  private boolean isBoundary(Node node) {
    return node instanceof Aggregation
        || node instanceof Eval
        || node instanceof Window
        || node instanceof Chart
        || node instanceof RareTopN
        || node instanceof Parse
        || node instanceof Rex
        || node instanceof Bin
        || node instanceof Convert;
  }

  @Override
  public List<RangeQueryBuilder> visitChildren(Node node, Void context) {
    return Collections.emptyList();
  }

  @Override
  public List<RangeQueryBuilder> visitAnd(And node, Void context) {
    List<RangeQueryBuilder> bounds = new ArrayList<>(node.getLeft().accept(this, context));
    bounds.addAll(node.getRight().accept(this, context));
    return bounds;
  }

  @Override
  public List<RangeQueryBuilder> visitCompare(Compare node, Void context) {
    return bound(node.getOperator(), node.getLeft(), node.getRight());
  }

  @Override
  public List<RangeQueryBuilder> visitFunction(Function node, Void context) {
    List<UnresolvedExpression> args = node.getFuncArgs();
    if (args.size() == 2 && OPERATORS.containsKey(node.getFuncName())) {
      return bound(node.getFuncName(), args.get(0), args.get(1));
    }
    return Collections.emptyList();
  }

  private List<RangeQueryBuilder> bound(
      String operator, UnresolvedExpression left, UnresolvedExpression right) {
    String relation = OPERATORS.get(operator);
    if (relation == null || !isTimeField(left) || !(right instanceof Literal)) {
      return Collections.emptyList();
    }
    Optional<Long> millis = toEpochMillis((Literal) right);
    if (millis.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(widened(relation, millis.get()));
  }

  private boolean isTimeField(UnresolvedExpression expr) {
    UnresolvedExpression name = expr instanceof Field ? ((Field) expr).getField() : expr;
    return name instanceof QualifiedName && name.toString().equals(TIME_FIELD);
  }

  private RangeQueryBuilder widened(String relation, long millis) {
    RangeQueryBuilder range = QueryBuilders.rangeQuery(TIME_FIELD);
    // The engine may apply a session time zone to a zone-less literal, so the plan-time bound must
    // be provably wider than whatever the engine later evaluates.
    boolean lower = relation.equals("gt") || relation.equals("gte");
    // Saturate on overflow: both extremes are maximally wide, which is always safe for a superset.
    long bound =
        lower ? subtractSaturating(millis, SLOP_MILLIS) : addSaturating(millis, SLOP_MILLIS);
    switch (relation) {
      case "gt":
        return range.gt(bound);
      case "gte":
        return range.gte(bound);
      case "lt":
        return range.lt(bound);
      default:
        return range.lte(bound);
    }
  }

  private static long addSaturating(long value, long delta) {
    try {
      return Math.addExact(value, delta);
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  private static long subtractSaturating(long value, long delta) {
    try {
      return Math.subtractExact(value, delta);
    } catch (ArithmeticException underflow) {
      return Long.MIN_VALUE;
    }
  }

  private Optional<Long> toEpochMillis(Literal literal) {
    Object value = literal.getValue();
    if (value instanceof Integer || value instanceof Long) {
      return Optional.of(((Number) value).longValue());
    }
    if (value instanceof String) {
      return parseIso((String) value);
    }
    return Optional.empty();
  }

  private Optional<Long> parseIso(String text) {
    try {
      return Optional.of(Instant.parse(text).toEpochMilli());
    } catch (RuntimeException ignored) {
      // fall through
    }
    try {
      return Optional.of(OffsetDateTime.parse(text).toInstant().toEpochMilli());
    } catch (RuntimeException ignored) {
      // fall through
    }
    String normalized = text.contains(" ") ? text.replace(' ', 'T') : text;
    try {
      return Optional.of(LocalDateTime.parse(normalized).toInstant(ZoneOffset.UTC).toEpochMilli());
    } catch (RuntimeException ignored) {
      // fall through
    }
    try {
      return Optional.of(
          LocalDate.parse(normalized).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
    } catch (RuntimeException ignored) {
      return Optional.empty();
    }
  }
}
