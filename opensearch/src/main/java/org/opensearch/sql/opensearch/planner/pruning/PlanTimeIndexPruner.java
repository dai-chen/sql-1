/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.pruning;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.PlanTimeRewriter;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

/**
 * Prunes indices that cannot match a query's time range by rewriting the {@link Relation}'s index
 * expression before analysis, using {@code _field_caps} with an {@code index_filter} as a can_match
 * probe.
 *
 * <p>{@code _field_caps} does not surface per-index failures — {@code
 * TransportFieldCapabilitiesAction} swallows them — so an index whose shards all fail is
 * indistinguishable from one legitimately pruned. The step-6 guards reduce but do not eliminate
 * that exposure, which is why the feature is off by default.
 */
@Log4j2
public class PlanTimeIndexPruner implements PlanTimeRewriter {

  // Below this many matched indices a pruning round trip cannot save enough shards to be worth it.
  private static final int MIN_INDICES = 5;
  private static final Set<String> ALLOWED_TIME_TYPES = Set.of("date", "date_nanos");

  private final OpenSearchClient client;
  private final Settings settings;
  private final TimeRangeFilterExtractor extractor = new TimeRangeFilterExtractor();

  public PlanTimeIndexPruner(OpenSearchClient client, Settings settings) {
    this.client = client;
    this.settings = settings;
  }

  @Override
  public UnresolvedPlan rewrite(UnresolvedPlan plan) {
    try {
      if (!Boolean.TRUE.equals(settings.getSettingValue(Settings.Key.CALCITE_PRUNING_ENABLED))) {
        return plan;
      }
      Optional<QueryBuilder> extractedFilter = extractor.extract(plan);
      if (extractedFilter.isEmpty()) {
        return plan;
      }

      UnresolvedPlan parent = null;
      UnresolvedPlan current = plan;
      while (!(current instanceof Relation)) {
        List<? extends Node> children = current.getChild();
        if (children == null || children.size() != 1) {
          return plan;
        }
        parent = current;
        current = (UnresolvedPlan) children.get(0);
      }
      Relation relation = (Relation) current;

      // A datasource-qualified name has multiple parts joined by '.'; rewriting the flattened
      // string back into a single QualifiedName collapses that structure and corrupts resolution.
      if (relation.getTableQualifiedName().getParts().size() > 1) {
        return plan;
      }

      String origExpr = relation.getTableQualifiedName().toString();
      if (origExpr.contains(":") || !origExpr.contains("*")) {
        return plan;
      }
      String[] origParts = origExpr.split(",");

      FieldCapabilitiesResponse unfiltered =
          client.fieldCaps(
              new FieldCapabilitiesRequest()
                  .indices(origParts)
                  .fields(TimeRangeFilterExtractor.TIME_FIELD));
      String[] matched = unfiltered.getIndices();
      if (matched.length < MIN_INDICES) {
        return plan;
      }
      // can_match only proves range disjointness for date fields, so a non-date mapping could be
      // pruned under foreign comparison semantics.
      Map<String, FieldCapabilities> types =
          unfiltered.getField(TimeRangeFilterExtractor.TIME_FIELD);
      if (types == null || types.isEmpty() || !ALLOWED_TIME_TYPES.containsAll(types.keySet())) {
        return plan;
      }

      FieldCapabilitiesResponse filtered =
          client.fieldCaps(
              new FieldCapabilitiesRequest()
                  .indices(origParts)
                  .fields(TimeRangeFilterExtractor.TIME_FIELD)
                  .indexFilter(extractedFilter.get()));
      String[] survivors = filtered.getIndices();
      if (survivors.length == 0 || survivors.length >= matched.length) {
        return plan;
      }

      Set<String> excluded = new LinkedHashSet<>(Arrays.asList(matched));
      Arrays.asList(survivors).forEach(excluded::remove);

      // Exclude by name rather than list survivors, so an index created by a rollover between this
      // probe and the search is still included.
      StringBuilder rewritten = new StringBuilder(origExpr);
      for (String name : excluded) {
        rewritten.append(",-").append(name);
      }
      Relation pruned = new Relation(new QualifiedName(rewritten.toString()));

      log.debug(
          "Plan-time pruning excluded {} of {} matched indices", excluded.size(), matched.length);

      if (parent == null) {
        return pruned;
      }
      parent.attach(pruned);
      return plan;
    } catch (Exception e) {
      // Fail open: pruning is an optimization and must never change results or fail a query.
      log.warn("Plan-time index pruning failed; querying the full index expression", e);
      return plan;
    }
  }
}
