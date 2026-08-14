/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

public class FragmentDeserializer {

  public static final String TABLE_NAME = "shard_source";

  public static RelNode deserialize(
      String base64Plan,
      List<String> fieldNames,
      List<SqlTypeName> fieldTypes,
      SchemaPlus rootSchema) {
    String json = new String(Base64.getDecoder().decode(base64Plan), StandardCharsets.UTF_8);

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Use VolcanoPlanner so the deserialized nodes can later be converted to Enumerable
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    for (RelOptRule rule : Bindables.RULES) {
      planner.addRule(rule);
    }
    planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(CoreRules.FILTER_CALC_MERGE);
    planner.addRule(CoreRules.PROJECT_CALC_MERGE);
    planner.addRule(CoreRules.CALC_MERGE);
    planner.addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema,
            calciteSchema.path(null),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties()));

    try {
      return new RelJsonReader(cluster, catalogReader, rootSchema.unwrap(SchemaPlus.class))
          .read(json);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize plan fragment", e);
    }
  }

  public static SchemaPlus buildSchema(List<String> fieldNames, List<SqlTypeName> fieldTypes) {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    rootSchema.add(TABLE_NAME, new DocValuesScannableTable(fieldNames, fieldTypes));
    return rootSchema;
  }
}
