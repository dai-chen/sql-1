/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/** Defines the validated RelJson wire format and input-row contract for a shard fragment. */
final class CalciteFragmentSerde {

  static final String SCHEMA_NAME = "OpenSearch";
  static final String MATCHING_ROWS_KEY = "calcite_exec.matching_rows";

  private static final int MAX_FRAGMENT_BYTES = 1 << 20;
  private static final int MAX_REL_NODES = 128;
  private static final Set<String> ALLOWED_REL_OPS =
      Set.of(
          "LogicalTableScan", "LogicalFilter", "LogicalProject", "LogicalCalc", "LogicalAggregate");
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<>() {};

  private CalciteFragmentSerde() {}

  static String serialize(RelNode fragment) {
    JsonBuilder jsonBuilder = new JsonBuilder();
    RelJsonWriter writer = new RelJsonWriter(jsonBuilder, RelJsonSerializer::configureRelJson);
    fragment.explain(writer);
    LinkedHashMap<String, Object> json = MAPPER.readValue(writer.asString(), MAP_TYPE);
    removeClassFields(json);
    validate(json, null);
    return MAPPER.writeValueAsString(json);
  }

  static String serializeType(RelDataType rowType) {
    JsonBuilder jsonBuilder = new JsonBuilder();
    Object jsonType = RelJsonSerializer.createRelJson(jsonBuilder).toJson(rowType);
    return jsonBuilder.toJsonString(jsonType);
  }

  static DecodedPlan deserialize(
      String fragmentJson, String rowTypeJson, String tableName, Object matchingRows) {
    validateSerializedFragment(fragmentJson, tableName);
    if (rowTypeJson.length() > MAX_FRAGMENT_BYTES) {
      throw new IllegalArgumentException("Calcite shard row type exceeds size limit");
    }
    try {
      RelDataType rowType =
          RelJsonSerializer.createRelJson(null)
              .toType(OpenSearchTypeFactory.TYPE_FACTORY, MAPPER.readValue(rowTypeJson, MAP_TYPE));
      SchemaPlus root = rowsSchema(rowType, tableName, MATCHING_ROWS_KEY);
      VolcanoPlanner planner = new VolcanoPlanner();
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      RelOptCluster cluster =
          RelOptCluster.create(planner, new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY));
      RelJsonReader reader =
          new RelJsonReader(
              cluster, catalogReader(root), root, RelJsonSerializer::configureRelJson);
      RelNode plan = reader.read(fragmentJson);
      return new DecodedPlan(plan, root, Map.of(MATCHING_ROWS_KEY, matchingRows));
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to deserialize Calcite shard fragment", e);
    }
  }

  static LogicalTableScan tableScan(
      RelOptCluster cluster, RelDataType rowType, String tableName, String dataKey) {
    SchemaPlus root = rowsSchema(rowType, tableName, dataKey);
    RelOptTable table = catalogReader(root).getTableForMember(List.of(SCHEMA_NAME, tableName));
    return LogicalTableScan.create(cluster, table, List.of());
  }

  private static SchemaPlus rowsSchema(RelDataType rowType, String tableName, String dataKey) {
    SchemaPlus root = Frameworks.createRootSchema(false);
    SchemaPlus schema = root.add(SCHEMA_NAME, new AbstractSchema() {});
    schema.add(tableName, new CalciteRowsTable(rowType, dataKey));
    return root;
  }

  private static CalciteCatalogReader catalogReader(SchemaPlus root) {
    return new CalciteCatalogReader(
        CalciteSchema.from(root),
        List.of(),
        OpenSearchTypeFactory.TYPE_FACTORY,
        new CalciteConnectionConfigImpl(new Properties()));
  }

  static BitSet requiredInputFields(RelNode plan, int inputFieldCount) {
    BitSet requiredOutput = new BitSet(plan.getRowType().getFieldCount());
    requiredOutput.set(0, plan.getRowType().getFieldCount());
    BitSet requiredInput = new BitSet(inputFieldCount);
    collectRequiredInputFields(plan, requiredOutput, requiredInput);
    if (requiredInput.length() > inputFieldCount) {
      requiredInput.clear(inputFieldCount, requiredInput.length());
    }
    return requiredInput;
  }

  private static void collectRequiredInputFields(
      RelNode node, BitSet requiredOutput, BitSet requiredInput) {
    if (node instanceof LogicalTableScan) {
      requiredInput.or(requiredOutput);
      return;
    }
    if (node instanceof LogicalProject project) {
      BitSet childRequired = new BitSet(project.getInput().getRowType().getFieldCount());
      for (int output = requiredOutput.nextSetBit(0);
          output >= 0;
          output = requiredOutput.nextSetBit(output + 1)) {
        if (output < project.getProjects().size()) {
          addInputRefs(project.getProjects().get(output), childRequired);
        }
      }
      collectRequiredInputFields(project.getInput(), childRequired, requiredInput);
      return;
    }
    if (node instanceof LogicalFilter filter) {
      BitSet childRequired = (BitSet) requiredOutput.clone();
      addInputRefs(filter.getCondition(), childRequired);
      collectRequiredInputFields(filter.getInput(), childRequired, requiredInput);
      return;
    }
    if (node instanceof LogicalCalc calc) {
      BitSet childRequired = new BitSet(calc.getInput().getRowType().getFieldCount());
      for (int output = requiredOutput.nextSetBit(0);
          output >= 0;
          output = requiredOutput.nextSetBit(output + 1)) {
        if (output < calc.getProgram().getProjectList().size()) {
          addInputRefs(
              calc.getProgram().expandLocalRef(calc.getProgram().getProjectList().get(output)),
              childRequired);
        }
      }
      if (calc.getProgram().getCondition() != null) {
        addInputRefs(
            calc.getProgram().expandLocalRef(calc.getProgram().getCondition()), childRequired);
      }
      collectRequiredInputFields(calc.getInput(), childRequired, requiredInput);
      return;
    }
    if (node instanceof LogicalAggregate aggregate) {
      BitSet childRequired = new BitSet(aggregate.getInput().getRowType().getFieldCount());
      List<Integer> groups = aggregate.getGroupSet().asList();
      for (int output = requiredOutput.nextSetBit(0);
          output >= 0;
          output = requiredOutput.nextSetBit(output + 1)) {
        if (output < aggregate.getGroupCount()) {
          childRequired.set(groups.get(output));
        } else {
          int callIndex = output - aggregate.getGroupCount();
          if (callIndex < aggregate.getAggCallList().size()) {
            addAggregateInputRefs(aggregate.getAggCallList().get(callIndex), childRequired);
          }
        }
      }
      collectRequiredInputFields(aggregate.getInput(), childRequired, requiredInput);
      return;
    }
    for (RelNode input : node.getInputs()) {
      BitSet allFields = new BitSet(input.getRowType().getFieldCount());
      allFields.set(0, input.getRowType().getFieldCount());
      collectRequiredInputFields(input, allFields, requiredInput);
    }
  }

  private static void addInputRefs(RexNode expression, BitSet required) {
    RelOptUtil.InputFinder.bits(expression).forEach(required::set);
  }

  private static void addAggregateInputRefs(AggregateCall call, BitSet required) {
    call.getArgList().forEach(required::set);
    call.rexList.forEach(expression -> addInputRefs(expression, required));
    if (call.filterArg >= 0) {
      required.set(call.filterArg);
    }
  }

  private static void validateSerializedFragment(String json, String expectedTable) {
    if (json.length() > MAX_FRAGMENT_BYTES) {
      throw new IllegalArgumentException("Calcite shard fragment exceeds size limit");
    }
    validate(MAPPER.readValue(json, MAP_TYPE), expectedTable);
  }

  private static void validate(Map<String, Object> root, String expectedTable) {
    Object relsValue = root.get("rels");
    if (!(relsValue instanceof List<?> rels) || rels.isEmpty() || rels.size() > MAX_REL_NODES) {
      throw new IllegalArgumentException("Invalid Calcite shard fragment relation list");
    }
    for (Object value : rels) {
      if (!(value instanceof Map<?, ?> rel)) {
        throw new IllegalArgumentException("Invalid Calcite shard fragment relation");
      }
      Object relOp = rel.get("relOp");
      if (!(relOp instanceof String name) || !ALLOWED_REL_OPS.contains(name)) {
        throw new IllegalArgumentException("Unsupported Calcite shard relOp [" + relOp + "]");
      }
      if ("LogicalTableScan".equals(name) && expectedTable != null) {
        Object table = rel.get("table");
        if (!(table instanceof List<?> path) || !path.equals(List.of(SCHEMA_NAME, expectedTable))) {
          throw new IllegalArgumentException("Unexpected Calcite shard table " + table);
        }
      }
    }
    rejectClassFields(root);
  }

  private static void removeClassFields(Object value) {
    if (value instanceof Map<?, ?> map) {
      map.keySet().removeIf("class"::equals);
      map.values().forEach(CalciteFragmentSerde::removeClassFields);
    } else if (value instanceof List<?> list) {
      list.forEach(CalciteFragmentSerde::removeClassFields);
    }
  }

  private static void rejectClassFields(Object value) {
    if (value instanceof Map<?, ?> map) {
      if (map.containsKey("class")) {
        throw new IllegalArgumentException("Calcite shard fragment cannot name Java classes");
      }
      map.values().forEach(CalciteFragmentSerde::rejectClassFields);
    } else if (value instanceof List<?> list) {
      list.forEach(CalciteFragmentSerde::rejectClassFields);
    }
  }

  record DecodedPlan(RelNode plan, SchemaPlus rootSchema, Map<String, Object> dataContextValues) {}
}
