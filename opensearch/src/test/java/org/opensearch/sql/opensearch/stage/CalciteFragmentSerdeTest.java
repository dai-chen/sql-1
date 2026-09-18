/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

class CalciteFragmentSerdeTest {

  private static final String TABLE = "_matching_rows_0";

  @Test
  void aggregateFragmentRoundTripsAndExecutes() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("service", SqlTypeName.VARCHAR)
            .add("bytes", SqlTypeName.BIGINT)
            .build();
    LogicalTableScan scan =
        CalciteFragmentSerde.tableScan(cluster(), inputType, TABLE, "test.rows");
    AggregateCall sum =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            "total");
    ImmutableBitSet groupSet = ImmutableBitSet.of(0);
    RelNode aggregate = LogicalAggregate.create(scan, groupSet, List.of(groupSet), List.of(sum));

    String json = CalciteFragmentSerde.serialize(aggregate);
    CalciteFragmentSerde.DecodedPlan decoded =
        CalciteFragmentSerde.deserialize(
            json,
            CalciteFragmentSerde.serializeType(inputType),
            TABLE,
            List.of(new Object[] {"api", 4L}, new Object[] {"web", 2L}, new Object[] {"api", 6L}));

    List<List<Object>> rows =
        EnumerableFragmentExecutor.execute(
                decoded.plan(), decoded.rootSchema(), decoded.dataContextValues())
            .stream()
            .map(List::of)
            .sorted(Comparator.comparing(row -> (String) row.getFirst()))
            .toList();
    assertEquals(List.of(List.of("api", 10L), List.of("web", 2L)), rows);
  }

  @Test
  void checkedLongSumRoundTripsAndRejectsOverflow() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY.builder().add("value", SqlTypeName.BIGINT).build();
    LogicalTableScan scan =
        CalciteFragmentSerde.tableScan(cluster(), inputType, TABLE, "test.rows");
    AggregateCall sum =
        AggregateCall.create(
            PPLBuiltinOperators.CHECKED_LONG_SUM,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            OpenSearchTypeFactory.TYPE_FACTORY.createTypeWithNullability(
                OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
            "total");
    RelNode aggregate =
        LogicalAggregate.create(
            scan, ImmutableBitSet.of(), List.of(ImmutableBitSet.of()), List.of(sum));

    CalciteFragmentSerde.DecodedPlan decoded =
        CalciteFragmentSerde.deserialize(
            CalciteFragmentSerde.serialize(aggregate),
            CalciteFragmentSerde.serializeType(inputType),
            TABLE,
            List.of(new Object[] {Long.MAX_VALUE}, new Object[] {1L}));

    assertThrows(
        ArithmeticException.class,
        () ->
            EnumerableFragmentExecutor.execute(
                decoded.plan(), decoded.rootSchema(), decoded.dataContextValues()));
  }

  @Test
  void pplArrayOperatorRoundTrips() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY.builder().add("value", SqlTypeName.INTEGER).build();
    RelOptCluster cluster = cluster();
    LogicalTableScan scan = CalciteFragmentSerde.tableScan(cluster, inputType, TABLE, "test.rows");
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                rexBuilder.makeCall(PPLBuiltinOperators.ARRAY, rexBuilder.makeInputRef(scan, 0))),
            List.of("values"));

    CalciteFragmentSerde.DecodedPlan decoded =
        CalciteFragmentSerde.deserialize(
            CalciteFragmentSerde.serialize(project),
            CalciteFragmentSerde.serializeType(inputType),
            TABLE,
            List.of());

    assertEquals(project.getRowType(), decoded.plan().getRowType());
  }

  @Test
  void registeredUdfAndUdtRoundTripWithoutTransportingClasses() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("address", SqlTypeName.VARCHAR)
            .add("unused", SqlTypeName.INTEGER)
            .build();
    RelOptCluster cluster = cluster();
    LogicalTableScan scan = CalciteFragmentSerde.tableScan(cluster, inputType, TABLE, "test.rows");
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                cluster
                    .getRexBuilder()
                    .makeCall(
                        PPLBuiltinOperators.IP, cluster.getRexBuilder().makeInputRef(scan, 0)),
                cluster.getRexBuilder().makeInputRef(scan, 1)),
            List.of("address", "unused"));

    String json = CalciteFragmentSerde.serialize(project);
    assertFalse(json.contains("\"class\""));
    CalciteFragmentSerde.DecodedPlan decoded =
        CalciteFragmentSerde.deserialize(
            json,
            CalciteFragmentSerde.serializeType(inputType),
            TABLE,
            List.<Object[]>of(new Object[] {"192.168.0.1", 1}));

    List<Object[]> rows =
        EnumerableFragmentExecutor.execute(
            decoded.plan(), decoded.rootSchema(), decoded.dataContextValues());
    assertEquals(new ExprIpValue("192.168.0.1"), rows.getFirst()[0]);
  }

  @Test
  void mappedIpInputExecutesAfterRoundTrip() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("address", OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.IP))
            .add("value", SqlTypeName.INTEGER)
            .build();
    RelOptCluster cluster = cluster();
    LogicalTableScan scan = CalciteFragmentSerde.tableScan(cluster, inputType, TABLE, "test.rows");
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelNode filter =
        LogicalFilter.create(
            scan,
            rexBuilder.makeCall(
                PPLBuiltinOperators.CIDRMATCH,
                rexBuilder.makeInputRef(scan, 0),
                rexBuilder.makeLiteral("192.168.0.0/16")));

    CalciteFragmentSerde.DecodedPlan decoded =
        CalciteFragmentSerde.deserialize(
            CalciteFragmentSerde.serialize(filter),
            CalciteFragmentSerde.serializeType(inputType),
            TABLE,
            List.of(new Object[] {"192.168.0.1", 1}, new Object[] {"10.0.0.1", 2}));

    List<Object[]> rows =
        EnumerableFragmentExecutor.execute(
            decoded.plan(), decoded.rootSchema(), decoded.dataContextValues());
    assertEquals(1, rows.size());
    assertEquals("192.168.0.1", rows.getFirst()[0]);
  }

  @Test
  void classBearingFragmentIsRejectedBeforeRelJsonReader() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY.builder().add("value", SqlTypeName.BIGINT).build();
    RelNode scan = CalciteFragmentSerde.tableScan(cluster(), inputType, TABLE, "test.rows");
    String json =
        CalciteFragmentSerde.serialize(scan)
            .replaceFirst("\"relOp\"", "\"class\":\"java.lang.Runtime\",\"relOp\"");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CalciteFragmentSerde.deserialize(
                    json, CalciteFragmentSerde.serializeType(inputType), TABLE, List.of()));
    assertEquals("Calcite shard fragment cannot name Java classes", error.getMessage());
  }

  @Test
  void requiredInputFieldsExcludeUnusedColumns() {
    RelDataType inputType =
        OpenSearchTypeFactory.TYPE_FACTORY
            .builder()
            .add("unused_0", SqlTypeName.VARCHAR)
            .add("service", SqlTypeName.VARCHAR)
            .add("unused_1", SqlTypeName.BIGINT)
            .add("unused_2", SqlTypeName.VARCHAR)
            .add("bytes", SqlTypeName.BIGINT)
            .build();
    RelOptCluster cluster = cluster();
    LogicalTableScan scan = CalciteFragmentSerde.tableScan(cluster, inputType, TABLE, "test.rows");
    RexBuilder rexBuilder = cluster.getRexBuilder();
    LogicalProject project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 1), rexBuilder.makeInputRef(scan, 4)),
            List.of("service", "bytes"));
    AggregateCall sum =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            "total");
    ImmutableBitSet groupSet = ImmutableBitSet.of(0);
    RelNode aggregate = LogicalAggregate.create(project, groupSet, List.of(groupSet), List.of(sum));

    BitSet expected = new BitSet();
    expected.set(1);
    expected.set(4);
    assertEquals(expected, CalciteFragmentSerde.requiredInputFields(aggregate, 5));
  }

  private static RelOptCluster cluster() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY));
  }
}
