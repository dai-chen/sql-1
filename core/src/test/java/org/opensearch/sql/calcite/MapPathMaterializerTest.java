/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.sql.ast.dsl.AstDSL.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.*;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.QueryType;

/**
 * Unit tests for {@link MapPathMaterializer}.
 *
 * <p>Input schema: {@code [id INTEGER, doc MAP<VARCHAR, ANY>]}. When a command references a dotted
 * path like {@code doc.user.name}, the materializer should inject a {@link LogicalProject} that
 * adds {@code ITEM($1, 'user.name')} as a flat column named {@code doc.user.name}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MapPathMaterializerTest {

  @Mock DataSourceService dataSourceService;
  @Mock RelOptCluster cluster;
  @Mock RelOptPlanner planner;
  @Mock RelMetadataQuery mq;
  @Mock RelNode input;
  @Mock Connection connection;
  @Mock FrameworkConfig frameworkConfig;

  RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  OpenSearchRelBuilder relBuilder;
  CalcitePlanContext context;
  MapPathMaterializer materializer;
  MockedStatic<CalciteToolsHelper> mockedStatic;
  RelDataType mapType;

  @BeforeEach
  public void setUp() {
    when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    when(mq.isVisibleInExplain(any(), any())).thenReturn(true);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(cluster.traitSet()).thenReturn(RelTraitSet.createEmpty());
    when(cluster.traitSetOf(Convention.NONE))
        .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
    when(cluster.getPlanner()).thenReturn(planner);
    when(planner.getExecutor()).thenReturn(null);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    mapType =
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createTypeWithNullability(
                TYPE_FACTORY.createSqlType(SqlTypeName.ANY), true));
    RelDataType inputRowType =
        TYPE_FACTORY.createStructType(List.of(intType, mapType), List.of("id", "doc"));
    when(input.getCluster()).thenReturn(cluster);
    when(input.getRowType()).thenReturn(inputRowType);

    relBuilder = new OpenSearchRelBuilder(null, cluster, null);
    mockedStatic = Mockito.mockStatic(CalciteToolsHelper.class);
    mockedStatic.when(() -> CalciteToolsHelper.connect(any(), any())).thenReturn(connection);
    mockedStatic.when(() -> CalciteToolsHelper.create(any(), any(), any())).thenReturn(relBuilder);
    context = CalcitePlanContext.create(frameworkConfig, SysLimit.DEFAULT, QueryType.PPL);

    // Spy rexVisitor: resolves "doc.x.y" to real ITEM($1, 'x.y') RexCall
    CalciteRexNodeVisitor spyRexVisitor =
        spy(new CalciteRexNodeVisitor(new CalciteRelNodeVisitor(dataSourceService)));
    doAnswer(
            inv -> {
              UnresolvedExpression expr = inv.getArgument(0);
              if (expr instanceof Field f) {
                String path = f.getField().toString();
                String nested = path.contains(".") ? path.substring(path.indexOf('.') + 1) : path;
                return rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM,
                    rexBuilder.makeInputRef(mapType, 1),
                    rexBuilder.makeLiteral(nested));
              }
              throw new IllegalArgumentException("Cannot resolve: " + expr);
            })
        .when(spyRexVisitor)
        .analyze(any(UnresolvedExpression.class), any(CalcitePlanContext.class));
    materializer = new MapPathMaterializer(spyRexVisitor);
  }

  @AfterEach
  public void tearDown() {
    mockedStatic.close();
  }

  private static Field field(String name) {
    return new Field(QualifiedName.of(name));
  }

  /** Dummy child plan for DSL methods that require an input. */
  private static final UnresolvedPlan DUMMY_CHILD = new Relation(QualifiedName.of("dummy"));

  /** Asserts the relBuilder top is a LogicalProject matching the expected explain string. */
  private void assertProjectEquals(String expected, RelNode actual) {
    assertInstanceOf(LogicalProject.class, actual, "Expected LogicalProject");
    assertEquals(expected, actual.explain().replaceAll("\\r\\n", "\n"));
  }

  // ---- No materialization ----

  @Test
  public void testNonUnresolvedPlanPassesThrough() {
    relBuilder.push(input);
    assertSame(
        input,
        materializer.materializePaths(input, mock(org.opensearch.sql.ast.Node.class), context));
  }

  @Test
  public void testExpressionBasedCommandPassesThrough() {
    // Filter resolves fields via expressions, not by name — no materialization needed
    relBuilder.push(input);
    assertSame(input, materializer.materializePaths(input, new Filter(field("x")), context));
  }

  @Test
  public void testProjectNotExcludedSkipped() {
    // Non-excluded Project resolves fields as expressions, not by name
    relBuilder.push(input);
    assertSame(
        input,
        materializer.materializePaths(
            input, new Project(List.of(field("doc.user.name"))), context));
  }

  @Test
  public void testStreamWindowNullGroupListSkipped() {
    relBuilder.push(input);
    assertSame(
        input,
        materializer.materializePaths(
            input,
            new StreamWindow(emptyList(), null, false, 10, false, false, null, null),
            context));
  }

  @Test
  public void testFieldAlreadyInSchemaIsSkipped() {
    // Schema already has "doc.user.name" — no materialization needed
    RelDataType rowWithField =
        TYPE_FACTORY.createStructType(
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                mapType,
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
            List.of("id", "doc", "doc.user.name"));
    RelNode inputWithField = mock(RelNode.class);
    when(inputWithField.getCluster()).thenReturn(cluster);
    when(inputWithField.getRowType()).thenReturn(rowWithField);
    relBuilder.push(inputWithField);

    assertSame(
        inputWithField,
        materializer.materializePaths(
            inputWithField,
            new Replace(
                List.of(new ReplacePair(stringLiteral("a"), stringLiteral("b"))),
                Set.of(field("doc.user.name"))),
            context));
  }

  @Test
  public void testFieldNotResolvableIsSkipped() {
    // rexVisitor throws — materialization silently skipped, command handles the error
    relBuilder.push(input);
    CalciteRexNodeVisitor failingVisitor =
        spy(new CalciteRexNodeVisitor(new CalciteRelNodeVisitor(dataSourceService)));
    doThrow(new IllegalArgumentException("Field not found"))
        .when(failingVisitor)
        .analyze(any(UnresolvedExpression.class), any(CalcitePlanContext.class));

    assertSame(
        input,
        new MapPathMaterializer(failingVisitor)
            .materializePaths(
                input,
                new RareTopN(
                    RareTopN.CommandType.TOP,
                    2,
                    emptyList(),
                    List.of(field("nonexistent.path")),
                    emptyList()),
                context));
  }

  // ---- Materialization per command ----
  // Each command that does symbol-based field matching should trigger materialization
  // when a dotted MAP path is referenced. The expected project is always:
  //   LogicalProject(id=[$0], doc=[$1], <path>=[ITEM($1, '<nested>')])

  static Stream<Arguments> singleFieldCommands() {
    return Stream.of(
        // Category A: field name matching
        Arguments.of(
            "rename doc.user.name as username",
            rename(DUMMY_CHILD, map("doc.user.name", "username"))),
        Arguments.of(
            "fillnull using doc.user.name = 'N/A'",
            fillNull(DUMMY_CHILD, List.of(Pair.of(field("doc.user.name"), stringLiteral("N/A"))))),
        Arguments.of(
            "replace 'a' WITH 'b' IN doc.user.name",
            new Replace(
                List.of(new ReplacePair(stringLiteral("a"), stringLiteral("b"))),
                Set.of(field("doc.user.name")))),
        Arguments.of(
            "fields - doc.user.name",
            projectWithArg(
                DUMMY_CHILD,
                List.of(argument("exclude", booleanLiteral(true))),
                field("doc.user.name"))),
        Arguments.of(
            "lookup ... doc.user.name",
            new Lookup(
                null,
                java.util.Map.of("name", "doc.user.name"),
                Lookup.OutputStrategy.APPEND,
                java.util.Map.of())),
        Arguments.of("mvcombine doc.user.name", mvcombine(field("doc.user.name"))),
        // Category B: projection naming
        Arguments.of(
            "top 2 doc.user.name",
            rareTopN(
                DUMMY_CHILD,
                RareTopN.CommandType.TOP,
                List.of(argument("noOfResults", intLiteral(2))),
                emptyList(),
                field("doc.user.name"))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("singleFieldCommands")
  public void testMaterializesDocUserName(String description, UnresolvedPlan command) {
    relBuilder.push(input);
    materializer.materializePaths(input, command, context);
    assertProjectEquals(
        "LogicalProject(id=[$0], doc=[$1], doc.user.name=[ITEM($1, 'user.name')])\n",
        relBuilder.peek());
  }

  static Stream<Arguments> otherPathCommands() {
    return Stream.of(
        Arguments.of(
            "flatten doc.user",
            new Flatten(field("doc.user"), emptyList()),
            "LogicalProject(id=[$0], doc=[$1], doc.user=[ITEM($1, 'user')])\n"),
        Arguments.of(
            "expand doc.tags",
            new Expand(field("doc.tags"), null),
            "LogicalProject(id=[$0], doc=[$1], doc.tags=[ITEM($1, 'tags')])\n"),
        Arguments.of(
            "addtotals doc.user.age",
            new AddTotals(List.of(field("doc.user.age")), java.util.Map.of()),
            "LogicalProject(id=[$0], doc=[$1], doc.user.age=[ITEM($1, 'user.age')])\n"),
        Arguments.of(
            "addcoltotals doc.user.age",
            new AddColTotals(List.of(field("doc.user.age")), java.util.Map.of()),
            "LogicalProject(id=[$0], doc=[$1], doc.user.age=[ITEM($1, 'user.age')])\n"),
        Arguments.of(
            "streamstats ... by doc.user.city",
            new StreamWindow(
                emptyList(), List.of(field("doc.user.city")), false, 10, false, false, null, null),
            "LogicalProject(id=[$0], doc=[$1], doc.user.city=[ITEM($1, 'user.city')])\n"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("otherPathCommands")
  public void testMaterializesOtherPaths(
      String description, UnresolvedPlan command, String expectedProject) {
    relBuilder.push(input);
    materializer.materializePaths(input, command, context);
    assertProjectEquals(expectedProject, relBuilder.peek());
  }

  @Test
  public void testRareTopNWithGroupByMaterializesBothFields() {
    // rare doc.user.name by doc.user.city → both field and group-by materialized
    relBuilder.push(input);
    materializer.materializePaths(
        input,
        rareTopN(
            DUMMY_CHILD,
            RareTopN.CommandType.RARE,
            defaultTopArgs(),
            List.of(field("doc.user.city")),
            field("doc.user.name")),
        context);
    assertProjectEquals(
        "LogicalProject(id=[$0], doc=[$1], doc.user.name=[ITEM($1, 'user.name')],"
            + " doc.user.city=[ITEM($1, 'user.city')])\n",
        relBuilder.peek());
  }
}
