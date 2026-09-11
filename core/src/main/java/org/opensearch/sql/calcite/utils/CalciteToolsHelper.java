/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Calcite project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.calcite.utils;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRules;
import org.opensearch.sql.calcite.plan.rule.PPLSimplifyDedupRule;
import org.opensearch.sql.calcite.profile.PlanProfileBuilder;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.monitor.profile.QueryProfiling;

/**
 * Calcite Tools Helper. This class is used to create customized: 1. Connection 2. JavaTypeFactory
 * 3. RelBuilder 4. RelRunner 5. CalcitePreparingStmt. TODO delete it in future if possible.
 */
public class CalciteToolsHelper {
  /** Create a RelBuilder with testing */
  public static RelBuilder create(FrameworkConfig config) {
    return RelBuilder.create(config);
  }

  /** Create a RelBuilder with typeFactory */
  public static OpenSearchRelBuilder create(
      FrameworkConfig config, JavaTypeFactory typeFactory, Connection connection) {
    return withPrepare(
        config,
        typeFactory,
        connection,
        (cluster, relOptSchema, rootSchema, statement) ->
            new OpenSearchRelBuilder(config.getContext(), cluster, relOptSchema));
  }

  public static Connection connect(FrameworkConfig config, JavaTypeFactory typeFactory) {
    final Properties info = new Properties();
    if (config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
      info.setProperty(
          CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
          config.getTypeSystem().getClass().getName());
    }
    try {
      return new OpenSearchDriver().connect("jdbc:calcite:", info, null, typeFactory);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static RelBuilderFactory proto(final Context context) {
    return (cluster, schema) -> new OpenSearchRelBuilder(context, cluster, schema);
  }

  /**
   * This method copied from {@link Frameworks#withPrepare(FrameworkConfig,
   * Frameworks.BasePrepareAction)}. The purpose is the method {@link
   * CalciteFactory#newConnection(UnregisteredDriver, AvaticaFactory, String, Properties)} create
   * connection with null instance of JavaTypeFactory. So we add a parameter JavaTypeFactory.
   */
  private static <R> R withPrepare(
      FrameworkConfig config,
      JavaTypeFactory typeFactory,
      Connection connection,
      Frameworks.BasePrepareAction<R> action) {
    try {
      final Properties info = new Properties();
      if (config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
        info.setProperty(
            CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
            config.getTypeSystem().getClass().getName());
      }
      final CalciteServerStatement statement =
          connection.createStatement().unwrap(CalciteServerStatement.class);
      return new OpenSearchPrepareImpl().perform(statement, config, typeFactory, action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class OpenSearchDriver extends Driver {

    public Connection connect(
        String url, Properties info, CalciteSchema rootSchema, JavaTypeFactory typeFactory)
        throws SQLException {
      // Add current timestamp in nanos as hook
      Instant now = Instant.now();
      long nanosSinceEpoch = now.getEpochSecond() * 1_000_000_000L + now.getNano();
      Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(nanosSinceEpoch));
      CalciteJdbc41Factory factory = new CalciteJdbc41Factory();
      AvaticaConnection connection =
          factory.newConnection((Driver) this, factory, url, info, rootSchema, typeFactory);
      this.handler.onConnectionInit(connection);
      return connection;
    }

    @Override
    public CalcitePrepare createPrepare() {
      if (prepareFactory != null) {
        return prepareFactory.get();
      }
      return new OpenSearchPrepareImpl();
    }
  }

  /** do nothing, just extend for a public construct for new */
  public static class OpenSearchRelBuilder extends RelBuilder {
    public OpenSearchRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }

    @Override
    public AggCall avg(boolean distinct, String alias, RexNode operand) {
      return aggregateCall(
          SqlParserPos.ZERO,
          PPLBuiltinOperators.AVG_NULLABLE,
          distinct,
          false,
          false,
          null,
          null,
          ImmutableList.of(),
          alias,
          ImmutableList.of(),
          ImmutableList.of(operand));
    }
  }

  public static class OpenSearchPrepareImpl extends CalcitePrepareImpl {
    /**
     * Similar to {@link CalcitePrepareImpl#perform(CalciteServerStatement, FrameworkConfig,
     * Frameworks.BasePrepareAction)}, but with a custom typeFactory.
     */
    public <R> R perform(
        CalciteServerStatement statement,
        FrameworkConfig config,
        JavaTypeFactory typeFactory,
        Frameworks.BasePrepareAction<R> action) {
      final CalcitePrepare.Context prepareContext = statement.createPrepareContext();
      SchemaPlus defaultSchema = config.getDefaultSchema();
      final CalciteSchema schema =
          defaultSchema != null
              ? CalciteSchema.from(defaultSchema)
              : prepareContext.getRootSchema();
      CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(
              schema.root(), schema.path(null), typeFactory, prepareContext.config());
      final RexBuilder rexBuilder = new RexBuilder(typeFactory);
      final RelOptPlanner planner =
          createPlanner(
              prepareContext, Contexts.of(prepareContext.config()), config.getCostFactory());
      registerCustomizedRules(planner);
      final RelOptCluster cluster = createCluster(planner, rexBuilder);
      return action.apply(cluster, catalogReader, prepareContext.getRootSchema().plus(), statement);
    }

    private void registerCustomizedRules(RelOptPlanner planner) {
      OpenSearchRules.OPEN_SEARCH_OPT_RULES.forEach(planner::addRule);
    }

    /**
     * Customize CalcitePreparingStmt. Override {@link CalcitePrepareImpl#getPreparingStmt} and
     * return {@link OpenSearchCalcitePreparingStmt}
     */
    @Override
    protected CalcitePrepareImpl.CalcitePreparingStmt getPreparingStmt(
        CalcitePrepare.Context context,
        Type elementType,
        CalciteCatalogReader catalogReader,
        RelOptPlanner planner) {
      final JavaTypeFactory typeFactory = context.getTypeFactory();
      final EnumerableRel.Prefer prefer;
      if (elementType == Object[].class) {
        prefer = EnumerableRel.Prefer.ARRAY;
      } else {
        prefer = EnumerableRel.Prefer.CUSTOM;
      }
      final Convention resultConvention =
          enableBindable ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
      return new OpenSearchCalcitePreparingStmt(
          this,
          context,
          catalogReader,
          typeFactory,
          context.getRootSchema(),
          prefer,
          createCluster(planner, new RexBuilder(typeFactory)),
          resultConvention,
          createConvertletTable());
    }
  }

  /**
   * Similar to {@link CalcitePrepareImpl.CalcitePreparingStmt}. Customize the logic to convert an
   * EnumerableTableScan to BindableTableScan.
   */
  public static class OpenSearchCalcitePreparingStmt
      extends CalcitePrepareImpl.CalcitePreparingStmt {

    protected final RelOptCluster cluster;

    public OpenSearchCalcitePreparingStmt(
        CalcitePrepareImpl prepare,
        CalcitePrepare.Context context,
        CatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        CalciteSchema schema,
        EnumerableRel.Prefer prefer,
        RelOptCluster cluster,
        Convention resultConvention,
        SqlRexConvertletTable convertletTable) {
      super(
          prepare,
          context,
          catalogReader,
          typeFactory,
          schema,
          prefer,
          cluster,
          resultConvention,
          convertletTable);
      this.cluster = cluster;
    }

    @Override
    protected PreparedResult implement(RelRoot root) {
      ProfileContext profileContext = QueryProfiling.current();
      if (profileContext.isEnabled()) {
        PlanProfileBuilder.ProfilePlan plan = PlanProfileBuilder.profile(root.rel);
        profileContext.setPlanRoot(plan.planRoot());
        root = root.withRel(plan.rel());
      }
      if (root.rel instanceof Scannable scannable) {
        Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);
        RelDataType resultType = root.rel.getRowType();
        boolean isDml = root.kind.belongsTo(SqlKind.DML);
        final Bindable bindable = dataContext -> scannable.scan();

        return new PreparedResultImpl(
            resultType,
            requireNonNull(parameterRowType, "parameterRowType"),
            requireNonNull(fieldOrigins, "fieldOrigins"),
            root.collation.getFieldCollations().isEmpty()
                ? ImmutableList.of()
                : ImmutableList.of(root.collation),
            root.rel,
            mapTableModOp(isDml, root.kind),
            isDml) {
          @Override
          public String getCode() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Bindable getBindable(Meta.CursorFactory cursorFactory) {
            return bindable;
          }

          @Override
          public Type getElementType() {
            return resultType.getFieldList().size() == 1 ? Object.class : Object[].class;
          }
        };
      }
      // Fall through to our own Enumerable implementation, which is byte-for-byte Calcite's
      // except for the Janino parent classloader. Guarded on the actual convention: when
      // enableBindable is set the root is a BindableRel and Calcite's own path is correct.
      if (root.rel instanceof EnumerableRel) {
        return implementEnumerable(root);
      }
      return super.implement(root);
    }

    /**
     * Implements the Enumerable path with a Janino classloader fix.
     *
     * <p>Calcite's {@code EnumerableInterpretable.getBindable()} hardcodes {@code
     * EnumerableInterpretable.class.getClassLoader()} as Janino's parent classloader. The SQL
     * plugin declares {@code extendedPlugins = ['analytics-engine;optional=true']}, so when
     * analytics-engine is installed its classloader becomes the parent and -- delegation being
     * parent-first -- its copy of calcite-core wins over the one in the SQL bundle. That loader
     * cannot see {@code org.opensearch.sql.*}, so generated code referencing our UDFs fails to
     * compile with {@code Cannot determine simple type name "org"}, breaking every window-function
     * plan (timechart, eventstats, top/rare).
     *
     * <p>This replicates Calcite's implementation but passes this class's classloader (the SQL
     * plugin's, a child) which can see both parent and child classes.
     *
     * <p>Note {@code CalciteClassLoaderHelper} does not solve this: it sets the thread context
     * classloader, and no released Calcite reads it -- CALCITE-3745 deliberately kept the declaring
     * class's own loader. It was paired with a patched calcite-core that did read the TCCL,
     * vendored as a binary; that jar was dropped and only the inert helper remains.
     *
     * @see <a href="https://github.com/opensearch-project/sql/issues/5306">sql#5306</a>
     */
    private PreparedResult implementEnumerable(RelRoot root) {
      Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);
      RelDataType resultType = root.rel.getRowType();
      boolean isDml = root.kind.belongsTo(SqlKind.DML);
      EnumerableRel enumerable = (EnumerableRel) root.rel;

      if (!root.isRefTrivial()) {
        List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>();
        final org.apache.calcite.rex.RexBuilder rexBuilder =
            enumerable.getCluster().getRexBuilder();
        for (java.util.Map.Entry<Integer, String> field : root.fields) {
          projects.add(rexBuilder.makeInputRef(enumerable, field.getKey()));
        }
        org.apache.calcite.rex.RexProgram program =
            org.apache.calcite.rex.RexProgram.create(
                enumerable.getRowType(), projects, null, root.validatedRowType, rexBuilder);
        enumerable =
            org.apache.calcite.adapter.enumerable.EnumerableCalc.create(enumerable, program);
      }

      // internalParameters is private in CalcitePreparingStmt but is the same map handed to the
      // DataContext, so stashed values (table scan references) must land in it, not a copy.
      java.util.Map<String, Object> parameters;
      try {
        java.lang.reflect.Field f =
            CalcitePrepareImpl.CalcitePreparingStmt.class.getDeclaredField("internalParameters");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> p = (java.util.Map<String, Object>) f.get(this);
        parameters = p;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to access internalParameters", e);
      }

      // Ordering matches Calcite's implement(): _conformance is set before toBindable.
      parameters.put("_conformance", context.config().conformance());

      CatalogReader.THREAD_LOCAL.set(catalogReader);
      final Bindable bindable;
      try {
        bindable = compileWithPluginClassLoader(enumerable, parameters);
      } finally {
        CatalogReader.THREAD_LOCAL.remove();
      }

      return new PreparedResultImpl(
          resultType,
          requireNonNull(parameterRowType, "parameterRowType"),
          requireNonNull(fieldOrigins, "fieldOrigins"),
          root.collation.getFieldCollations().isEmpty()
              ? ImmutableList.of()
              : ImmutableList.of(root.collation),
          root.rel,
          mapTableModOp(isDml, root.kind),
          isDml) {
        @Override
        public String getCode() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Bindable getBindable(Meta.CursorFactory cursorFactory) {
          return bindable;
        }

        @Override
        public Type getElementType() {
          return resultType.getFieldList().size() == 1 ? Object.class : Object[].class;
        }
      };
    }

    /**
     * Compiled-Bindable cache, mirroring Calcite's own BINDABLE_CACHE in {@code
     * EnumerableInterpretable}. Bypassing Calcite's implementation to fix the Janino classloader
     * also bypassed its cache, so every execution of the same query recompiled the generated class
     * and the fresh class never stayed around long enough for C2 to optimise it. That is a silent
     * ~150-300ms per-query tax on the Calcite path, and in an A/B benchmark it penalises the
     * baseline and flatters the alternative -- measured while auditing this PoC.
     *
     * <p>Keyed on the generated source, exactly as Calcite keys its own cache, so two structurally
     * identical plans share a compiled class. Bounded and soft-valued so it cannot retain classes
     * under heap pressure.
     */
    private static final com.google.common.cache.Cache<String, Class<Bindable>> BINDABLE_CACHE =
        com.google.common.cache.CacheBuilder.newBuilder()
            .concurrencyLevel(
                org.apache.calcite.config.CalciteSystemProperty.BINDABLE_CACHE_CONCURRENCY_LEVEL
                    .value())
            .maximumSize(
                org.apache.calcite.config.CalciteSystemProperty.BINDABLE_CACHE_MAX_SIZE.value())
            .softValues()
            .build();

    /**
     * Equivalent to {@code EnumerableInterpretable.toBindable()} + {@code getBindable()}, but with
     * this class's classloader as Janino's parent. commons-compiler resolves from the parent
     * classloader at runtime, hence reflection rather than a direct call.
     */
    private static Bindable compileWithPluginClassLoader(
        EnumerableRel rel, java.util.Map<String, Object> parameters) {
      try {
        org.apache.calcite.adapter.enumerable.EnumerableRelImplementor relImplementor =
            new org.apache.calcite.adapter.enumerable.EnumerableRelImplementor(
                rel.getCluster().getRexBuilder(), parameters);
        org.apache.calcite.linq4j.tree.ClassDeclaration expr =
            relImplementor.implementRoot(rel, EnumerableRel.Prefer.ARRAY);
        String s =
            org.apache.calcite.linq4j.tree.Expressions.toString(
                expr.memberDeclarations, "\n", false);
        Hook.JAVA_PLAN.run(s);

        ClassLoader classLoader = CalciteToolsHelper.class.getClassLoader();
        Class<?> factoryFactoryClass =
            classLoader.loadClass("org.codehaus.commons.compiler.CompilerFactoryFactory");
        Object compilerFactory =
            factoryFactoryClass
                .getMethod("getDefaultCompilerFactory", ClassLoader.class)
                .invoke(null, classLoader);
        Object compiler =
            compilerFactory.getClass().getMethod("newSimpleCompiler").invoke(compilerFactory);
        compiler
            .getClass()
            .getMethod("setParentClassLoader", ClassLoader.class)
            .invoke(compiler, classLoader);

        String fullCode =
            "public final class "
                + expr.name
                + " implements "
                + Bindable.class.getName()
                + ", "
                + org.apache.calcite.runtime.Typed.class.getName()
                + " {\n"
                + s
                + "\n}\n";
        Class<Bindable> cached = BINDABLE_CACHE.getIfPresent(fullCode);
        if (cached == null) {
          compiler.getClass().getMethod("cook", String.class).invoke(compiler, fullCode);
          ClassLoader compiledClassLoader =
              (ClassLoader) compiler.getClass().getMethod("getClassLoader").invoke(compiler);
          @SuppressWarnings("unchecked")
          Class<Bindable> compiled = (Class<Bindable>) compiledClassLoader.loadClass(expr.name);
          BINDABLE_CACHE.put(fullCode, compiled);
          cached = compiled;
        }
        return cached.getDeclaredConstructors()[0].newInstance() instanceof Bindable b ? b : null;
      } catch (Exception e) {
        throw org.apache.calcite.util.Util.throwAsRuntime(e);
      }
    }

    @Override
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator, CatalogReader catalogReader, SqlToRelConverter.Config config) {
      return new OpenSearchSqlToRelConverter(
          this, validator, catalogReader, this.cluster, convertletTable, config);
    }

    @Override
    protected RelRoot trimUnusedFields(RelRoot root) {
      final SqlToRelConverter.Config config =
          SqlToRelConverter.config()
              .withTrimUnusedFields(shouldTrim(root.rel))
              .withExpand(THREAD_EXPAND.get())
              .withInSubQueryThreshold(requireNonNull(THREAD_INSUBQUERY_THRESHOLD.get()));
      // PPL analyzes into a pre-built RelNode before prepareStatement(rel). Reuse the incoming
      // RelNode's cluster here so prepare-time trimming does not create replacement nodes under a
      // different planner than the rest of the tree.
      final SqlToRelConverter converter =
          new OpenSearchSqlToRelConverter(
              this,
              getSqlValidator(),
              catalogReader,
              root.rel.getCluster(),
              convertletTable,
              config);
      final boolean ordered = !root.collation.getFieldCollations().isEmpty();
      final boolean dml = SqlKind.DML.contains(root.kind);
      return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
    }

    private static boolean shouldTrim(RelNode rootRel) {
      // For now, don't trim if there are more than 3 joins. The projects
      // near the leaves created by trim migrate past joins and seem to
      // prevent join-reordering.
      return THREAD_TRIM.get() || RelOptUtil.countJoins(rootRel) < 2;
    }
  }

  public static class OpenSearchSqlToRelConverter extends SqlToRelConverter {
    protected final RelBuilder relBuilder;

    public OpenSearchSqlToRelConverter(
        ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config config) {
      this(
          viewExpander,
          validator,
          catalogReader,
          cluster,
          convertletTable,
          preserveHintStrategies(cluster, config),
          true);
    }

    private OpenSearchSqlToRelConverter(
        ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config effectiveConfig,
        boolean ignored) {
      super(viewExpander, validator, catalogReader, cluster, convertletTable, effectiveConfig);
      this.relBuilder =
          effectiveConfig
              .getRelBuilderFactory()
              .create(
                  cluster,
                  validator != null
                      ? validator.getCatalogReader().unwrap(RelOptSchema.class)
                      : null)
              .transform(effectiveConfig.getRelBuilderConfigTransform());
    }

    @Override
    protected RelFieldTrimmer newFieldTrimmer() {
      return new OpenSearchRelFieldTrimmer(validator, this.relBuilder);
    }

    // SqlToRelConverter always installs the hint strategy table from its config onto the cluster.
    // When prepare-time trimming reuses an incoming RelNode cluster, preserve any PPL-specific
    // aggregate hint strategies that were already registered during analysis.
    private static Config preserveHintStrategies(RelOptCluster cluster, Config config) {
      if (config.getHintStrategyTable() == HintStrategyTable.EMPTY
          && cluster.getHintStrategies() != HintStrategyTable.EMPTY) {
        return config.withHintStrategyTable(cluster.getHintStrategies());
      }
      return config;
    }
  }

  public static class OpenSearchRelRunners {
    private static boolean isNonPushdownEnumerableAggregate(String message) {
      return message.contains("Error while preparing plan")
          && message.contains("CalciteEnumerableNestedAggregate");
    }

    // Detect if error is due to window functions in unsupported context (bins on time fields)
    private static boolean isWindowBinOnTimeField(SQLException e) {
      String errorMsg = e.getMessage();
      return errorMsg != null
          && errorMsg.contains("Error while preparing plan")
          && errorMsg.contains("WIDTH_BUCKET");
    }

    // Traverse Calcite SQL exceptions in search of the root cause, since Calcite's outer error
    // messages aren't really usable for users
    private static String rootCauseMessage(Throwable e) {
      String rc = null;
      if (e.getCause() != null) {
        rc = rootCauseMessage(e.getCause());
      }
      for (int i = 0; rc == null && i < e.getSuppressed().length; i++) {
        rc = rootCauseMessage(e.getSuppressed()[i]);
      }
      return rc != null ? rc : e.getMessage();
    }

    private static void enrichErrorsForSpecialCases(ErrorReport.Builder report, SQLException e) {
      if (e.getMessage().contains("Error while preparing plan [") && e.getCause() != null) {
        // Generic 'something went wrong' planning error, try to get the cause
        int planStart = e.getMessage().indexOf('[');
        int planEnd = e.getMessage().lastIndexOf(']');
        report
            .context("plan", e.getMessage().substring(planStart + 1, planEnd))
            .details(rootCauseMessage(e));
      }
      if (isWindowBinOnTimeField(e)) {
        report
            .details(
                "The 'bins' parameter on timestamp fields requires: (1) pushdown to be enabled"
                    + " (controlled by plugins.calcite.pushdown.enabled, enabled by default), and"
                    + " (2) the timestamp field to be used as an aggregation bucket (e.g., 'stats"
                    + " count() by @timestamp').")
            .code(ErrorCode.UNSUPPORTED_OPERATION)
            .context("is_window_bin_on_time_field", true)
            .suggestion("check pushdown is enabled and review the aggregation");
      }
    }

    /**
     * Runs a relational expression by existing connection. This class copied from {@link
     * org.apache.calcite.tools.RelRunners#run(RelNode)}
     */
    public static PreparedStatement run(CalcitePlanContext context, RelNode rel) {
      try (ProfileScope optimizePhase = ProfileScope.open(MetricName.OPTIMIZE)) {
        final RelShuttle shuttle =
            new RelHomogeneousShuttle() {
              @Override
              public RelNode visit(TableScan scan) {
                final RelOptTable table = scan.getTable();
                if (scan instanceof LogicalTableScan
                    && Bindables.BindableTableScan.canHandle(table)) {
                  // Always replace the LogicalTableScan with BindableTableScan
                  // because it's implementation does not require a "schema" as context.
                  return Bindables.BindableTableScan.create(scan.getCluster(), table);
                }
                return super.visit(scan);
              }
            };
        rel = rel.accept(shuttle);

        try (Connection connection = context.connection) {
          final RelRunner runner = connection.unwrap(RelRunner.class);
          return runner.prepareStatement(rel);
        } catch (SQLException e) {
          // Detect if error is due to window functions in unsupported context (bins on time fields)
          ErrorReport.Builder report =
              ErrorReport.wrap(e)
                  .location("while compiling the optimized query plan for physical execution")
                  .code(ErrorCode.PLANNING_ERROR);
          enrichErrorsForSpecialCases(report, e);
          throw report.build();
        }
      }
    }
  }

  /** Try to optimize the plan by using HepPlanner */
  private static final List<RelOptRule> hepRuleList =
      List.of(FilterMergeRule.Config.DEFAULT.toRule(), PPLSimplifyDedupRule.DEDUP_SIMPLIFY_RULE);

  private static final HepProgram HEP_PROGRAM =
      new HepProgramBuilder().addRuleCollection(hepRuleList).build();

  // PPLSimplifyDedupRule collapses the ROW_NUMBER window form of dedup into a LogicalDedup so
  // DedupPushdownRule can push it into a Lucene scan. The analytics engine has no such pushdown and
  // cannot plan a LogicalDedup, so its optimization runs the standard window form directly.
  private static final HepProgram ANALYTICS_HEP_PROGRAM =
      new HepProgramBuilder()
          .addRuleCollection(
              hepRuleList.stream()
                  .filter(rule -> rule != PPLSimplifyDedupRule.DEDUP_SIMPLIFY_RULE)
                  .toList())
          .build();

  public static RelNode optimize(RelNode plan, CalcitePlanContext context) {
    Util.discard(context);
    HepPlanner planner = new HepPlanner(HEP_PROGRAM);
    planner.setRoot(plan);
    return planner.findBestExp();
  }

  public static RelNode optimizeForAnalytics(RelNode plan, CalcitePlanContext context) {
    Util.discard(context);
    HepPlanner planner = new HepPlanner(ANALYTICS_HEP_PROGRAM);
    planner.setRoot(plan);
    return planner.findBestExp();
  }
}
