/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.serde.ExtendedRelJson;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;

/**
 * Codec that serializes a shard-local Calcite {@link RelNode} subtree to base64-encoded RelJson and
 * deserializes the reverse. The round-trip preserves PPL operators via {@link ExtendedRelJson} and
 * the chained operator table from {@link RelJsonSerializer#getPplSqlOperatorTable()}.
 */
public final class RelFragmentCodec {

  /**
   * DataContext stash-slot key used by the shard row-source table to retrieve buffered rows. US-005
   * will inject rows through this key; for US-003 the table returns an empty enumerable when the
   * slot is absent.
   */
  public static final String SHARD_ROWS_STASH_KEY = "calcite_exec.shard_rows";

  /** Schema name used in the table path: [OpenSearch, &lt;indexName&gt;]. */
  public static final String SCHEMA_NAME = "OpenSearch";

  /**
   * Maps OpenSearch field type names (the wire format defined in the design doc
   * docs/dev/poc-staged-calcite-exec-design.md) to Calcite {@link SqlTypeName}. Case-insensitive
   * lookup. FLOAT maps to REAL to match the canonical conversion in {@link
   * OpenSearchTypeFactory#convertExprTypeToRelDataType}.
   */
  private static final Map<String, SqlTypeName> OS_TYPE_TO_SQL_TYPE =
      Map.ofEntries(
          Map.entry("keyword", SqlTypeName.VARCHAR),
          Map.entry("text", SqlTypeName.VARCHAR),
          Map.entry("long", SqlTypeName.BIGINT),
          Map.entry("integer", SqlTypeName.INTEGER),
          Map.entry("double", SqlTypeName.DOUBLE),
          Map.entry("float", SqlTypeName.REAL),
          Map.entry("boolean", SqlTypeName.BOOLEAN),
          Map.entry("date", SqlTypeName.TIMESTAMP),
          // Struct/nested types are read from _source as opaque VARCHAR (JSON string)
          Map.entry("object", SqlTypeName.VARCHAR),
          Map.entry("nested", SqlTypeName.VARCHAR));

  private RelFragmentCodec() {}

  /**
   * Looks up the Calcite {@link SqlTypeName} for a given OpenSearch field type name. Throws if the
   * type is unrecognized. Package-private for use by {@link CalciteExecAggregator} when building
   * the runtime DataContext schema.
   */
  static SqlTypeName osTypeToSqlType(String osType) {
    SqlTypeName sqlType = OS_TYPE_TO_SQL_TYPE.get(osType.toLowerCase(Locale.ROOT));
    if (sqlType == null) {
      throw new IllegalArgumentException("Unrecognized OpenSearch field type '" + osType + "'");
    }
    return sqlType;
  }

  /**
   * Serializes a {@link RelNode} tree to a base64-encoded RelJson string.
   *
   * @param relNode the plan fragment to serialize
   * @return base64-encoded JSON representation of the plan
   */
  public static String serialize(RelNode relNode) {
    JsonBuilder jsonBuilder = new JsonBuilder();
    // For serialization (write path), ExtendedRelJson.create() suffices — no operatorTable or
    // inputTranslator is needed because the writer only calls toJson() methods.
    RelJsonWriter writer =
        new RelJsonWriter(jsonBuilder, relJson -> ExtendedRelJson.create(jsonBuilder));
    relNode.explain(writer);
    String json = writer.asString();
    return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Deserializes a base64-encoded RelJson string back into a {@link RelNode} tree. Builds a {@link
   * RelOptCluster}, registers a schema named "OpenSearch" containing the shard row-source table
   * with the given row type, and configures the plugin operator table.
   *
   * @param base64Plan the base64-encoded RelJson plan
   * @param indexName the index name that appears in the TableScan's table path
   * @param fields ordered list of field descriptors (name + wire type) for the shard row-source
   *     table; the wire type string is an OpenSearch field type name (e.g. "long", "keyword") as
   *     defined in docs/dev/poc-staged-calcite-exec-design.md
   * @return the deserialized RelNode tree
   */
  public static RelNode deserialize(
      String base64Plan,
      String indexName,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields) {
    String json = new String(Base64.getDecoder().decode(base64Plan), StandardCharsets.UTF_8);

    RelDataTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    // Register trait defs so the planner can handle convention and collation conversions
    // during US-005's EnumerableConvention optimization phase.
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    // Build the row type from caller-supplied field descriptors
    RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
    for (CalciteExecAggregationBuilder.FieldDescriptor fd : fields) {
      SqlTypeName sqlType = OS_TYPE_TO_SQL_TYPE.get(fd.getType().toLowerCase(Locale.ROOT));
      if (sqlType == null) {
        throw new IllegalArgumentException(
            String.format(
                "Unrecognized OpenSearch field type '%s' for field '%s'",
                fd.getType(), fd.getName()));
      }
      RelDataType fieldType =
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true);
      rowTypeBuilder.add(fd.getName(), fieldType);
    }
    RelDataType rowType = rowTypeBuilder.build();

    // Register the schema: rootSchema -> "OpenSearch" sub-schema -> table with indexName
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus openSearchSchema = rootSchema.add(SCHEMA_NAME, new AbstractSchema() {});
    openSearchSchema.add(indexName, new ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema,
            List.of(),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties()));

    try {
      RelJsonReader reader =
          new RelJsonReader(
              cluster,
              catalogReader,
              rootSchema,
              relJson ->
                  ExtendedRelJson.create(new JsonBuilder())
                      // withInputTranslator must be non-null (3-arg constructor requireNonNull's
                      // it); RelJsonReader resolves input refs internally and never invokes this.
                      .withInputTranslator(
                          (rj, input, map, relInput) -> {
                            throw new UnsupportedOperationException(
                                "RelJsonReader resolves input refs internally");
                          })
                      .withOperatorTable(RelJsonSerializer.getPplSqlOperatorTable()));
      return reader.read(json);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize RelNode fragment", e);
    }
  }

  /**
   * A stub {@link ScannableTable} representing a row source fed via a {@link DataContext} stash
   * slot. Defaults to {@link #SHARD_ROWS_STASH_KEY} for the shard side; the coordinator side uses
   * {@link StagePlanner#GATHERED_ROWS_STASH_KEY} via the 2-arg constructor.
   */
  public static class ShardRowSourceTable extends AbstractTable implements ScannableTable {

    private final RelDataType rowType;
    private final String stashKey;

    /** Creates a table reading from the default shard rows stash key. */
    public ShardRowSourceTable(RelDataType rowType) {
      this(rowType, SHARD_ROWS_STASH_KEY);
    }

    /** Creates a table reading from the specified stash key (enables reuse for gathered rows). */
    public ShardRowSourceTable(RelDataType rowType, String stashKey) {
      this.rowType = rowType;
      this.stashKey = stashKey;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }

    @SuppressWarnings(
        "unchecked") // DataContext stash returns Object; the contract is List<Object[]>
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      Object stashed = root.get(stashKey);
      if (stashed instanceof List) {
        return Linq4j.asEnumerable((List<Object[]>) stashed);
      }
      return Linq4j.emptyEnumerable();
    }
  }
}
