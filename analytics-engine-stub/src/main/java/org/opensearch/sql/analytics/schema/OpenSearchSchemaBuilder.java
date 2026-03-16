/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics.schema;

import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

/**
 * Builds a Calcite {@link SchemaPlus} from OpenSearch {@link ClusterState} index mappings. One
 * Calcite table per index. Uses standard Calcite types (no UDTs).
 *
 * <p>Copied from analytics-engine plugin (opensearch-project/OpenSearch sandbox/plugins).
 */
public class OpenSearchSchemaBuilder {

  private OpenSearchSchemaBuilder() {}

  /** Builds a Calcite SchemaPlus from the given ClusterState. */
  @SuppressWarnings("unchecked")
  public static SchemaPlus buildSchema(ClusterState clusterState) {
    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();

    for (Map.Entry<String, IndexMetadata> entry : clusterState.metadata().indices().entrySet()) {
      String indexName = entry.getKey();
      MappingMetadata mapping = entry.getValue().mapping();
      if (mapping == null) {
        continue;
      }

      Map<String, Object> properties =
          (Map<String, Object>) mapping.sourceAsMap().get("properties");
      if (properties == null) {
        continue;
      }

      schemaPlus.add(indexName, buildTable(properties));
    }

    return schemaPlus;
  }

  /** Maps an OpenSearch field type string to a Calcite SqlTypeName. */
  public static SqlTypeName mapFieldType(String opensearchType) {
    return switch (opensearchType) {
      case "keyword", "text", "ip" -> SqlTypeName.VARCHAR;
      case "long" -> SqlTypeName.BIGINT;
      case "integer" -> SqlTypeName.INTEGER;
      case "short" -> SqlTypeName.SMALLINT;
      case "byte" -> SqlTypeName.TINYINT;
      case "double" -> SqlTypeName.DOUBLE;
      case "float" -> SqlTypeName.FLOAT;
      case "boolean" -> SqlTypeName.BOOLEAN;
      case "date" -> SqlTypeName.TIMESTAMP;
      default -> SqlTypeName.VARCHAR;
    };
  }

  @SuppressWarnings("unchecked")
  private static AbstractTable buildTable(Map<String, Object> properties) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
          Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
          String fieldType = (String) fieldProps.get("type");
          if (fieldType == null || "nested".equals(fieldType) || "object".equals(fieldType)) {
            continue;
          }
          builder.add(
              fieldEntry.getKey(),
              typeFactory.createTypeWithNullability(
                  typeFactory.createSqlType(mapFieldType(fieldType)), true));
        }
        return builder.build();
      }
    };
  }
}
