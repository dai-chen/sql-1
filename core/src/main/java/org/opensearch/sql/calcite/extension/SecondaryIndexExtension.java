package org.opensearch.sql.calcite.extension;

import java.util.function.UnaryOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** Secondary Index Extension: Flint DDL (skipping index, covering index, materialized view). */
public class SecondaryIndexExtension implements SqlExtension {

  @Override
  public SqlOperatorTable operatorTable() {
    return SqlOperatorTables.of();
  }

  @Override
  public SqlParser.Config extendParserConfig(SqlParser.Config base) {
    return base;
  }

  @Override
  public SqlConformance conformance() {
    return SqlConformanceEnum.DEFAULT;
  }

  @Override
  public UnaryOperator<String> preprocessor() {
    return UnaryOperator.identity();
  }
}
