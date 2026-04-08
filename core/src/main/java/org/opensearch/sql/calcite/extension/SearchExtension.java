package org.opensearch.sql.calcite.extension;

import java.util.function.UnaryOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.config.Lex;
import org.opensearch.sql.calcite.conformance.OpenSearchSqlConformance;
import org.opensearch.sql.calcite.operator.OpenSearchSqlOperatorTable;
import org.opensearch.sql.calcite.parser.impl.OpenSearchSqlParserImpl;
import org.opensearch.sql.calcite.preprocess.QueryPreprocessor;

/** Search Extension: relevance functions, named parameters, identifier rules. */
public class SearchExtension implements SqlExtension {

  @Override
  public SqlOperatorTable operatorTable() {
    return SqlOperatorTables.chain(
        SqlStdOperatorTable.instance(),
        OpenSearchSqlOperatorTable.instance());
  }

  @Override
  public SqlParser.Config extendParserConfig(SqlParser.Config base) {
    return base
        .withParserFactory(OpenSearchSqlParserImpl.FACTORY)
        .withLex(Lex.MYSQL);
  }

  @Override
  public SqlConformance conformance() {
    return OpenSearchSqlConformance.INSTANCE;
  }

  @Override
  public UnaryOperator<String> preprocessor() {
    return QueryPreprocessor::process;
  }
}
