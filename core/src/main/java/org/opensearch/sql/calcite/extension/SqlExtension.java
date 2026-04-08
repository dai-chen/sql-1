package org.opensearch.sql.calcite.extension;

import java.util.function.UnaryOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;

/**
 * A language extension that adds capabilities beyond ANSI SQL Core.
 * Extensions are organized by capability (not by engine) and contribute
 * to one or more parser/planner seams.
 *
 * <p>Unified SQL always composes: SearchExtension + SecondaryIndexExtension.
 * This is an internal organizational model — externally they are first-class
 * features of one coherent language specification.
 */
public interface SqlExtension {

  /** Operators/functions this extension adds to the operator table. */
  SqlOperatorTable operatorTable();

  /** Parser config adjustments (quoting, casing, keyword de-reservation). */
  SqlParser.Config extendParserConfig(SqlParser.Config base);

  /** Conformance overrides. */
  SqlConformance conformance();

  /** Pre-processor transforms (before Calcite parsing). */
  UnaryOperator<String> preprocessor();
}
