/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.flint;

import org.antlr.v4.runtime.CommonTokenStream;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsLexer;
import org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser;

/**
 * Flint Spark query simple test.
 */
public class FlintSparkQueryDispatcher {

  /**
   * EMR serverless client
   */
  private final EmrServerlessClient emrClient = new EmrServerlessClient();

  public void dispatch(String query) {
    String[] prefixes = {
        "search source =",
        "search source=",
        "source=",
        "source =",
        "CREATE TABLE",
        "CREATE EXTERNAL TABLE"
    };

    // 1) Try prefix match for DQL and table DDL (both are defined in PPL and Spark grammar file)
    for (String prefix : prefixes) {
      if (query.startsWith(prefix)) {
        // TODO: Find data source name and forward entire query
        emrClient.submitLivyQuery(query);
        return;
      }
    }

    // 2) Try Flint parser
    dispatchFlintIndexCommand(query);

    // alternatively, parse by regex
  }

  private void dispatchFlintIndexCommand(String query) {
    FlintSparkSqlExtensionsLexer lexer =
        new FlintSparkSqlExtensionsLexer(
            new CaseInsensitiveCharStream(query));

    FlintSparkSqlExtensionsParser parser =
        new FlintSparkSqlExtensionsParser(
            new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());

    parser.singleStatement().accept(new FlintSparkQueryParser(null, emrClient, query));
  }
}
