/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.flint;

import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.CreateCoveringIndexStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.CreateMaterializedViewStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.CreateSkippingIndexStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.DescribeSkippingIndexStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.DropCoveringIndexStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.DropSkippingIndexStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.IdentifierContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.MultipartIdentifierContext;
import static org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsParser.RefreshSkippingIndexStatementContext;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.ppl.antlr.parser.FlintSparkSqlExtensionsBaseVisitor;

@RequiredArgsConstructor
class FlintSparkQueryParser extends FlintSparkSqlExtensionsBaseVisitor<Void> {

  /**
   * Assume this unique name is available in cluster setting
   */
  private final String clusterName = "1234567890:test";

  /**
   * Data source service for reading data source configs
   */
  private final DataSourceService dataSourceService;

  /**
   * EMR serverless client
   */
  private final EmrServerlessClient emrClient;

  /**
   * Original user query
   */
  private final String query;

  // ------------------------------------
  //   Flint Skipping Index Statements
  // ------------------------------------

  @Override
  public Void visitCreateSkippingIndexStatement(CreateSkippingIndexStatementContext ctx) {
    MultipartIdentifierContext tableName = ctx.tableName;
    parseDataSource(ctx.tableName);
    emrClient.startJobRun(jobName(tableName.getText()), query);
    return null;
  }

  @Override
  public Void visitDropSkippingIndexStatement(DropSkippingIndexStatementContext ctx) {
    parseDataSource(ctx.tableName);
    emrClient.cancelJobRun(jobName(ctx.tableName.getText()));
    return null;
  }

  @Override
  public Void visitRefreshSkippingIndexStatement(RefreshSkippingIndexStatementContext ctx) {
    parseDataSource(ctx.tableName);
    emrClient.submitLivyQuery(query);
    return null;
  }

  @Override
  public Void visitDescribeSkippingIndexStatement(DescribeSkippingIndexStatementContext ctx) {
    parseDataSource(ctx.tableName);
    emrClient.submitLivyQuery(query);
    return null;
  }

  // ------------------------------------
  //   Flint Covering Index Statements
  // ------------------------------------

  @Override
  public Void visitCreateCoveringIndexStatement(CreateCoveringIndexStatementContext ctx) {
    MultipartIdentifierContext tableName = ctx.tableName;
    IdentifierContext indexName = ctx.indexName;
    parseDataSource(ctx.tableName);

    String jobName = jobName(tableName.getText() + ":" + indexName.getText());
    emrClient.startJobRun(jobName, query);
    return null;
  }

  @Override
  public Void visitDropCoveringIndexStatement(DropCoveringIndexStatementContext ctx) {
    MultipartIdentifierContext tableName = ctx.tableName;
    IdentifierContext indexName = ctx.indexName;

    parseDataSource(ctx.tableName);
    String jobName = jobName(tableName.getText() + ":" + indexName.getText());
    emrClient.cancelJobRun(jobName);
    return null;
  }

  // --------------------------------------
  //   Flint Materialized View Statements
  // --------------------------------------

  @Override
  public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatementContext ctx) {
    System.out.println("MV query definition: " + ctx.query);
    MultipartIdentifierContext mvName = ctx.mvName;

    parseDataSource(ctx.mvName);
    emrClient.startJobRun(jobName(mvName.getText()), query);
    return null;
  }

  private void parseDataSource(MultipartIdentifierContext dsCtx) {
    // TODO: parse assume role and other info from data source metadata
    String dataSourceName = dsCtx.parts.get(0).getText(); // Assume full name catalog.[schema].table given
    // DataSourceMetadata metadata = dataSourceService.getDataSourceMetadata(dataSourceName);
  }

  private String jobName(String flintIndexName) {
    return clusterName + ":" + flintIndexName;
  }
}
