/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for {@code RECOVER INDEX JOB jobId}. */
public class SqlRecoverIndexJob extends SqlCall {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("RECOVER INDEX JOB", SqlKind.OTHER);

  private final SqlIdentifier jobId;

  public SqlRecoverIndexJob(SqlParserPos pos, SqlIdentifier jobId) {
    super(pos);
    this.jobId = jobId;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(jobId);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("RECOVER INDEX JOB");
    jobId.unparse(writer, 0, 0);
  }

  public SqlIdentifier getJobId() {
    return jobId;
  }
}
