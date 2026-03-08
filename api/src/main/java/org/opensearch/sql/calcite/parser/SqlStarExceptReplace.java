/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SqlNode representing SELECT * EXCEPT (columns) REPLACE (expr AS col) syntax.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code * EXCEPT (a, b)}
 *   <li>{@code * REPLACE (x + 1 AS c)}
 *   <li>{@code t.* EXCEPT (a) REPLACE (x + 1 AS c)}
 * </ul>
 */
public class SqlStarExceptReplace extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("STAR_EXCEPT_REPLACE", org.apache.calcite.sql.SqlKind.OTHER);

  private final SqlNode star;
  private final @Nullable SqlNodeList except;
  private final @Nullable SqlNodeList replace;

  public SqlStarExceptReplace(
      SqlParserPos pos, SqlNode star, @Nullable SqlNodeList except, @Nullable SqlNodeList replace) {
    super(pos);
    this.star = star;
    this.except = except;
    this.replace = replace;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(star);
    if (except != null) {
      operands.add(except);
    }
    if (replace != null) {
      operands.add(replace);
    }
    return operands;
  }

  public SqlNode getStar() {
    return star;
  }

  public @Nullable SqlNodeList getExcept() {
    return except;
  }

  public @Nullable SqlNodeList getReplace() {
    return replace;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    star.unparse(writer, 0, 0);
    if (except != null) {
      writer.keyword("EXCEPT");
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (int i = 0; i < except.size(); i++) {
        writer.sep(",");
        except.get(i).unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (replace != null) {
      writer.keyword("REPLACE");
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (int i = 0; i < replace.size(); i++) {
        writer.sep(",");
        replace.get(i).unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }
}
