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
package org.apache.calcite.sql;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Parse tree node that represents a PIVOT applied to a table reference
 * (or sub-query).
 *
 * <p>Syntax:
 * <blockquote>{@code
 * SELECT *
 * FROM query PIVOT (agg, ... FOR axis, ... IN (in, ...)) AS alias}
 * </blockquote>
 */
public class SqlPivot extends SqlCall {

  public final SqlNode query;
  public final SqlNodeList aggList;
  public final SqlNodeList axisList;
  public final SqlNodeList inList;

  static final Operator OPERATOR = new Operator(SqlKind.PIVOT);

  //~ Constructors -----------------------------------------------------------

  public SqlPivot(SqlParserPos pos, SqlNode query, SqlNodeList aggList,
      SqlNodeList axisList, SqlNodeList inList) {
    super(pos);
    this.query = Objects.requireNonNull(query);
    this.aggList = Objects.requireNonNull(aggList);
    this.axisList = Objects.requireNonNull(axisList);
    this.inList = Objects.requireNonNull(inList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override @Nonnull public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, aggList, axisList, inList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    query.unparse(writer, leftPrec, 0);
    writer.keyword("PIVOT");
    writer.sep("(");
    aggList.unparse(writer, 0, 0);
    writer.keyword("FOR");
    // force parentheses if there is more than one axis
    final int leftPrec1 = axisList.size() > 1 ? 1 : 0;
    axisList.unparse(writer, leftPrec1, 0);
    writer.keyword("IN");
    writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
        stripList(inList));
    writer.sep(")");
  }

  private static SqlNodeList stripList(SqlNodeList list) {
    return list.getList().stream().map(SqlPivot::strip)
        .collect(SqlNode.toList(list.pos));
  }

  /** Converts a single-element SqlNodeList to its constituent node.
   * For example, "(1)" becomes "1";
   * "(2) as a" becomes "2 as a";
   * "(3, 4)" remains "(3, 4)";
   * "(5, 6) as b" remains "(5, 6) as b". */
  private static SqlNode strip(SqlNode e) {
    switch (e.getKind()) {
    case AS:
      final SqlCall call = (SqlCall) e;
      final List<SqlNode> operands = call.getOperandList();
      return SqlStdOperatorTable.AS.createCall(e.pos,
          strip(operands.get(0)), operands.get(1));
    default:
      if (e instanceof SqlNodeList && ((SqlNodeList) e).size() == 1) {
        return ((SqlNodeList) e).get(0);
      }
      return e;
    }
  }

  /** Pivot operator. */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }
  }
}
