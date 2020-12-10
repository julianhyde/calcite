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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree node that represents a PIVOT applied to a table reference
 * (or sub-query).
 *
 * <p>Syntax:
 * <blockquote><pre>{@code
 * SELECT *
 * FROM query
 * UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] (
 *   columns FOR columns IN ( columns [ AS values ], ...))
 *
 * where:
 *
 * columns: column
 *        | '(' column, ... ')'
 * values:  value
 *        | '(' value, ... ')'
 * }</pre></blockquote>
 */
public class SqlUnpivot extends SqlCall {

  public SqlNode query;
  public final @Nullable Boolean includeNulls;
  public final SqlNodeList fooList;
  public final SqlNodeList axisList;
  public final SqlNodeList inList;

  static final Operator OPERATOR = new Operator(SqlKind.UNPIVOT);

  //~ Constructors -----------------------------------------------------------

  public SqlUnpivot(SqlParserPos pos, SqlNode query, Boolean includeNulls,
      SqlNodeList fooList, SqlNodeList axisList, SqlNodeList inList) {
    super(pos);
    this.query = Objects.requireNonNull(query);
    this.includeNulls = includeNulls;
    this.fooList = Objects.requireNonNull(fooList);
    this.axisList = Objects.requireNonNull(axisList);
    this.inList = Objects.requireNonNull(inList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, fooList, axisList, inList);
  }

  @SuppressWarnings("nullness")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    // Only 'query' is mutable. (It is required for validation.)
    switch (i) {
    case 0:
      query = operand;
      break;
    default:
      super.setOperand(i, operand);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    query.unparse(writer, leftPrec, 0);
    writer.keyword("UNPIVOT");
    if (includeNulls != null) {
      writer.print(includeNulls ? "INCLUDE NULLS" : "EXCLUDE NULLS");
    }
    final SqlWriter.Frame frame = writer.startList("(", ")");
    // force parentheses if there is more than one foo
    final int leftPrec1 = fooList.size() > 1 ? 1 : 0;
    fooList.unparse(writer, leftPrec1, 0);
    writer.sep("FOR");
    // force parentheses if there is more than one axis
    final int leftPrec2 = axisList.size() > 1 ? 1 : 0;
    axisList.unparse(writer, leftPrec2, 0);
    writer.sep("IN");
    writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
        SqlPivot.stripList(inList));
    writer.endList(frame);
  }

  /** Pivot operator. */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }
  }
}
