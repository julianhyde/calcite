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

import org.apache.calcite.sql.fun.SqlOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node that represents an {@code ORDER BY} on a query other than a
 * {@code SELECT} (e.g. {@code VALUES} or {@code UNION}).
 *
 * <p>It is a purely syntactic operator, and is eliminated by
 * {@link org.apache.calcite.sql.validate.SqlValidatorImpl}.performUnconditionalRewrites
 * and replaced with the ORDER_OPERAND of SqlSelect.
 */
public class SqlOrderBy extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      SqlOperators.create(SqlKind.ORDER_BY)
          // lower precedence than SELECT to avoid extra parentheses
          .withPrecedence(0, true)
          .withUnparse(SqlOrderBy::unparse)
          .withCallFactory(SqlOrderBy::create)
          .toSpecial();

  public final SqlNode query;
  public final SqlNodeList orderList;
  public final @Nullable SqlNode offset;
  public final @Nullable SqlNode fetch;

  //~ Constructors -----------------------------------------------------------

  public SqlOrderBy(SqlParserPos pos, SqlNode query, SqlNodeList orderList,
      @Nullable SqlNode offset, @Nullable SqlNode fetch) {
    super(pos);
    this.query = query;
    this.orderList = orderList;
    this.offset = offset;
    this.fetch = fetch;
  }

  private static SqlCall create(SqlOperator operator, @Nullable SqlLiteral qualifier,
      SqlParserPos pos, List<? extends @Nullable SqlNode> operands) {
    return new SqlOrderBy(pos, requireNonNull(operands.get(0), "query"),
        (SqlNodeList) requireNonNull(operands.get(1), "orderList"),
        operands.get(2), operands.get(3));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.ORDER_BY;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, orderList, offset, fetch);
  }

  private static void unparse(SqlWriter writer, SqlOperator operator,
      SqlCall call, int leftPrec, int rightPrec) {
    SqlOrderBy orderBy = (SqlOrderBy) call;
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
    orderBy.query.unparse(writer, operator.getLeftPrec(),
        operator.getRightPrec());
    if (orderBy.orderList != SqlNodeList.EMPTY) {
      writer.sep("ORDER BY");
      writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
          orderBy.orderList);
    }
    if (orderBy.offset != null || orderBy.fetch != null) {
      writer.fetchOffset(orderBy.fetch, orderBy.offset);
    }
    writer.endList(frame);
  }
}
