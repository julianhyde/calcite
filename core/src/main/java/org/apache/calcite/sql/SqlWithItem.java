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

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.sql.SqlOperator.MDX_PRECEDENCE;

import static java.util.Objects.requireNonNull;

/**
 * An item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 */
public class SqlWithItem extends SqlCall {
  public SqlIdentifier name;
  public @Nullable SqlNodeList columnList;
  public SqlLiteral recursive;
  public SqlNode query;

  static final SqlOperator OPERATOR =
      SqlOperators.create(SqlKind.WITH_ITEM)
          .withPrecedence(0, true)
          .withUnparse(SqlWithItem::unparse)
          .withCallFactory(SqlWithItem::create)
          .operator();

  @Deprecated // to be removed before 2.0
  public SqlWithItem(SqlParserPos pos, SqlIdentifier name,
      @Nullable SqlNodeList columnList, SqlNode query) {
    this(pos, name, columnList, query,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO));
  }

  public SqlWithItem(SqlParserPos pos, SqlIdentifier name,
      @Nullable SqlNodeList columnList, SqlNode query,
      SqlLiteral recursive) {
    super(pos);
    this.name = name;
    this.columnList = columnList;
    this.recursive = recursive;
    this.query = query;
  }

  static SqlCall create(SqlOperator operator, @Nullable SqlLiteral qualifier,
      SqlParserPos pos, List<? extends @Nullable SqlNode> operands) {
    checkArgument(qualifier == null);
    checkArgument(operands.size() == 3);
    return new SqlWithItem(pos,
        requireNonNull((SqlIdentifier) operands.get(0), "name"),
        requireNonNull((SqlNodeList) operands.get(1), "columnList"),
        requireNonNull(operands.get(2), "query"),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.WITH_ITEM;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query, recursive);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      name = requireNonNull((SqlIdentifier) operand, "name");
      break;
    case 1:
      columnList = (@Nullable SqlNodeList) operand;
      break;
    case 2:
      query = requireNonNull(operand, "operand");
      break;
    case 3:
      recursive = (SqlLiteral) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  static void unparse(SqlWriter writer, SqlOperator operator, SqlCall call,
      int leftPrec, int rightPrec) {
    final SqlWithItem withItem = (SqlWithItem) call;
    withItem.name.unparse(writer, operator.getLeftPrec(),
        operator.getRightPrec());
    if (withItem.columnList != null) {
      withItem.columnList.unparse(writer, operator.getLeftPrec(),
          operator.getRightPrec());
    }
    writer.keyword("AS");
    withItem.query.unparse(writer, MDX_PRECEDENCE, MDX_PRECEDENCE);
  }
}
