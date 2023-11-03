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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlTableRef</code> is a node of a parse tree which represents
 * a table reference.
 *
 * <p>It can be attached with a sql hint statement, see {@link SqlHint} for details.
 */
public class SqlTableRef extends SqlCall {

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier tableName;
  private final SqlNodeList hints;

  //~ Static fields/initializers ---------------------------------------------

  static final SqlOperator OPERATOR =
      SqlOperators.create(SqlKind.TABLE_REF)
          .withCallFactory((operator, qualifier, pos, operands) ->
              new SqlTableRef(pos,
                  (SqlIdentifier) requireNonNull(operands.get(0), "tableName"),
                  (SqlNodeList) requireNonNull(operands.get(1), "hints")))
          .operator();

  //~ Constructors -----------------------------------------------------------

  public SqlTableRef(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList hints) {
    super(pos);
    this.tableName = tableName;
    this.hints = hints;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(tableName, hints);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    tableName.unparse(writer, leftPrec, rightPrec);
    if (!this.hints.isEmpty()) {
      writer.getDialect().unparseTableScanHints(writer, this.hints, leftPrec, rightPrec);
    }
  }
}
