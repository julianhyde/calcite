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
package org.apache.calcite.sql.babel.postgresql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.Symbolizable;
import org.apache.calcite.sql.fun.SqlOperators;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node representing a {@code BEGIN} clause.
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-begin.html">BEGIN specification</a>
 */
public class SqlBegin extends SqlCall {
  public static final SqlOperator OPERATOR =
      SqlOperators.create("BEGIN")
          .withPrecedence(32, false)
          .withCallFactory((operator, qualifier, pos, operands) ->
              new SqlBegin(pos,
                  requireNonNull((SqlNodeList) operands.get(0),
                      "transactionModeList")))
          .operator();

  private final SqlNodeList transactionModeList;

  protected SqlBegin(final SqlParserPos pos, final SqlNodeList transactionModeList) {
    super(pos);
    this.transactionModeList = transactionModeList;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(transactionModeList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("BEGIN");
    transactionModeList.unparse(writer, -1, -1);
  }

  /**
   * Transaction mode.
   */
  public enum TransactionMode implements Symbolizable {
    READ_WRITE,
    READ_ONLY,
    DEFERRABLE,
    NOT_DEFERRABLE,
    ISOLATION_LEVEL_SERIALIZABLE,
    ISOLATION_LEVEL_REPEATABLE_READ,
    ISOLATION_LEVEL_READ_COMMITTED,
    ISOLATION_LEVEL_READ_UNCOMMITTED;

    @Override public String toString() {
      return super.toString().replace("_", " ");
    }
  }
}
