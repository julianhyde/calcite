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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * <code>STRING_AGG</code> aggregate function
 * returns the concatenation of its group rows;
 * it is the PostgreSQL and BigQuery equivalent of {@code LISTAGG}.
 *
 * <p>{@code STRING_AGG(v, sep ORDER BY x, y)} is implemented by
 * rewriting to {@code LISTAGG(v, sep) WITHIN GROUP (ORDER BY x, y)}.
 *
 * @see SqlListaggAggFunction
 */
class SqlStringAggAggFunction extends SqlListaggAggFunction {
  SqlStringAggAggFunction() {
    super(SqlKind.STRING_AGG);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    List<SqlNode> operandList = call.getOperandList();
    if (operandList.size() > 0
        && Util.last(operandList) instanceof SqlNodeList) {
      // Remove the last argument if it is "ORDER BY". The parser stashes the
      // ORDER BY clause in the argument list but it does not take part in
      // type derivation.
      operandList = Util.skipLast(operandList);
    }
    return super.deriveType(validator, scope,
        createCall(call.getFunctionQuantifier(), call.getParserPosition(),
            operandList));
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    writer.keyword(getName());
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    final SqlLiteral quantifier = call.getFunctionQuantifier();
    if (quantifier != null) {
      quantifier.unparse(writer, 0, 0);
    }
    for (SqlNode operand : call.getOperandList()) {
      if (operand instanceof SqlNodeList) {
        writer.sep("ORDER BY");
      } else {
        writer.sep(",");
      }
      operand.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }
}
