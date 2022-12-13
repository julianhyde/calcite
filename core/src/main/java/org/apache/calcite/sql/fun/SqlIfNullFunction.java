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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.util.List;

public class SqlIfNullFunction extends SqlFunction {
  //~ Constructors

  public SqlIfNullFunction() {
    super("IFNULL",
        SqlKind.COALESCE,
        ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE),
        null,
        OperandTypes.SAME_SAME,
        SqlFunctionCategory.SYSTEM);

    // private static class IfNullOperandTypeChecker implements SqlOperandTypeChecker {
    //
    //   @Override public boolean checkOperandCount(){
    //     return SqlOperandCountRanges(2)
    //   }
    // }
  }

  //~ Methods ----------------------------------------------------------------

  // override SqlOperator
  @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    validateQuantifier(validator, call); // check DISTINCT/ALL

    List<SqlNode> operands = call.getOperandList();

    checkOperandCount(
        validator,
        getOperandTypeChecker(),
        call);

    SqlParserPos pos = call.getParserPosition();

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);

    whenList.add(SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operands.get(0)));
    thenList.add(SqlNode.clone(operands.get(0)));

    SqlNode elseExpr = Util.last(operands);
    return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
  }
}
