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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Strategy interface to process operands of an operator call.
 *
 * @see OperandHandlers
 */
public interface SqlOperandHandler {
  default <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call,
      boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
    List<SqlNode> operands = call.getOperandList();
    for (int i = 0; i < operands.size(); i++) {
      argHandler.visitChild(visitor, call, i, operands.get(i));
    }
  }

  default void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    for (SqlNode operand : call.getOperandList()) {
      operand.validateExpr(validator, operandScope);
    }
  }

  default RelDataType deriveOperandType(SqlValidator validator,
      SqlValidatorScope scope, int i, SqlNode operand) {
    return validator.deriveType(scope, operand);
  }

  default SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return call;
  }
}
