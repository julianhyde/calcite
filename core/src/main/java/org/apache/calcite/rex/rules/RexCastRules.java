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
package org.apache.calcite.rex.rules;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Rules relating to {@code CAST} and types. */
public class RexCastRules {
  private RexCastRules() {
  }

  public static final RexRule CAST = RexRule.ofCall(b ->
          b.ofKind(SqlKind.CAST).anyInputs(),
      RexCastRules::simplifyCast);

  static RexNode simplifyCast(RexRule.Context cx, RexCall e) {
    final RexNode operand = e.getOperands().get(0);
    final RexBuilder rexBuilder = cx.rexBuilder();
    final RexExecutor executor = cx.executor();
    RelDataTypeFactory typeFactory = cx.typeFactory();
    if (SqlTypeUtil.equalOrNarrowsNullability(typeFactory, e.getType(),
        operand.getType())) {
      return operand;
    }
    if (RexUtil.isLosslessCast(operand)) {
      // x :: y below means cast(x as y) (which is PostgreSQL-specific cast by the way)
      // A) Remove lossless casts:
      // A.1) intExpr :: bigint :: int => intExpr
      // A.2) char2Expr :: char(5) :: char(2) => char2Expr
      // B) There are cases when we can't remove two casts, but we could probably remove inner one
      // B.1) char2expression :: char(4) :: char(5) -> char2expression :: char(5)
      // B.2) char2expression :: char(10) :: char(5) -> char2expression :: char(5)
      // B.3) char2expression :: varchar(10) :: char(5) -> char2expression :: char(5)
      // B.4) char6expression :: varchar(10) :: char(5) -> char6expression :: char(5)
      // C) Simplification is not possible:
      // C.1) char6expression :: char(3) :: char(5) -> must not be changed
      //      the input is truncated to 3 chars, so we can't use char6expression :: char(5)
      // C.2) varchar2Expr :: char(5) :: varchar(2) -> must not be changed
      //      the input have to be padded with spaces (up to 2 chars)
      // C.3) char2expression :: char(4) :: varchar(5) -> must not be changed
      //      would not have the padding

      // The approach seems to be:
      // 1) Ensure inner cast is lossless (see if above)
      // 2) If operand of the inner cast has the same type as the outer cast,
      //    remove two casts except C.2 or C.3-like pattern (== inner cast is CHAR)
      // 3) If outer cast is lossless, remove inner cast (B-like cases)

      // Here we try to remove two casts in one go (A-like cases)
      RexNode intExpr = ((RexCall) operand).operands.get(0);
      // intExpr == CHAR detects A.1
      // operand != CHAR detects C.2
      if ((intExpr.getType().getSqlTypeName() == SqlTypeName.CHAR
          || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)
          && SqlTypeUtil.equalOrNarrowsNullability(typeFactory, e.getType(),
              intExpr.getType())) {
        return intExpr;
      }
      // Here we try to remove inner cast (B-like cases)
      if (RexUtil.isLosslessCast(intExpr.getType(), operand.getType())
          && (e.getType().getSqlTypeName() == operand.getType().getSqlTypeName()
          || e.getType().getSqlTypeName() == SqlTypeName.CHAR
          || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)
          && SqlTypeCoercionRule.instance().canApplyFrom(
              intExpr.getType().getSqlTypeName(), e.getType().getSqlTypeName())) {
        return rexBuilder.makeCast(e.getType(), intExpr);
      }
    }
    switch (operand.getKind()) {
    case LITERAL:
      final RexLiteral literal = (RexLiteral) operand;

      // First, try to remove the cast without changing the value.
      // makeCast and canRemoveCastFromLiteral have the same logic, so we are
      // sure to be able to remove the cast.
      if (literal.canRemoveCast(e.getType())) {
        return rexBuilder.makeCast(e.getType(), operand);
      }

      // Next, try to convert the value to a different type,
      // e.g. CAST('123' as integer)
      switch (literal.getTypeName()) {
      case TIME:
        switch (e.getType().getSqlTypeName()) {
        case TIMESTAMP:
          return e;
        default:
          break;
        }
        break;
      default:
        break;
      }
      final List<RexNode> reducedValues = new ArrayList<>();
      final RexNode simplifiedExpr = rexBuilder.makeCast(e.getType(), operand);
      executor.reduce(rexBuilder, ImmutableList.of(simplifiedExpr), reducedValues);
      final RexNode e2 =
          requireNonNull(Iterables.getOnlyElement(reducedValues));
      return e.equals(e2) ? e : e2; // return the original if possible

    default:
      if (operand == e.getOperands().get(0)) {
        return e;
      } else {
        return rexBuilder.makeCast(e.getType(), operand);
      }
    }
  }

}
