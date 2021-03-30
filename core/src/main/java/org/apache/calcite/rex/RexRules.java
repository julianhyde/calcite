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
package org.apache.calcite.rex;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Objects;

/**
 * Instances of {@link RexRule}.
 */
public abstract class RexRules {
  private RexRules() {
  }

  /** Matches a character literal whose value is '%'. */
  static RexRule.Done isPercentLiteral(RexRule.OperandBuilder b) {
    return b.isLiteral(literal ->
        Objects.equals(literal.getValueAs(String.class), "%"));
  }

  /** Rule that converts "x LIKE '%'" or "x LIKE '%' ESCAPE y"
   * to "x IS NOT NULL". */
  public static final RexRule LIKE =
      new RexRule() {
        @Override public Done describe(OperandBuilder b) {
          return b.ofKind(SqlKind.LIKE)
              .overloadedInputs(OperandBuilder::any, RexRules::isPercentLiteral)
              .inputs(OperandBuilder::any, RexRules::isPercentLiteral,
                  OperandBuilder::any);
        }

        @Override public RexNode apply(Context cx, RexNode e) {
          final RexCall like = (RexCall) e;
          final RexNode x = like.operands.get(0);
          return cx.rexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, x);
        }
      };

  /** Rule that converts "NULLIF(x, y) IS NULL" to "x = y" if "x" is never
   * null. */
  public static final RexRule NULLIF_IS_NULL =
      new RexRule() {
        @Override public Done describe(OperandBuilder b) {
          return b.ofKind(SqlKind.IS_NULL).oneInput(b2 ->
              b2.ofKind(SqlKind.NULLIF)
                  .inputs(OperandBuilder::notNull, OperandBuilder::any));
        }

        @Override public RexNode apply(Context cx, RexNode e) {
          final RexCall isNull = (RexCall) e;
          final RexCall nullif = (RexCall) isNull.operands.get(0);
          final RexNode x = nullif.operands.get(0);
          final RexNode y = nullif.operands.get(1);
          return cx.rexBuilder().makeCall(SqlStdOperatorTable.EQUALS, x, y);
        }
      };
}
