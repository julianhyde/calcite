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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexRule.OperandBuilder;
import org.apache.calcite.rex.rules.RexCaseRules;
import org.apache.calcite.rex.rules.RexCastRules;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;

import java.util.Objects;

/**
 * Instances of {@link RexRule}.
 */
public abstract class RexRules {
  private RexRules() {
  }

  /** Matches a character literal whose value is '%'. */
  static RexRule.Done isPercentLiteral(OperandBuilder b) {
    return b.isLiteral(literal ->
        Objects.equals(literal.getValueAs(String.class), "%"));
  }

  /** Rule that simplifies CEIL/FLOOR function on top of CEIL/FLOOR.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>{@code FLOOR(FLOOR($0, HOUR), DAY)} &rarr;
   *     {@code FLOOR($0, DAY)}
   *
   * <li>{@code CEIL(CEIL($0, SECOND), DAY)} &rarr;
   *     {@code CEIL($0, DAY)}
   *
   * <li>{@code FLOOR(FLOOR($0, DAY), SECOND)} unchanged
   * </ul>
   */
  public static final RexRule CEIL_FLOOR =
      RexRule.ofCall(b ->
              b.ofKind(SqlKind.CEIL, SqlKind.FLOOR)
                  .inputs(b2 ->
                          b2.ofKind(SqlKind.CEIL, SqlKind.FLOOR)
                              .inputs(OperandBuilder::any, OperandBuilder::any),
                      OperandBuilder::any),
          RexRules::simplifyCeilFloor);

  /** Helper for {@link #CEIL_FLOOR}. */
  static RexNode simplifyCeilFloor(RexRule.Context cx, RexCall e) {
    assert e.getOperands().size() == 2 : "Only match FLOOR/CEIL of date";
    final RexNode operand = e.getOperands().get(0); // simplify(e.getOperands().get(0), UNKNOWN);
    if (e.getKind() == operand.getKind()) {
      assert e.getKind() == SqlKind.CEIL || e.getKind() == SqlKind.FLOOR;
      // CEIL/FLOOR on top of CEIL/FLOOR
      final RexCall child = (RexCall) operand;
      assert e.getOperands().size() == 2 : "Only match FLOOR/CEIL of date";
      final RexLiteral parentFlag = (RexLiteral) e.operands.get(1);
      final TimeUnitRange parentFlagValue =
          parentFlag.getValueAs(TimeUnitRange.class);
      final RexLiteral childFlag = (RexLiteral) child.operands.get(1);
      final TimeUnitRange childFlagValue =
          childFlag.getValueAs(TimeUnitRange.class);
      if (parentFlagValue != null && childFlagValue != null) {
        if (canRollUp(parentFlagValue.startUnit, childFlagValue.startUnit)) {
          return e.clone(e.getType(),
              ImmutableList.of(child.getOperands().get(0), parentFlag));
        }
      }
    }
    return e.clone(e.getType(),
        ImmutableList.of(operand, e.getOperands().get(1)));
  }

  /** Rule that simplifies "-(-x)" to "x". */
  public static final RexRule MINUS_PREFIX =
      RexRule.ofCall(b -> b.ofKind(SqlKind.MINUS_PREFIX)
              .inputs(b2 -> b2.ofKind(SqlKind.MINUS_PREFIX)
              .anyInputs()),
          RexRules::simplifyUnaryMinus);

  /** Helps {@link #MINUS_PREFIX}. */
  static RexNode simplifyUnaryMinus(RexRule.Context cx, RexCall call) {
    // -(-(x)) ==> x
    final RexNode a = call.operands.get(0);
    return ((RexCall) a).operands.get(0);
  }

  /** Rule that simplifies "+x" to "x". */
  public static final RexRule PLUS_PREFIX =
      RexRule.ofCall(b -> b.ofKind(SqlKind.PLUS_PREFIX).anyInputs(),
          RexRules::simplifyUnaryPlus);

  /** Helps {@link #PLUS_PREFIX}. */
  static RexNode simplifyUnaryPlus(RexRule.Context cx, RexCall call) {
    return call.operands.get(0);
  }

  /** Returns whether we can rollup from inner time unit
   * to outer time unit. */
  static boolean canRollUp(TimeUnit outer, TimeUnit inner) {
    // Special handling for QUARTER as it is not in the expected
    // order in TimeUnit
    switch (outer) {
    case YEAR:
    case MONTH:
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      switch (inner) {
      case YEAR:
      case QUARTER:
      case MONTH:
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
      case MICROSECOND:
        if (inner == TimeUnit.QUARTER) {
          return outer == TimeUnit.YEAR;
        }
        return outer.ordinal() <= inner.ordinal();
      default:
        break;
      }
      break;
    case QUARTER:
      switch (inner) {
      case QUARTER:
      case MONTH:
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
      case MICROSECOND:
        return true;
      default:
        break;
      }
      break;
    default:
      break;
    }
    return false;
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

        @Override public RexNode apply(Context cx, RexNode e,
            RexUnknownAs unknownAs) {
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

        @Override public RexNode apply(Context cx, RexNode e,
            RexUnknownAs unknownAs) {
          final RexCall isNull = (RexCall) e;
          final RexCall nullif = (RexCall) isNull.operands.get(0);
          final RexNode x = nullif.operands.get(0);
          final RexNode y = nullif.operands.get(1);
          return cx.rexBuilder().makeCall(SqlStdOperatorTable.EQUALS, x, y);
        }
      };

  /** Program used by RexSimplify. */
  public static final RexRuleProgram PROGRAM_FOR_SIMPLIFY =
      RexRuleProgram.of(
          ImmutableList.of(RexCastRules.CAST,
              RexCaseRules.CASE,
              RexCaseRules.COALESCE,
              CEIL_FLOOR,
              PLUS_PREFIX,
              MINUS_PREFIX));

}
