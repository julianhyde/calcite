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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRule;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.rex.RexUnknownAs.UNKNOWN;

/** Rules relating to {@code CASE}, {@code COALESCE} and types. */
public class RexCaseRules {
  public static final RexRule CASE = RexRule.ofWeakCall(b ->
          b.ofKind(SqlKind.CASE).anyInputs(),
      RexCaseRules::simplifyCase);

  public static final RexRule COALESCE = RexRule.ofCall(b ->
          b.ofKind(SqlKind.COALESCE).anyInputs(),
      RexCaseRules::simplifyCoalesce);

  /** Helper for {@link #CASE};
   * implements {@link org.apache.calcite.rex.RexRule.RexCallTransformer}. */
  static RexNode simplifyCase(RexRule.Context cx, RexCall call,
      RexUnknownAs unknownAs) {
    final RexBuilder rexBuilder = cx.rexBuilder();
    final RelDataTypeFactory typeFactory = cx.typeFactory();
    final List<CaseBranch> inputBranches =
        CaseBranch.fromCaseOperands(rexBuilder, new ArrayList<>(call.getOperands()));

    // run simplification on all operands
    RexRule.Context condSimplifier =
        cx.withPredicates(RelOptPredicateList.EMPTY);
    RexRule.Context valueSimplifier = cx;
    RelDataType caseType = call.getType();

    boolean conditionNeedsSimplify = false;
    CaseBranch lastBranch = null;
    final List<CaseBranch> branches = new ArrayList<>();
    for (CaseBranch inputBranch : inputBranches) {
      // simplify the condition
      RexNode newCond = condSimplifier.simplify(inputBranch.cond, RexUnknownAs.FALSE);
      if (newCond.isAlwaysFalse()) {
        // If the condition is false, we do not need to add it
        continue;
      }

      // simplify the value
      RexNode newValue = valueSimplifier.simplify(inputBranch.value, unknownAs);

      // create new branch
      if (lastBranch != null) {
        if (lastBranch.value.equals(newValue)
            && RexUtil.isSafeExpression(newCond)) {
          // In this case, last branch and new branch have the same conclusion,
          // hence we create a new composite condition and we do not add it to
          // the final branches for the time being.
          newCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, lastBranch.cond, newCond);
          conditionNeedsSimplify = true;
        } else {
          // If we reach here, the new branch cannot be merged with the last
          // one, hence we are going to add the last branch to the final
          // branches. If the last branch was merged, then we will simplify it
          // first; otherwise, we just add it.
          CaseBranch branch = generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch);
          if (!branch.cond.isAlwaysFalse()) {
            // If the condition is not false, we add it to the final result
            branches.add(branch);
            if (branch.cond.isAlwaysTrue()) {
              // If the condition is always true, we are done
              lastBranch = null;
              break;
            }
          }
          conditionNeedsSimplify = false;
        }
      }
      lastBranch = new CaseBranch(newCond, newValue);

      if (newCond.isAlwaysTrue()) {
        // If the condition is always true, we are done (useful in first loop iteration)
        break;
      }
    }
    if (lastBranch != null) {
      // we need to add the last pending branch once we have finished
      // with the for loop
      CaseBranch branch = generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch);
      if (!branch.cond.isAlwaysFalse()) {
        branches.add(branch);
      }
    }

    if (branches.size() == 1) {
      // we can just return the value in this case (matching the case type)
      final RexNode value = branches.get(0).value;
      if (SqlTypeUtil.equalOrNarrowsNullability(typeFactory,
          caseType, value.getType())) {
        return value;
      } else {
        return rexBuilder.makeAbstractCast(caseType, value);
      }
    }

    if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      final RexNode result = simplifyBooleanCase(cx, branches, caseType);
      if (result != null) {
        if (SqlTypeUtil.equalOrNarrowsNullability(typeFactory, caseType,
            result.getType())) {
          return cx.simplify(result, unknownAs);
        } else {
          // If the simplification would widen the nullability
          RexNode simplified = cx.simplify(result, UNKNOWN);
          if (!simplified.getType().isNullable()) {
            return simplified;
          } else {
            return rexBuilder.makeCast(call.getType(), simplified);
          }
        }
      }
    }
    List<RexNode> newOperands = CaseBranch.toCaseOperands(branches);
    if (newOperands.equals(call.getOperands())) {
      return call;
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, newOperands);
  }

  private static @Nullable RexNode simplifyBooleanCase(RexRule.Context cx,
      List<CaseBranch> inputBranches, RelDataType branchType) {
    RexNode result;

    // Prepare all condition/branches for boolean interpretation. It's done here
    // make these interpretation changes available to case2or simplifications
    // but not interfere with the normal simplification recursion.
    List<CaseBranch> branches = new ArrayList<>();
    for (CaseBranch branch : inputBranches) {
      if ((branches.size() > 0
          && !RexUtil.isSafeExpression(branch.cond))
          || !RexUtil.isSafeExpression(branch.value)) {
        return null;
      }
      final RexNode cond = cx.isTrue(branch.cond);
      final RexNode value;
      if (!branchType.equals(branch.value.getType())) {
        value = cx.rexBuilder().makeAbstractCast(branchType, branch.value);
      } else {
        value = branch.value;
      }
      branches.add(new CaseBranch(cond, value));
    }

    result = simplifyBooleanCaseGeneric(cx, branches);
    return result;
  }

  /**
   * Generic boolean case simplification.
   *
   * <p>Rewrites:
   * <pre>{@code
   * CASE
   *   WHEN p1 THEN x
   *   WHEN p2 THEN y
   *   ELSE z
   * END
   * }</pre>
   * to
   * <pre>{@code
   * (p1 AND x)
   * OR (p2 AND y AND NOT p1)
   * OR (true AND z AND NOT p1 AND NOT p2)
   * }</pre>
   */
  private static RexNode simplifyBooleanCaseGeneric(RexRule.Context cx,
      List<CaseBranch> branches) {
    final boolean booleanBranches = branches.stream().allMatch(branch ->
        branch.value.isAlwaysTrue()
            || branch.value.isAlwaysFalse());
    final List<RexNode> terms = new ArrayList<>();
    final List<RexNode> notTerms = new ArrayList<>();
    for (CaseBranch branch : branches) {
      boolean useBranch = !branch.value.isAlwaysFalse();
      if (useBranch) {
        final RexNode branchTerm;
        if (branch.value.isAlwaysTrue()) {
          branchTerm = branch.cond;
        } else {
          branchTerm = cx.rexBuilder().makeCall(SqlStdOperatorTable.AND, branch.cond, branch.value);
        }
        terms.add(RexUtil.andNot(cx.rexBuilder(), branchTerm, notTerms));
      }
      if (booleanBranches && useBranch) {
        // we are safe to ignore this branch because for boolean true branches:
        // a || (b && !a) === a || b
      } else {
        notTerms.add(branch.cond);
      }
    }
    return RexUtil.composeDisjunction(cx.rexBuilder(), terms);
  }

  /** Helper for {@link #COALESCE};
   * implements {@link org.apache.calcite.rex.RexRule.RexCallTransformer}. */
  static RexNode simplifyCoalesce(RexRule.Context cx, RexCall call) {
    final RexBuilder rexBuilder = cx.rexBuilder();
    final Set<RexNode> operandSet = new HashSet<>();
    final List<RexNode> operands = new ArrayList<>();
    for (RexNode operand : call.getOperands()) {
      operand = cx.simplify(operand, UNKNOWN);
      if (!RexUtil.isNull(operand)
          && operandSet.add(operand)) {
        operands.add(operand);
      }
      if (!operand.getType().isNullable()) {
        break;
      }
    }
    switch (operands.size()) {
    case 0:
      return rexBuilder.makeNullLiteral(call.type);
    case 1:
      return operands.get(0);
    default:
      if (operands.equals(call.operands)) {
        return call;
      }
      return call.clone(call.type, operands);
    }
  }

  /**
   * If boolean is true, simplify cond in input branch and return new branch.
   * Otherwise, simply return input branch.
   */
  private static CaseBranch generateBranch(boolean simplifyCond,
      RexRule.Context simplifier, CaseBranch branch) {
    if (simplifyCond) {
      // the previous branch was merged, time to simplify it and
      // add it to the final result
      return new CaseBranch(
          simplifier.simplify(branch.cond, RexUnknownAs.FALSE), branch.value);
    }
    return branch;
  }

  /** Object to describe a CASE branch. */
  static final class CaseBranch {
    private final RexNode cond;
    private final RexNode value;

    CaseBranch(RexNode cond, RexNode value) {
      this.cond = cond;
      this.value = value;
    }

    @Override public String toString() {
      return cond + " => " + value;
    }

    /** Given "CASE WHEN p1 THEN v1 ... ELSE e END"
     * returns [(p1, v1), ..., (true, e)]. */
    private static List<CaseBranch> fromCaseOperands(RexBuilder rexBuilder,
        List<RexNode> operands) {
      List<CaseBranch> ret = new ArrayList<>();
      for (int i = 0; i < operands.size() - 1; i += 2) {
        ret.add(new CaseBranch(operands.get(i), operands.get(i + 1)));
      }
      ret.add(new CaseBranch(rexBuilder.makeLiteral(true), Util.last(operands)));
      return ret;
    }

    private static List<RexNode> toCaseOperands(List<CaseBranch> branches) {
      List<RexNode> ret = new ArrayList<>();
      for (int i = 0; i < branches.size() - 1; i++) {
        CaseBranch branch = branches.get(i);
        ret.add(branch.cond);
        ret.add(branch.value);
      }
      CaseBranch lastBranch = Util.last(branches);
      assert lastBranch.cond.isAlwaysTrue();
      ret.add(lastBranch.value);
      return ret;
    }
  }

}
