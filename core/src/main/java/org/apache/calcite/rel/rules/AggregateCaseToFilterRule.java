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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Rule that converts CASE-style filtered aggregates into true filtered
 * aggregates.
 *
 * <p>For example,
 *
 * <blockquote>
 *   <tt>SELECT SUM(CASE WHEN gender = 'F' THEN salary END)<br>
 *   FROM Emp</tt>
 * </blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 *   <tt>SELECT SUM(salary) FILTER(WHERE gender = 'F')<br>
 *   FROM Emp</tt>
 * </blockquote>
 */
public class AggregateCaseToFilterRule extends RelOptRule {
  public static final AggregateCaseToFilterRule INSTANCE =
      new AggregateCaseToFilterRule(RelFactories.LOGICAL_BUILDER, null);

  /** Creates an AggregateCaseToFilterRule. */
  protected AggregateCaseToFilterRule(RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand(Aggregate.class, operand(Project.class, any())),
        relBuilderFactory, description);
  }

  @Override public boolean matches(final RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (isOneArgAggregateCall(aggregateCall)
          && isThreeArgCase(
              project.getProjects()
                  .get(Iterables.getOnlyElement(aggregateCall.getArgList())))) {
        return true;
      }
    }

    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<AggregateCall> newCalls =
        new ArrayList<>(aggregate.getAggCallList().size());
    final List<RexNode> newProjects = new ArrayList<>(project.getProjects());
    final List<RexNode> newCasts = new ArrayList<>();

    for (int fieldNumber : aggregate.getGroupSet()) {
      newCasts.add(
          rexBuilder.makeInputRef(
              project.getProjects().get(fieldNumber).getType(), fieldNumber));
    }

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      AggregateCall newCall =
          transform(aggregateCall, project, newProjects);

      newCalls.add(newCall == null ? aggregateCall : newCall);

      // Possibly CAST the new aggregator to an appropriate type.
      final int i = newCasts.size();
      final RelDataType oldType =
          aggregate.getRowType().getFieldList().get(i).getType();
      if (newCall == null) {
        newCasts.add(rexBuilder.makeInputRef(oldType, i));
      } else {
        newCasts.add(
            rexBuilder.makeCast(oldType,
                rexBuilder.makeInputRef(newCall.getType(), i)));
      }
    }

    if (!newCalls.equals(aggregate.getAggCallList())) {
      final RelBuilder relBuilder = call
          .builder()
          .push(project.getInput())
          .project(newProjects);

      final RelBuilder.GroupKey groupKey = relBuilder.groupKey(
          aggregate.getGroupSet(),
          aggregate.getGroupSets()
      );

      final RelNode newAggregate =
          relBuilder.aggregate(groupKey, newCalls).project(newCasts).build();

      call.transformTo(newAggregate);
      call.getPlanner().setImportance(aggregate, 0.0);
    }
  }

  private @Nullable AggregateCall transform(AggregateCall aggregateCall,
      Project project, List<RexNode> newProjects) {
    final RelOptCluster cluster = project.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    if (!isOneArgAggregateCall(aggregateCall)) {
      return null;
    }
    final List<RexNode> projects = project.getProjects();
    final RexNode rexNode =
        projects.get(Iterables.getOnlyElement(aggregateCall.getArgList()));

    if (!isThreeArgCase(rexNode)) {
      return null;
    }
    final RexCall caseCall = (RexCall) rexNode;

    // If one arg is null and the other is not, reverse them and set "flip",
    // which negates the filter.
    final boolean flip = RexLiteral.isNullLiteral(caseCall.getOperands().get(1))
        && !RexLiteral.isNullLiteral(caseCall.getOperands().get(2));
    final RexNode arg1 = caseCall.getOperands().get(flip ? 2 : 1);
    final RexNode arg2 = caseCall.getOperands().get(flip ? 1 : 2);

    // Operand 1: Filter
    final SqlPostfixOperator op =
        flip ? SqlStdOperatorTable.IS_FALSE : SqlStdOperatorTable.IS_TRUE;
    final RexNode filterFromCase =
        rexBuilder.makeCall(op, caseCall.getOperands().get(0));

    // Combine the CASE filter with an honest-to-goodness SQL FILTER, if the
    // latter is present.
    final RexNode filter;
    if (aggregateCall.filterArg >= 0) {
      filter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
          projects.get(aggregateCall.filterArg), filterFromCase);
    } else {
      filter = filterFromCase;
    }

    if (aggregateCall.isDistinct()) {
      // Just one style supported:
      //   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
      // =>
      //   COUNT(DISTINCT y) FILTER(WHERE x = 'foo')

      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT
          && RexLiteral.isNullLiteral(arg2)) {
        newProjects.add(arg1);
        newProjects.add(filter);
        return AggregateCall.create(SqlStdOperatorTable.COUNT, true, false,
            false, ImmutableList.of(newProjects.size() - 2),
            newProjects.size() - 1, RelCollations.EMPTY,
            aggregateCall.getType(), aggregateCall.getName());
      }
    } else {
      // Four styles supported:
      //
      // A1: AGG(CASE WHEN x = 'foo' THEN cnt END)
      //   => operands (x = 'foo', cnt, null)
      // A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
      //   => operands (x = 'foo', cnt, 0); must be SUM
      // B: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
      //   => operands (x = 'foo', 1, 0); must be SUM
      // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
      //   => operands (x = 'foo', 'dummy', null)

      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT
          && arg1.isA(SqlKind.LITERAL)
          && !RexLiteral.isNullLiteral(arg1)
          && RexLiteral.isNullLiteral(arg2)) {
        // Case C
        newProjects.add(filter);
        return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
            false, ImmutableList.of(), newProjects.size() - 1,
            RelCollations.EMPTY, aggregateCall.getType(),
            aggregateCall.getName());
      } else if (aggregateCall.getAggregation().getKind() == SqlKind.SUM
          && isIntLiteral(arg1) && RexLiteral.intValue(arg1) == 1
          && isIntLiteral(arg2) && RexLiteral.intValue(arg2) == 0) {
        // Case B
        newProjects.add(filter);
        final RelDataType dataType =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
            false, ImmutableList.of(), newProjects.size() - 1,
            RelCollations.EMPTY, dataType, aggregateCall.getName());
      } else if (RexLiteral.isNullLiteral(arg2) /* Case A1 */
          || (aggregateCall.getAggregation().getKind() == SqlKind.SUM
              && isIntLiteral(arg2)
              && RexLiteral.intValue(arg2) == 0) /* Case A2 */) {
        newProjects.add(arg1);
        newProjects.add(filter);
        return AggregateCall.create(aggregateCall.getAggregation(), false,
            false, false, ImmutableList.of(newProjects.size() - 2),
            newProjects.size() - 1, RelCollations.EMPTY,
            aggregateCall.getType(), aggregateCall.getName());
      }
    }
    return null;
  }

  private static boolean isOneArgAggregateCall(AggregateCall aggregateCall) {
    return aggregateCall.getArgList().size() == 1;
  }

  private static boolean isThreeArgCase(final RexNode rexNode) {
    return rexNode.getKind() == SqlKind.CASE
        && ((RexCall) rexNode).getOperands().size() == 3;
  }

  private static boolean isIntLiteral(final RexNode rexNode) {
    return rexNode instanceof RexLiteral
        && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName());
  }
}

// End AggregateCaseToFilterRule.java
