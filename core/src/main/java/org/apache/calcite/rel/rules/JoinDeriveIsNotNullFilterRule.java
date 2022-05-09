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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that derives IS NOT NULL filter from inner join.
 */
@Value.Enclosing
public class JoinDeriveIsNotNullFilterRule
    extends RelRule<JoinDeriveIsNotNullFilterRule.Config> implements TransformationRule {

  public JoinDeriveIsNotNullFilterRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    final RelMetadataQuery mq = call.getMetadataQuery();

    final RexNode condition = join.getCondition();
    final List<RexNode> nodes = RelOptUtil.conjunctions(condition);
    final ImmutableBitSet.Builder notNullableKeys = ImmutableBitSet.builder();
    nodes.forEach(node -> {
      if (Strong.isStrong(node)) {
        notNullableKeys.addAll(RelOptUtil.InputFinder.bits(node));
      }
    });

    final ImmutableBitSet.Builder leftKeys = ImmutableBitSet.builder();
    final ImmutableBitSet.Builder rightKeys = ImmutableBitSet.builder();

    final int offset = join.getLeft().getRowType().getFieldCount();
    notNullableKeys.build().asList().forEach(i -> {
      if (i < offset) {
        leftKeys.set(i);
      } else {
        rightKeys.set(i - offset);
      }
    });

    final RelNode newLeftNode = createIsNotNullFilter(join.getLeft(), leftKeys.build(),
        relBuilder, mq);
    final RelNode newRightNode = createIsNotNullFilter(join.getRight(), rightKeys.build(),
        relBuilder, mq);

    if (newLeftNode != join.getLeft() || newRightNode != join.getRight()) {
      final List<RelNode> inputs = new ArrayList<>(2);
      inputs.add(newLeftNode);
      inputs.add(newRightNode);
      final RelNode newJoin = join.copy(join.getTraitSet(), inputs);
      call.transformTo(newJoin);
    }
  }

  private RelNode createIsNotNullFilter(RelNode input, ImmutableBitSet keys,
      RelBuilder relBuilder, RelMetadataQuery mq) {
    final RelOptPredicateList relOptPredicateList = mq.getPulledUpPredicates(input);
    final RexExecutor executor =
        Util.first(input.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final RexSimplify simplify = new RexSimplify(rexBuilder, relOptPredicateList, executor);
    final List<RexNode> nodes = new ArrayList<>();
    keys.asList().forEach(i -> {
      final RexNode expr = relBuilder
          .call(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(input, i));
      RexNode simplified = simplify.simplify(expr);
      if (!simplified.isAlwaysTrue()) {
        nodes.add(expr);
      }
    });
    return nodes.isEmpty() ? input : relBuilder.push(input).filter(nodes).build();
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable public interface Config extends RelRule.Config {
    ImmutableJoinDeriveIsNotNullFilterRule.Config DEFAULT =
        ImmutableJoinDeriveIsNotNullFilterRule.Config
        .of().withOperandSupplier(
            b -> b.operand(LogicalJoin.class).predicate(
                join -> join.getJoinType() == JoinRelType.INNER
                    && !join.getCondition().isAlwaysTrue())
            .anyInputs());

    @Override default JoinDeriveIsNotNullFilterRule toRule() {
      return new JoinDeriveIsNotNullFilterRule(this);
    }
  }
}
