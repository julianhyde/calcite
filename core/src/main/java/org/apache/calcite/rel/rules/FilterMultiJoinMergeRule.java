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

import org.apache.calcite.plan.RelOptNewRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Planner rule that merges a
 * {@link Filter} into a {@link MultiJoin},
 * creating a richer {@code MultiJoin}.
 *
 * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
 */
public class FilterMultiJoinMergeRule extends RelOptNewRule
    implements TransformationRule {
  public static final FilterMultiJoinMergeRule INSTANCE =
      Config.EMPTY.as(Config.class)
          .withOperandFor(Filter.class, MultiJoin.class)
          .toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterMultiJoinMergeRule. */
  protected FilterMultiJoinMergeRule(Config config) {
    super(config);
  }

  @Deprecated
  public FilterMultiJoinMergeRule(RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated
  public FilterMultiJoinMergeRule(Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(filterClass, MultiJoin.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Config config() {
    return (Config) config;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    MultiJoin multiJoin = call.rel(1);

    // Create a new post-join filter condition
    // Conditions are nullable, so ImmutableList can't be used here
    List<RexNode> filters = Arrays.asList(
        filter.getCondition(),
        multiJoin.getPostJoinFilter());

    final RexBuilder rexBuilder = multiJoin.getCluster().getRexBuilder();
    MultiJoin newMultiJoin =
        new MultiJoin(
            multiJoin.getCluster(),
            multiJoin.getInputs(),
            multiJoin.getJoinFilter(),
            multiJoin.getRowType(),
            multiJoin.isFullOuterJoin(),
            multiJoin.getOuterJoinConditions(),
            multiJoin.getJoinTypes(),
            multiJoin.getProjFields(),
            multiJoin.getJoinFieldRefCountsMap(),
            RexUtil.composeConjunction(rexBuilder, filters, true));

    call.transformTo(newMultiJoin);
  }

  /** Rule configuration. */
  public interface Config extends RelOptNewRule.Config {
    @Override default FilterMultiJoinMergeRule toRule() {
      return new FilterMultiJoinMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends MultiJoin> multiJoinClass) {
      return withOperandSupplier(b ->
          b.operand(filterClass).oneInput(b2 ->
              b2.operand(multiJoinClass).anyInputs()))
          .as(Config.class);
    }
  }
}
