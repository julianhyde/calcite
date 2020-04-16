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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/** Rule that matches Aggregate. */
public class MaterializedViewOnlyAggregateRule
    extends MaterializedViewAggregateRule {

  public static final MaterializedViewOnlyAggregateRule INSTANCE =
      makeConfig(RelFactories.LOGICAL_BUILDER)
          .withOperandSupplier(b -> b.operand(Aggregate.class).anyInputs())
          .withDescription("MaterializedViewAggregateRule(Aggregate)")
          .as(MaterializedViewRule.Config.class)
          .withGenerateUnionRewriting(true)
          .withUnionRewritingPullProgram(null)
          .withFastBailOut(false)
          .as(MaterializedViewOnlyAggregateRule.Config.class)
          .toRule();

  private MaterializedViewOnlyAggregateRule(Config config) {
    super(config);
  }

  @Deprecated
  public MaterializedViewOnlyAggregateRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
    this(INSTANCE.config()
        .withGenerateUnionRewriting(generateUnionRewriting)
        .withUnionRewritingPullProgram(unionRewritingPullProgram)
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated
  public MaterializedViewOnlyAggregateRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      RelOptRule filterProjectTransposeRule,
      RelOptRule filterAggregateTransposeRule,
      RelOptRule aggregateProjectPullUpConstantsRule,
      RelOptRule projectMergeRule) {
    this(INSTANCE.config()
        .withGenerateUnionRewriting(generateUnionRewriting)
        .withUnionRewritingPullProgram(unionRewritingPullProgram)
        .withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class)
        .withFilterProjectTransposeRule(filterProjectTransposeRule)
        .withFilterAggregateTransposeRule(filterAggregateTransposeRule)
        .withAggregateProjectPullUpConstantsRule(
            aggregateProjectPullUpConstantsRule)
        .withProjectMergeRule(projectMergeRule)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    perform(call, null, aggregate);
  }

  /** Rule configuration. */
  public interface Config extends MaterializedViewAggregateRule.Config {
    @Override default MaterializedViewOnlyAggregateRule toRule() {
      return new MaterializedViewOnlyAggregateRule(this);
    }
  }
}
