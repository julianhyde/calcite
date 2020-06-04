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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptNewRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that combines two
 * {@link org.apache.calcite.rel.logical.LogicalFilter}s.
 */
public class FilterMergeRule extends RelOptNewRule
    implements SubstitutionRule {
  public static final FilterMergeRule INSTANCE =
      Config.EMPTY
          .as(Config.class)
          .withOperandFor(Filter.class)
          .toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterMergeRule. */
  protected FilterMergeRule(Config config) {
    super(config);
  }

  @Deprecated
  public FilterMergeRule(RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public FilterMergeRule(RelFactories.FilterFactory filterFactory) {
    this(RelBuilder.proto(Contexts.of(filterFactory)));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter topFilter = call.rel(0);
    final Filter bottomFilter = call.rel(1);

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(bottomFilter.getInput())
        .filter(bottomFilter.getCondition(), topFilter.getCondition());

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelOptNewRule.Config {
    @Override default FilterMergeRule toRule() {
      return new FilterMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass) {
      return withOperandSupplier(b ->
          b.operand(filterClass).oneInput(b2 ->
              b2.operand(filterClass).anyInputs()))
          .as(Config.class);
    }
  }
}
