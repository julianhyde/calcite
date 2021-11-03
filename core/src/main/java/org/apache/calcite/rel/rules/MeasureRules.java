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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Collection of planner rules that deal with measures.
 *
 * <p>A typical rule pushes down {@code M2V(measure)}
 * until it reaches a {@code V2M(expression)}.
 *
 * @see org.apache.calcite.sql.fun.SqlInternalOperators#M2V
 * @see org.apache.calcite.sql.fun.SqlInternalOperators#V2M
 */
public abstract class MeasureRules {

  private MeasureRules() { }

  /** Rule that matches a {@link Filter} that contains a {@code M2V} call
   * on top of a {@link Sort} and pushes down the {@code M2V} call. */
  public static final RelOptRule FILTER_SORT =
      FilterSortMeasureRule.SortMeasureRuleConfig.DEFAULT
          .as(FilterSortMeasureRule.SortMeasureRuleConfig.class)
          .toRule();

  /** Rule that ...
   *
   * @see MeasureRules#FILTER_SORT */
  @SuppressWarnings("WeakerAccess")
  public static class FilterSortMeasureRule
      extends RelRule<FilterSortMeasureRule.SortMeasureRuleConfig>
      implements TransformationRule {
    /** Creates a FilterSortMeasureRule. */
    protected FilterSortMeasureRule(SortMeasureRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RexNode condition = filter.getCondition();
      if (condition.equals(filter.getCondition())) {
        return;
      }
      final RelBuilder relBuilder =
          relBuilderFactory.create(filter.getCluster(), null);
      relBuilder.push(filter.getInput())
          .filter(condition);
      call.transformTo(relBuilder.build());
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface SortMeasureRuleConfig extends Config {
      SortMeasureRuleConfig DEFAULT = ImmutableSortMeasureRuleConfig.of()
          .withOperandSupplier(b ->
              b.operand(Filter.class)
                  .oneInput(b2 -> b2.operand(Sort.class)
                      .anyInputs()));

      @Override default FilterSortMeasureRule toRule() {
        return new FilterSortMeasureRule(this);
      }
    }
  }

  /** Rule that matches a {@link Project} that contains a {@code M2V} call
   * on top of a {@link Sort} and pushes down the {@code M2V} call. */
  public static final RelOptRule PROJECT_SORT =
      ProjectSortMeasureRuleConfig.DEFAULT
          .as(ProjectSortMeasureRuleConfig.class)
          .toRule();

  /** Rule that ...
   *
   * @see MeasureRules#PROJECT_SORT */
  @SuppressWarnings("WeakerAccess")
  public static class ProjectSortMeasureRule
      extends RelRule<ProjectSortMeasureRuleConfig>
      implements TransformationRule {
    /** Creates a ProjectSortMeasureRule. */
    protected ProjectSortMeasureRule(ProjectSortMeasureRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelBuilder relBuilder = call.builder();
      relBuilder.push(project.getInput());
      call.transformTo(relBuilder.build());
    }
  }

  /** Configuration for {@link ProjectSortMeasureRule}. */
  @Value.Immutable
  public interface ProjectSortMeasureRuleConfig extends RelRule.Config {
    ProjectSortMeasureRuleConfig DEFAULT =
        ImmutableProjectSortMeasureRuleConfig.of().withOperandSupplier(b ->
            b.operand(Project.class)
                .oneInput(b2 -> b2.operand(Sort.class)
                    .anyInputs()));

    @Override default ProjectSortMeasureRule toRule() {
      return new ProjectSortMeasureRule(this);
    }
  }

}
