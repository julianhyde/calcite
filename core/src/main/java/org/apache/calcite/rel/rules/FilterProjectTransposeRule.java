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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import java.util.Collections;
import java.util.function.Predicate;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Project}.
 */
public class FilterProjectTransposeRule extends RelOptNewRule
    implements TransformationRule {
  /** The default instance of
   * {@link org.apache.calcite.rel.rules.FilterProjectTransposeRule}.
   *
   * <p>It does not allow a Filter to be pushed past the Project if
   * {@link RexUtil#containsCorrelation there is a correlation condition})
   * anywhere in the Filter, since in some cases it can prevent a
   * {@link org.apache.calcite.rel.core.Correlate} from being de-correlated.
   */
  public static final FilterProjectTransposeRule INSTANCE =
      Config.EMPTY
          .as(Config.class)
          .withOperandFor(Filter.class,
              f -> !RexUtil.containsCorrelation(f.getCondition()),
              Project.class, p -> true)
          .withCopyFilter(true)
          .withCopyProject(true)
          .toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterProjectTransposeRule. */
  protected FilterProjectTransposeRule(Config config) {
    super(config);
  }

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>Equivalent to the rule created by
   * {@link #FilterProjectTransposeRule(Class, Predicate, Class, Predicate, boolean, boolean, RelBuilderFactory)}
   * with some default predicates that do not allow a filter to be pushed
   * past the project if there is a correlation condition anywhere in the
   * filter (since in some cases it can prevent a
   * {@link org.apache.calcite.rel.core.Correlate} from being de-correlated).
   */
  @Deprecated
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass,
      boolean copyFilter, boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(filterClass,
            f -> !RexUtil.containsCorrelation(f.getCondition()),
            projectClass, project -> true)
        .withCopyFilter(copyFilter)
        .withCopyProject(copyProject));
  }

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>If {@code copyFilter} is true, creates the same kind of Filter as
   * matched in the rule, otherwise it creates a Filter using the RelBuilder
   * obtained by the {@code relBuilderFactory}.
   * Similarly for {@code copyProject}.
   *
   * <p>Defining predicates for the Filter (using {@code filterPredicate})
   * and/or the Project (using {@code projectPredicate} allows making the rule
   * more restrictive.
   */
  @Deprecated
  public <F extends Filter, P extends Project> FilterProjectTransposeRule(
      Class<F> filterClass,
      Predicate<? super F> filterPredicate,
      Class<P> projectClass,
      Predicate<? super P> projectPredicate,
      boolean copyFilter, boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b ->
            b.operand(filterClass).predicate(filterPredicate)
                .oneInput(b2 ->
                    b2.operand(projectClass).predicate(projectPredicate)
                        .anyInputs()))
            .as(Config.class)
            .withCopyFilter(copyFilter)
            .withCopyProject(copyProject));
  }

  @Deprecated // to be removed before 2.0
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Project> projectClass,
      RelFactories.ProjectFactory projectFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(RelBuilder.proto(filterFactory, projectFactory))
        .withOperandSupplier(b ->
            b.operand(filterClass)
                .predicate(filter ->
                    !RexUtil.containsCorrelation(filter.getCondition()))
                .oneInput(b2 ->
                    b2.operand(projectClass)
                        .predicate(project -> true)
                        .anyInputs()))
        .as(Config.class)
        .withCopyFilter(filterFactory == null)
        .withCopyProject(projectFactory == null));
  }

  @Deprecated
  protected FilterProjectTransposeRule(
      RelOptRuleOperand operand,
      boolean copyFilter,
      boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(INSTANCE.config
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class)
        .withCopyFilter(copyFilter)
        .withCopyProject(copyProject));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Config config() {
    return (Config) config;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project project = call.rel(1);

    if (RexOver.containsOver(project.getProjects(), null)) {
      // In general a filter cannot be pushed below a windowing calculation.
      // Applying the filter before the aggregation function changes
      // the results of the windowing invocation.
      //
      // When the filter is on the PARTITION BY expression of the OVER clause
      // it can be pushed down. For now we don't support this.
      return;
    }
    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushPastProject(filter.getCondition(), project);

    final RelBuilder relBuilder = call.builder();
    RelNode newFilterRel;
    if (config().isCopyFilter()) {
      final RelNode input = project.getInput();
      final RelTraitSet traitSet = filter.getTraitSet()
          .replaceIfs(RelCollationTraitDef.INSTANCE,
              () -> Collections.singletonList(
                      input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)))
          .replaceIfs(RelDistributionTraitDef.INSTANCE,
              () -> Collections.singletonList(
                      input.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE)));
      newCondition = RexUtil.removeNullabilityCast(relBuilder.getTypeFactory(), newCondition);
      newFilterRel = filter.copy(traitSet, input, newCondition);
    } else {
      newFilterRel =
          relBuilder.push(project.getInput()).filter(newCondition).build();
    }

    RelNode newProjRel =
        config().isCopyProject()
            ? project.copy(project.getTraitSet(), newFilterRel,
                project.getProjects(), project.getRowType())
            : relBuilder.push(newFilterRel)
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build();

    call.transformTo(newProjRel);
  }

  /** Rule configuration.
   *
   * <p>If {@code copyFilter} is true, creates the same kind of Filter as
   * matched in the rule, otherwise it creates a Filter using the RelBuilder
   * obtained by the {@code relBuilderFactory}.
   * Similarly for {@code copyProject}.
   *
   * <p>Defining predicates for the Filter (using {@code filterPredicate})
   * and/or the Project (using {@code projectPredicate} allows making the rule
   * more restrictive. */
  public interface Config extends RelOptNewRule.Config {
    @Override default FilterProjectTransposeRule toRule() {
      return new FilterProjectTransposeRule(this);
    }

    /** Whether to create a {@link Filter} of the same convention as the
     * matched Filter. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isCopyFilter();

    /** Sets {@link #isCopyFilter()}. */
    Config withCopyFilter(boolean copyFilter);

    /** Whether to create a {@link Project} of the same convention as the
     * matched Project. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isCopyProject();

    /** Sets {@link #isCopyProject()}. */
    Config withCopyProject(boolean copyProject);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Predicate<Filter> filterPredicate,
        Class<? extends Project> projectClass,
        Predicate<Project> projectPredicate) {
      return withOperandSupplier(b ->
          b.operand(filterClass).predicate(filterPredicate).oneInput(b2 ->
              b2.operand(projectClass).predicate(projectPredicate).anyInputs()))
          .as(Config.class);
    }
  }
}
