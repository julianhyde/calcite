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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

  /** Rule that matches an {@link Aggregate}
   * that contains a {@code M2V} call
   * and pushes down the {@code M2V} call into a {@link Project}. */
  public static final RelOptRule AGGREGATE =
      AggregateMeasureRuleConfig.DEFAULT
          .toRule();

  /** Configuration for {@link AggregateMeasureRule}. */
  @Value.Immutable
  public interface AggregateMeasureRuleConfig extends RelRule.Config {
    AggregateMeasureRuleConfig DEFAULT = ImmutableAggregateMeasureRuleConfig.of()
        .withOperandSupplier(b ->
            b.operand(Aggregate.class)
                .predicate(b2 ->
                    b2.getAggCallList().stream().anyMatch(c ->
                        c.getAggregation() == SqlInternalOperators.AGG_M2V))
                .anyInputs());

    @Override default AggregateMeasureRule toRule() {
      return new AggregateMeasureRule(this);
    }
  }

  /** Rule that matches an {@link Aggregate} with at least one call to
   * {@link SqlInternalOperators#AGG_M2V} and converts those calls
   * to {@link SqlInternalOperators#M2X}.
   *
   * <p>Converts
   *
   * <pre>{@code
   * Aggregate(a, b, AGG_M2V(c), SUM(d), AGG_M2V(e))
   *   R
   * }</pre>
   *
   * <p>to
   *
   * <pre>{@code
   * Aggregate(a, b, SINGLE_VALUE(c), SUM(d), SINGLE_VALUE(e))
   *   Project(a, b, c, d, e, M2X(c, SAME_PARTITION(a, b)),
   *        M2X(e, SAME_PARTITION(a, b)))
   *     R
   * }</pre>
   *
   * <p>We rely on those {@code M2X} calls being pushed down until they merge
   * with {@code V2M2} and {@link ProjectMeasureRule} can apply.
   *
   * @see MeasureRules#AGGREGATE
   * @see AggregateMeasureRuleConfig */
  @SuppressWarnings("WeakerAccess")
  public static class AggregateMeasureRule
      extends RelRule<AggregateMeasureRuleConfig>
      implements TransformationRule {
    /** Creates a AggregateMeasureRule. */
    protected AggregateMeasureRule(AggregateMeasureRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final RelBuilder b = call.builder();
      b.push(aggregate.getInput());
      final List<Function<RelBuilder, RelBuilder.AggCall>> aggCallList =
          new ArrayList<>();
      final List<RexNode> extraProjects = new ArrayList<>();
      aggregate.getAggCallList().forEach(c -> {
        if (c.getAggregation().kind == SqlKind.AGG_M2V) {
          final int arg = Iterables.getOnlyElement(c.getArgList());
          final int i = b.fields().size() + extraProjects.size();
          extraProjects.add(
              b.call(SqlInternalOperators.M2X, b.field(arg),
                  b.call(SqlInternalOperators.SAME_PARTITION,
                      b.fields(aggregate.getGroupSet()))));
          aggCallList.add(b2 ->
              b2.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, b2.field(i)));
        } else {
          aggCallList.add(b2 -> b2.aggregateCall(c));
        }
      });
      b.projectPlus(extraProjects);
      b.aggregate(
          b.groupKey(aggregate.getGroupSet(), aggregate.groupSets),
          bind(aggCallList).apply(b));
      call.transformTo(b.build());
    }

    /** Converts a list of functions into a function that returns a list.
     * It is named after the Monad bind operator. */
    private static <T, E> Function<T, List<E>> bind(List<Function<T, E>> list) {
      return t -> {
        final ImmutableList.Builder<E> builder = ImmutableList.builder();
        list.forEach(f -> builder.add(f.apply(t)));
        return builder.build();
      };
    }
  }

  /** Rule that merges an {@link Aggregate}
   * onto a {@code Project} that contains a {@code M2X} call. */
  // TODO rename field and class
  public static final RelOptRule PROJECT =
      ProjectMeasureRuleConfig.DEFAULT
          .toRule();

  /** Configuration for {@link ProjectMeasureRule}. */
  @Value.Immutable
  public interface ProjectMeasureRuleConfig extends RelRule.Config {
    ProjectMeasureRuleConfig DEFAULT = ImmutableProjectMeasureRuleConfig.of()
        .withOperandSupplier(b ->
            b.operand(Aggregate.class)
                .predicate(aggregate ->
                    aggregate.getAggCallList().stream().allMatch(c ->
                        c.getAggregation() == SqlStdOperatorTable.SINGLE_VALUE))
                .oneInput(b2 ->
                    b2.operand(Project.class)
                        .predicate(RexUtil.find(SqlKind.V2M)::inProject)
                        .anyInputs()));

    @Override default ProjectMeasureRule toRule() {
      return new ProjectMeasureRule(this);
    }
  }

  /** Rule that merges an {@link Aggregate} onto a {@link Project}.
   *
   * <p>Converts
   *
   * <pre>{@code
   * Aggregate(a, b, SINGLE_VALUE(d) AS e)
   *   Project(a, b, M2X(M2V(SUM(c) + 1), SAME_PARTITION(a, b)) AS d)
   *     R
   * }</pre>
   *
   * <p>to
   *
   * <pre>{@code
   * Project(a, b, sum_c + 1 AS e),
   *   Aggregate(a, b, SUM(c) AS sum_c)
   *     R
   * }</pre>
   *
   * @see ProjectMeasureRuleConfig */
  @SuppressWarnings("WeakerAccess")
  public static class ProjectMeasureRule
      extends RelRule<ProjectMeasureRuleConfig>
      implements TransformationRule {
    /** Creates a ProjectMeasureRule. */
    protected ProjectMeasureRule(ProjectMeasureRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final RelBuilder b = call.builder();
      b.push(project)
          .aggregateRex(
              b.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets()),
              true,
              Util.transform(aggregate.getAggCallList(),
                  aggregateCall -> toRex(aggregateCall, project)));
      call.transformTo(b.build());
    }

    private RexNode toRex(AggregateCall aggregateCall, Project project) {
      switch (aggregateCall.getAggregation().kind) {
      case SINGLE_VALUE:
        final int arg = Iterables.getOnlyElement(aggregateCall.getArgList());
        final RexNode e = project.getProjects().get(arg);
        switch (e.getKind()) {
        case M2X:
          final RexCall callM2x = (RexCall) e;
          switch (callM2x.operands.get(0).getKind()) {
          case V2M:
            final RexCall callV2m = (RexCall) callM2x.operands.get(0);
            return callV2m.operands.get(0);
          }
        }
      }
      return toRex(aggregateCall);
    }

    private RexNode toRex(AggregateCall aggregateCall) {
      throw new UnsupportedOperationException();
    }
  }

  /** Rule that matches a {@link Filter} that contains a {@code M2V} call
   * on top of a {@link Sort} and pushes down the {@code M2V} call. */
  public static final RelOptRule FILTER_SORT =
      FilterSortMeasureRuleConfig.DEFAULT
          .as(FilterSortMeasureRuleConfig.class)
          .toRule();

  /** Configuration for {@link FilterSortMeasureRule}. */
  @Value.Immutable
  public interface FilterSortMeasureRuleConfig extends RelRule.Config {
    FilterSortMeasureRuleConfig DEFAULT = ImmutableFilterSortMeasureRuleConfig.of()
        .withOperandSupplier(b ->
            b.operand(Filter.class)
                .oneInput(b2 -> b2.operand(Sort.class)
                    .anyInputs()));

    @Override default FilterSortMeasureRule toRule() {
      return new FilterSortMeasureRule(this);
    }
  }

  /** Rule that ...
   *
   * @see MeasureRules#FILTER_SORT
   * @see FilterSortMeasureRuleConfig */
  @SuppressWarnings("WeakerAccess")
  public static class FilterSortMeasureRule
      extends RelRule<FilterSortMeasureRuleConfig>
      implements TransformationRule {
    /** Creates a FilterSortMeasureRule. */
    protected FilterSortMeasureRule(FilterSortMeasureRuleConfig config) {
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
      final Sort sort = call.rel(1);
      final RelBuilder relBuilder = call.builder();

      // Given
      //   Project [$0, 1 + M2V(2)]  (a)
      //     Sort $1 desc
      //       R
      // transform to
      //   Project [$0, 1 + $2]  (b)
      //     Sort $1 desc
      //       Project [$0, $1, M2V(2)]  (c)
      //         R
      //
      // projects is [$0, 1 + M2V(2)] (see a)
      // newProjects is [$0, 1 + $2]
      // map.keys() is [M2V(2)] (see c)

      final List<RexNode> projects = project.getAliasedProjects(relBuilder);
      final Map<RexCall, RexInputRef> map = new LinkedHashMap<>();
      final List<RexNode> newProjects =
          new RexShuttle() {
            @Override public RexNode visitCall(RexCall call) {
              if (call.getKind() == SqlKind.M2V) {
                return map.computeIfAbsent(call, c ->
                    relBuilder.getRexBuilder().makeInputRef(call.getType(),
                        projects.size() + map.size()));
              }
              return super.visitCall(call);
            }
          }.apply(projects);

      relBuilder.push(sort.getInput())
          .projectPlus(map.keySet())
          .sortLimit(sort.offset == null ? 0 : RexLiteral.intValue(sort.offset),
              sort.fetch == null ? -1 : RexLiteral.intValue(sort.fetch),
              sort.getSortExps())
          .project(newProjects);
      call.transformTo(relBuilder.build());
    }
  }

  /** Configuration for {@link ProjectSortMeasureRule}. */
  @Value.Immutable
  public interface ProjectSortMeasureRuleConfig extends RelRule.Config {
    ProjectSortMeasureRuleConfig DEFAULT =
        ImmutableProjectSortMeasureRuleConfig.of().withOperandSupplier(b ->
            b.operand(Project.class)
                .predicate(RexUtil.M2V_FINDER::inProject)
                .oneInput(b2 -> b2.operand(Sort.class)
                    .anyInputs()));

    @Override default ProjectSortMeasureRule toRule() {
      return new ProjectSortMeasureRule(this);
    }
  }

}