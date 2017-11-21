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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
public class RelFactories {
  public static final ProjectFactory DEFAULT_PROJECT_FACTORY =
      LogicalProject.FACTORY;

  public static final FilterFactory DEFAULT_FILTER_FACTORY =
      LogicalFilter.FACTORY;

  public static final JoinFactory DEFAULT_JOIN_FACTORY = LogicalJoin.FACTORY;

  public static final CorrelateFactory DEFAULT_CORRELATE_FACTORY =
      new CorrelateFactoryImpl();

  public static final SemiJoinFactory DEFAULT_SEMI_JOIN_FACTORY =
      new SemiJoinFactoryImpl();

  public static final SortFactory DEFAULT_SORT_FACTORY =
      new SortFactoryImpl();

  public static final AggregateFactory DEFAULT_AGGREGATE_FACTORY =
      new AggregateFactoryImpl();

  public static final MatchFactory DEFAULT_MATCH_FACTORY =
      new MatchFactoryImpl();

  public static final SetOpFactory DEFAULT_SET_OP_FACTORY =
      LogicalUnion.FACTORY;

  public static final ValuesFactory DEFAULT_VALUES_FACTORY =
      new ValuesFactoryImpl();

  public static final TableScanFactory DEFAULT_TABLE_SCAN_FACTORY =
      new TableScanFactoryImpl();

  /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
   * create logical relational expressions for everything. */
  public static final RelBuilderFactory LOGICAL_BUILDER =
      RelBuilder.proto(
          Contexts.of(DEFAULT_PROJECT_FACTORY,
              DEFAULT_FILTER_FACTORY,
              DEFAULT_JOIN_FACTORY,
              DEFAULT_SEMI_JOIN_FACTORY,
              DEFAULT_SORT_FACTORY,
              DEFAULT_AGGREGATE_FACTORY,
              DEFAULT_MATCH_FACTORY,
              DEFAULT_SET_OP_FACTORY,
              DEFAULT_VALUES_FACTORY,
              DEFAULT_TABLE_SCAN_FACTORY));

  private RelFactories() {
  }

  /**
   * Can create a
   * {@link org.apache.calcite.rel.logical.LogicalProject} of the
   * appropriate type for this rule's calling convention.
   */
  public interface ProjectFactory extends RelFactory<Project> {
    /** Creates a project. */
    RelNode createProject(RelNode input, List<? extends RexNode> childExprs,
        List<String> fieldNames);
  }

  /** Partial implementation of {@link ProjectFactory}. */
  public abstract static class ProjectFactoryImpl implements ProjectFactory {
    public RelNode copy(Project project) {
      return createProject(project.getInput(), project.getProjects(),
          project.getRowType().getFieldNames());
    }

    protected RelTraitSet traits(RelNode input) {
      return input.getCluster().traitSetOf(Convention.NONE);
    }
  }

  /**
   * Can create a {@link Sort} of the appropriate type
   * for this rule's calling convention.
   */
  public interface SortFactory extends RelFactory<Sort> {
    /** Creates a sort. */
    RelNode createSort(RelNode input, RelCollation collation, RexNode offset,
        RexNode fetch);

    @Deprecated // to be removed before 2.0
    RelNode createSort(RelTraitSet traits, RelNode input,
        RelCollation collation, RexNode offset, RexNode fetch);
  }

  /**
   * Implementation of {@link RelFactories.SortFactory} that
   * returns a vanilla {@link Sort}.
   */
  private static class SortFactoryImpl implements SortFactory {
    public RelNode createSort(RelNode input, RelCollation collation,
        RexNode offset, RexNode fetch) {
      return LogicalSort.create(input, collation, offset, fetch);
    }

    @Deprecated // to be removed before 2.0
    public RelNode createSort(RelTraitSet traits, RelNode input,
        RelCollation collation, RexNode offset, RexNode fetch) {
      return createSort(input, collation, offset, fetch);
    }

    public RelNode copy(Sort sort) {
      return createSort(sort.getInput(), sort.collation, sort.offset,
          sort.fetch);
    }
  }

  /**
   * Can create a {@link SetOp} for a particular kind of
   * set operation (UNION, EXCEPT, INTERSECT) and of the appropriate type
   * for this rule's calling convention.
   */
  public interface SetOpFactory extends RelFactory<SetOp> {
    /** Creates a set operation. */
    RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all);
  }

  /**
   * Implementation of {@link RelFactories.SetOpFactory} that
   * returns a vanilla {@link SetOp} for the particular kind of set
   * operation (UNION, EXCEPT, INTERSECT).
   */
  public abstract static class SetOpFactoryImpl implements SetOpFactory {
    public RelNode copy(SetOp setOp) {
      return createSetOp(setOp.kind, setOp.inputs, setOp.all);
    }

    protected RelTraitSet traits(List<RelNode> inputs) {
      final RelOptCluster cluster = inputs.get(0).getCluster();
      return cluster.traitSetOf(Convention.NONE);
    }
  }

  /**
   * Can create a {@link LogicalAggregate} of the appropriate type
   * for this rule's calling convention.
   */
  public interface AggregateFactory extends RelFactory<Aggregate> {
    /** Creates an aggregate. */
    RelNode createAggregate(RelNode input, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls);
  }

  /**
   * Implementation of {@link RelFactories.AggregateFactory}
   * that returns a vanilla {@link LogicalAggregate}.
   */
  private static class AggregateFactoryImpl implements AggregateFactory {
    @SuppressWarnings("deprecation")
    public RelNode createAggregate(RelNode input, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
      return LogicalAggregate.create(input, indicator,
          groupSet, groupSets, aggCalls);
    }

    public RelNode copy(Aggregate aggregate) {
      return createAggregate(aggregate.getInput(), aggregate.indicator,
          aggregate.groupSet, aggregate.groupSets, aggregate.aggCalls);
    }
  }

  /**
   * Can create a {@link LogicalFilter} of the appropriate type
   * for this rule's calling convention.
   */
  public interface FilterFactory extends RelFactory<Filter> {
    /** Creates a filter. */
    RelNode createFilter(RelNode input, RexNode condition);
  }

  /** Partial implementation of {@link FilterFactory}. */
  public abstract static class FilterFactoryImpl implements FilterFactory {
    public RelNode copy(Filter filter) {
      return createFilter(filter.getInput(), filter.condition);
    }

    /** Derives a trait set for a filter with a given input. */
    protected RelTraitSet traits(final RelNode input) {
      final RelOptCluster cluster = input.getCluster();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      return cluster.traitSetOf(Convention.NONE)
          .replaceIfs(RelCollationTraitDef.INSTANCE,
              new Supplier<List<RelCollation>>() {
                public List<RelCollation> get() {
                  return RelMdCollation.filter(mq, input);
                }
              })
          .replaceIf(RelDistributionTraitDef.INSTANCE,
              new Supplier<RelDistribution>() {
                public RelDistribution get() {
                  return RelMdDistribution.filter(mq, input);
                }
              });
    }
  }

  /**
   * Can create a join of the appropriate type for a rule's calling convention.
   *
   * <p>The result is typically a {@link Join}.
   */
  public interface JoinFactory extends RelFactory<Join> {
    /**
     * Creates a join.
     *
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param variablesSet     Set of variables that are set by the
     *                         LHS and used by the RHS and are not available to
     *                         nodes above this LogicalJoin in the tree
     * @param joinType         Join type
     * @param semiJoinDone     Whether this join has been translated to a
     *                         semi-join
     */
    RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType,
        boolean semiJoinDone);

    @Deprecated // to be removed before 2.0
    RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone);
  }

  /**
   * Implementation of {@link JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  public abstract static class JoinFactoryImpl implements JoinFactory {
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone) {
      return createJoin(left, right, condition,
          CorrelationId.setOf(variablesStopped), joinType, semiJoinDone);
    }

    public RelNode copy(Join join) {
      return createJoin(join.getLeft(), join.getRight(), join.condition,
          join.variablesSet, join.joinType, join.isSemiJoinDone());
    }

    protected RelTraitSet traits(RelNode left, RelNode right) {
      return left.getCluster().traitSetOf(Convention.NONE);
    }
  }

  /**
   * Can create a correlate of the appropriate type for a rule's calling
   * convention.
   *
   * <p>The result is typically a {@link Correlate}.
   */
  public interface CorrelateFactory extends RelFactory<Correlate> {
    /**
     * Creates a correlate.
     *
     * @param left             Left input
     * @param right            Right input
     * @param correlationId    Variable name for the row of left input
     * @param requiredColumns  Required columns
     * @param joinType         Join type
     */
    RelNode createCorrelate(RelNode left, RelNode right,
        CorrelationId correlationId, ImmutableBitSet requiredColumns,
        SemiJoinType joinType);
  }

  /**
   * Implementation of {@link CorrelateFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate}.
   */
  private static class CorrelateFactoryImpl implements CorrelateFactory {
    public RelNode createCorrelate(RelNode left, RelNode right,
        CorrelationId correlationId, ImmutableBitSet requiredColumns,
        SemiJoinType joinType) {
      return LogicalCorrelate.create(left, right, correlationId,
          requiredColumns, joinType);
    }

    public RelNode copy(Correlate correlate) {
      return createCorrelate(correlate.getLeft(), correlate.getRight(),
          correlate.correlationId, correlate.requiredColumns,
          correlate.getJoinType());
    }
  }

  /**
   * Can create a semi-join of the appropriate type for a rule's calling
   * convention.
   */
  public interface SemiJoinFactory extends RelFactory<SemiJoin> {
    /**
     * Creates a semi-join.
     *
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     */
    RelNode createSemiJoin(RelNode left, RelNode right, RexNode condition);
  }

  /**
   * Implementation of {@link SemiJoinFactory} that returns a vanilla
   * {@link SemiJoin}.
   */
  private static class SemiJoinFactoryImpl implements SemiJoinFactory {
    public RelNode createSemiJoin(RelNode left, RelNode right,
        RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      return SemiJoin.create(left, right,
        condition, joinInfo.leftKeys, joinInfo.rightKeys);
    }

    public RelNode copy(SemiJoin semiJoin) {
      return createSemiJoin(semiJoin.getLeft(), semiJoin.getRight(),
          semiJoin.getCondition());
    }
  }

  /**
   * Can create a {@link Values} of the appropriate type for a rule's calling
   * convention.
   */
  public interface ValuesFactory extends RelFactory<Values> {
    /**
     * Creates a Values.
     */
    RelNode createValues(RelOptCluster cluster, RelDataType rowType,
        List<ImmutableList<RexLiteral>> tuples);
  }

  /**
   * Implementation of {@link ValuesFactory} that returns a
   * {@link LogicalValues}.
   */
  private static class ValuesFactoryImpl implements ValuesFactory {
    public RelNode createValues(RelOptCluster cluster, RelDataType rowType,
        List<ImmutableList<RexLiteral>> tuples) {
      return LogicalValues.create(cluster, rowType,
          ImmutableList.copyOf(tuples));
    }

    public RelNode copy(Values values) {
      return createValues(values.getCluster(), values.getRowType(),
          values.getTuples());
    }
  }

  /**
   * Can create a {@link TableScan} of the appropriate type for a rule's calling
   * convention.
   */
  public interface TableScanFactory extends RelFactory<TableScan> {
    /**
     * Creates a {@link TableScan}.
     */
    RelNode createScan(RelOptCluster cluster, RelOptTable table);
  }

  /**
   * Implementation of {@link TableScanFactory} that returns a
   * {@link LogicalTableScan}.
   */
  private static class TableScanFactoryImpl implements TableScanFactory {
    public RelNode createScan(RelOptCluster cluster, RelOptTable table) {
      return LogicalTableScan.create(cluster, table);
    }

    public RelNode copy(TableScan tableScan) {
      return createScan(tableScan.getCluster(), tableScan.getTable());
    }
  }

  /**
   * Can create a {@link Match} of
   * the appropriate type for a rule's calling convention.
   */
  public interface MatchFactory extends RelFactory<Match> {
    /** Creates a {@link Match}. */
    RelNode createMatch(RelNode input, RexNode pattern,
        RelDataType rowType, boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys,
        RexNode interval);
  }

  /**
   * Implementation of {@link MatchFactory}
   * that returns a {@link LogicalMatch}.
   */
  private static class MatchFactoryImpl implements MatchFactory {
    public RelNode createMatch(RelNode input, RexNode pattern,
        RelDataType rowType, boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, List<RexNode> partitionKeys, RelCollation orderKeys,
        RexNode interval) {
      return LogicalMatch.create(input, rowType, pattern, strictStart,
          strictEnd, patternDefinitions, measures, after, subsets, allRows,
          partitionKeys, orderKeys, interval);
    }

    public RelNode copy(Match match) {
      return createMatch(match.getInput(), match.getPattern(),
          match.getRowType(), match.isStrictStart(), match.isStrictEnd(),
          match.patternDefinitions, match.measures, match.after, match.subsets,
          match.isAllRows(), match.partitionKeys, match.orderKeys,
          match.interval);
    }
  }
}

// End RelFactories.java
