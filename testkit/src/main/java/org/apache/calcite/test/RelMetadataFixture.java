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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parameters for a Metadata test.
 */
public class RelMetadataFixture {
  public static final RelMetadataFixture DEFAULT =
      new RelMetadataFixture(SqlToRelTestBase.createTesterStatic(),
          RelSupplier.NONE, false, true, r -> r);

  private final SqlToRelTestBase.Tester tester;
  private final RelSupplier relSupplier;
  private final boolean convertAsCalc;
  private final boolean typeCoercion;
  private final UnaryOperator<RelNode> relTransform;

  private RelMetadataFixture(SqlToRelTestBase.Tester tester,
      RelSupplier relSupplier, boolean convertAsCalc, boolean typeCoercion,
      UnaryOperator<RelNode> relTransform) {
    this.tester = tester;
    this.relSupplier = relSupplier;
    this.convertAsCalc = convertAsCalc;
    this.typeCoercion = typeCoercion;
    this.relTransform = relTransform;
  }

  public RelMetadataFixture sql(String sql) {
    final RelSupplier relSupplier = RelSupplier.of(sql);
    return relSupplier.equals(this.relSupplier) ? this
        : new RelMetadataFixture(tester, relSupplier, convertAsCalc,
            typeCoercion, relTransform);
  }

  public RelMetadataFixture withRelFn(Function<RelBuilder, RelNode> relFn) {
    final RelSupplier relSupplier = RelSupplier.of(relFn);
    return relSupplier.equals(this.relSupplier) ? this
        : new RelMetadataFixture(tester, relSupplier, convertAsCalc,
            typeCoercion, relTransform);
  }

  public RelMetadataFixture withTester(
      UnaryOperator<SqlToRelTestBase.Tester> transform) {
    final SqlToRelTestBase.Tester tester = transform.apply(this.tester);
    return new RelMetadataFixture(tester, relSupplier, convertAsCalc,
        typeCoercion, relTransform);
  }

  public RelMetadataFixture convertingProjectAsCalc() {
    return new RelMetadataFixture(tester, relSupplier, true,
        typeCoercion, relTransform);
  }

  public RelMetadataFixture withCatalogReaderFactory(
      SqlTestFactory.MockCatalogReaderFactory factory) {
    return withTester(t -> t.withCatalogReaderFactory(factory));
  }

  public RelMetadataFixture withClusterFactory(UnaryOperator<RelOptCluster> factory) {
    return withTester(t -> t.withClusterFactory(factory));
  }

  public RelMetadataFixture withTypeCoercion(boolean typeCoercion) {
    return typeCoercion == this.typeCoercion ? this
        : new RelMetadataFixture(tester, relSupplier, convertAsCalc,
            typeCoercion, relTransform);
  }

  public RelMetadataFixture withRelTransform(UnaryOperator<RelNode> relTransform) {
    final UnaryOperator<RelNode> relTransform1 =
        this.relTransform.andThen(relTransform)::apply;
    return new RelMetadataFixture(tester, relSupplier, convertAsCalc,
        typeCoercion, relTransform1);
  }

  public RelMetadataFixture assertCpuCost(Matcher<Double> matcher,
      String reason) {
    RelNode rel = toRel();
    RelOptCost cost = computeRelSelfCost(rel);
    assertThat(reason + "\n"
            + "sql:" + relSupplier + "\n"
            + "plan:" + RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES),
        cost.getCpu(), matcher);
    return this;
  }

  private static RelOptCost computeRelSelfCost(RelNode rel) {
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPlanner planner = new VolcanoPlanner();
    return rel.computeSelfCost(planner, mq);
  }

  public RelMetadataFixture assertRowsUnique(Matcher<Boolean> matcher,
      String reason) {
    return assertRowsUnique(new boolean[]{false, true}, matcher, reason);
  }

  public RelMetadataFixture assertRowsUnique(boolean ignoreNulls,
      Matcher<Boolean> matcher, String reason) {
    return assertRowsUnique(new boolean[]{ignoreNulls}, matcher, reason);
  }

  RelMetadataFixture assertRowsUnique(boolean[] ignoreNulls,
      Matcher<Boolean> matcher, String reason) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    for (boolean ignoreNull : ignoreNulls) {
      Boolean rowsUnique = mq.areRowsUnique(rel, ignoreNull);
      assertThat(reason + "\n"
              + "sql:" + relSupplier + "\n"
              + "plan:" + RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES),
          rowsUnique, matcher);
    }
    return this;
  }

  /** Only for use by RelSupplier. Must be package-private. */
  RelNode sqlToRel(String sql) {
    return tester.enableTypeCoercion(typeCoercion)
        .convertSqlToRel(sql).rel;
  }

  public RelNode toRel() {
    final RelNode rel = relSupplier.apply2(this);
    if (convertAsCalc) {
      Project project = (Project) rel;
      RexProgram program = RexProgram.create(
          project.getInput().getRowType(),
          project.getProjects(),
          null,
          project.getRowType(),
          project.getCluster().getRexBuilder());
      return LogicalCalc.create(project.getInput(), program);
    }
    return relTransform.apply(rel);
  }

  public void checkPercentageOriginalRows(Matcher<Double> matcher) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Double result = mq.getPercentageOriginalRows(rel);
    assertNotNull(result);
    assertThat(result, matcher);
  }

  private Set<RelColumnOrigin> checkColumnOrigin() {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    return mq.getColumnOrigins(rel, 0);
  }

  public void checkNoColumnOrigin() {
    Set<RelColumnOrigin> result = checkColumnOrigin();
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  public static void checkColumnOrigin(
      RelColumnOrigin rco,
      String expectedTableName,
      String expectedColumnName,
      boolean expectedDerived) {
    RelOptTable actualTable = rco.getOriginTable();
    List<String> actualTableName = actualTable.getQualifiedName();
    assertThat(
        Iterables.getLast(actualTableName),
        equalTo(expectedTableName));
    assertThat(
        actualTable.getRowType()
            .getFieldList()
            .get(rco.getOriginColumnOrdinal())
            .getName(),
        equalTo(expectedColumnName));
    assertThat(rco.isDerived(), equalTo(expectedDerived));
  }

  public void checkSingleColumnOrigin(String expectedTableName,
      String expectedColumnName, boolean expectedDerived) {
    Set<RelColumnOrigin> result = checkColumnOrigin();
    assertNotNull(result);
    assertThat(result.size(), is(1));
    RelColumnOrigin rco = result.iterator().next();
    checkColumnOrigin(
        rco, expectedTableName, expectedColumnName, expectedDerived);
  }

  // WARNING:  this requires the two table names to be different
  public void checkTwoColumnOrigin(
      String expectedTableName1,
      String expectedColumnName1,
      String expectedTableName2,
      String expectedColumnName2,
      boolean expectedDerived) {
    Set<RelColumnOrigin> result = checkColumnOrigin();
    assertNotNull(result);
    assertThat(result.size(), is(2));
    for (RelColumnOrigin rco : result) {
      RelOptTable actualTable = rco.getOriginTable();
      List<String> actualTableName = actualTable.getQualifiedName();
      String actualUnqualifiedName = Iterables.getLast(actualTableName);
      if (actualUnqualifiedName.equals(expectedTableName1)) {
        checkColumnOrigin(
            rco,
            expectedTableName1,
            expectedColumnName1,
            expectedDerived);
      } else {
        checkColumnOrigin(
            rco,
            expectedTableName2,
            expectedColumnName2,
            expectedDerived);
      }
    }
  }

  /**
   * Checks result of getting unique keys for sql.
   */
  public void checkGetUniqueKeys(Set<ImmutableBitSet> expectedUniqueKeySet) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result, notNullValue());
    assertEquals(ImmutableSortedSet.copyOf(expectedUniqueKeySet),
        ImmutableSortedSet.copyOf(result),
        () -> "unique keys, sql: " + relSupplier + ", rel: " + RelOptUtil.toString(rel));
    assertUniqueConsistent(rel);
  }

  /**
   * Asserts that {@link RelMetadataQuery#getUniqueKeys(RelNode)}
   * and {@link RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet)}
   * return consistent results.
   */
  public void assertUniqueConsistent(RelNode rel) {
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(rel);
    final ImmutableBitSet allCols =
        ImmutableBitSet.range(0, rel.getRowType().getFieldCount());
    for (ImmutableBitSet key : allCols.powerSet()) {
      Boolean result2 = mq.areColumnsUnique(rel, key);
      assertEquals(isUnique(uniqueKeys, key), SqlFunctions.isTrue(result2),
          () -> "areColumnsUnique. key: " + key + ", uniqueKeys: " + uniqueKeys
              + ", rel: " + RelOptUtil.toString(rel));
    }
  }

  /**
   * Returns whether {@code key} is unique, that is, whether it or a subset
   * is in {@code uniqueKeys}.
   */
  private static boolean isUnique(Set<ImmutableBitSet> uniqueKeys,
      ImmutableBitSet key) {
    for (ImmutableBitSet uniqueKey : uniqueKeys) {
      if (key.contains(uniqueKey)) {
        return true;
      }
    }
    return false;
  }

  /** Checks {@link RelMetadataQuery#getRowCount(RelNode)},
   * {@link RelMetadataQuery#getMaxRowCount(RelNode)},
   * and {@link RelMetadataQuery#getMinRowCount(RelNode)}. */
  public RelMetadataFixture assertThatRowCount(Matcher<Number> rowCountMatcher,
      Matcher<Number> minRowCountMatcher, Matcher<Number> maxRowCountMatcher) {
    final RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final Double rowCount = mq.getRowCount(rel);
    assertThat(rowCount, notNullValue());
    assertThat(rowCount, rowCountMatcher);

    final Double min = mq.getMinRowCount(rel);
    assertThat(min, notNullValue());
    assertThat(min, minRowCountMatcher);

    final Double max = mq.getMaxRowCount(rel);
    assertThat(max, notNullValue());
    assertThat(max, maxRowCountMatcher);
    return this;
  }

  /** Checks {@link RelMetadataQuery#getSelectivity(RelNode, RexNode)}. */
  public RelMetadataFixture assertThatSelectivity(Matcher<Double> matcher) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Double result = mq.getSelectivity(rel, null);
    assertThat(result, notNullValue());
    assertThat(result, matcher);
    return this;
  }

  /** Checks
   * {@link RelMetadataQuery#getDistinctRowCount(RelNode, ImmutableBitSet, RexNode)}
   * with a null predicate. */
  public RelMetadataFixture assertThatDistinctRowCount(ImmutableBitSet groupKey,
      Matcher<Double> matcher) {
    return assertThatDistinctRowCount(r -> groupKey, matcher);
  }

  /** Checks
   * {@link RelMetadataQuery#getDistinctRowCount(RelNode, ImmutableBitSet, RexNode)}
   * with a null predicate, deriving the group key from the {@link RelNode}. */
  public RelMetadataFixture assertThatDistinctRowCount(
      Function<RelNode, ImmutableBitSet> groupKeyFn,
      Matcher<Double> matcher) {
    final RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final ImmutableBitSet groupKey = groupKeyFn.apply(rel);
    Double result = mq.getDistinctRowCount(rel, groupKey, null);
    assertThat(result, matcher);
    return this;
  }

  /** Checks the {@link RelNode} produced by {@link #toRel}. */
  public RelMetadataFixture assertThatRel(Matcher<RelNode> matcher) {
    final RelNode rel = toRel();
    assertThat(rel, matcher);
    return this;
  }

  /** Checks {@link RelMetadataQuery#getUniqueKeys(RelNode)}. */
  public RelMetadataFixture assertThatUniqueKeys(
      Matcher<Iterable<ImmutableBitSet>> matcher) {
    final RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result, matcher);
    return this;
  }

  /** Checks {@link RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet)}. */
  public RelMetadataFixture assertThatAreColumnsUnique(ImmutableBitSet columns,
      Matcher<Boolean> matcher) {
    return assertThatAreColumnsUnique(r -> columns, r -> r, matcher);
  }

  /** Checks {@link RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet)},
   * deriving parameters via functions. */
  public RelMetadataFixture assertThatAreColumnsUnique(
      Function<RelNode, ImmutableBitSet> columnsFn,
      UnaryOperator<RelNode> relFn,
      Matcher<Boolean> matcher) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final ImmutableBitSet columns = columnsFn.apply(rel);
    final RelNode rel2 = relFn.apply(rel);
    final Boolean areColumnsUnique = mq.areColumnsUnique(rel2, columns);
    assertThat(areColumnsUnique, matcher);
    return this;
  }

  /** Checks {@link RelMetadataQuery#areRowsUnique(RelNode)}. */
  public RelMetadataFixture assertThatAreRowsUnique(Matcher<Boolean> matcher) {
    RelNode rel = toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Boolean areRowsUnique = mq.areRowsUnique(rel);
    assertThat(areRowsUnique, matcher);
    return this;
  }
}
