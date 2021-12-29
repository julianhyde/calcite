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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.test.SqlNewTestFactory;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.SqlToRelTestBase.Tester;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Closer;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.calcite.test.Matchers.relIsValid;
import static org.apache.calcite.test.SqlToRelTestBase.NL;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or
 * rules via {@link DiffRepository}.
 */
abstract class RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  /** Creates a fixture for a test. Derived class must override and set
   * {@link Sql#diffRepos}. */
  Sql fixture() {
    return Sql.DEFAULT;
  }

  /** Creates a fixture and sets its SQL statement. */
  protected final Sql sql(String sql) {
    return fixture().sql(sql);
  }

  /** Initiates a test case with a given {@link RelNode} supplier. */
  protected final Sql relFn(Function<RelBuilder, RelNode> relFn) {
    return fixture().relFn(relFn);
  }

  /** Allows fluent testing. */
  static class Sql {
    static final Sql DEFAULT =
        new Sql(SqlToRelFixture.TESTER, SqlNewTestFactory.INSTANCE, null,
            RelSupplier.NONE, null, null,
            ImmutableMap.of(), (f, r) -> r, (f, r) -> r,
            false, false)
            .withFactory(f ->
                f.withValidatorConfig(c ->
                    c.withIdentifierExpansion(true)))
            .withRelBuilderConfig(b -> b.withPruneInputOfAggregate(false));

    final Tester tester;
    final RelSupplier relSupplier;
    final SqlNewTestFactory factory;
    final DiffRepository diffRepos;
    final HepProgram preProgram;
    final RelOptPlanner planner;
    final ImmutableMap<Hook, Consumer<Object>> hooks;
    final BiFunction<Sql, RelNode, RelNode> before;
    final BiFunction<Sql, RelNode, RelNode> after;
    final boolean decorrelate;
    final boolean trim;

    Sql(Tester tester, SqlNewTestFactory factory,
        @Nullable DiffRepository diffRepos,
        RelSupplier relSupplier, HepProgram preProgram, RelOptPlanner planner,
        ImmutableMap<Hook, Consumer<Object>> hooks,
        BiFunction<Sql, RelNode, RelNode> before,
        BiFunction<Sql, RelNode, RelNode> after, boolean decorrelate, boolean trim) {
      this.tester = requireNonNull(tester, "tester");
      this.factory = factory;
      this.diffRepos = diffRepos;
      this.relSupplier = requireNonNull(relSupplier, "relSupplier");
      this.before = requireNonNull(before, "before");
      this.after = requireNonNull(after, "after");
      this.preProgram = preProgram;
      this.planner = planner;
      this.hooks = requireNonNull(hooks, "hooks");
      this.decorrelate = decorrelate;
      this.trim = trim;
    }

    public Sql withDiffRepos(DiffRepository diffRepos) {
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql withRelSupplier(RelSupplier relSupplier) {
      return relSupplier.equals(this.relSupplier) ? this
          : new Sql(tester, factory, diffRepos, relSupplier, preProgram,
              planner, hooks, before, after, decorrelate, trim);
    }

    public Sql sql(String sql) {
      return withRelSupplier(RelSupplier.of(sql));
    }

    Sql relFn(Function<RelBuilder, RelNode> relFn) {
      return withRelSupplier(RelSupplier.of(relFn));
    }

    public Sql withBefore(BiFunction<Sql, RelNode, RelNode> before) {
      final BiFunction<Sql, RelNode, RelNode> before0 = this.before;
      final BiFunction<Sql, RelNode, RelNode> before2 =
          (sql, r) -> before.apply(this, before0.apply(this, r));
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before2, after, decorrelate, trim);
    }

    public Sql withAfter(BiFunction<Sql, RelNode, RelNode> after) {
      final BiFunction<Sql, RelNode, RelNode> after0 = this.after;
      final BiFunction<Sql, RelNode, RelNode> after2 =
          (sql, r) -> after.apply(this, after0.apply(this, r));
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after2, decorrelate, trim);
    }

    public Sql withDynamicTable() {
      return withTester(t -> // TODO remove this line and next
          t.withCatalogReaderFactory(MockCatalogReaderDynamic::create))
          .withFactory(f ->
              f.withCatalogReader(MockCatalogReaderDynamic::create));
    }

    public Sql withFactory(UnaryOperator<SqlNewTestFactory> transform) {
      final SqlNewTestFactory factory = transform.apply(this.factory);
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql withTester(UnaryOperator<Tester> transform) { // TODO dont transform tester
      final Tester tester = transform.apply(this.tester);
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql withPre(HepProgram preProgram) {
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql withPreRule(RelOptRule... rules) {
      final HepProgramBuilder builder = HepProgram.builder();
      for (RelOptRule rule : rules) {
        builder.addRuleInstance(rule);
      }
      return withPre(builder.build());
    }

    public Sql with(RelOptPlanner planner) {
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql with(HepProgram program) {
      return with(new HepPlanner(program));
    }

    public Sql withRule(RelOptRule... rules) {
      final HepProgramBuilder builder = HepProgram.builder();
      for (RelOptRule rule : rules) {
        builder.addRuleInstance(rule);
      }
      return with(builder.build());
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Sql withHook(Hook hook, Consumer<T> handler) {
      final ImmutableMap<Hook, Consumer<Object>> hooks =
          FlatLists.append((Map) this.hooks, hook, (Consumer) handler);
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public <V> Sql withProperty(Hook hook, V value) {
      return withHook(hook, Hook.propertyJ(value));
    }

    public Sql withRelBuilderSimplify(boolean simplify) {
      return withProperty(Hook.REL_BUILDER_SIMPLIFY, simplify);
    }

    public Sql withExpand(final boolean expand) {
      return withConfig(c -> c.withExpand(expand));
    }

    public Sql withConfig(UnaryOperator<SqlToRelConverter.Config> transform) {
      return withTester(tester -> tester.withConfig(transform)) // TODO remove this line
          .withFactory(f -> f.withSqlToRelConfig(transform));
    }

    public Sql withRelBuilderConfig(
        UnaryOperator<RelBuilder.Config> transform) {
      return withConfig(c -> c.addRelBuilderConfigTransform(transform));
    }

    public Sql withLateDecorrelation(final boolean b) {
      return withTester(tester -> tester.withLateDecorrelation(b));
    }

    public Sql withDecorrelate(final boolean decorrelate) {
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim);
    }

    public Sql withTrim(final boolean trim) {
      return new Sql(tester, factory, diffRepos, relSupplier, preProgram,
          planner, hooks, before, after, decorrelate, trim)
          .withConfig(c -> c.withTrimUnusedFields(trim)); // TODO remove trim field
    }

    public Sql withCatalogReaderFactory(
        SqlNewTestFactory.CatalogReaderFactory factory) {
      return withTester(tester -> tester.withCatalogReaderFactory(factory))
          .withFactory(f -> f.withCatalogReader(factory));
    }

    public Sql withConformance(final SqlConformance conformance) {
      return withFactory(f ->
          f.withValidatorConfig(c -> c.withConformance(conformance))
              .withOperatorTable(t ->
                  conformance.allowGeometry()
                      ? SqlOperatorTables.chain(t,
                      SqlOperatorTables.spatialInstance())
                      : t));
    }

    public Sql withContext(final UnaryOperator<Context> transform) {
      return withTester(tester -> tester.withContext(transform)) // TODO remove 1
          .withFactory(f -> f.withPlannerContext(transform));
    }

    public RelNode toRel() {
      return relSupplier.apply(this);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given rule,
     * with an optional pre-program specified by {@link #withPre(HepProgram)}
     * to prepare the tree.
     */
    public void check() {
      check(false);
    }

    /**
     * Checks that the plan is the same before and after executing a given
     * planner. Useful for checking circumstances where rules should not fire.
     */
    public void checkUnchanged() {
      check(true);
    }

    private void check(boolean unchanged) {
      try (Closer closer = new Closer()) {
        for (Map.Entry<Hook, Consumer<Object>> entry : hooks.entrySet()) {
          closer.add(entry.getKey().addThread(entry.getValue()));
        }
        checkPlanning(unchanged);
      }
    }

    /**
     * Checks the plan for a given {@link RelNode} supplier before/after executing a given rule,
     * with a pre-program to prepare the tree.
     *
     * @param unchanged   Whether the rule is to have no effect
     */
    private void checkPlanning(boolean unchanged) {
      final RelNode relInitial = toRel();

      assertNotNull(relInitial);
      List<RelMetadataProvider> list = new ArrayList<>();
      list.add(DefaultRelMetadataProvider.INSTANCE);
      RelMetadataProvider plannerChain =
          ChainedRelMetadataProvider.of(list);
      final RelOptCluster cluster = relInitial.getCluster();
      cluster.setMetadataProvider(plannerChain);

      // Rather than a single mutable 'RelNode r', this method uses lots of
      // final variables (relInitial, r1, relBefore, and so forth) so that the
      // intermediate states of planning are visible in the debugger.
      final RelNode r1;
      if (preProgram == null) {
        r1 = relInitial;
      } else {
        HepPlanner prePlanner = new HepPlanner(preProgram);
        prePlanner.setRoot(relInitial);
        r1 = prePlanner.findBestExp();
      }
      final RelNode relBefore = before.apply(this, r1);
      assertThat(relBefore, notNullValue());

      final String planBefore = NL + RelOptUtil.toString(relBefore);
      final DiffRepository diffRepos = diffRepos();
      diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
      assertThat(relBefore, relIsValid());

      final RelNode r2;
      if (planner instanceof VolcanoPlanner) {
        r2 = planner.changeTraits(relBefore,
            relBefore.getTraitSet().replace(EnumerableConvention.INSTANCE));
      } else {
        r2 = relBefore;
      }
      planner.setRoot(r2);
      final RelNode r3 = planner.findBestExp();

      final RelNode r4;
      if (tester.isLateDecorrelate()) {
        final String planMid = NL + RelOptUtil.toString(r3);
        diffRepos.assertEquals("planMid", "${planMid}", planMid);
        assertThat(r3, relIsValid());
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(cluster, null);
        r4 = RelDecorrelator.decorrelateQuery(r3, relBuilder);
      } else {
        r4 = r3;
      }
      final RelNode relAfter = after.apply(this, r4);
      final String planAfter = NL + RelOptUtil.toString(relAfter);
      if (unchanged) {
        assertThat(planAfter, is(planBefore));
      } else {
        diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
        if (planBefore.equals(planAfter)) {
          throw new AssertionError("Expected plan before and after is the same.\n"
              + "You must use unchanged=true or call checkUnchanged");
        }
      }
      assertThat(relAfter, relIsValid());
    }

    public Sql withVolcanoPlanner(boolean topDown) {
      return withVolcanoPlanner(topDown, p ->
          RelOptUtil.registerDefaultRules(p, false, false));
    }

    public Sql withVolcanoPlanner(boolean topDown, Consumer<VolcanoPlanner> init) {
      final VolcanoPlanner planner = new VolcanoPlanner();
      planner.setTopDownOpt(topDown);
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      init.accept(planner);
      return with(planner)
          .withDecorrelate(true)
          .withFactory(f ->
              f.withCluster(cluster ->
                  RelOptCluster.create(planner, cluster.getRexBuilder())));
    }

    public Sql withSubQueryRules() {
      return withExpand(false)
          .withRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
              CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
              CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    }

    /** Returns the diff repository, checking that it is not null.
     * (It is allowed to be null because some tests that don't use a diff
     * repository.) */
    public DiffRepository diffRepos() {
      return DiffRepository.castNonNull(diffRepos);
    }
  }

}
