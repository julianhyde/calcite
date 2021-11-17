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
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.SqlToRelTestBase.Tester;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Closer;

import com.google.common.collect.ImmutableList;
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

  protected Tester createTester() { // TODO override in Sql, not tester
    return new SqlToRelTestBase() {
    }.tester.withDecorrelation(false);
  }

  /** Creates a fixture for a test. Derived class must override and set
   * {@link Sql#diffRepos}. */
  Sql fixture() {
    final Tester tester = createTester();
    return new Sql(tester, null, RelSupplier.NONE, null, null,
        ImmutableMap.of(), ImmutableList.of(), (f, r) -> r, (f, r) -> r)
        .withRelBuilderConfig(b -> b.withPruneInputOfAggregate(false));
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
    final Tester tester;
    final RelSupplier relSupplier;
    final DiffRepository diffRepos;
    final HepProgram preProgram;
    final RelOptPlanner planner;
    final ImmutableMap<Hook, Consumer<Object>> hooks;
    final ImmutableList<Function<Tester, Tester>> transforms;
    final BiFunction<Sql, RelNode, RelNode> before;
    final BiFunction<Sql, RelNode, RelNode> after;

    Sql(Tester tester, @Nullable DiffRepository diffRepos,
        RelSupplier relSupplier, HepProgram preProgram, RelOptPlanner planner,
        ImmutableMap<Hook, Consumer<Object>> hooks,
        ImmutableList<Function<Tester, Tester>> transforms,
        BiFunction<Sql, RelNode, RelNode> before,
        BiFunction<Sql, RelNode, RelNode> after) {
      this.tester = requireNonNull(tester, "tester");
      this.diffRepos = diffRepos;
      this.relSupplier = requireNonNull(relSupplier, "relSupplier");
      this.before = requireNonNull(before, "before");
      this.after = requireNonNull(after, "after");
      this.preProgram = preProgram;
      this.planner = planner;
      this.hooks = requireNonNull(hooks, "hooks");
      this.transforms = requireNonNull(transforms, "transforms");
    }

    public Sql withDiffRepos(DiffRepository diffRepos) {
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
    }

    public Sql withRelSupplier(RelSupplier relSupplier) {
      return relSupplier.equals(this.relSupplier) ? this
          : new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
              transforms, before, after);
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
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before2, after);
    }

    public Sql withAfter(BiFunction<Sql, RelNode, RelNode> after) {
      final BiFunction<Sql, RelNode, RelNode> after0 = this.after;
      final BiFunction<Sql, RelNode, RelNode> after2 =
          (sql, r) -> after.apply(this, after0.apply(this, r));
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after2);
    }

    public Sql withDynamicTable() {
      return withTester(t ->
          t.withCatalogReaderFactory(MockCatalogReaderDynamic::new));
    }

    public Sql withTester(UnaryOperator<Tester> transform) { // TODO dont transform tester
      final Tester tester2 = transform.apply(tester);
      return new Sql(tester2, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
    }

    public Sql withPre(HepProgram preProgram) {
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
    }

    public Sql withPreRule(RelOptRule... rules) {
      final HepProgramBuilder builder = HepProgram.builder();
      for (RelOptRule rule : rules) {
        builder.addRuleInstance(rule);
      }
      return withPre(builder.build());
    }

    public Sql with(RelOptPlanner planner) {
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
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

    /** Adds a transform that will be applied to {@link #tester}
     * just before running the query. */
    private Sql withTransform(Function<Tester, Tester> transform) {
      final ImmutableList<Function<Tester, Tester>> transforms =
          FlatLists.append(this.transforms, transform);
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
    }

    /** Adds a hook and a handler for that hook. Calcite will create a thread
     * hook (by calling {@link Hook#addThread(Consumer)})
     * just before running the query, and remove the hook afterwards. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Sql withHook(Hook hook, Consumer<T> handler) {
      final ImmutableMap<Hook, Consumer<Object>> hooks =
          FlatLists.append((Map) this.hooks, hook, (Consumer) handler);
      return new Sql(tester, diffRepos, relSupplier, preProgram, planner, hooks,
          transforms, before, after);
    }

    // CHECKSTYLE: IGNORE 1
    /** @deprecated Use {@link #withHook(Hook, Consumer)}. */
    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public <T> Sql withHook(Hook hook,
        com.google.common.base.Function<T, Void> handler) {
      return withHook(hook, (Consumer<T>) handler::apply);
    }

    public <V> Sql withProperty(Hook hook, V value) {
      return withHook(hook, Hook.propertyJ(value));
    }

    public Sql expand(final boolean b) {
      return withConfig(c -> c.withExpand(b));
    }

    public Sql withConfig(UnaryOperator<SqlToRelConverter.Config> transform) {
      return withTransform(tester -> tester.withConfig(transform));
    }

    public Sql withRelBuilderConfig(
        UnaryOperator<RelBuilder.Config> transform) {
      return withConfig(c -> c.addRelBuilderConfigTransform(transform));
    }

    public Sql withLateDecorrelation(final boolean b) {
      return withTransform(tester -> tester.withLateDecorrelation(b));
    }

    public Sql withDecorrelation(final boolean b) {
      return withTransform(tester -> tester.withDecorrelation(b));
    }

    public Sql withTrim(final boolean b) {
      return withTransform(tester -> tester.withTrim(b));
    }

    public Sql withCatalogReaderFactory(
        SqlTestFactory.MockCatalogReaderFactory factory) {
      return withTransform(tester -> tester.withCatalogReaderFactory(factory));
    }

    public Sql withConformance(final SqlConformance conformance) {
      return withTransform(tester -> tester.withConformance(conformance));
    }

    public Sql withContext(final UnaryOperator<Context> transform) {
      return withTransform(tester -> tester.withContext(transform));
    }

    public RelNode toRel() {
      return relSupplier.apply(this);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given rule,
     * with a optional pre-program specified by {@link #withPre(HepProgram)}
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
      final Tester tester = tester();
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
          .withDecorrelation(true)
          .withTester(t ->
              t.withClusterFactory(cluster ->
                  RelOptCluster.create(planner, cluster.getRexBuilder())));
    }

    /** Returns the diff repository, checking that it is not null.
     * (It is allowed to be null because some tests that don't use a diff
     * repository.) */
    public DiffRepository diffRepos() {
      return DiffRepository.castNonNull(diffRepos);
    }

    public Tester tester() {
      Tester t = tester;
      for (Function<Tester, Tester> transform : transforms) {
        t = transform.apply(t);
      }
      return t;
    }
  }

  /** The source of a {@link RelNode} for running a test. */
  interface RelSupplier extends Function<Sql, RelNode> {
    RelSupplier NONE = fixture -> {
      throw new UnsupportedOperationException();
    };
    static RelSupplier of(String sql) {
      if (sql.contains(" \n")) {
        throw new AssertionError("trailing whitespace");
      }
      return fixture -> {
        // TODO: define toString and equals methods
        String sql2 = fixture.diffRepos().expand("sql", sql);
        return fixture.tester().convertSqlToRel(sql2).rel;
      };
    }

    /** RelBuilder config based on the "scott" schema. */
    FrameworkConfig FRAMEWORK_CONFIG =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(
                CalciteAssert.addSchema(
                    Frameworks.createRootSchema(true),
                    CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
            .build();

    static RelSupplier of(Function<RelBuilder, RelNode> relFn) {
      return fixture ->
          relFn.apply(RelBuilder.create(FRAMEWORK_CONFIG));
    }
  }
}
