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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;
import org.apache.calcite.util.TestUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Parameters for a SQL-to-RelNode test.
 */
public class SqlToRelFixture {
  public static final SqlToRelTestBase.Tester TESTER =
      new SqlToRelTestBase.TesterImpl(false, false, false, true, null,
          null, MockRelOptPlanner::new, UnaryOperator.identity(),
          SqlConformanceEnum.DEFAULT, UnaryOperator.identity(),
          SqlToRelTestBase.DEFAULT_TYPE_FACTORY_SUPPLIER)
          .withConfig(c ->
              c.withTrimUnusedFields(true)
                  .withExpand(true)
                  .addRelBuilderConfigTransform(b ->
                      b.withAggregateUnique(true)
                          .withPruneInputOfAggregate(false)));

  public static final SqlToRelFixture DEFAULT =
      new SqlToRelFixture("?", true, TESTER, false,
          UnaryOperator.identity(), TESTER.getConformance(), false, null);

  private final String sql;
  private final @Nullable DiffRepository diffRepos;
  private final boolean decorrelate;
  private final SqlToRelTestBase.Tester tester;
  private final boolean trim;
  private final UnaryOperator<SqlToRelConverter.Config> config;
  private final SqlConformance conformance;
  private final boolean expression;

  SqlToRelFixture(String sql, boolean decorrelate,
      SqlToRelTestBase.Tester tester, boolean trim,
      UnaryOperator<SqlToRelConverter.Config> config,
      SqlConformance conformance, boolean expression,
      @Nullable DiffRepository diffRepos) {
    this.sql = requireNonNull(sql, "sql");
    this.diffRepos = diffRepos;
    if (sql.contains(" \n")) {
      throw new AssertionError("trailing whitespace");
    }
    this.decorrelate = decorrelate;
    this.tester = requireNonNull(tester, "tester");
    this.trim = trim;
    this.config = requireNonNull(config, "config");
    this.conformance = requireNonNull(conformance, "conformance");
    this.expression = expression;
  }

  public void ok() {
    convertsTo("${plan}");
  }

  public void throws_(String message) {
    try {
      ok();
    } catch (Throwable throwable) {
      assertThat(TestUtil.printStackTrace(throwable), containsString(message));
    }
  }

  public void convertsTo(String plan) {
    tester.withDecorrelation(decorrelate)
        .withConformance(conformance)
        .withConfig(config)
        .withConfig(c -> c.withTrimUnusedFields(true))
        .assertConvertsTo(diffRepos(), sql, plan, trim, expression);
  }

  public DiffRepository diffRepos() {
    return DiffRepository.castNonNull(diffRepos);
  }

  public SqlToRelFixture withSql(String sql) {
    return sql.equals(this.sql) ? this
        : new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
            expression, diffRepos);
  }

  /**
   * Sets whether this is an expression (as opposed to a whole query).
   */
  public SqlToRelFixture expression(boolean expression) {
    return this.expression == expression ? this
        : new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
            expression, diffRepos);
  }

  public SqlToRelFixture withConfig(UnaryOperator<SqlToRelConverter.Config> config) {
    final UnaryOperator<SqlToRelConverter.Config> config2 =
        this.config.andThen(requireNonNull(config, "config"))::apply;
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config2, conformance,
        expression, diffRepos);
  }

  public SqlToRelFixture withExpand(boolean expand) {
    return withConfig(b -> b.withExpand(expand));
  }

  public SqlToRelFixture withDecorrelate(boolean decorrelate) {
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
        expression, diffRepos);
  }

  public SqlToRelFixture withTester(SqlToRelTestBase.Tester tester) {
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
        expression, diffRepos);
  }

  // TODO: change the config, not the tester
  public SqlToRelFixture with(UnaryOperator<SqlToRelTestBase.Tester> testerTransform) {
    return withTester(testerTransform.apply(tester));
  }

  public SqlToRelFixture withTrim(boolean trim) {
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
        expression, diffRepos);
  }

  public SqlConformance getConformance() {
    return conformance;
  }

  public SqlToRelFixture withConformance(SqlConformance conformance) {
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
        expression, diffRepos);
  }

  public SqlToRelFixture withDiffRepos(DiffRepository diffRepos) {
    return new SqlToRelFixture(sql, decorrelate, tester, trim, config, conformance,
        expression, diffRepos);
  }

  public SqlToRelFixture withDynamicTable() {
    return with(t ->
        t.withCatalogReaderFactory(MockCatalogReaderDynamic::new));
  }

  public RelRoot toRoot() {
    return tester.convertSqlToRel(sql);
  }

  public RelNode toRel() {
    return toRoot().rel;
  }
}
