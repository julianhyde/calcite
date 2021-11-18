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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * Helper.
 */
class SqlPrettyWriterTestFixture {
  private final @Nullable DiffRepository diffRepos;
  public final String sql;
  public final boolean expr;
  public final @Nullable String desc;
  public final String formatted;
  public final UnaryOperator<SqlWriterConfig> transform;

  SqlPrettyWriterTestFixture(@Nullable DiffRepository diffRepos, String sql,
      boolean expr, @Nullable String desc, String formatted,
      UnaryOperator<SqlWriterConfig> transform) {
    this.diffRepos = diffRepos;
    this.sql = requireNonNull(sql, "sql");
    this.expr = expr;
    this.desc = desc;
    this.formatted = requireNonNull(formatted, "formatted");
    this.transform = requireNonNull(transform, "transform");
  }

  SqlPrettyWriterTestFixture withWriter(
      UnaryOperator<SqlWriterConfig> transform) {
    requireNonNull(transform, "transform");
    final UnaryOperator<SqlWriterConfig> transform1 =
        this.transform.andThen(transform)::apply;
    return new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
        transform1);
  }

  SqlPrettyWriterTestFixture withSql(String sql) {
    return sql.equals(this.sql)
        ? this
        : new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
            transform);
  }

  SqlPrettyWriterTestFixture withExpr(boolean expr) {
    return this.expr == expr
        ? this
        : new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
            transform);
  }

  SqlPrettyWriterTestFixture withDiffRepos(DiffRepository diffRepos) {
    return Objects.equals(this.diffRepos, diffRepos)
        ? this
        : new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
            transform);
  }

  /** Returns the diff repository, checking that it is not null.
   * (It is allowed to be null because some tests that don't use a diff
   * repository.) */
  public DiffRepository diffRepos() {
    return DiffRepository.castNonNull(diffRepos);
  }

  SqlPrettyWriterTestFixture expectingDesc(@Nullable String desc) {
    return Objects.equals(this.desc, desc)
        ? this
        : new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
            transform);
  }

  SqlPrettyWriterTestFixture expectingFormatted(String formatted) {
    return Objects.equals(this.formatted, formatted)
        ? this
        : new SqlPrettyWriterTestFixture(diffRepos, sql, expr, desc, formatted,
            transform);
  }

  /** Parses a SQL query. To use a different parser, override this method. */
  protected SqlNode parseQuery(String sql) {
    SqlNode node;
    try {
      node = SqlParser.create(sql).parseQuery();
    } catch (SqlParseException e) {
      String message = "Received error while parsing SQL '" + sql + "'"
          + "; error is:\n"
          + e.toString();
      throw new AssertionError(message);
    }
    return node;
  }

  SqlPrettyWriterTestFixture check() {
    final SqlWriterConfig config =
        transform.apply(SqlPrettyWriter.config()
            .withDialect(AnsiSqlDialect.DEFAULT));
    final SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
    final SqlNode node;
    if (expr) {
      final SqlCall valuesCall = (SqlCall) parseQuery("VALUES (" + sql + ")");
      final SqlCall rowCall = valuesCall.operand(0);
      node = rowCall.operand(0);
    } else {
      node = parseQuery(sql);
    }

    // Describe settings
    if (desc != null) {
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      prettyWriter.describe(pw, true);
      pw.flush();
      final String desc = sw.toString();
      diffRepos().assertEquals("desc", this.desc, desc);
    }

    // Format
    final String formatted = prettyWriter.format(node);
    diffRepos().assertEquals("formatted", this.formatted, formatted);

    // Now parse the result, and make sure it is structurally equivalent
    // to the original.
    final String actual2 = formatted.replace("`", "\"");
    final SqlNode node2;
    if (expr) {
      final SqlCall valuesCall =
          (SqlCall) parseQuery("VALUES (" + actual2 + ")");
      final SqlCall rowCall = valuesCall.operand(0);
      node2 = rowCall.operand(0);
    } else {
      node2 = parseQuery(actual2);
    }
    assertTrue(node.equalsDeep(node2, Litmus.THROW));

    return this;
  }

}
