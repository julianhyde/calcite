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
package org.apache.calcite.tools;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.ImmutableBeans;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Utility that extracts constants from a SQL query.
 *
 * <p>Simple use:
 *
 * <blockquote><code>
 * final String sql =<br>
 *     "select 'x' from emp where deptno < 10";<br>
 * final Hoist.Hoisted hoisted =<br>
 *     Hoist.create(Hoist.config()).hoist();<br>
 * print(hoisted); // "select ?0 from emp where deptno < ?1"
 * </code></blockquote>
 *
 * <p>For more advanced formatting, use {@link Hoisted#substitute(Function)}.
 *
 * <p>Adjust {@link Config} to use a different parser or parsing options.
 */
public class Hoist {
  private final Config config;

  /** Creates a Config. */
  public static Config config() {
    return ImmutableBeans.create(Config.class)
        .withParserConfig(SqlParser.configBuilder().build());
  }

  /** Creates a Hoist. */
  public static Hoist create(Config config) {
    return new Hoist(config);
  }

  private Hoist(Config config) {
    this.config = Objects.requireNonNull(config);
  }

  public Hoisted hoist(String sql) {
    final List<Variable> variables = new ArrayList<>();
    final SqlParser parser = SqlParser.create(sql, config.parserConfig());
    final SqlNode node;
    try {
      node = parser.parseQuery();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
    node.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlLiteral literal) {
        final SqlParserPos pos = literal.getParserPosition();
        final int start = SqlParserUtil.lineColToIndex(sql, pos.getLineNum(),
            pos.getColumnNum());
        final int end = SqlParserUtil.lineColToIndex(sql, pos.getEndLineNum(),
            pos.getEndColumnNum()) + 1;
        variables.add(new Variable(variables.size(), literal, start, end));
        return super.visit(literal);
      }
    });
    return new Hoisted(sql, variables);
  }

  /** Configuration. */
  public interface Config {
    /** Returns the configuration for the SQL parser. */
    @ImmutableBeans.Property
    @Nonnull
    SqlParser.Config parserConfig();

    /** Sets {@link #parserConfig()}. */
    Config withParserConfig(SqlParser.Config parserConfig);
  }

  /** Variable. */
  public static class Variable {
    /** Zero-based ordinal in statement. */
    public final int ordinal;
    /** Parse tree node (typically a literal). */
    public final SqlNode node;
    /** Zero-based position within the SQL text of start of node. */
    public final int start;
    /** Zero-based position within the SQL text after end of node. */
    public final int end;

    private Variable(int ordinal, SqlNode node, int start, int end) {
      this.ordinal = ordinal;
      this.node = Objects.requireNonNull(node);
      this.start = start;
      this.end = end;
      Preconditions.checkArgument(ordinal >= 0);
      Preconditions.checkArgument(start >= 0);
      Preconditions.checkArgument(start <= end);
    }
  }

  /** Result of hoisting. */
  public static class Hoisted {
    public final String originalSql;
    public final List<Variable> variables;

    Hoisted(String originalSql, List<Variable> variables) {
      super();
      this.originalSql = originalSql;
      this.variables = ImmutableList.copyOf(variables);
    }

    @Override public String toString() {
      return substitute(v -> "?" + v.ordinal);
    }

    /** Returns the SQL string with variables replaced according to the
     * given substitution function. */
    public String substitute(Function<Variable, String> fn) {
      final StringBuilder b = new StringBuilder(originalSql);
      for (Variable variable : Lists.reverse(variables)) {
        final String s = fn.apply(variable);
        b.replace(variable.start, variable.end, s);
      }
      return b.toString();
    }
  }
}
