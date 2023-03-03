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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/** Description of the dialects that are enabled for a particular test.
 *
 * <p>Each dialect has a name, optionally a connection factory,
 * and a state (enabled, recording, replaying).
 *
 * <p>It is immutable.
 */
class DialectTestConfig {
  final ImmutableMap<String, Dialect> dialectMap;

  /** The code of the reference dialect. If not null, the queries from this
   * dialect as used as exemplars for other dialects: the other dialects are
   * expected to return the same set of rows as the reference. */
  final @Nullable DialectCode refDialectCode;

  /** The name of the class relative to which the resource file containing
   * query responses is located. */
  @SuppressWarnings("rawtypes")
  private final Class testClass;

  /** A function that maps a dialect name to the name of the file containing
   * its query responses. */
  private final Function<String, String> function;

  private DialectTestConfig(Map<String, Dialect> dialectMap,
      @Nullable DialectCode refDialectCode,
      @SuppressWarnings("rawtypes") Class testClass,
      Function<String, String> function) {
    this.dialectMap = ImmutableMap.copyOf(dialectMap);
    this.refDialectCode = refDialectCode;
    this.testClass = requireNonNull(testClass, "testClass");
    this.function = requireNonNull(function, "function");
  }

  /** Creates a DialectTestConfig. */
  static DialectTestConfig of(Iterable<Dialect> dialects) {
    final ImmutableMap.Builder<String, Dialect> map = ImmutableMap.builder();
    dialects.forEach(dialect -> map.put(dialect.name, dialect));
    return new DialectTestConfig(map.build(), null, RelToSqlConverterTest.class,
        UnaryOperator.identity());
  }

  /** Applies a transform to the dialect with a given code.
   *
   * <p>Throws if there is no such dialect. */
  public DialectTestConfig withDialect(DialectCode code,
      UnaryOperator<Dialect> dialectTransform) {
    return withDialect(code.name(), dialectTransform);
  }

  /** Applies a transform to each dialect. */
  public DialectTestConfig withDialects(
      UnaryOperator<Dialect> dialectTransform) {
    final ImmutableMap.Builder<String, Dialect> b =
        ImmutableMap.builder();
    dialectMap.forEach((name, dialect) ->
        b.put(dialect.name, dialectTransform.apply(dialect)));
    final ImmutableMap<String, Dialect> dialectMap2 = b.build();
    if (dialectMap2.equals(dialectMap)) {
      return this;
    }
    return new DialectTestConfig(dialectMap2, refDialectCode, testClass,
        function);
  }

  /** Applies a transform to the dialect with a given name.
   *
   * <p>Throws if there is no such dialect. */
  public DialectTestConfig withDialect(String name,
      UnaryOperator<Dialect> dialectTransform) {
    final Dialect dialect = dialectMap.get(name);
    final Dialect dialect2 = dialectTransform.apply(dialect);
    if (dialect == dialect2) {
      return this;
    }
    final Map<String, Dialect> dialectMap2 = new LinkedHashMap<>(dialectMap);
    dialectMap2.put(name, dialect2);
    return new DialectTestConfig(dialectMap2, refDialectCode, testClass,
        function);
  }

  /** Sets the name of the reference dialect. */
  public DialectTestConfig withReference(DialectCode refDialectCode) {
    if (refDialectCode == this.refDialectCode) {
      return this;
    }
    return new DialectTestConfig(dialectMap, refDialectCode, testClass,
        function);
  }

  /** Sets the path for any given dialect's corpus. */
  public DialectTestConfig withPath(
      @SuppressWarnings("rawtypes") Class testClass,
      Function<String, String> function) {
    if (testClass == this.testClass && function == this.function) {
      return this;
    }
    return new DialectTestConfig(dialectMap, refDialectCode, testClass,
        function);
  }

  /** Returns the dialect with the given code. */
  public Dialect get(DialectCode dialectCode) {
    return requireNonNull(dialectMap.get(dialectCode.name()),
        () -> "dialect " + dialectCode);
  }

  /** Definition of a dialect. */
  static class Dialect {
    /** The name of this dialect. */
    final String name;

    /** The code of this dialect.
     * Having a code isn't strictly necessary, but it makes tests more concise. */
    final DialectCode code;

    /** The dialect object. */
    final SqlDialect sqlDialect;

    /** Whether the dialect is enabled in the test. */
    final boolean enabled;

    /** Whether the test should execute queries in this dialect. If there is a
     * reference, compares the results to the reference. */
    final boolean execute;

    Dialect(String name, DialectCode code, SqlDialect sqlDialect,
        boolean enabled, boolean execute) {
      this.name = requireNonNull(name, "name");
      this.code = requireNonNull(code, "code");
      this.sqlDialect = requireNonNull(sqlDialect, "sqlDialect");
      this.enabled = enabled;
      this.execute = execute;
    }

    /** Creates a Dialect based on a
     *  {@link org.apache.calcite.sql.SqlDialect.DatabaseProduct}. */
    public static Dialect of(DialectCode dialectCode,
        SqlDialect.DatabaseProduct databaseProduct) {
      return of(dialectCode, databaseProduct.getDialect());
    }

    /** Creates a Dialect. */
    public static Dialect of(DialectCode dialectCode, SqlDialect dialect) {
      return new Dialect(dialectCode.name(), dialectCode, dialect, true, false);
    }

    @Override public String toString() {
      return name;
    }

    public Dialect withEnabled(boolean enabled) {
      if (enabled == this.enabled) {
        return this;
      }
      return new Dialect(name, code, sqlDialect, enabled, execute);
    }

    public Dialect withExecute(boolean execute) {
      if (execute == this.execute) {
        return this;
      }
      return new Dialect(name, code, sqlDialect, enabled, execute);
    }

    /** Performs an action with the dialect's connection,
     * or no-ops if no connection. */
    public void withConnection(Consumer<Connection> consumer) {
      // TODO:
    }

    /** Performs an action with a statement from the dialect's connection,
     * or no-ops if no connection. */
    public void withStatement(Consumer<Statement> consumer) {
      withConnection(connection -> {
        try (Statement statement = connection.createStatement()) {
          consumer.accept(statement);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
