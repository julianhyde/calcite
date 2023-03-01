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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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

  /** The name of the reference dialect. If not null, the queries from this
   * dialect as used as exemplars for other dialects: the other dialects are
   * expected to return the same set of rows as the reference. */
  final @Nullable String refDialectName;

  /** The name of the class relative to which the resource file containing
   * query responses is located. */
  @SuppressWarnings("rawtypes")
  private final Class testClass;

  /** A function that maps a dialect name to the name of the file containing
   * its query responses. */
  private final Function<String, String> function;

  private DialectTestConfig(Map<String, Dialect> dialectMap,
      @Nullable String refDialectName,
      @SuppressWarnings("rawtypes") Class testClass,
      Function<String, String> function) {
    this.dialectMap = ImmutableMap.copyOf(dialectMap);
    this.refDialectName = refDialectName;
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
    return new DialectTestConfig(dialectMap2, refDialectName, testClass,
        function);
  }

  /** Sets the name of the reference dialect. */
  public DialectTestConfig withReference(String refDialectName) {
    if (Objects.equals(refDialectName, this.refDialectName)) {
      return this;
    }
    return new DialectTestConfig(dialectMap, refDialectName, testClass,
        function);
  }

  /** Sets the path for any given dialect's corpus. */
  public DialectTestConfig withPath(
      @SuppressWarnings("rawtypes") Class testClass,
      Function<String, String> function) {
    if (testClass == this.testClass && function == this.function) {
      return this;
    }
    return new DialectTestConfig(dialectMap, refDialectName, testClass,
        function);
  }

  /** Definition of a dialect. */
  static class Dialect {
    /** The name of this dialect. */
    final String name;

    /** The dialect object. */
    final SqlDialect sqlDialect;

    /** Whether the test should execute queries in this dialect. If there is a
     * reference, compares the results to the reference. */
    final boolean execute;

    Dialect(String name, SqlDialect sqlDialect, boolean execute) {
      this.name = requireNonNull(name, "name");
      this.sqlDialect = requireNonNull(sqlDialect, "sqlDialect");
      this.execute = execute;
    }

    /** Creates a Dialect based on a
     *  {@link org.apache.calcite.sql.SqlDialect.DatabaseProduct}. */
    public static Dialect of(String name,
        SqlDialect.DatabaseProduct databaseProduct) {
      return of(name, databaseProduct.getDialect());
    }

    /** Creates a Dialect. */
    public static Dialect of(String name, SqlDialect dialect) {
      return new Dialect(name, dialect, false);
    }

    @Override public String toString() {
      return name;
    }

    public Dialect withExecute(boolean execute) {
      if (execute == this.execute) {
        return this;
      }
      return new Dialect(name, sqlDialect, execute);
    }
  }
}
