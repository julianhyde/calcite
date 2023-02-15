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

/** Description of the dialects that are enabled for a particular test.
 *
 * <p>Each dialect has a name, optionally a connection factory,
 * and a state (enabled, recording, replaying).
 *
 * <p>It is immutable.
 */
class DialectTestConfig {
  final ImmutableMap<String, Dialect> dialectMap;

  private DialectTestConfig(ImmutableMap<String, Dialect> dialectMap) {
    this.dialectMap = dialectMap;
  }

  /** Creates a DialectTestConfig. */
  static DialectTestConfig of(Iterable<Dialect> dialects) {
    final ImmutableMap.Builder<String, Dialect> map = ImmutableMap.builder();
    dialects.forEach(dialect -> map.put(dialect.name, dialect));
    return new DialectTestConfig(map.build());
  }

  /** Definition of a dialect. */
  static class Dialect {
    final String name;

    Dialect(String name, SqlDialect dialect) {
      this.name = name;
    }

    /** Creates a Dialect based on a
     *  {@link org.apache.calcite.sql.SqlDialect.DatabaseProduct}. */
    public static Dialect of(String name,
        SqlDialect.DatabaseProduct databaseProduct) {
      return of(name, databaseProduct.getDialect());
    }

    /** Creates a Dialect. */
    public static Dialect of(String name, SqlDialect dialect) {
      return new Dialect(name, dialect);
    }
  }
}
