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
package org.apache.calcite.sql.fun;

import org.apache.calcite.config.CalciteConnectionProperty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A library is a collection of SQL functions and operators.
 *
 * <p>Typically, such collections are associated with a particular dialect or
 * database. For example, {@link SqlSemantic#ORACLE} is a collection of functions
 * that are in the Oracle database but not the SQL standard.
 *
 * <p>In {@link SqlLibraryOperatorTableFactory} this annotation is applied to
 * function definitions to include them in a particular library. It allows
 * an operator to belong to more than one library.
 *
 * @see LibraryOperator
 */
public enum SqlSemantic {
  /** The standard operators. */
  STANDARD("", "standard"),
  /** Geospatial operators. */
  SPATIAL("s", "spatial"),
  /** A collection of operators that are in Google BigQuery but not in standard
   * SQL. */
  BIG_QUERY("b", "bigquery"),
  /** A collection of operators that are in Apache Hive but not in standard
   * SQL. */
  HIVE("h", "hive"),
  /** A collection of operators that are in MySQL but not in standard SQL. */
  MYSQL("m", "mysql"),
  /** A collection of operators that are in Oracle but not in standard SQL. */
  ORACLE("o", "oracle"),
  /** A collection of operators that are in PostgreSQL but not in standard
   * SQL. */
  POSTGRESQL("p", "postgresql"),
  /** A collection of operators that are in Apache Spark but not in standard
   * SQL. */
  SPARK("s", "spark");

  /** Abbreviation for the library used in SQL reference. */
  public final String abbrev;

  /** Name of this library when it appears in the connect string;
   * see {@link CalciteConnectionProperty#FUN}. */
  public final String fun;

  SqlSemantic(String abbrev, String fun) {
    this.abbrev = Objects.requireNonNull(abbrev);
    this.fun = Objects.requireNonNull(fun);
    Preconditions.checkArgument(
        fun.equals(name().toLowerCase(Locale.ROOT).replace("_", "")));
  }

  /** Looks up a value.
   * Returns null if not found.
   * You can use upper- or lower-case name. */
  public static @Nullable SqlSemantic of(String name) {
    return MAP.get(name);
  }

  /** Parses a comma-separated string such as "standard,oracle". */
  public static List<SqlSemantic> parse(String libraryNameList) {
    final ImmutableList.Builder<SqlSemantic> list = ImmutableList.builder();
    for (String libraryName : libraryNameList.split(",")) {
      SqlSemantic library = Objects.requireNonNull(
          SqlSemantic.of(libraryName), () -> "library does not exist: " + libraryName);
      list.add(library);
    }
    return list.build();
  }

  public static final Map<String, SqlSemantic> MAP;

  static {
    final ImmutableMap.Builder<String, SqlSemantic> builder =
        ImmutableMap.builder();
    for (SqlSemantic value : values()) {
      builder.put(value.name(), value);
      builder.put(value.fun, value);
    }
    MAP = builder.build();
  }
}
