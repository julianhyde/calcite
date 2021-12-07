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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlNewTestFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.function.UnaryOperator;

/** Test configuration for validator tests. */
@Value.Immutable
public interface SqlValidatorTestConfig {
  /** Returns the dialect (may be null). */
  @Value.Default
  // CALCITE-4831: remove the second nullable annotation once immutables/#1261 is fixed
  default @javax.annotation.Nullable @Nullable SqlDialect dialect() {
    return CalciteSqlDialect.DEFAULT;
  }

  /**
   * Sets {@link #dialect()}.
   */
  SqlValidatorTestConfig withDialect(@Nullable SqlDialect dialect);

  /**
   * Returns how identifiers are quoted, default DOUBLE_QUOTE.
   */
  @Value.Default
  default Quoting quoting() {
    return Quoting.DOUBLE_QUOTE;
  }

  /**
   * Sets {@link #quoting()}.
   */
  SqlValidatorTestConfig withQuoting(Quoting quoting);

  @Value.Default
  default Casing quotedCasing() {
    return Casing.UNCHANGED;
  }

  /**
   * Sets {@link #quotedCasing()}.
   */
  SqlValidatorTestConfig withQuotedCasing(Casing casing);

  @Value.Default
  default Casing unquotedCasing() {
    return Casing.TO_UPPER;
  }

  /**
   * Sets {@link #unquotedCasing()}.
   */
  SqlValidatorTestConfig withUnquotedCasing(Casing casing);

  @Value.Default
  default SqlConformance conformance() {
    return SqlConformanceEnum.DEFAULT;
  }

  /**
   * Sets {@link #conformance()}.
   */
  SqlValidatorTestConfig withConformance(SqlConformance conformance);

  /** Generates a parser configuration. */
  default SqlParser.Config toParserConfig() { // TODO: get parser config from SqlNewTestFactory
    return SqlParser.Config.DEFAULT
        .withQuoting(quoting())
        .withConformance(conformance())
        .withUnquotedCasing(unquotedCasing())
        .withQuotedCasing(quotedCasing());
  }

  SqlValidatorTestConfig withTypeCoercion(boolean typeCoercion);

  @Value.Default
  default boolean typeCoercion() {
    return true;
  }

  SqlValidatorTestConfig withCaseSensitive(boolean caseSensitive);

  @Value.Default
  default boolean caseSensitive() {
    return true;
  }

  SqlValidatorTestConfig withOperatorTable(SqlOperatorTable operatorTable);

  @Value.Default
  default SqlOperatorTable operatorTable() {
    return SqlStdOperatorTable.instance();
  }

  @Value.Default
  default UnaryOperator<SqlValidator> validatorTransform() {
    return v -> v;
  }

  SqlValidatorTestConfig withValidatorTransform(
      UnaryOperator<SqlValidator> transform);

  default SqlValidator getValidator() {
    return testFactory().getValidator();
  }

  default SqlValidator.Config validatorConfig() {
    final SqlConformance conformance = conformance();
    final boolean lenientOperatorLookup = lenientOperatorLookup();
    final boolean enableTypeCoercion = typeCoercion();
    return SqlValidator.Config.DEFAULT
        .withSqlConformance(conformance)
        .withTypeCoercionEnabled(enableTypeCoercion)
        .withLenientOperatorLookup(lenientOperatorLookup);
  }

  @Value.Default
  default boolean lenientOperatorLookup() {
    return false;
  }

  @Value.Default
  default SqlNewTestFactory testFactory() {
    return SqlNewTestFactory.INSTANCE;
  }
}
