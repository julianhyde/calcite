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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.SqlValidatorTestCase;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link org.apache.calcite.test.SqlValidatorTestCase.Tester}.
 */
public class SqlValidatorTesterImpl implements SqlValidatorTestCase.Tester {

  /** Default instance. */
  public static final SqlValidatorTesterImpl DEFAULT =
      new SqlValidatorTesterImpl();

  public SqlValidatorTesterImpl() {
  }

  @Override public SqlNode parseQuery(SqlParser.Config parserConfig, String sql)
      throws SqlParseException {
    return null;
  }

  @Override public SqlNode parseAndValidate(SqlValidator validator, String sql) {
    return null;
  }

  @Override public void validateAndThen(StringAndPos sap,
      SqlTester.ValidatedNodeConsumer consumer) { // TODO
  }

  @Override public <R> R validateAndApply(StringAndPos sap,
      SqlTester.ValidatedNodeFunction<R> function) {
    return null;
  }

  @Override public void forEachQueryValidateAndThen(StringAndPos expression,
      SqlTester.ValidatedNodeConsumer consumer) {
  }

  @Override public SqlValidator getValidator() {
    return null;
  }

  @Override public void assertExceptionIsThrown(StringAndPos sap,
      SqlParser.Config parserConfig, @Nullable String expectedMsgPattern) {
  }

  @Override public RelDataType getColumnType(String sql) {
    return null;
  }

  @Override public RelDataType getResultType(String sql) {
    return null;
  }

  @Override public void checkColumnType(String sql, String expected) {
  }

  @Override public SqlConformance getConformance() {
    return null;
  }
}
