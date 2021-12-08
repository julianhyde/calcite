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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.validate.SqlValidator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;

/**
 * Callback for testing SQL queries and expressions.
 *
 * <p>The idea is that when you define an operator (or another piece of SQL
 * functionality), you can define the logical behavior of that operator once, as
 * part of that operator. Later you can define one or more physical
 * implementations of that operator, and test them all using the same set of
 * tests.
 *
 * <p>Specific implementations of <code>SqlTester</code> might evaluate the
 * queries in different ways, for example, using a C++ versus Java calculator.
 * An implementation might even ignore certain calls altogether.
 */
public interface SqlTester extends AutoCloseable {
  // TODO: make sure that 'param factory' occurs in each method javadoc

  //~ Enums ------------------------------------------------------------------

  /**
   * Name of a virtual machine that can potentially implement an operator.
   */
  enum VmName {
    FENNEL, JAVA, EXPAND
  }

  //~ Methods ----------------------------------------------------------------

  /** Parses and validates a query, then calls an action on the result. */
  void validateAndThen(SqlNewTestFactory factory, StringAndPos sap,
      ValidatedNodeConsumer consumer);

  /** Given an expression, generates several queries that include that
   * expression, and for each, parses and validates, then calls an action on
   * the result. */
  void forEachQueryValidateAndThen(SqlNewTestFactory factory,
      StringAndPos expression, ValidatedNodeConsumer consumer);

  /**
   * Checks that a query is valid, or, if invalid, throws the right
   * message at the right location.
   *
   * <p>If <code>expectedMsgPattern</code> is null, the query must
   * succeed.
   *
   * <p>If <code>expectedMsgPattern</code> is not null, the query must
   * fail, and give an error location of (expectedLine, expectedColumn)
   * through (expectedEndLine, expectedEndColumn).
   *
   * @param factory            Factory
   * @param sap                SQL statement
   * @param expectedMsgPattern If this parameter is null the query must be
   */
  void assertExceptionIsThrown(SqlNewTestFactory factory, StringAndPos sap,
      @Nullable String expectedMsgPattern);

  /**
   * Returns the data type of the sole column of a SQL query.
   *
   * <p>For example, <code>getResultType("VALUES (1")</code> returns
   * <code>INTEGER</code>.
   *
   * <p>Fails if query returns more than one column.
   *
   * @see #getResultType(SqlNewTestFactory, String)
   */
  RelDataType getColumnType(SqlNewTestFactory factory, String sql);

  /**
   * Returns the data type of the row returned by a SQL query.
   *
   * <p>For example, <code>getResultType("VALUES (1, 'foo')")</code>
   * returns <code>RecordType(INTEGER EXPR$0, CHAR(3) EXPR#1)</code>.
   */
  RelDataType getResultType(SqlNewTestFactory factory, String sql);

  /**
   * Tests that a scalar SQL expression returns the expected result and the
   * expected type. For example,
   *
   * <blockquote>
   * <pre>checkScalar("1.1 + 2.9", "4.0", "DECIMAL(2, 1) NOT NULL");</pre>
   * </blockquote>
   *
   * @param factory    Factory
   * @param expression Scalar expression
   * @param result     Expected result
   * @param resultType Expected result type
   */
  void checkScalar(SqlNewTestFactory factory,
      String expression,
      Object result,
      String resultType);

  /**
   * Tests that a scalar SQL expression returns the expected exact numeric
   * result as an integer. For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("1 + 2", "3");</pre>
   * </blockquote>
   *
   * @param factory    Factory
   * @param expression Scalar expression
   * @param result     Expected result
   */
  void checkScalarExact(
      SqlNewTestFactory factory, String expression,
      String result);

  /**
   * Tests that a scalar SQL expression returns the expected exact numeric
   * result. For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("1 + 2", "3");</pre>
   * </blockquote>
   *
   * @param factory      Factory
   * @param expression   Scalar expression
   * @param expectedType Type we expect the result to have, including
   *                     nullability, precision and scale, for example
   *                     <code>DECIMAL(2, 1) NOT NULL</code>.
   * @param result       Expected result
   */
  void checkScalarExact(SqlNewTestFactory factory,
      String expression,
      String expectedType,
      String result);

  /**
   * Tests that a scalar SQL expression returns expected appoximate numeric
   * result. For example,
   *
   * <blockquote>
   * <pre>checkScalarApprox("1.0 + 2.1", "3.1");</pre>
   * </blockquote>
   *
   * @param factory        Factory
   * @param expression     Scalar expression
   * @param expectedType   Type we expect the result to have, including
   *                       nullability, precision and scale, for example
   *                       <code>DECIMAL(2, 1) NOT NULL</code>.
   * @param expectedResult Expected result
   * @param delta          Allowed margin of error between expected and actual
   *                       result
   */
  void checkScalarApprox(
      SqlNewTestFactory factory, String expression,
      String expectedType,
      double expectedResult,
      double delta);

  /**
   * Tests that a scalar SQL expression returns the expected boolean result.
   * For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("TRUE AND FALSE", Boolean.TRUE);</pre>
   * </blockquote>
   *
   * <p>The expected result can be null:
   *
   * <blockquote>
   * <pre>checkScalarExact("NOT UNKNOWN", null);</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result (null signifies NULL).
   */
  void checkBoolean(SqlNewTestFactory factory,
      String expression,
      @Nullable Boolean result);

  /**
   * Tests that a scalar SQL expression returns the expected string result.
   * For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("'ab' || 'c'", "abc");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result
   * @param resultType Expected result type
   */
  void checkString(SqlNewTestFactory factory,
      String expression,
      String result,
      String resultType);

  /**
   * Tests that a SQL expression returns the SQL NULL value. For example,
   *
   * <blockquote>
   * <pre>checkNull("CHAR_LENGTH(CAST(NULL AS VARCHAR(3))");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   */
  void checkNull(SqlNewTestFactory factory, String expression);

  /**
   * Tests that a SQL expression has a given type. For example,
   *
   * <blockquote>
   * <code>checkType("SUBSTR('hello' FROM 1 FOR 3)",
   * "VARCHAR(3) NOT NULL");</code>
   * </blockquote>
   *
   * <p>This method checks length/precision, scale, and whether the type allows
   * NULL values, so is more precise than the type-checking done by methods
   * such as {@link #checkScalarExact}.
   *
   * @param expression Scalar expression
   * @param type       Type string
   */
  void checkType(SqlNewTestFactory factory,
      String expression,
      String type);

  /**
   * Checks that a query returns one column of an expected type. For example,
   * <code>checkType("VALUES (1 + 2)", "INTEGER NOT NULL")</code>.
   *
   * @param sql  Query expression
   * @param type Type string
   */
  void checkColumnType(SqlNewTestFactory factory,
      String sql,
      String type);

  /**
   * Tests that a SQL query returns a single column with the given type. For
   * example,
   *
   * <blockquote>
   * <pre>check("VALUES (1 + 2)", "3", SqlTypeName.Integer);</pre>
   * </blockquote>
   *
   * <p>If <code>result</code> is null, the expression must yield the SQL NULL
   * value. If <code>result</code> is a {@link java.util.regex.Pattern}, the
   * result must match that pattern.
   *
   * @param query       SQL query
   * @param typeChecker Checks whether the result is the expected type; must
   *                    not be null
   * @param result      Expected result
   * @param delta       The acceptable tolerance between the expected and actual
   */
  void check(SqlNewTestFactory factory,
      String query,
      TypeChecker typeChecker,
      @Nullable Object result,
      double delta);

  /**
   * Tests that a SQL query returns a result of expected type and value.
   * Checking of type and value are abstracted using {@link TypeChecker}
   * and {@link ResultChecker} functors.
   *
   * @param factory       Factory
   * @param query         SQL query
   * @param typeChecker   Checks whether the result is the expected type
   * @param parameterChecker Checks whether the parameters are of expected
   *                      types
   * @param resultChecker Checks whether the result has the expected value
   */
  void check(SqlNewTestFactory factory,
      String query,
      TypeChecker typeChecker,
      ParameterChecker parameterChecker,
      ResultChecker resultChecker);

  /**
   * Declares that this test is for a given operator. So we can check that all
   * operators are tested.
   *
   * @param operator             Operator
   * @param unimplementedVmNames Names of virtual machines for which this
   */
  void setFor(
      SqlOperator operator,
      VmName... unimplementedVmNames);

  /**
   * Checks that an aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkAgg("AVG(DISTINCT x)", new String[] {"2", "3",
   * null, "3" }, new Double(2.5), 0);</code>
   *
   * @param expr        Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
   * @param inputValues Array of input values, e.g. <code>["1", null,
   *                    "2"]</code>.
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkAgg(SqlNewTestFactory factory,
      String expr,
      String[] inputValues,
      Object result,
      double delta);

  /**
   * Checks that an aggregate expression with multiple args returns the expected
   * result.
   *
   * @param expr        Aggregate expression, e.g. <code>AGG_FUNC(x, x2, x3)</code>
   * @param inputValues Nested array of input values, e.g. <code>[
   *                    ["1", null, "2"]
   *                    ["3", "4", null]
   *                    ]</code>.
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkAggWithMultipleArgs(SqlNewTestFactory factory,
      String expr,
      String[][] inputValues,
      Object result,
      double delta);

  /**
   * Checks that a windowed aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkWinAgg("FIRST_VALUE(x)", new String[] {"2",
   * "3", null, "3" }, "INTEGER NOT NULL", 2, 0d);</code>
   *
   * @param expr        Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
   * @param inputValues Array of input values, e.g. <code>["1", null,
   *                    "2"]</code>.
   * @param type        Expected result type
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkWinAgg(SqlNewTestFactory factory,
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      Object result,
      double delta);

  /**
   * Tests that an aggregate expression fails at run time.
   *
   * @param expr An aggregate expression
   * @param inputValues Array of input values
   * @param expectedError Pattern for expected error
   * @param runtime       If true, must fail at runtime; if false, must fail at
   *                      validate time
   */
  void checkAggFails(SqlNewTestFactory factory,
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime);

  /**
   * Tests that a scalar SQL expression fails at run time.
   *
   * @param factory       Factory
   * @param expression    SQL scalar expression
   * @param expectedError Pattern for expected error. If !runtime, must
   *                      include an error location.
   * @param runtime       If true, must fail at runtime; if false, must fail at
   *                      validate time
   */
  void checkFails(SqlNewTestFactory factory,
      StringAndPos expression,
      String expectedError,
      boolean runtime);

  /** As {@link #checkFails(SqlNewTestFactory, StringAndPos, String, boolean)},
   * but with a string that contains carets. */
  default void checkFails(SqlNewTestFactory factory,
      String expression,
      String expectedError,
      boolean runtime) {
    checkFails(factory, StringAndPos.of(expression), expectedError, runtime);
  }

  /**
   * Tests that a SQL query fails at prepare time.
   *
   * @param factory       Factory
   * @param sap           SQL query and error position
   * @param expectedError Pattern for expected error. Must
   *                      include an error location.
   */
  void checkQueryFails(SqlNewTestFactory factory, StringAndPos sap,
      String expectedError);

  /**
   * Tests that a SQL query succeeds at prepare time.
   *
   * @param factory       Factory
   * @param sql           SQL query
   */
  void checkQuery(SqlNewTestFactory factory, String sql);

  //~ Inner Interfaces -------------------------------------------------------

  /** Type checker. */
  interface TypeChecker {
    void checkType(RelDataType type);
  }

  /** Parameter checker. */
  interface ParameterChecker {
    void checkParameters(RelDataType parameterRowType);
  }

  /** Result checker. */
  interface ResultChecker {
    void checkResult(ResultSet result) throws Exception;
  }

  /** Action that is called after validation.
   *
   * @see #validateAndThen
   * @see #forEachQueryValidateAndThen
   */
  interface ValidatedNodeConsumer {
    void accept(StringAndPos sap, SqlValidator validator,
        SqlNode validatedNode);
  }

  /** A function to apply to the result of validation.
   *
   * @param <R> Result type of the function
   *
   * @see AbstractSqlTester#validateAndApply */
  interface ValidatedNodeFunction<R> {
    R apply(StringAndPos sap, SqlValidator validator, SqlNode validatedNode);
  }
}
