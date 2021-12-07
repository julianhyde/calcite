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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * Abstract implementation of
 * {@link org.apache.calcite.test.SqlValidatorTestCase.Tester}
 * that talks to a mock catalog.
 *
 * <p>This is to implement the default behavior: testing is only against the
 * {@link SqlValidator}.
 */
public abstract class AbstractSqlTester implements SqlTester, AutoCloseable {
  protected final SqlTestFactory factory;
  protected final UnaryOperator<SqlValidator> validatorTransform;

  public AbstractSqlTester(SqlTestFactory factory,
      UnaryOperator<SqlValidator> validatorTransform) {
    this.factory = requireNonNull(factory, "factory");
    this.validatorTransform =
        requireNonNull(validatorTransform, "validatorTransform");
  }

  @Override public final SqlTestFactory getFactory() {
    return factory;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This default implementation does nothing.
   */
  @Override public void close() {
    // no resources to release
  }

  @Override public final SqlConformance getConformance() {
    return (SqlConformance) factory.get("conformance");
  }

  @Override public final SqlValidator getValidator() {
    return factory.getValidator();
  }

  @Override public void assertExceptionIsThrown(StringAndPos sap,
      @Nullable String expectedMsgPattern) {
    final SqlValidator validator;
    final SqlNode sqlNode;
    try {
      sqlNode = parseQuery(sap.sql);
      validator = getValidator();
    } catch (Throwable e) {
      SqlTests.checkEx(e, expectedMsgPattern, sap, SqlTests.Stage.PARSE);
      return;
    }

    Throwable thrown = null;
    try {
      validator.validate(sqlNode);
    } catch (Throwable ex) {
      thrown = ex;
    }

    SqlTests.checkEx(thrown, expectedMsgPattern, sap, SqlTests.Stage.VALIDATE);
  }

  protected void checkParseEx(Throwable e, @Nullable String expectedMsgPattern,
      StringAndPos sap) {
    try {
      throw e;
    } catch (SqlParseException spe) {
      String errMessage = spe.getMessage();
      if (expectedMsgPattern == null) {
        throw new RuntimeException("Error while parsing query:" + sap, spe);
      } else if (errMessage == null
          || !errMessage.matches(expectedMsgPattern)) {
        throw new RuntimeException("Error did not match expected ["
            + expectedMsgPattern + "] while parsing query ["
            + sap + "]", spe);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Error while parsing query: " + sap, t);
    }
  }

  @Override public RelDataType getColumnType(String sql) {
    return validateAndApply(StringAndPos.of(sql), (sql1, validator, n) -> {
      final RelDataType rowType =
          validator.getValidatedNodeType(n);
      final List<RelDataTypeField> fields = rowType.getFieldList();
      assertThat("expected query to return 1 field", fields.size(), is(1));
      return fields.get(0).getType();
    });
  }

  @Override public RelDataType getResultType(String sql) {
    return validateAndApply(StringAndPos.of(sql), (sql1, validator, n) ->
        validator.getValidatedNodeType(n));
  }

  @Override public SqlNode parseAndValidate(SqlValidator validator, String sql) {
    SqlNode sqlNode;
    try {
      sqlNode = parseQuery(sql);
    } catch (Throwable e) {
      throw new RuntimeException("Error while parsing query: " + sql, e);
    }
    return validator.validate(sqlNode);
  }

  @Override public SqlNode parseQuery(String sql) throws SqlParseException {
    SqlParser parser = factory.createParser(sql);
    return parser.parseQuery();
  }

  @Override public void checkColumnType(String sql, String expected) {
    validateAndThen(StringAndPos.of(sql), checkColumnTypeAction(is(expected)));
  }

  private static ValidatedNodeConsumer checkColumnTypeAction(
      Matcher<String> matcher) {
    return (sql1, validator, validatedNode) -> {
      final RelDataType rowType =
          validator.getValidatedNodeType(validatedNode);
      final List<RelDataTypeField> fields = rowType.getFieldList();
      assertEquals(1, fields.size(), "expected query to return 1 field");
      final RelDataType actualType = fields.get(0).getType();
      String actual = SqlTests.getTypeString(actualType);
      assertThat(actual, matcher);
    };
  }

  @Override public void checkType(String expression, String type) {
    forEachQueryValidateAndThen(StringAndPos.of(expression),
        checkColumnTypeAction(is(type)));
  }

  @Override public SqlTester withQuoting(Quoting quoting) {
    return with("quoting", quoting);
  }

  @Override public SqlTester withQuotedCasing(Casing casing) {
    return with("quotedCasing", casing);
  }

  @Override public SqlTester withUnquotedCasing(Casing casing) {
    return with("unquotedCasing", casing);
  }

  @Override public SqlTester withCaseSensitive(boolean sensitive) {
    return with("caseSensitive", sensitive);
  }

  @Override public SqlTester withLenientOperatorLookup(boolean lenient) {
    return with("lenientOperatorLookup", lenient);
  }

  @Override public SqlTester withLex(Lex lex) {
    return withQuoting(lex.quoting)
        .withCaseSensitive(lex.caseSensitive)
        .withQuotedCasing(lex.quotedCasing)
        .withUnquotedCasing(lex.unquotedCasing);
  }

  @Override public SqlTester withConformance(SqlConformance conformance) {
    final SqlTester tester = with("conformance", conformance);
    if (conformance instanceof SqlConformanceEnum) {
      return tester
          .withConnectionFactory(
              CalciteAssert.EMPTY_CONNECTION_FACTORY
                  .with(CalciteConnectionProperty.CONFORMANCE, conformance));
    } else {
      return tester;
    }
  }

  @Override public SqlTester enableTypeCoercion(boolean enabled) {
    return with("enableTypeCoercion", enabled);
  }

  @Override public SqlTester withOperatorTable(SqlOperatorTable operatorTable) {
    return with("operatorTable", operatorTable);
  }

  @Override public SqlTester withConnectionFactory(
      CalciteAssert.ConnectionFactory connectionFactory) {
    return with("connectionFactory", connectionFactory);
  }

  // TODO obsolete this method
  protected final SqlTester with(final String name, final Object value) {
    return this; // with(factory.with(name, value));
  }

  protected abstract SqlTester with(SqlTestFactory factory);

  // SqlTester methods

  @Override public void setFor(
      SqlOperator operator,
      VmName... unimplementedVmNames) {
    // do nothing
  }

  @Override public void checkAgg(
      String expr,
      String[] inputValues,
      Object result,
      double delta) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  @Override public void checkAggWithMultipleArgs(
      String expr,
      String[][] inputValues,
      Object result,
      double delta) {
    String query =
        SqlTests.generateAggQueryWithMultipleArgs(expr, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  @Override public void checkWinAgg(
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      Object result,
      double delta) {
    String query =
        SqlTests.generateWinAggQuery(
            expr, windowSpec, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  @Override public void checkScalar(
      String expression,
      Object result,
      String resultType) {
    checkType(expression, resultType);
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.ANY_TYPE_CHECKER, result, 0);
    }
  }

  @Override public void checkScalarExact(
      String expression,
      String result) {
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.INTEGER_TYPE_CHECKER, result, 0);
    }
  }

  @Override public void checkScalarExact(
      String expression,
      String expectedType,
      String result) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, result, 0);
    }
  }

  @Override public void checkScalarApprox(
      String expression,
      String expectedType,
      double expectedResult,
      double delta) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, expectedResult, delta);
    }
  }

  @Override public void checkBoolean(
      String expression,
      @Nullable Boolean result) {
    if (null == result) {
      checkNull(expression);
    } else {
      for (String sql : buildQueries(expression)) {
        check(sql, SqlTests.BOOLEAN_TYPE_CHECKER, result.toString(), 0);
      }
    }
  }

  @Override public void checkString(
      String expression,
      String result,
      String expectedType) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, result, 0);
    }
  }

  @Override public void checkNull(String expression) {
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.ANY_TYPE_CHECKER, null, 0);
    }
  }

  @Override public final void check(
      String query,
      TypeChecker typeChecker,
      @Nullable Object result,
      double delta) {
    check(query, typeChecker, SqlTests.ANY_PARAMETER_CHECKER,
        SqlTests.createChecker(result, delta));
  }

  @Override public void check(String query, TypeChecker typeChecker,
      ParameterChecker parameterChecker, ResultChecker resultChecker) {
    // This implementation does NOT check the result!
    // All it does is check the return type.
    requireNonNull(typeChecker, "typeChecker");
    requireNonNull(parameterChecker, "parameterChecker");
    requireNonNull(resultChecker, "resultChecker");

    // Parse and validate. There should be no errors.
    // There must be 1 column. Get its type.
    RelDataType actualType = getColumnType(query);

    // Check result type.
    typeChecker.checkType(actualType);

    SqlValidator validator = getValidator();
    SqlNode n = parseAndValidate(validator, query);
    final RelDataType parameterRowType = validator.getParameterRowType(n);
    parameterChecker.checkParameters(parameterRowType);
  }

  @Override public void validateAndThen(StringAndPos sap,
      ValidatedNodeConsumer consumer) {
    final SqlValidator validator = validatorTransform.apply(getValidator());
    SqlNode rewrittenNode = parseAndValidate(validator, sap.sql);
    consumer.accept(sap, validator, rewrittenNode);
  }

  @Override public <R> R validateAndApply(StringAndPos sap,
      ValidatedNodeFunction<R> function) {
    final SqlValidator validator = validatorTransform.apply(getValidator());
    SqlNode rewrittenNode = parseAndValidate(validator, sap.sql);
    return function.apply(sap, validator, rewrittenNode);
  }

  @Override public void forEachQueryValidateAndThen(StringAndPos expression,
      ValidatedNodeConsumer consumer) {
    buildQueries(expression.addCarets())
        .forEach(query -> validateAndThen(StringAndPos.of(query), consumer));
  }

  @Override public void checkFails(StringAndPos sap, String expectedError,
      boolean runtime) {
    if (runtime) {
      // We need to test that the expression fails at runtime.
      // Ironically, that means that it must succeed at prepare time.
      SqlValidator validator = getValidator();
      final String sql = buildQuery(sap.addCarets());
      SqlNode n = parseAndValidate(validator, sql);
      assertNotNull(n);
    } else {
      checkQueryFails(StringAndPos.of(buildQuery(sap.addCarets())),
          expectedError);
    }
  }

  @Override public void checkQueryFails(StringAndPos sap, String expectedError) {
    assertExceptionIsThrown(sap, expectedError);
  }

  @Override public void checkAggFails(
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime) {
    final String sql =
        SqlTests.generateAggQuery(expr, inputValues);
    if (runtime) {
      SqlValidator validator = getValidator();
      SqlNode n = parseAndValidate(validator, sql);
      assertNotNull(n);
    } else {
      checkQueryFails(StringAndPos.of(sql), expectedError);
    }
  }

  @Override public void checkQuery(String sql) {
    assertExceptionIsThrown(StringAndPos.of(sql), null);
  }

  public static String buildQuery(String expression) {
    return "values (" + expression + ")";
  }

  public static String buildQueryAgg(String expression) {
    return "select " + expression + " from (values (1)) as t(x) group by x";
  }

  /**
   * Builds a query that extracts all literals as columns in an underlying
   * select.
   *
   * <p>For example,</p>
   *
   * <blockquote>{@code 1 < 5}</blockquote>
   *
   * <p>becomes</p>
   *
   * <blockquote>{@code SELECT p0 < p1
   * FROM (VALUES (1, 5)) AS t(p0, p1)}</blockquote>
   *
   * <p>Null literals don't have enough type information to be extracted.
   * We push down {@code CAST(NULL AS type)} but raw nulls such as
   * {@code CASE 1 WHEN 2 THEN 'a' ELSE NULL END} are left as is.</p>
   *
   * @param expression Scalar expression
   * @return Query that evaluates a scalar expression
   */
  protected String buildQuery2(String expression) {
    if (expression.matches("(?i).*percentile_(cont|disc).*")) {
      // PERCENTILE_CONT requires its argument to be a literal,
      // so converting its argument to a column will cause false errors.
      return buildQuery(expression);
    }
    // "values (1 < 5)"
    // becomes
    // "select p0 < p1 from (values (1, 5)) as t(p0, p1)"
    SqlNode x;
    final String sql = "values (" + expression + ")";
    try {
      x = parseQuery(sql);
    } catch (SqlParseException e) {
      throw TestUtil.rethrow(e);
    }
    final Collection<SqlNode> literalSet = new LinkedHashSet<>();
    x.accept(
        new SqlShuttle() {
          private final List<SqlOperator> ops =
              ImmutableList.of(
                  SqlStdOperatorTable.LITERAL_CHAIN,
                  SqlStdOperatorTable.LOCALTIME,
                  SqlStdOperatorTable.LOCALTIMESTAMP,
                  SqlStdOperatorTable.CURRENT_TIME,
                  SqlStdOperatorTable.CURRENT_TIMESTAMP);

          @Override public SqlNode visit(SqlLiteral literal) {
            if (!isNull(literal)
                && literal.getTypeName() != SqlTypeName.SYMBOL) {
              literalSet.add(literal);
            }
            return literal;
          }

          @Override public SqlNode visit(SqlCall call) {
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlUnresolvedFunction) {
              final SqlUnresolvedFunction unresolvedFunction = (SqlUnresolvedFunction) operator;
              final SqlOperator lookup = SqlValidatorUtil.lookupSqlFunctionByID(
                  SqlStdOperatorTable.instance(),
                  unresolvedFunction.getSqlIdentifier(),
                  unresolvedFunction.getFunctionType());
              if (lookup != null) {
                operator = lookup;
                call = operator.createCall(call.getFunctionQuantifier(),
                    call.getParserPosition(), call.getOperandList());
              }
            }
            if (operator == SqlStdOperatorTable.CAST
                && isNull(call.operand(0))) {
              literalSet.add(call);
              return call;
            } else if (ops.contains(operator)) {
              // "Argument to function 'LOCALTIME' must be a
              // literal"
              return call;
            } else {
              return super.visit(call);
            }
          }

          private boolean isNull(SqlNode sqlNode) {
            return sqlNode instanceof SqlLiteral
                && ((SqlLiteral) sqlNode).getTypeName()
                == SqlTypeName.NULL;
          }
        });
    final List<SqlNode> nodes = new ArrayList<>(literalSet);
    nodes.sort((o1, o2) -> {
      final SqlParserPos pos0 = o1.getParserPosition();
      final SqlParserPos pos1 = o2.getParserPosition();
      int c = -Utilities.compare(pos0.getLineNum(), pos1.getLineNum());
      if (c != 0) {
        return c;
      }
      return -Utilities.compare(pos0.getColumnNum(), pos1.getColumnNum());
    });
    String sql2 = sql;
    final List<Pair<String, String>> values = new ArrayList<>();
    int p = 0;
    for (SqlNode literal : nodes) {
      final SqlParserPos pos = literal.getParserPosition();
      final int start =
          SqlParserUtil.lineColToIndex(
              sql, pos.getLineNum(), pos.getColumnNum());
      final int end =
          SqlParserUtil.lineColToIndex(
              sql,
              pos.getEndLineNum(),
              pos.getEndColumnNum()) + 1;
      String param = "p" + p++;
      values.add(Pair.of(sql2.substring(start, end), param));
      sql2 = sql2.substring(0, start)
          + param
          + sql2.substring(end);
    }
    if (values.isEmpty()) {
      values.add(Pair.of("1", "p0"));
    }
    return "select "
        + sql2.substring("values (".length(), sql2.length() - 1)
        + " from (values ("
        + Util.commaList(Pair.left(values))
        + ")) as t("
        + Util.commaList(Pair.right(values))
        + ")";
  }

  /**
   * Converts a scalar expression into a list of SQL queries that
   * evaluate it.
   *
   * @param expression Scalar expression
   * @return List of queries that evaluate an expression
   */
  private Iterable<String> buildQueries(final String expression) {
    // Why an explicit iterable rather than a list? If there is
    // a syntax error in the expression, the calling code discovers it
    // before we try to parse it to do substitutions on the parse tree.
    return () -> new Iterator<String>() {
      int i = 0;

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override public String next() {
        switch (i++) {
        case 0:
          return buildQuery(expression);
        case 1:
          return buildQuery2(expression);
        default:
          throw new NoSuchElementException();
        }
      }

      @Override public boolean hasNext() {
        return i < 2;
      }
    };
  }

}
