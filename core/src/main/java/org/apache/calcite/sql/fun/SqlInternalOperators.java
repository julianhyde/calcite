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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static org.apache.calcite.sql.SqlOperator.MDX_PRECEDENCE;

import static java.util.Objects.requireNonNull;

/**
 * Contains internal operators.
 *
 * <p>These operators are always created directly, not by looking up a function
 * or operator by name or syntax, and therefore this class does not implement
 * interface {@link SqlOperatorTable}.
 */
public abstract class SqlInternalOperators {
  // TODO move down

  /** {@code EXTEND} operator.
   *
   * <p>Adds columns to a table's schema, as in
   * {@code SELECT ... FROM emp EXTEND (horoscope VARCHAR(100))}.
   *
   * <p>Not standard SQL. Added to Calcite to support Phoenix, but can be used
   * to achieve schema-on-query against other adapters.
   */
  public static final SqlOperator EXTEND =
      SqlBasicOperator.create("EXTEND")
          .withKind(SqlKind.EXTEND)
          .withPrecedence(MDX_PRECEDENCE, true)
          .withUnparse(SqlInternalOperators::unparseExtend)
          .toInternal();

  /** Operator that indicates that the result is a json string. */
  public static final SqlOperator JSON_TYPE_OPERATOR =
      SqlBasicOperator.create(SqlKind.JSON_TYPE)
          .withPrecedence(MDX_PRECEDENCE, true)
          .withReturnTypeInference(
              ReturnTypes.explicit(SqlTypeName.ANY)
                  .andThen(SqlTypeTransforms.TO_NULLABLE))
          .withOperandChecker(OperandTypes.CHARACTER)
          .withUnparse(UnparseHandler::unparseUsingOperand0);
  /**
   * The internal "$SLICE" operator takes a multiset of records and returns a
   * multiset of the first column of those records.
   *
   * <p>It is introduced when multisets of scalar types are created, in order
   * to keep types consistent. For example, <code>MULTISET [5]</code> has type
   * <code>INTEGER MULTISET</code> but is translated to an expression of type
   * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal
   * representation of multisets, every element must be a record. Applying the
   * "$SLICE" operator to this result converts the type back to an <code>
   * INTEGER MULTISET</code> multiset value.
   *
   * <p><code>$SLICE</code> is often translated away when the multiset type is
   * converted back to scalar values.
   */
  public static final SqlInternalOperator SLICE =
      new SqlInternalOperator(
          "$SLICE",
          SqlKind.OTHER,
          0,
          false,
          ReturnTypes.MULTISET_PROJECT0,
          null,
          OperandTypes.RECORD_COLLECTION) {
      };

  /**
   * The internal "$ELEMENT_SLICE" operator returns the first field of the
   * only element of a multiset.
   *
   * <p>It is introduced when multisets of scalar types are created, in order
   * to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code>
   * is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5
   * EXPR$0))</code> It is translated away when the multiset type is converted
   * back to scalar values.
   *
   * <p>NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but
   * I'm not deleting the operator, because some multiset tests are disabled,
   * and we may need this operator to get them working!
   */
  public static final SqlInternalOperator ELEMENT_SLICE =
      new SqlInternalOperator(
          "$ELEMENT_SLICE",
          SqlKind.OTHER,
          0,
          false,
          ReturnTypes.MULTISET_RECORD,
          null,
          OperandTypes.MULTISET) {
        @Override public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          SqlUtil.unparseFunctionSyntax(this, writer, call, false);
        }
      };

  /**
   * The internal "$SCALAR_QUERY" operator returns a scalar value from a
   * record type. It assumes the record type only has one field, and returns
   * that field as the output.
   */
  public static final SqlInternalOperator SCALAR_QUERY =
      new SqlInternalOperator(
          "$SCALAR_QUERY",
          SqlKind.SCALAR_QUERY,
          0,
          false,
          ReturnTypes.RECORD_TO_SCALAR,
          null,
          OperandTypes.RECORD_TO_SCALAR) {
        @Override public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          final SqlWriter.Frame frame = writer.startList("(", ")");
          call.operand(0).unparse(writer, 0, 0);
          writer.endList(frame);
        }

        @Override public boolean argumentMustBeScalar(int ordinal) {
          // Obvious, really.
          return false;
        }
      };

  /**
   * The internal {@code $STRUCT_ACCESS} operator is used to access a
   * field of a record.
   *
   * <p>In contrast with {@link SqlStdOperatorTable#DOT} operator, it never
   * appears in an {@link SqlNode} tree and allows to access fields by position
   * and not by name.
   */
  public static final SqlInternalOperator STRUCT_ACCESS =
      SqlBasicOperator.create("$STRUCT_ACCESS")
          .toInternal();

  private SqlInternalOperators() {
  }

  /** Similar to {@link SqlStdOperatorTable#ROW}, but does not print "ROW".
   *
   * <p>For arguments [1, TRUE], ROW would print "{@code ROW (1, TRUE)}",
   * but this operator prints "{@code (1, TRUE)}". */
  public static final SqlRowOperator ANONYMOUS_ROW =
      new SqlRowOperator("$ANONYMOUS_ROW") {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          @SuppressWarnings("assignment.type.incompatible")
          List<@Nullable SqlNode> operandList = call.getOperandList();
          writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
              SqlNodeList.of(call.getParserPosition(), operandList));
        }
      };

  /** Similar to {@link #ANONYMOUS_ROW}, but does not print "ROW" or
   * parentheses.
   *
   * <p>For arguments [1, TRUE], prints "{@code 1, TRUE}".  It is used in
   * contexts where parentheses have been printed (because we thought we were
   * about to print "{@code (ROW (1, TRUE))}") and we wish we had not. */
  public static final SqlRowOperator ANONYMOUS_ROW_NO_PARENTHESES =
      new SqlRowOperator("$ANONYMOUS_ROW_NO_PARENTHESES") {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          final SqlWriter.Frame frame =
              writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL);
          for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, leftPrec, rightPrec);
          }
          writer.endList(frame);
        }
      };

  /** "$THROW_UNLESS(condition, message)" throws an error with the given message
   * if condition is not TRUE, otherwise returns TRUE. */
  public static final SqlInternalOperator THROW_UNLESS =
      SqlBasicOperator.create("$THROW_UNLESS")
          .toInternal();

  /** An IN operator for Druid.
   *
   * <p>Unlike the regular
   * {@link SqlStdOperatorTable#IN} operator it may
   * be used in {@link RexCall}. It does not require that
   * its operands have consistent types. */
  public static final SqlInOperator DRUID_IN =
      new SqlInOperator(SqlKind.DRUID_IN);

  /** A NOT IN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlInOperator DRUID_NOT_IN =
      new SqlInOperator(SqlKind.DRUID_NOT_IN);

  /** A BETWEEN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlBetweenOperator DRUID_BETWEEN =
      new SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, false) {
        @Override public SqlKind getKind() {
          return SqlKind.DRUID_BETWEEN;
        }

        @Override public boolean validRexOperands(int count, Litmus litmus) {
          return litmus.succeed();
        }
      };

  /** Separator expression inside GROUP_CONCAT, e.g. '{@code SEPARATOR ','}'. */
  public static final SqlOperator SEPARATOR =
      SqlBasicOperator.create(SqlKind.SEPARATOR)
          .withPrecedence(20, false)
          .toInternal();

  /** {@code DISTINCT} operator, occurs within {@code GROUP BY} clause. */
  public static final SqlInternalOperator GROUP_BY_DISTINCT =
      new SqlRollupOperator("GROUP BY DISTINCT", SqlKind.GROUP_BY_DISTINCT);

  /** Fetch operator is ONLY used for its precedence during unparsing. */
  public static final SqlOperator FETCH =
      SqlBasicOperator.create("FETCH")
          .withPrecedence(SqlStdOperatorTable.UNION.getLeftPrec() - 2, true);

  /** 2-argument form of the special minus-date operator
   * to be used with BigQuery subtraction functions. It differs from
   * the standard MINUS_DATE operator in that it has 2 arguments,
   * and subtracts an interval from a datetime. */
  public static final SqlDatetimeSubtractionOperator MINUS_DATE2 =
      new SqlDatetimeSubtractionOperator("MINUS_DATE2", ReturnTypes.ARG0_NULLABLE);

  /** Offset operator is ONLY used for its precedence during unparsing. */
  public static final SqlOperator OFFSET =
      SqlBasicOperator.create("OFFSET")
          .withPrecedence(SqlStdOperatorTable.UNION.getLeftPrec() - 2, true);

  /** Aggregate function that always returns a given literal. */
  public static final SqlAggFunction LITERAL_AGG =
      SqlLiteralAggFunction.INSTANCE;

  /** Converts a basic operator to internal.
   * Allows us to keep some deprecated operators that used to be internal. */
  static SqlInternalOperator toInternal(SqlOperator operator) {
    if (operator instanceof SqlInternalOperator) {
      return (SqlInternalOperator) operator;
    }
    return ((SqlBasicOperator) operator).toInternal();
  }

  /** Unparses the {@link #EXTEND} operator. */
  static void unparseExtend(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlOperator operator = call.getOperator();
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec());
    writer.setNeedWhitespace(true);
    writer.sep(operator.getName());
    final SqlNodeList list = call.operand(1);
    final SqlWriter.Frame frame2 = writer.startList("(", ")");
    for (Ord<SqlNode> node2 : Ord.zip(list)) {
      if (node2.i > 0 && node2.i % 2 == 0) {
        writer.sep(",");
      }
      node2.e.unparse(writer, 2, 3);
    }
    writer.endList(frame2);
    writer.endList(frame);
  }

  /** Subject to change. */
  private static class SqlBasicOperator extends SqlOperator {
    private final UnparseHandler unparseHandler;

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    /** Private constructor. Use {@link #create}. */
    private SqlBasicOperator(String name, SqlKind kind, int leftPrecedence,
        int rightPrecedence, SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        UnparseHandler unparseHandler) {
      super(name, kind, leftPrecedence, rightPrecedence,
          returnTypeInference, operandTypeInference, operandTypeChecker);
      this.unparseHandler = requireNonNull(unparseHandler, "unparseHandler");
    }

    static SqlBasicOperator create(SqlKind kind, String name) {
      return new SqlBasicOperator(name, kind, 0, 0,
          ReturnTypes.ARG0, null, OperandTypes.VARIADIC,
          UnparseHandler::unparseUsingSyntax);
    }

    static SqlBasicOperator create(SqlKind kind) {
      return create(kind, kind.name());
    }

    static SqlBasicOperator create(String name) {
      return create(SqlKind.OTHER, name);
    }

    @Override public SqlReturnTypeInference getReturnTypeInference() {
      return requireNonNull(super.getReturnTypeInference(),
          "returnTypeInference");
    }

    @Override public SqlOperandTypeChecker getOperandTypeChecker() {
      return requireNonNull(super.getOperandTypeChecker(),
          "operandTypeChecker");
    }

    @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
        int rightPrec) {
      unparseHandler.unparse(writer, call, leftPrec, rightPrec);
    }

    SqlBasicOperator withKind(SqlKind kind) {
      return new SqlBasicOperator(getName(), kind, getLeftPrec(),
          getRightPrec(), getReturnTypeInference(),
          getOperandTypeInference(), getOperandTypeChecker(), unparseHandler);
    }

    SqlBasicOperator withPrecedence(int prec, boolean leftAssoc) {
      return new SqlBasicOperator(getName(), kind, leftPrec(prec, leftAssoc),
          rightPrec(prec, leftAssoc), getReturnTypeInference(),
          getOperandTypeInference(), getOperandTypeChecker(), unparseHandler);
    }

    SqlBasicOperator withReturnTypeInference(
        SqlReturnTypeInference returnTypeInference) {
      return new SqlBasicOperator(getName(), kind, getLeftPrec(),
          getRightPrec(), returnTypeInference,
          getOperandTypeInference(), getOperandTypeChecker(), unparseHandler);
    }


    SqlBasicOperator withOperandChecker(
        SqlSingleOperandTypeChecker operandTypeChecker) {
      return new SqlBasicOperator(getName(), kind, getLeftPrec(),
          getRightPrec(), getReturnTypeInference(),
          getOperandTypeInference(), operandTypeChecker, unparseHandler);
    }

    SqlBasicOperator withUnparse(UnparseHandler unparseHandler) {
      return new SqlBasicOperator(getName(), kind, getLeftPrec(),
          getRightPrec(), getReturnTypeInference(),
          getOperandTypeInference(), getOperandTypeChecker(), unparseHandler);
    }

    /** Converts this operator to an
     * {@link org.apache.calcite.sql.SqlInternalOperator}. */
    SqlInternalOperator toInternal() {
      return new SqlInternalOperator(this.getName(), getKind(),
          prec(getLeftPrec(), getRightPrec()),
          isLeftAssoc(getLeftPrec(), getRightPrec()), getReturnTypeInference(),
          getOperandTypeInference(), getOperandTypeChecker()) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          unparseHandler.unparse(writer, call, leftPrec, rightPrec);
        }
      };
    }
  }

  /** Functional interface equivalent to
   * {@link org.apache.calcite.sql.SqlOperator#unparse}. */
  @FunctionalInterface
  interface UnparseHandler {
    void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec);

    /** Implementation of {@link UnparseHandler} that uses an operator's
     * syntax. */
    static void unparseUsingSyntax(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec) {
      final SqlOperator operator = call.getOperator();
      final SqlSyntax syntax = operator.getSyntax();
      syntax.unparse(writer, operator, call, leftPrec, rightPrec);
    }

    /** Implementation of {@link UnparseHandler} that delegates to
     * operand #0. */
    static void unparseUsingOperand0(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec) {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
    }
  }
}
