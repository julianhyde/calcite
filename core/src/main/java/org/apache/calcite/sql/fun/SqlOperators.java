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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Helps implement {@link org.apache.calcite.sql.SqlSpecialOperator}
 * and {@link SqlInternalOperator}.
 */
public abstract class SqlOperators {
  /** Creates an operator. */
  static OperatorBuilder create(SqlKind kind, String name) {
    return new OperatorBuilderImpl(name, kind, 0, 0,
        ReturnTypes.ARG0, null, OperandTypes.VARIADIC,
        UnparseHandler::unparseUsingSyntax,
        CallFactory::basic);
  }

  public static OperatorBuilder create(SqlKind kind) {
    return create(kind, kind.name());
  }

  static OperatorBuilder create(String name) {
    return create(SqlKind.OTHER, name);
  }

  /** Converts a basic operator to internal.
   * Allows us to keep some deprecated operators that used to be internal. */
  static SqlInternalOperator toInternal(SqlOperator operator) {
    if (operator instanceof SqlInternalOperator) {
      return (SqlInternalOperator) operator;
    }
    return ((SqlBasicOperator) operator).toInternal();
  }

  /** Given the precedence of an operator and whether it is left-associative,
   * returns the value for {@link SqlOperator#getLeftPrec()}. */
  public static int leftPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (!leftAssoc) {
      ++prec;
    }
    return prec;
  }

  /** Given the precedence of an operator and whether it is left-associative,
   * returns the value for {@link SqlOperator#getRightPrec()}. */
  public static int rightPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (leftAssoc) {
      ++prec;
    }
    return prec;
  }

  /** Given the values of {@link SqlOperator#getLeftPrec()} and
   * {@link SqlOperator#getRightPrec()}, returns the precedence of the
   * operator.
   *
   * <p>Converse of {@link #leftPrec} and {@link #rightPrec}, in the sense that
   * {@code prec(leftPrec(p, a), rightPrec(p, a))} returns {@code p}
   * for all {@code p} and {@code a}. */
  protected static int prec(int leftPrec, int rightPrec) {
    return Math.min(leftPrec, rightPrec);
  }

  /** Given the values of {@link SqlOperator#getLeftPrec()} and
   * {@link SqlOperator#getRightPrec()}, returns whether the operator is
   * left-associative.
   *
   * <p>Converse of {@link #leftPrec} and {@link #rightPrec}, in the sense that
   * {@code isLeftAssoc(leftPrec(p, a), rightPrec(p, a))} returns {@code a}
   * for all {@code p} and {@code a}. */
  protected static boolean isLeftAssoc(int leftPrec, int rightPrec) {
    return leftPrec > rightPrec;
  }

  /** Subject to change. */
  static class SqlBasicOperator extends SqlOperator {
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

    public SqlInternalOperator toInternal() {
      throw new UnsupportedOperationException(); // TODO
    }
  }

  /** Subject to change. */
  static class OperatorBuilderImpl implements OperatorBuilder {
    final String name;
    final SqlKind kind;
    final int leftPrec;
    final int rightPrec;
    final SqlReturnTypeInference returnTypeInference;
    final @Nullable SqlOperandTypeInference operandTypeInference;
    final SqlOperandTypeChecker operandTypeChecker;
    final UnparseHandler unparseHandler;
    final CallFactory callFactory;

    /** Private constructor. Use {@link #create}. */
    private OperatorBuilderImpl(String name, SqlKind kind, int leftPrec,
        int rightPrec, SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        UnparseHandler unparseHandler, CallFactory callFactory) {
      this.name = name;
      this.kind = kind;
      this.leftPrec = leftPrec;
      this.rightPrec = rightPrec;
      this.returnTypeInference =
          requireNonNull(returnTypeInference, "returnTypeInference");
      this.operandTypeInference = operandTypeInference;
      this.operandTypeChecker =
          requireNonNull(operandTypeChecker, "operandTypeChecker");
      this.unparseHandler = requireNonNull(unparseHandler, "unparseHandler");
      this.callFactory = callFactory;
    }

    @Override public OperatorBuilder withCallFactory(CallFactory callFactory) {
      return new OperatorBuilderImpl(name, kind, leftPrec, rightPrec,
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public OperatorBuilderImpl withKind(SqlKind kind) {
      return new OperatorBuilderImpl(name, kind, leftPrec, rightPrec,
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public OperatorBuilderImpl withPrecedence(int prec,
        boolean leftAssoc) {
      return new OperatorBuilderImpl(name, kind,
          leftPrec(prec, leftAssoc), rightPrec(prec, leftAssoc),
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public OperatorBuilderImpl withReturnTypeInference(
        SqlReturnTypeInference returnTypeInference) {
      return new OperatorBuilderImpl(name, kind, leftPrec, rightPrec,
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public OperatorBuilderImpl withOperandChecker(
        SqlOperandTypeChecker operandTypeChecker) {
      return new OperatorBuilderImpl(name, kind, leftPrec, rightPrec,
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public OperatorBuilderImpl withUnparse(
        UnparseHandler unparseHandler) {
      return new OperatorBuilderImpl(name, kind, leftPrec, rightPrec,
          returnTypeInference, operandTypeInference, operandTypeChecker,
          unparseHandler, callFactory);
    }

    @Override public SqlInternalOperator toInternal() {
      return new SqlInternalOperator(name, kind,
          prec(leftPrec, rightPrec), isLeftAssoc(leftPrec, rightPrec),
          returnTypeInference, operandTypeInference, operandTypeChecker) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          unparseHandler.unparse(writer, call, leftPrec, rightPrec);
        }

        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier, SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return callFactory.createCall(this, functionQualifier, pos,
              ImmutableNullableList.copyOf(operands));
        }
      };
    }

    @Override public SqlOperator operator() {
      return toInternal();
    }
  }

  /** Builds an operator. */
  public interface OperatorBuilder {
    SqlOperator operator();
    OperatorBuilder withCallFactory(CallFactory callFactory);

    OperatorBuilder withKind(SqlKind kind);

    OperatorBuilder withPrecedence(int precedence, boolean leftAssoc);

    OperatorBuilder withUnparse(UnparseHandler unparseHandler);

    OperatorBuilder withReturnTypeInference(
        SqlReturnTypeInference returnTypeInference);

    OperatorBuilder withOperandChecker(
        SqlOperandTypeChecker operandTypeChecker);

    /** Converts this builder to an {@link SqlInternalOperator}. */
    SqlInternalOperator toInternal();
  }

  /** Creates a {@link SqlCall} or subclass specific to the operator. */
  public interface CallFactory {
    SqlCall createCall(SqlOperator operator,
        @Nullable SqlLiteral qualifier, SqlParserPos pos,
        List<? extends @Nullable SqlNode> operands);

    /** Basic implementation of
     * {@link org.apache.calcite.sql.fun.SqlOperators.CallFactory} that creates
     * a {@link org.apache.calcite.sql.SqlBasicCall}. Matches the behavior of
     * {@link SqlOperator#createCall(SqlLiteral, SqlParserPos, SqlNode...)}. */
    static SqlCall basic(SqlOperator operator,
        @Nullable SqlLiteral qualifier, SqlParserPos pos,
        List<? extends @Nullable SqlNode> operands) {
      return new SqlBasicCall(operator, operands, pos, qualifier);
    }
  }
}
