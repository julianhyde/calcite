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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMeasureOperator;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Contains internal operators.
 *
 * <p>These operators are always created directly, not by looking up a function
 * or operator by name or syntax, and therefore this class does not implement
 * interface {@link SqlOperatorTable}.
 */
public abstract class SqlInternalOperators {
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
      new SqlInternalOperator("$THROW_UNLESS", SqlKind.OTHER);

  /** <code>MEASURE</code> operator wraps an expression in the SELECT clause
   * that is a measure. It always occurs inside a call to "AS". */
  public static final SqlMeasureOperator MEASURE =
      new SqlMeasureOperator();

  /** {@code V2M} operator converts a measure to a value. */
  public static final SqlOperator M2V =
      new SqlInternalOperator("M2V", SqlKind.M2V, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE), null,
          OperandTypes.ANY);

  /** {@code V2M} operator converts a value to a measure. */
  public static final SqlOperator V2M =
      new SqlInternalOperator("V2M", SqlKind.V2M, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_MEASURE), null,
          OperandTypes.ANY);

  /** {@code AGG_M2M} aggregate function takes a measure as its argument and
   * returns a measure. It is used to propagate measures through the
   * {@code Aggregate} relational operator.
   *
   * @see SqlLibraryOperators#AGGREGATE */
  public static final SqlOperator AGG_M2M =
      SqlBasicAggFunction.create(SqlKind.AGG_M2M, ReturnTypes.ARG0,
          OperandTypes.ANY);

  /** {@code AGG_M2V} aggregate function takes a measure as its argument and
   * returns value. */
  public static final SqlOperator AGG_M2V =
      SqlBasicAggFunction.create(SqlKind.AGG_M2V,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE),
          OperandTypes.ANY);

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
      new SqlInternalOperator("SEPARATOR", SqlKind.SEPARATOR, 20, false,
          ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.ANY);

}
