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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for table-valued function windowing operator (TUMBLE, HOP and SESSION).
 */
public class SqlWindowTableFunction extends SqlFunction
    implements SqlTableFunction {
  /**
   * Type-inference strategy whereby the result type of a table function call is a ROW,
   * which is combined from the operand #0(TABLE parameter)'s schema and two
   * additional fields:
   *
   * <ol>
   *  <li>window_start: TIMESTAMP type to indicate a window's start
   *  <li>window_end: TIMESTAMP type to indicate a window's end
   * </ol>
   */
  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
      SqlWindowTableFunction::inferRowType;

  /** Creates a window table function with a given name. */
  public SqlWindowTableFunction(String name) {
    super(name, SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null, null,
        SqlFunctionCategory.SYSTEM);
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return ARG0_TABLE_FUNCTION_WINDOWING;
  }

  protected boolean throwValidationSignatureErrorOrReturnFalse(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (throwOnFailure) {
      throw callBinding.newValidationSignatureError();
    } else {
      return false;
    }
  }

  protected void validateColumnNames(SqlValidator validator,
      List<String> fieldNames, List<SqlNode> unvalidatedColumnNames) {
    final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
    for (SqlNode descOperand : unvalidatedColumnNames) {
      final String colName = ((SqlIdentifier) descOperand).getSimple();
      if (matcher.frequency(fieldNames, colName) == 0) {
        throw SqlUtil.newContextException(descOperand.getParserPosition(),
            RESOURCE.unknownIdentifier(colName));
      }
    }
  }

  /**
   * Overrides SqlOperator.argumentMustBeScalar because the first parameter of
   * table-value function windowing is an explicit TABLE parameter,
   * which is not scalar.
   */
  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal != 0;
  }

  private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
    final RelDataType inputRowType = opBinding.getOperandType(0);
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    return typeFactory.builder()
        .kind(inputRowType.getStructKind())
        .addAll(inputRowType.getFieldList())
        .add("window_start", timestampType)
        .add("window_end", timestampType)
        .build();
  }
}
