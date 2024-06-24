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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static java.util.Objects.requireNonNull;

/**
 * Table function that is able to expand itself to a relational expression.
 */
public abstract class SqlTableMacro extends SqlFunction
    implements SqlTableFunction {
  /** Creates a table macro. */
  public SqlTableMacro(String name, SqlIdentifier sqlIdentifier, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandMetadata operandMetadata,
      SqlFunctionCategory category) {
    super(name, requireNonNull(sqlIdentifier, "sqlIdentifier"), kind,
        requireNonNull(returnTypeInference, "returnTypeInference"),
        requireNonNull(operandTypeInference, "operandTypeInference"),
        requireNonNull(operandMetadata, "operandMetadata"), category);
  }

  @Override public SqlOperandMetadata getOperandTypeChecker() {
    return (SqlOperandMetadata) requireNonNull(super.getOperandTypeChecker());
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::inferRowType;
  }

  /** Returns the table in this UDF. */
  public abstract TranslatableTable getTable(SqlOperatorBinding callBinding);

  /** Infers the row type of the returned table. */
  protected RelDataType inferRowType(SqlOperatorBinding callBinding) {
    final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
    final TranslatableTable table = getTable(callBinding);
    return table.getRowType(typeFactory);
  }
}
