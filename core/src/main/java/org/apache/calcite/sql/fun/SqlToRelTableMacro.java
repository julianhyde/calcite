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

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlTableMacro;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

/**
 * Table macro that takes a SQL string as an argument and expands to the
 * relational algebra for that SQL string.
 */
class SqlToRelTableMacro extends SqlTableMacro {
  SqlToRelTableMacro(String name) {
    super(name, new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            ImmutableList.of(SqlTypeFamily.CHARACTER),
            tf -> ImmutableList.of(tf.createSqlType(SqlTypeName.CHAR)),
            i -> ImmutableList.of("sql").get(i),
            i -> true),
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
  }

  @Override public TranslatableTable getTable(SqlOperatorBinding callBinding) {
    SqlCallBinding callBinding1 = (SqlCallBinding) callBinding;
    final SqlValidatorCatalogReader catalogReader =
        callBinding1.getValidator().getCatalogReader();
    final CalciteSchema schema =
        Schemas.subSchema(catalogReader.getRootSchema(),
            catalogReader.getSchemaPaths().get(0));
    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT.withLex(Lex.BIG_QUERY))
            .defaultSchema(requireNonNull(schema, "schema").plus())
            .build();
    final Planner planner = Frameworks.getPlanner(config);
    final SqlLiteral operand = (SqlLiteral) callBinding1.operand(0);
    final SqlNode sqlNode;
    try {
      sqlNode = planner.parse(operand.getValueAs(String.class));
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
    final SqlNode sqlNode1;
    try {
      sqlNode1 = planner.validate(sqlNode);
    } catch (ValidationException e) {
      throw new RuntimeException(e);
    }
    RelRoot rel;
    try {
      rel = planner.rel(sqlNode1);
    } catch (RelConversionException e) {
      throw new RuntimeException(e);
    }
    return new RelTable(rel);
  }

  /** Table that expands to a relational expression. */
  private static class RelTable extends AbstractTable
      implements TranslatableTable {
    private final RelRoot relRoot;

    RelTable(RelRoot relRoot) {
      this.relRoot = relRoot;
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return relRoot.project();
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.copyType(relRoot.validatedRowType);
    }
  }
}
