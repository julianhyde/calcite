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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandHandler;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import net.hydromatic.lookml.Facade;
import net.hydromatic.steelwheels.data.hsqldb.SteelwheelsHsqldb;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Functions related to LookML. */
public class SqlLookmlFunctions {
  private SqlLookmlFunctions() {}

  static SqlBasicFunction lookmlViewString() {
    return SqlBasicFunction.create("LOOKML_VIEW_STRING", ReturnTypes.CHAR,
            OperandTypes.CHARACTER_CHARACTER)
        .withOperandHandler(
            new SqlOperandHandler() {
              @Override public SqlNode rewriteCall(SqlValidator validator,
                  SqlCall call) {
                final ImmutableMap<String, SchemaFactory> schemaMap =
                    ImmutableMap.of("steelwheels",
                        SqlLookmlFunctions::createSteelwheelsSchema);
                final Facade<?> facade =
                    Facade.simple().withSchemaMap(schemaMap);
                final SqlLiteral exploreNameLiteral = call.operand(0);
                final String exploreName =
                    exploreNameLiteral.getValueAs(String.class);
                final SqlLiteral lookmlModelLiteral = call.operand(1);
                final String lookmlModel =
                    lookmlModelLiteral.getValueAs(String.class);
                final Facade.SqlQuery sqlModel =
                    facade.modelToSql(lookmlModel, exploreName);
                return SqlLiteral.createCharString(sqlModel.sql(),
                    SqlParserPos.ZERO);
              }
            });
  }

  static SqlOperator lookmlView() {
    return new SqlUserDefinedTableMacro(
        new SqlIdentifier("LOOKML_VIEW", SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR,
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            ImmutableList.of(SqlTypeFamily.CHARACTER,
                SqlTypeFamily.CHARACTER),
            typeFactory -> ImmutableList.of(
                typeFactory.createSqlType(SqlTypeName.CHAR),
                typeFactory.createSqlType(SqlTypeName.CHAR)),
            i -> Arrays.asList("exploreName", "lookmlModel").get(i),
            i -> true),
        requireNonNull(TableMacroImpl.create(ViewFn.class)));
  }

  static SqlOperator sqlToRel() {
    return new SqlUserDefinedTableMacro(
        new SqlIdentifier("SQL_TO_REL", SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR,
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            ImmutableList.of(SqlTypeFamily.CHARACTER),
            tf -> ImmutableList.of(tf.createSqlType(SqlTypeName.CHAR)),
            i -> ImmutableList.of("sql").get(i),
            i -> true),
        requireNonNull(TableMacroImpl.create(Dummy.class))) {
      @Override public TranslatableTable getTable(
          SqlOperatorBinding callBinding) {
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
        return new MyTranslatableTable2(rel);
      }
    };
  }

  /** Creates a Calcite schema backed by the STEELWHEELS database;
   * implements {@link org.apache.calcite.schema.SchemaFactory}. */
  static Schema createSteelwheelsSchema(SchemaPlus parent, String name,
      Map<String, Object> map) {
    Map<String, Object> map2 = new LinkedHashMap<>(map);
    map2.put("jdbcPassword", SteelwheelsHsqldb.PASSWORD);
    map2.put("jdbcUser", SteelwheelsHsqldb.USER);
    map2.put("jdbcUrl", SteelwheelsHsqldb.URI);
    map2.put("jdbcDriver", "org.hsqldb.jdbc.JDBCDriver");
    return JdbcSchema.Factory.INSTANCE.create(parent, name, map2);
  }

  /** Implementation of {@link #lookmlView}. */
  public static class ViewFn {
    /** Called via reflection from
     * {@link org.apache.calcite.schema.impl.TableMacroImpl}. */
    public TranslatableTable eval(String exploreName, String lookmlModel) {
      return new MyTranslatableTable(exploreName, lookmlModel, false);
    }
  }

  /** Implementation of {@link #lookmlViewString}. */
  public static class ViewStringFn {
    /** Called via reflection from
     * {@link org.apache.calcite.schema.impl.TableMacroImpl}. */
    public TranslatableTable eval(String exploreName, String lookmlModel) {
      return new MyTranslatableTable(exploreName, lookmlModel, true);
    }
  }

  /** Table that either returns a SQL string for a given LookML model
   * or expands to the relational algebra of that SQL string. */
  private static class MyTranslatableTable implements TranslatableTable {
    private final Facade.SqlQuery sqlModel;
    private final Facade<?> facade;
    private final boolean string;

    MyTranslatableTable(String exploreName, String lookmlModel, boolean string) {
      this.string = string;
      this.facade =
          Facade.simple()
              .withSchemaMap(
                  ImmutableMap.of("steelwheels",
                      SqlLookmlFunctions::createSteelwheelsSchema));
      this.sqlModel = facade.modelToSql(lookmlModel, exploreName);
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      if (string) {
        final FrameworkConfig config = Frameworks.newConfigBuilder().build();
        final RelBuilder b = RelBuilder.create(config);
        return b
            .values(new String[]{"sql"}, b.literal(sqlModel))
            .build();
      } else {
        throw new UnsupportedOperationException("toRel");
      }
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      if (string) {
        return typeFactory.builder()
            .add("sql", typeFactory.createSqlType(SqlTypeName.VARCHAR, -1))
            .build();
      } else {
        return typeFactory.copyType(sqlModel.rowType());
      }
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(1D, ImmutableList.of());
    }

    @Override public Schema.TableType getJdbcTableType() {
      throw new UnsupportedOperationException("getJdbcTableType");
    }

    @Override public boolean isRolledUp(String column) {
      throw new UnsupportedOperationException("isRolledUp");
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        SqlCall call, @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      throw new UnsupportedOperationException("rolledUpColumnValidInsideAgg");
    }
  }

  /** Table macro that takes a SQL string as an argument and expands to the
   * relational algebra for that SQL string. */
  private static class MyTranslatableTable2 implements TranslatableTable {
    private final RelRoot relRoot;

    MyTranslatableTable2(RelRoot relRoot) {
      this.relRoot = relRoot;
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return relRoot.project();
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.copyType(relRoot.validatedRowType);
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(1D, ImmutableList.of());
    }

    @Override public Schema.TableType getJdbcTableType() {
      throw new UnsupportedOperationException("getJdbcTableType");
    }

    @Override public boolean isRolledUp(String column) {
      throw new UnsupportedOperationException("isRolledUp");
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
        SqlCall call, @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      throw new UnsupportedOperationException("rolledUpColumnValidInsideAgg");
    }
  }

  /** Dummy function object that lets us use a
   * {@link SqlUserDefinedTableMacro}. */
  public static class Dummy {
    public TranslatableTable eval() {
      throw new UnsupportedOperationException("eval");
    }
  }
}
