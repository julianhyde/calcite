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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.util.SourceStringReader;

import com.google.common.base.Suppliers;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * As {@link SqlNewTestFactory} but has no state, and therefore
 * configuration is passed to each method.
*/
public class SqlNewTestFactory {
  public static final SqlNewTestFactory INSTANCE =
      new SqlNewTestFactory(MockCatalogReaderSimple::create,
          SqlValidatorUtil::newValidator,
          CalciteAssert.EMPTY_CONNECTION_FACTORY
              .with(
                  new CalciteAssert.AddSchemaSpecPostProcessor(
                      CalciteAssert.SchemaSpec.HR)),
          SqlParser.Config.DEFAULT,
          SqlValidator.Config.DEFAULT,
          SqlStdOperatorTable.instance())
      .withOperatorTable(o -> {
        MockSqlOperatorTable opTab = new MockSqlOperatorTable(o);
        MockSqlOperatorTable.addRamp(opTab);
        return opTab;
      });

  public final CalciteAssert.ConnectionFactory connectionFactory;
  private final CatalogReaderFactory catalogReaderFactory;
  private final ValidatorFactory validatorFactory;

  private final Supplier<RelDataTypeFactory> typeFactorySupplier;
  private final SqlOperatorTable operatorTable;
  private final Supplier<SqlValidatorCatalogReader> catalogReaderSupplier;
  private final SqlParser.Config parserConfig;
  public final SqlValidator.Config validatorConfig;

  protected SqlNewTestFactory(CatalogReaderFactory catalogReaderFactory,
      ValidatorFactory validatorFactory,
      CalciteAssert.ConnectionFactory connectionFactory,
      SqlParser.Config parserConfig,
      SqlValidator.Config validatorConfig, SqlOperatorTable operatorTable) {
    this.catalogReaderFactory = catalogReaderFactory;
    this.validatorFactory = validatorFactory;
    this.connectionFactory = connectionFactory;
    this.operatorTable = operatorTable;
    this.typeFactorySupplier = Suppliers.memoize(() ->
        createTypeFactory(validatorConfig.sqlConformance()))::get;
    this.catalogReaderSupplier = Suppliers.memoize(() ->
        catalogReaderFactory.create(typeFactorySupplier.get(),
            parserConfig.caseSensitive()))::get;
    this.parserConfig = parserConfig;
    this.validatorConfig = validatorConfig;
  }

  /** Creates a parser. */
  public SqlParser createParser(String sql) {
    SqlParser.Config parserConfig = parserConfig();
    return SqlParser.create(new SourceStringReader(sql), parserConfig);
  }

  /** Creates a validator. */
  public SqlValidator createValidator() {
    return validatorFactory.create(operatorTable, catalogReaderSupplier.get(),
        typeFactorySupplier.get(), validatorConfig);
  }

  public SqlAdvisor createAdvisor() {
    SqlValidator validator = createValidator();
    if (validator instanceof SqlValidatorWithHints) {
      return new SqlAdvisor((SqlValidatorWithHints) validator, parserConfig);
    }
    throw new UnsupportedOperationException(
        "Validator should implement SqlValidatorWithHints, actual validator is " + validator);
  }

  public SqlNewTestFactory withCatalogReader(CatalogReaderFactory catalogReaderFactory) {
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withValidator(ValidatorFactory validatorFactory) {
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withValidatorConfig(
      UnaryOperator<SqlValidator.Config> transform) {
    final SqlValidator.Config validatorConfig =
        transform.apply(this.validatorConfig);
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  private static RelDataTypeFactory createTypeFactory(SqlConformance conformance) {
    RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
    if (conformance.shouldConvertRaggedUnionTypesToVarying()) {
      typeSystem = new DelegatingTypeSystem(typeSystem) {
        @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
          return true;
        }
      };
    }
    return new JavaTypeFactoryImpl(typeSystem);
  }

  public SqlNewTestFactory withParserConfig(
      UnaryOperator<SqlParser.Config> transform) {
    final SqlParser.Config parserConfig = transform.apply(this.parserConfig);
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withConnectionFactory(
      CalciteAssert.ConnectionFactory connectionFactory) {
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withOperatorTable(
      UnaryOperator<SqlOperatorTable> transform) {
    final SqlOperatorTable operatorTable =
        transform.apply(this.operatorTable);
    return new SqlNewTestFactory(catalogReaderFactory,
        validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlParser.Config parserConfig() {
    return parserConfig;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactorySupplier.get();
  }

  /**
   * Creates {@link SqlValidator} for tests.
   */
  public interface ValidatorFactory {
    SqlValidator create(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlValidator.Config config);
  }

  /** Creates a {@link SqlValidatorCatalogReader} for tests. */
  @FunctionalInterface
  public interface CatalogReaderFactory {
    SqlValidatorCatalogReader create(RelDataTypeFactory typeFactory,
        boolean caseSensitive);
  }
}
