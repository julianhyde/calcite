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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * As {@link SqlNewTestFactory} but has no state, and therefore
 * configuration is passed to each method.
*/
public class SqlNewTestFactory {
  public static final ImmutableMap<String, Object> DEFAULT_OPTIONS =
      ImmutableSortedMap.<String, Object>naturalOrder()
          .put("quoting", Quoting.DOUBLE_QUOTE)
          .put("quotedCasing", Casing.UNCHANGED)
          .put("unquotedCasing", Casing.TO_UPPER)
//          .put("caseSensitive", true)
//          .put("lenientOperatorLookup", false)
//          .put("enableTypeCoercion", true)
//          .put("conformance", SqlConformanceEnum.DEFAULT)
          .put("operatorTable", SqlStdOperatorTable.instance())
          .put("connectionFactory",
              CalciteAssert.EMPTY_CONNECTION_FACTORY
                  .with(
                      new CalciteAssert.AddSchemaSpecPostProcessor(
                          CalciteAssert.SchemaSpec.HR)))
          .build();

  public static final SqlNewTestFactory INSTANCE =
      new SqlNewTestFactory(DEFAULT_OPTIONS, MockCatalogReaderSimple::create,
          true, SqlValidatorUtil::newValidator,
          CalciteAssert.EMPTY_CONNECTION_FACTORY, SqlParser.Config.DEFAULT,
          SqlValidator.Config.DEFAULT,
          SqlStdOperatorTable.instance())
      .withOperatorTable(o -> {
        MockSqlOperatorTable opTab = new MockSqlOperatorTable(o);
        MockSqlOperatorTable.addRamp(opTab);
        return opTab;
      });

  private final ImmutableMap<String, Object> options;
  private final CalciteAssert.ConnectionFactory connectionFactory;
  private final CatalogReaderFactory catalogReaderFactory;
  private final boolean caseSensitive;
  private final ValidatorFactory validatorFactory;

  private final Supplier<RelDataTypeFactory> typeFactory;
  private final SqlOperatorTable operatorTable;
  private final Supplier<SqlValidatorCatalogReader> catalogReader;
  private final SqlParser.Config parserConfig;
  private final SqlValidator.Config validatorConfig;

  protected SqlNewTestFactory(ImmutableMap<String, Object> options,
      CatalogReaderFactory catalogReaderFactory, boolean caseSensitive,
      ValidatorFactory validatorFactory,
      CalciteAssert.ConnectionFactory connectionFactory,
      SqlParser.Config parserConfig,
      SqlValidator.Config validatorConfig, SqlOperatorTable operatorTable) {
    this.options = options;
    this.connectionFactory = connectionFactory;
    assert options.get("caseSensitive") == null;
    assert options.get("conformance") == null;
    assert options.get("lenientOperatorLookup") == null;
    assert options.get("enableTypeCoercion") == null; // TODO remove

    this.catalogReaderFactory = catalogReaderFactory;
    this.caseSensitive = caseSensitive;
    this.validatorFactory = validatorFactory;
    this.operatorTable = operatorTable;
    this.typeFactory = Suppliers.memoize(
        () -> createTypeFactory(validatorConfig.sqlConformance()));
    this.catalogReader = Suppliers.memoize(
        () -> catalogReaderFactory.create(typeFactory.get(), caseSensitive));
    this.parserConfig = parserConfig;
    this.validatorConfig = validatorConfig;
  }

  public SqlParser createParser(String sql) {
    SqlParser.Config parserConfig = parserConfig();
    return SqlParser.create(new SourceStringReader(sql), parserConfig);
  }

  public SqlValidator getValidator() {
    return validatorFactory.create(operatorTable, catalogReader.get(),
        typeFactory.get(), validatorConfig);
  }

  public SqlAdvisor createAdvisor() {
    SqlValidator validator = getValidator();
    if (validator instanceof SqlValidatorWithHints) {
      return new SqlAdvisor((SqlValidatorWithHints) validator, parserConfig);
    }
    throw new UnsupportedOperationException(
        "Validator should implement SqlValidatorWithHints, actual validator is " + validator);
  }

  public SqlNewTestFactory with(String name, Object value) {
    if (Objects.equals(value, options.get(name))) {
      return this;
    }
    ImmutableMap.Builder<String, Object> builder = ImmutableSortedMap.naturalOrder();
    // Protect from IllegalArgumentException: Multiple entries with same key
    for (Map.Entry<String, Object> entry : options.entrySet()) {
      if (name.equals(entry.getKey())) {
        continue;
      }
      builder.put(entry);
    }
    builder.put(name, value);
    return new SqlNewTestFactory(builder.build(), catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withCatalogReader(CatalogReaderFactory catalogReaderFactory) {
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withValidator(ValidatorFactory validatorFactory) {
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withValidatorConfig(
      UnaryOperator<SqlValidator.Config> transform) {
    final SqlValidator.Config validatorConfig =
        transform.apply(this.validatorConfig);
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public final Object get(String name) {
    return options.get(name);
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
    if (conformance.allowExtendedTrim()) {
      typeSystem = new DelegatingTypeSystem(typeSystem) {
        public boolean allowExtendedTrim() {
          return true;
        }
      };
    }
    return new JavaTypeFactoryImpl(typeSystem);
  }

  public SqlNewTestFactory withParserConfig(
      UnaryOperator<SqlParser.Config> transform) {
    final SqlParser.Config parserConfig = transform.apply(this.parserConfig);
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withConnectionFactory(
      CalciteAssert.ConnectionFactory connectionFactory) {
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlNewTestFactory withOperatorTable(
      UnaryOperator<SqlOperatorTable> transform) {
    final SqlOperatorTable operatorTable =
        transform.apply(this.operatorTable);
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
  }

  public SqlParser.Config parserConfig() {
    return parserConfig;
  }

  public SqlNewTestFactory withCaseSensitive(boolean caseSensitive) {
    return new SqlNewTestFactory(options, catalogReaderFactory,
        caseSensitive, validatorFactory, connectionFactory,
        parserConfig, validatorConfig, operatorTable);
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
