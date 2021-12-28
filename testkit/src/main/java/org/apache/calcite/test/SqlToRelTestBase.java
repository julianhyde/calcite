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
package org.apache.calcite.test;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlNewTestFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.apache.calcite.test.Matchers.relIsValid;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion from
 * SQL to relational algebra.
 *
 * <p>SQL statements to be translated can use the schema defined in
 * {@link MockCatalogReader}; note that this is slightly different from
 * Farrago's SALES schema. If you get a parser or validator error from your test
 * SQL, look down in the stack until you see "Caused by", which will usually
 * tell you the real error.
 */
public abstract class SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  protected static final String NL = System.getProperty("line.separator");

  protected static final Supplier<RelDataTypeFactory> DEFAULT_TYPE_FACTORY_SUPPLIER =
      Suppliers.memoize(() ->
          new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT))::get;

  //~ Instance fields --------------------------------------------------------

  //~ Methods ----------------------------------------------------------------

  /** Creates the test fixture that determines the behavior of tests.
   * Sub-classes that, say, test different parser implementations should
   * override. */
  public SqlToRelFixture fixture() {
    return SqlToRelFixture.DEFAULT;
  }

  /** Sets the SQL statement for a test. */
  public final SqlToRelFixture sql(String sql) {
    return fixture().expression(false).withSql(sql);
  }

  public final SqlToRelFixture expr(String sql) {
    return fixture().expression(true).withSql(sql);
  }

  //~ Inner Interfaces -------------------------------------------------------

  // TODO combine Tester with SqlTester
  /**
   * Helper class which contains default implementations of methods used for
   * running sql-to-rel conversion tests.
   */
  public interface Tester {
    /**
     * Converts a SQL string to a {@link RelNode} tree.
     *
     * @param factory Factory
     * @param sql SQL statement
     * @param decorrelate Whether to decorrelate
     * @param trim Whether to trim
     * @return Relational expression, never null
     */
    default RelRoot convertSqlToRel(SqlNewTestFactory factory,
        String sql, boolean decorrelate, boolean trim) {
      Pair<SqlValidator, RelRoot> pair =
          convertSqlToRel2(factory, sql, decorrelate, trim);
      return requireNonNull(pair.right);
    }

    Pair<SqlValidator, RelRoot> convertSqlToRel2(SqlNewTestFactory factory,
        String sql, boolean decorrelate, boolean trim);

    SqlNode parseQuery(SqlNewTestFactory factory, String sql) throws Exception;

    /**
     * Factory method to create a {@link SqlValidator}.
     */
    SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory);

    /**
     * Factory method for a
     * {@link org.apache.calcite.prepare.Prepare.CatalogReader}.
     */
    // TODO not used; remove
    Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory);

    /**
     * Returns the {@link SqlOperatorTable} to use.
     */
    SqlOperatorTable getOperatorTable();

    /**
     * Returns the SQL dialect to test.
     */
    SqlConformance getConformance();

    /**
     * Checks that a SQL statement converts to a given plan, optionally
     * trimming columns that are not needed.
     *
     * @param factory Factory
     * @param diffRepos Diff repository
     * @param sql  SQL query or expression
     * @param plan Expected plan
     * @param trim Whether to trim columns that are not needed
     * @param expression True if {@code sql} is an expression, false if it is a query
     */
    void assertConvertsTo(SqlNewTestFactory factory, DiffRepository diffRepos,
        String sql,
        String plan,
        boolean trim,
        boolean expression,
        boolean decorrelate);

    /** Returns a tester that optionally decorrelates queries after planner
     * rules have fired. */
    Tester withLateDecorrelation(boolean enable);

    /** Returns a tester that applies a transform to its
     * {@code SqlToRelConverter.Config} before it uses it. */
    Tester withConfig(UnaryOperator<SqlToRelConverter.Config> transform);

    /** Returns a tester with a {@link SqlConformance}. */
    // TODO: remove
    Tester withConformance(SqlConformance conformance);

    /** Returns a tester with a specified if allows type coercion. */
    Tester enableTypeCoercion(boolean typeCoercion);

    Tester withCatalogReaderFactory(
        SqlNewTestFactory.CatalogReaderFactory factory);

    Tester withClusterFactory(Function<RelOptCluster, RelOptCluster> function);

    boolean isLateDecorrelate();

    /** Returns a tester that uses a given context. */
    Tester withContext(UnaryOperator<Context> transform);

    /** Trims a RelNode. */
    RelNode trimRelNode(SqlNewTestFactory factory, RelNode relNode);

    SqlNode parseExpression(SqlNewTestFactory factory, String expr) throws Exception;

    /** Returns a tester that applies the given transform to a validator before
     * using it. */
    Tester withValidatorTransform(UnaryOperator<SqlValidator> transform);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Mock implementation of {@link RelOptSchema}.
   */
  protected static class MockRelOptSchema implements RelOptSchemaWithSampling {
    private final SqlValidatorCatalogReader catalogReader;
    private final RelDataTypeFactory typeFactory;

    public MockRelOptSchema(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      this.catalogReader = catalogReader;
      this.typeFactory = typeFactory;
    }

    @Override public RelOptTable getTableForMember(List<String> names) {
      final SqlValidatorTable table =
          catalogReader.getTable(names);
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = deduceMonotonicity(table);
      if (names.size() < 3) {
        String[] newNames2 = {"CATALOG", "SALES", ""};
        List<String> newNames = new ArrayList<>();
        int i = 0;
        while (newNames.size() < newNames2.length) {
          newNames.add(i, newNames2[i]);
          ++i;
        }
        names = newNames;
      }
      return createColumnSet(table, names, rowType, collationList);
    }

    private static List<RelCollation> deduceMonotonicity(SqlValidatorTable table) {
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = new ArrayList<>();

      // Deduce which fields the table is sorted on.
      int i = -1;
      for (RelDataTypeField field : rowType.getFieldList()) {
        ++i;
        final SqlMonotonicity monotonicity =
            table.getMonotonicity(field.getName());
        if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
          final RelFieldCollation.Direction direction =
              monotonicity.isDecreasing()
                  ? RelFieldCollation.Direction.DESCENDING
                  : RelFieldCollation.Direction.ASCENDING;
          collationList.add(
              RelCollations.of(new RelFieldCollation(i, direction)));
        }
      }
      return collationList;
    }

    @Override public RelOptTable getTableForMember(
        List<String> names,
        final String datasetName,
        boolean[] usedDataset) {
      final RelOptTable table = getTableForMember(names);

      // If they're asking for a sample, just for test purposes,
      // assume there's a table called "<table>:<sample>".
      RelOptTable datasetTable =
          new DelegatingRelOptTable(table) {
            @Override public List<String> getQualifiedName() {
              final List<String> list =
                  new ArrayList<>(super.getQualifiedName());
              list.set(
                  list.size() - 1,
                  list.get(list.size() - 1) + ":" + datasetName);
              return ImmutableList.copyOf(list);
            }
          };
      if (usedDataset != null) {
        assert usedDataset.length == 1;
        usedDataset[0] = true;
      }
      return datasetTable;
    }

    protected MockColumnSet createColumnSet(
        SqlValidatorTable table,
        List<String> names,
        final RelDataType rowType,
        final List<RelCollation> collationList) {
      return new MockColumnSet(names, rowType, collationList);
    }

    @Override public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override public void registerRules(RelOptPlanner planner) {
    }

    /** Mock column set. */
    protected class MockColumnSet implements RelOptTable {
      private final List<String> names;
      private final RelDataType rowType;
      private final List<RelCollation> collationList;

      protected MockColumnSet(
          List<String> names,
          RelDataType rowType,
          final List<RelCollation> collationList) {
        this.names = ImmutableList.copyOf(names);
        this.rowType = rowType;
        this.collationList = collationList;
      }

      @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
          return clazz.cast(this);
        }
        return null;
      }

      @Override public List<String> getQualifiedName() {
        return names;
      }

      @Override public double getRowCount() {
        // use something other than 0 to give costing tests
        // some room, and make emps bigger than depts for
        // join asymmetry
        if (Iterables.getLast(names).equals("EMP")) {
          return 1000;
        } else {
          return 100;
        }
      }

      @Override public RelDataType getRowType() {
        return rowType;
      }

      @Override public RelOptSchema getRelOptSchema() {
        return MockRelOptSchema.this;
      }

      @Override public RelNode toRel(ToRelContext context) {
        return LogicalTableScan.create(context.getCluster(), this,
            context.getTableHints());
      }

      @Override public List<RelCollation> getCollationList() {
        return collationList;
      }

      @Override public RelDistribution getDistribution() {
        return RelDistributions.BROADCAST_DISTRIBUTED;
      }

      @Override public boolean isKey(ImmutableBitSet columns) {
        return false;
      }

      @Override public List<ImmutableBitSet> getKeys() {
        return ImmutableList.of();
      }

      @Override public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
      }

      @Override public List<ColumnStrategy> getColumnStrategies() {
        throw new UnsupportedOperationException();
      }

      @Override public Expression getExpression(Class clazz) {
        return null;
      }

      @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        final RelDataType extendedRowType =
            getRelOptSchema().getTypeFactory().builder()
                .addAll(rowType.getFieldList())
                .addAll(extendedFields)
                .build();
        return new MockColumnSet(names, extendedRowType, collationList);
      }
    }
  }

  /** Table that delegates to a given table. */
  private static class DelegatingRelOptTable implements RelOptTable {
    private final RelOptTable parent;

    DelegatingRelOptTable(RelOptTable parent) {
      this.parent = parent;
    }

    @Override public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return parent.unwrap(clazz);
    }

    @Override public Expression getExpression(Class clazz) {
      return parent.getExpression(clazz);
    }

    @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      return parent.extend(extendedFields);
    }

    @Override public List<String> getQualifiedName() {
      return parent.getQualifiedName();
    }

    @Override public double getRowCount() {
      return parent.getRowCount();
    }

    @Override public RelDataType getRowType() {
      return parent.getRowType();
    }

    @Override public RelOptSchema getRelOptSchema() {
      return parent.getRelOptSchema();
    }

    @Override public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this,
          context.getTableHints());
    }

    @Override public List<RelCollation> getCollationList() {
      return parent.getCollationList();
    }

    @Override public RelDistribution getDistribution() {
      return parent.getDistribution();
    }

    @Override public boolean isKey(ImmutableBitSet columns) {
      return parent.isKey(columns);
    }

    @Override public List<ImmutableBitSet> getKeys() {
      return parent.getKeys();
    }

    @Override public List<RelReferentialConstraint> getReferentialConstraints() {
      return parent.getReferentialConstraints();
    }

    @Override public List<ColumnStrategy> getColumnStrategies() {
      return parent.getColumnStrategies();
    }
  }

  /**
   * Default implementation of {@link Tester}, using mock classes
   * {@link MockRelOptSchema} and {@link MockRelOptPlanner}.
   */
  public static class TesterImpl implements Tester {
    private SqlOperatorTable opTab;
    private final boolean enableLateDecorrelate;
    private final boolean enableTypeCoercion; // TODO remove
    private final SqlConformance conformance;
    private final SqlNewTestFactory.CatalogReaderFactory catalogReaderFactory;
    private final Function<RelOptCluster, RelOptCluster> clusterFactory;
    private final Supplier<RelDataTypeFactory> typeFactorySupplier =
        DEFAULT_TYPE_FACTORY_SUPPLIER; // TODO: remove
    private final UnaryOperator<SqlToRelConverter.Config> configTransform;
    private final UnaryOperator<Context> contextTransform; // TODO: replace with Context
    private final UnaryOperator<SqlValidator> validatorTransform;

    /** Creates a TesterImpl with default options. */
    protected TesterImpl() {
      this(false, true,
          MockCatalogReaderSimple::create, null,
          UnaryOperator.identity(), SqlConformanceEnum.DEFAULT,
          c -> Contexts.empty(), v -> v);
    }

    /**
     * Creates a TesterImpl.
     *
     * @param catalogReaderFactory Function to create catalog reader, or null
     * @param clusterFactory Called after a cluster has been created
     */
    protected TesterImpl(boolean enableLateDecorrelate,
        boolean enableTypeCoercion,
        SqlNewTestFactory.CatalogReaderFactory catalogReaderFactory,
        Function<RelOptCluster, RelOptCluster> clusterFactory,
        UnaryOperator<SqlToRelConverter.Config> configTransform,
        SqlConformance conformance, UnaryOperator<Context> contextTransform,
        UnaryOperator<SqlValidator> validatorTransform) {
      this.enableLateDecorrelate = enableLateDecorrelate;
      this.enableTypeCoercion = enableTypeCoercion;
      this.catalogReaderFactory =
          requireNonNull(catalogReaderFactory, "catalogReaderFactory");
      this.clusterFactory = clusterFactory;
      this.configTransform = requireNonNull(configTransform, "configTransform");
      this.conformance = requireNonNull(conformance, "conformance");
      this.contextTransform = requireNonNull(contextTransform, "contextTransform");
      this.validatorTransform = requireNonNull(validatorTransform, "validatorTransform");
    }

    @Override public Pair<SqlValidator, RelRoot> convertSqlToRel2(
        SqlNewTestFactory factory, String sql, boolean decorrelate,
        boolean trim) {
      requireNonNull(sql, "sql");
      final SqlNode sqlQuery;
      try {
        sqlQuery = parseQuery(factory, sql);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
      final SqlToRelConverter converter = factory.createSqlToRelConverter();
      final SqlValidator validator = requireNonNull(converter.validator);

      final SqlNode validatedQuery = validator.validate(sqlQuery);
      RelRoot root =
          converter.convertQuery(validatedQuery, false, true);
      assert root != null;
      if (decorrelate || trim) {
        root = root.withRel(converter.flattenTypes(root.rel, true));
      }
      if (decorrelate) {
        root = root.withRel(converter.decorrelate(sqlQuery, root.rel));
      }
      if (trim) {
        root = root.withRel(converter.trimUnusedFields(true, root.rel));
      }
      return Pair.of(validator, root);
    }

    @Override public RelNode trimRelNode(SqlNewTestFactory factory,
        RelNode relNode) {
      final SqlToRelConverter converter = factory.createSqlToRelConverter();
      RelNode r2 = converter.flattenTypes(relNode, true);
      return converter.trimUnusedFields(true, r2);
    }

    protected final RelDataTypeFactory getTypeFactory() { // TODO: remove
      return typeFactorySupplier.get();
    }

    @Override public SqlNode parseQuery(SqlNewTestFactory factory, String sql)
        throws Exception {
      SqlParser parser = factory.createParser(sql);
      return parser.parseQuery();
    }

    @Override public SqlNode parseExpression(SqlNewTestFactory factory,
        String expr) throws Exception {
      SqlParser parser = factory.createParser(expr);
      return parser.parseExpression();
    }

    @Override public SqlConformance getConformance() {
      return conformance;
    }

    @Override public SqlValidator createValidator(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      final SqlOperatorTable operatorTable = getOperatorTable();
      final SqlConformance conformance = getConformance();
      final List<SqlOperatorTable> list = new ArrayList<>();
      list.add(operatorTable);
      if (conformance.allowGeometry()) {
        list.add(SqlOperatorTables.spatialInstance());
      }
      SqlValidator validator = new FarragoTestValidator(
          SqlOperatorTables.chain(list),
          catalogReader,
          typeFactory,
          SqlValidator.Config.DEFAULT
              .withConformance(conformance)
              .withTypeCoercionEnabled(enableTypeCoercion)
              .withIdentifierExpansion(true));
      return validatorTransform.apply(validator);
    }

    @Override public final SqlOperatorTable getOperatorTable() {
      if (opTab == null) {
        opTab = createOperatorTable();
      }
      return opTab;
    }

    /**
     * Creates an operator table.
     *
     * @return New operator table
     */
    protected SqlOperatorTable createOperatorTable() {
      return getContext().maybeUnwrap(SqlOperatorTable.class)
          .orElseGet(() -> {
            final MockSqlOperatorTable opTab =
                new MockSqlOperatorTable(SqlStdOperatorTable.instance());
            MockSqlOperatorTable.addRamp(opTab);
            return opTab;
          });
    }

    private Context getContext() {
      return contextTransform.apply(Contexts.empty());
    }

    @Override public Prepare.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory) {
      return (Prepare.CatalogReader) catalogReaderFactory.create(typeFactory,
          true);
    }

    @Override public void assertConvertsTo(SqlNewTestFactory factory,
        DiffRepository diffRepos,
        String sql,
        String plan,
        boolean trim,
        boolean expression,
        boolean decorrelate) {
      if (expression) {
        assertExprConvertsTo(factory, diffRepos, sql, plan);
      } else {
        assertSqlConvertsTo(factory, diffRepos, sql, plan, trim, decorrelate);
      }
    }

    private void assertExprConvertsTo(SqlNewTestFactory factory,
        DiffRepository diffRepos, String expr, String plan) {
      String expr2 = diffRepos.expand("sql", expr);
      RexNode rex = convertExprToRex(factory, expr2);
      assertNotNull(rex);
      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + rex + NL;
      diffRepos.assertEquals("plan", plan, actual);
    }

    private void assertSqlConvertsTo(SqlNewTestFactory factory,
        DiffRepository diffRepos, String sql, String plan,
        boolean trim,
        boolean decorrelate) {
      String sql2 = diffRepos.expand("sql", sql);
      final Pair<SqlValidator, RelRoot> pair =
          convertSqlToRel2(factory, sql2, decorrelate, trim);
      final RelRoot root = requireNonNull(pair.right);
      final SqlValidator validator = requireNonNull(pair.left);
      RelNode rel = root.project();

      assertNotNull(rel);
      assertThat(rel, relIsValid());

      if (trim) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        final RelFieldTrimmer trimmer =
            createFieldTrimmer(validator, relBuilder);
        rel = trimmer.trim(rel);
        assertNotNull(rel);
        assertThat(rel, relIsValid());
      }

      // NOTE jvs 28-Mar-2006:  insert leading newline so
      // that plans come out nicely stacked instead of first
      // line immediately after CDATA start
      String actual = NL + RelOptUtil.toString(rel);
      diffRepos.assertEquals("plan", plan, actual);
    }

    private RexNode convertExprToRex(SqlNewTestFactory factory, String expr) {
      requireNonNull(expr, "expr");
      final SqlNode sqlQuery;
      try {
        sqlQuery = parseExpression(factory, expr);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }

      final SqlToRelConverter converter = factory.createSqlToRelConverter();
      final SqlValidator validator = requireNonNull(converter.validator);
      final SqlNode validatedQuery = validator.validate(sqlQuery);
      return converter.convertExpression(validatedQuery);
    }

    /**
     * Creates a RelFieldTrimmer.
     *
     * @param validator Validator
     * @param relBuilder Builder
     * @return Field trimmer
     */
    public RelFieldTrimmer createFieldTrimmer(SqlValidator validator,
        RelBuilder relBuilder) {
      return new RelFieldTrimmer(validator, relBuilder);
    }

    @Override public TesterImpl withLateDecorrelation(boolean enableLateDecorrelate) {
      return this.enableLateDecorrelate == enableLateDecorrelate
          ? this
          : new TesterImpl(
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, configTransform, conformance,
              contextTransform, validatorTransform);
    }

    @Override public Tester withConfig(UnaryOperator<SqlToRelConverter.Config> transform) {
      final UnaryOperator<SqlToRelConverter.Config> configTransform =
          this.configTransform.andThen(transform)::apply;
      return new TesterImpl(
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, configTransform, conformance,
          contextTransform, validatorTransform);
    }

    @Override public TesterImpl withConformance(SqlConformance conformance) {
      return conformance.equals(this.conformance)
          ? this
          : new TesterImpl(
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, configTransform, conformance,
              contextTransform, validatorTransform);
    }

    @Override public Tester enableTypeCoercion(boolean enableTypeCoercion) {
      return enableTypeCoercion == this.enableTypeCoercion
          ? this
          : new TesterImpl(
              enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
              clusterFactory, configTransform, conformance,
              contextTransform, validatorTransform);
    }

    @Override public Tester withCatalogReaderFactory(
        SqlNewTestFactory.CatalogReaderFactory catalogReaderFactory) {
      return new TesterImpl(
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, configTransform, conformance,
          contextTransform, validatorTransform);
    }

    @Override public Tester withClusterFactory(
        Function<RelOptCluster, RelOptCluster> clusterFactory) {
      return new TesterImpl(
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, configTransform, conformance,
          contextTransform, validatorTransform);
    }

    @Override public TesterImpl withContext(
        UnaryOperator<Context> contextTransform) {
      return new TesterImpl(
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, configTransform, conformance,
          contextTransform, validatorTransform);
    }

    @Override public boolean isLateDecorrelate() {
      return enableLateDecorrelate;
    }

    @Override public Tester withValidatorTransform(
        UnaryOperator<SqlValidator> transform) {
      return new TesterImpl(
          enableLateDecorrelate, enableTypeCoercion, catalogReaderFactory,
          clusterFactory, configTransform, conformance,
          contextTransform, validatorTransform);
    }
  }

  /** Validator for testing. */
  // TODO remove
  static class FarragoTestValidator extends SqlValidatorImpl {
    FarragoTestValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Config config) {
      super(opTab, catalogReader, typeFactory, config);
    }
  }

  /**
   * {@link RelOptTable.ViewExpander} implementation for testing usage.
   */
  // TODO: move to SqlNewTestFactory? was previously private
  public static class MockViewExpander implements RelOptTable.ViewExpander {
    private final SqlValidator validator;
    private final Prepare.CatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final SqlToRelConverter.Config config;

    public MockViewExpander(
        SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlToRelConverter.Config config) {
      this.validator = validator;
      this.catalogReader = catalogReader;
      this.cluster = cluster;
      this.config = config;
    }

    @Override public RelRoot expandView(RelDataType rowType, String queryString,
        List<String> schemaPath, List<String> viewPath) {
      try {
        SqlNode parsedNode = SqlParser.create(queryString).parseStmt();
        SqlNode validatedNode = validator.validate(parsedNode);
        SqlToRelConverter converter = new SqlToRelConverter(
            this, validator, catalogReader, cluster,
            StandardConvertletTable.INSTANCE, config);
        return converter.convertQuery(validatedNode, false, true);
      } catch (SqlParseException e) {
        throw new RuntimeException("Error happened while expanding view.", e);
      }
    }
  }

  /**
   * Custom implementation of Correlate for testing.
   */
  public static class CustomCorrelate extends Correlate {
    public CustomCorrelate(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        JoinRelType joinType) {
      super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
    }

    @Override public Correlate copy(RelTraitSet traitSet,
        RelNode left, RelNode right, CorrelationId correlationId,
        ImmutableBitSet requiredColumns, JoinRelType joinType) {
      return new CustomCorrelate(getCluster(), traitSet, left, right,
          correlationId, requiredColumns, joinType);
    }

    @Override public RelNode accept(RelShuttle shuttle) {
      return shuttle.visit(this);
    }
  }

}
