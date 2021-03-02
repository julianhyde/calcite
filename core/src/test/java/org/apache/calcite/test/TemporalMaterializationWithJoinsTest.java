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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;

/** Test for materalized views on joins. */
public class TemporalMaterializationWithJoinsTest extends SqlToRelTestBase {

  public static FrameworkConfig config() {
    SqlParser.Config sqlParseConfig = SqlParser.Config.DEFAULT
        .withUnquotedCasing(Casing.UNCHANGED).withLex(Lex.MYSQL);
    RelBuilder.Config relBuilderConfig = RelBuilder.Config.DEFAULT
        .withDedupAggregateCalls(false);
    Context context = Contexts.of(relBuilderConfig);
    Frameworks.ConfigBuilder buildingConfig = Frameworks.newConfigBuilder()
        .parserConfig(sqlParseConfig)
        .context(context);
    return buildingConfig.build();
  }

  public static RelBuilder relBuilder() {
    return RelBuilder.create(config());
  }

  public static RelNode orderItems(RelBuilder builder) {
    //              LogicalTableScan(table=[[looker, order_items]])
    //                      "id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //                            "order_id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //                            "amount" -> {BasicSqlType@28244} "DECIMAL(19, 0)"

    final RelDataTypeFactory.Builder typeBuilder = builder.getTypeFactory().builder();
    typeBuilder.add("ID", SqlTypeName.INTEGER);
    typeBuilder.add("ORDER_ID", SqlTypeName.INTEGER);
    typeBuilder.add("AMOUNT", SqlTypeName.DECIMAL);
    RelDataType vals = typeBuilder.build();
    RelOptAbstractTable t =
        new RelOptAbstractTable(builder.getRelOptSchema(), "orders_items",
            vals) {
        };
    return new TableScan(builder.getCluster(), builder.getCluster().traitSet(),
        Collections.emptyList(), t) {
    };
  }

  public static RelNode orders(RelBuilder builder) {
    // LogicalTableScan(table=[[looker, order_items]])
    //   "id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //   "created_at" -> {BasicSqlType@28244} "DECIMAL(19, 0)"

    final RelDataTypeFactory.Builder typeBuilder =
        builder.getTypeFactory().builder();
    typeBuilder.add("ID", SqlTypeName.INTEGER);
    typeBuilder.add("USER_ID", SqlTypeName.INTEGER);
    typeBuilder.add("ORDER_AMOUNT", SqlTypeName.DECIMAL);
    typeBuilder.add("STATUS", SqlTypeName.VARCHAR);
    typeBuilder.add("CREATED_AT", SqlTypeName.TIMESTAMP);
    RelDataType vals = typeBuilder.build();
    builder.clear();

    RelOptAbstractTable t =
        new RelOptAbstractTable(builder.getRelOptSchema(), "orders", vals) {
        };
    return new TableScan(builder.getCluster(), builder.getCluster().traitSet(),
        Collections.emptyList(), t) {
    };
  }

  /**
   * Makes the materialization rel.
   */
  public static RelNode makeMaterialization(RelBuilder builder) {
    // Attempting to replicate:
    //   LogicalAggregate(group=[{0}], order_items.order_size=[COUNT()],
    //       order_items.order_sum=[$SUM0($1)])
    //     LogicalProject(order_items.order_id=[$1],
    //         $f3=[lookerFirst(template<order_items.amount >(), $2, $1,
    //         lookerFirst(null:NULL, $0, $1, $2, $3))])
    //       LogicalTableScan(table=[[looker, order_items]])
    //         "id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //          "order_id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //          "amount" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    final RelNode orderItems = orderItems(builder);
    builder.clear();
    builder.push(orderItems);
    RexBuilder rexBuilder = relBuilder().getRexBuilder();
    builder.project(
        builder.field(1),
        builder.field(2));

    ImmutableBitSet bitSet = ImmutableBitSet.of(0);
    RelBuilder.GroupKey groupKey = builder.groupKey(bitSet);

    return builder.aggregate(groupKey,
        builder.count(false, "ORDER_ITEMS.ORDER_SIZE"),
        builder.aggregateCall(SqlStdOperatorTable.SUM0,
            builder.field(1)).as("ORDER_ITEMS.ORDER_SUM"))
        .build();
  }

  /**
   * Builds the query rel in 4 steps.
   */
  public static RelNode makeQueryRel(RelBuilder builder) {
    // (4)
    //   LogicalProject($f0=[toDate_DATE_YEAR_asString(FLOOR($0, FLAG(YEAR)))],
    //       $f1=[$1])
    //     LogicalAggregate(group=[{0}], agg#0=[COUNT()])
    //       LogicalProject($f0=[FLOOR($2, FLAG(YEAR))])

    //(3)
    //  LogicalJoin(condition=[=($0, $1)], joinType=[inner])
    //(1)
    //  LogicalProject(order_id=[$1])
    //    ExplicitlyAliasedTableScan(table=[[looker, order_items]])
    //      order_id -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //(2)
    // LogicalProject(id=[$0], created_at=[$4])
    //   ExplicitlyAliasedTableScan(table=[[looker, orders]])
    //     "id" -> {BasicSqlType@28244} "DECIMAL(19, 0)"
    //     "created_at" -> {BasicSqlType@28260} "TIMESTAMP(0)"

    //(1)
    final RelNode orderItems = orderItems(builder);
    builder.clear();
    builder.push(orderItems);
    builder.project(builder.alias(builder.field(1), "ORDER_ID"));
    final RelNode leftBase = builder.build();

    //(2)
    final RelNode orders = orders(builder);
    builder.clear();
    final RelNode rightBase =
        builder.push(orders)
            .project(builder.alias(builder.field(0), "ID"),
                builder.alias(builder.field(4), "CREATED_AT"))
            .build();

    //(3) --
    int leftJoinIndex =
        leftBase.getRowType().getFieldNames().indexOf("ORDER_ID");
    int rightJoinIndex = rightBase.getRowType().getFieldNames().indexOf("ID");

    RexInputRef leftKeyInputRef =
        RexInputRef.of(leftJoinIndex, leftBase.getRowType());
    RexInputRef rightKeyInputRef =
        RexInputRef.of(leftBase.getRowType().getFieldCount()
            + rightJoinIndex, rightBase.getRowType());
    RexNode joinCond =
        builder.call(SqlStdOperatorTable.EQUALS, leftKeyInputRef,
            rightKeyInputRef);
    builder.push(leftBase).push(rightBase).join(JoinRelType.INNER, joinCond);

    //(4)
    RexBuilder rexBuilder = builder.getRexBuilder();
    builder.project(
        rexBuilder.makeCall(
            SqlStdOperatorTable.FLOOR,
            builder.field(2),
            rexBuilder.makeFlag(TimeUnitRange.YEAR)));

    ImmutableBitSet bitSet = ImmutableBitSet.of(0);
    RelBuilder.GroupKey groupKey = builder.groupKey(bitSet);
    builder.aggregate(groupKey, builder.count());
    return builder.build();
  }

  public static RelNode virtualMaterializationScanRelNode(RelBuilder builder,
      RelNode eqRel, String tableName) {
    builder.clear();
    builder.push(eqRel);
    builder.as(tableName);
    final RelNode t = builder.build();
    RelOptAbstractTable table =
        new RelOptAbstractTable(builder.getRelOptSchema(), tableName,
            t.getRowType()) {
        };
    return new TableScan(t.getCluster(), t.getTraitSet().getDefault(),
        Collections.emptyList(), table) {
    };
  }

  public static String sql(RelNode rel) {
    SqlPrettyWriter writer =
        new SqlPrettyWriter(SqlPrettyWriter.config()
            .withClauseEndsLine(true)
            .withSelectListExtraIndentFlag(false)
            .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
            .withGroupByFolding(SqlWriterConfig.LineFolding.TALL)
            .withOrderByFolding(SqlWriterConfig.LineFolding.TALL)
            .withFromFolding(SqlWriterConfig.LineFolding.TALL));
    RelToSqlConverter relToSqlConverter =
        new RelToSqlConverter(CalciteSqlDialect.DEFAULT);
    SqlNode statement = relToSqlConverter.visitRoot(rel).asStatement();
    statement.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }

  public static void putSql(RelNode rel, String msg) {
    String sql = sql(rel);
    System.out.println(msg + ":");
    System.out.println(sql);
    System.out.println("-- .");
  }

  public static void putRel(RelNode rel, String msg) {
    if (msg.length() > 1) {
      System.out.println(msg);
    }
    System.out.println(RelOptUtil.toString(rel) + "- - - - -\n");
  }

  @Test void materializationWithJoinQueryOptimizes() {
    final RelBuilder builder = relBuilder();
    final RelNode matRel = makeMaterialization(builder);
    putRel(matRel, "Materialization Rel");
    putSql(matRel, "mat rel sql");

    final RelNode queryRel = makeQueryRel(builder);
    putRel(queryRel, "Query rel");
    putSql(queryRel, "Query Sql");

    // Attempt optimization?
    MaterializedViewOnlyAggregateRule rule =
        MaterializedViewOnlyAggregateRule.Config.DEFAULT
            .withGenerateUnionRewriting(false)
            .withUnionRewritingPullProgram(null)
            .as(MaterializedViewOnlyAggregateRule.Config.class)
            .toRule();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(rule)
        .build();
    HepPlanner planner = new HepPlanner(program);

    // Should be like our table scan:
    final String matViewName = "testing_mv";
    final RelNode prepMV = virtualMaterializationScanRelNode(builder, matRel, matViewName);
    putRel(prepMV, "Should be similar to our table scan");

    final RelOptMaterialization relOptMaterialization =
        new RelOptMaterialization(prepMV, matRel, null, Lists.newArrayList(matViewName));
    planner.addMaterialization(relOptMaterialization);
    planner.setRoot(queryRel);
    RelNode maybeOptimized = planner.findBestExp();

    putRel(maybeOptimized, "Optimized?");
    putSql(maybeOptimized, "Opti SQL");
    final String optiSql = sql(maybeOptimized);

    assertThat("Materialization failed", optiSql.contains(matViewName));

    // The project under aggregate field $f8 should reference $7 instead of $0
    //  LogicalProject(ORDER_ID=[$0], ORDER_ITEMS.ORDER_SIZE=[$1],
    //    ORDER_ITEMS.ORDER_SUM=[$2], ID=[$3], USER_ID=[$4], ORDER_AMOUNT=[$5],
    //    STATUS=[$6], CREATED_AT=[$7], $f8=[FLOOR($0, FLAG(YEAR))])
    final String optiRel = RelOptUtil.toString(maybeOptimized);
    assertThat("Referencing wrong input", !optiRel.contains("$f8=[FLOOR($0, FLAG(YEAR))]"));
    assertThat("Referencing wrong input", optiRel.contains("$f8=[FLOOR($7, FLAG(YEAR))]"));
  }
}
