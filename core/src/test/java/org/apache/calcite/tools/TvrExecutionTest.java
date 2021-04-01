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
package org.apache.calcite.tools;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.tvr.rules.TvrRuleCollection;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.util.Holder;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

/**
 * Demonstration program to use the Tempura optimizer in an end-to-end query.
 * This program generates a progressive physical plan and then uses Calcite's built-in executor to run the plan.
 * The output at each time point is printed to the console.
 *
 * The program runs on a static table generated by Java reflection based on a Java array.
 * The program simulates a progressive computing and progressive data arrival by consuming a part of the data at a time.
 *
 * The scan operator evenly divides the input data across each time point.
 * For example, if a table has 6 tuples, with 3 total runs, each run consumes 1/3 of input data (2 tuples)
 *
 */
public class TvrExecutionTest {

  // the early progressive run time points besides the original final run
  // for example, if the original query runs at time 24:00
  // 28800000 represents a time point at 8:00, 57600000 represents a time point at 16:00
  // there will be 2 early runs that produce partial results, and 1 final run that produces the final result
  //
  public static String instants = "28800000, 57600000";


  @Test
  public void testAvgAgg() throws Exception {
    String query = "select \"deptno\", avg(\"salary\") from \"hr\".\"emps\" group by \"deptno\"\n";
    run(query);
  }

  public void run(String query) throws ClassNotFoundException, SQLException {
    Hook.PROGRAM.add(holder -> {
      if (holder instanceof Holder) {
        ((Holder) holder).set(new Programs.TvrRuleSetProgram(RuleSets.ofList(TvrRuleCollection.tvrStandardRuleSet())));
      }
    });

    // must set ENABLE_ENUMERABLE in CalcitePrepareImpl to false
    // to prevent directly generating EnumerableTableScan instead of LogicalTableScan
    System.getProperties().setProperty("calcite.enable.enumerable", "false");

    Properties properties = new Properties();
    properties.setProperty(TvrUtils.PROGRESSIVE_ENABLE, "true");
    properties.setProperty(TvrUtils.ENABLE_VOLCANO_VISUALIZER, "false");
    properties.setProperty(TvrUtils.PROGRESSIVE_INSTANTS, instants);
    properties.setProperty(TvrUtils.PROGRESSIVE_META_AVAILABLE, "false");
    properties.setProperty(TvrUtils.PROGRESSIVE_TUPLE_NO_DUPLICATE, "true");
    properties.setProperty(TvrUtils.PROGRESSIVE_VIRTUAL_TABLESINK, "true");
    properties.setProperty(TvrUtils.PROGRESSIVE_REQUIRE_OUTPUT_VIEW, "true");

    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection =
            DriverManager.getConnection("jdbc:calcite:", properties);
    CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new TvrExecutionTest.HrSchema()));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
            statement.executeQuery(query);
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        buf.append(i > 1 ? "; " : "")
                .append(resultSet.getMetaData().getColumnLabel(i))
                .append("=")
                .append(resultSet.getObject(i));
      }
      System.out.println(buf.toString());
      buf.setLength(0);
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  public static class HrSchema {
    @Override
    public String toString() {
      return "HrSchema";
    }

    public final JdbcTest.Employee[] emps = {
            new JdbcTest.Employee(100, 10, "Bill", 10000, 1000),
            new JdbcTest.Employee(200, 20, "Eric", 8000, 500),
            new JdbcTest.Employee(150, 10, "Sebastian", 7000, null),
            new JdbcTest.Employee(250, 20, "Foo", 4000, 500),
            new JdbcTest.Employee(110, 10, "Theodore", 11500, 250),
            new JdbcTest.Employee(300, 20, "Bar", 3000, 500),
            new JdbcTest.Employee(100, 10, "Bill", 10000, 1000),
            new JdbcTest.Employee(200, 20, "Eric", 8000, 500),
            new JdbcTest.Employee(150, 10, "Sebastian", 7000, null),
            new JdbcTest.Employee(250, 20, "Foo", 4000, 500),
            new JdbcTest.Employee(110, 10, "Theodore", 11500, 250),
            new JdbcTest.Employee(300, 20, "Bar", 3000, 500),
    };

  }
}

// End JdbcExample.java
