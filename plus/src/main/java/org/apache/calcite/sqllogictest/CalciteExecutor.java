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

package org.apache.calcite.sqllogictest;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;
import net.hydromatic.sqllogictest.executors.SqlSltTestExecutor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class CalciteExecutor extends SqlSltTestExecutor {
  Logger logger = Logger.getLogger("CalciteExecutor");
  private final JdbcExecutor statementExecutor;
  private final Connection connection;

  public CalciteExecutor(JdbcExecutor statementExecutor) throws SQLException {
    this.statementExecutor = statementExecutor;
    // Build our connection
    this.connection = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE");
    CalciteConnection calciteConnection = this.connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    DataSource hsqldb = JdbcSchema.dataSource(
        "jdbc:hsqldb:mem:db",
        "org.hsqldb.jdbcDriver",
        "",
        ""
    );
    final String SCHEMA_NAME = "SLT";
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, SCHEMA_NAME, hsqldb, null, null);
    rootSchema.add(SCHEMA_NAME, jdbcSchema);
    calciteConnection.setSchema(SCHEMA_NAME);
  }

  boolean statement(SltSqlStatement statement) throws SQLException {
    this.statementExecutor.statement(statement);
    return true;
  }

  void query(SqlTestQuery query, TestStatistics statistics) throws UnsupportedEncodingException {
    String q = query.getQuery();
    logger.info(() -> "Executing query " + q);
    try (PreparedStatement ps = this.connection.prepareStatement(q)) {
      ps.execute();
      try (ResultSet resultSet = ps.getResultSet()) {
        this.statementExecutor.validate(query, resultSet, query.outputDescription, statistics);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    } catch (SQLException e) {
      StringPrintStream str = new StringPrintStream();
      e.printStackTrace(str.getPrintStream());
      statistics.addFailure(new TestStatistics.FailedTestDescription(
          query, str.toString()));
    }
  }

  @Override
  public TestStatistics execute(SLTTestFile file, ExecutionOptions options)
      throws IOException, SQLException {
    this.startTest();
    this.statementExecutor.establishConnection();
    this.statementExecutor.dropAllViews();
    this.statementExecutor.dropAllTables();

    TestStatistics result = new TestStatistics(options.stopAtFirstError);
    for (ISqlTestOperation operation : file.fileContents) {
      SLTSqlStatement stat = operation.as(SLTSqlStatement.class);
      if (stat != null) {
        boolean status;
        try {
          if (this.buggyOperations.contains(stat.statement)) {
            logger.info(() -> "Skipping buggy test " + stat.statement);
            status = stat.shouldPass;
          } else {
            status = this.statement(stat);
          }
        } catch (SQLException ex) {
          logger.warning("Statement failed " + stat.statement);
          status = false;
        }
        this.statementsExecuted++;
        if (this.validateStatus &&
            status != stat.shouldPass)
          throw new RuntimeException("Statement " + stat.statement + " status " + status + " expected " + stat.shouldPass);
      } else {
        SqlTestQuery query = operation.to(SqlTestQuery.class);
        if (this.buggyOperations.contains(query.getQuery())) {
          logger.info(() -> "Skipping buggy test " + query.getQuery());
          result.incIgnored();
          continue;
        }
        this.query(query, result);
      }
    }
    this.statementExecutor.closeConnection();
    this.reportTime(result.getPassed());
    logger.info(() -> "Finished executing " + file);
    return result;
  }
}
