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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import static org.apache.calcite.rel.rel2sql.DialectTestConfigs.JETHRO_DIALECT_SUPPLIER;

/** Dialect code. */
enum DialectCode {
  ANSI(new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT)),
  BIG_QUERY(SqlDialect.DatabaseProduct.BIG_QUERY),
  CALCITE(SqlDialect.DatabaseProduct.CALCITE),
  CLICKHOUSE(SqlDialect.DatabaseProduct.CLICKHOUSE),
  DB2(SqlDialect.DatabaseProduct.DB2),
  EXASOL(SqlDialect.DatabaseProduct.EXASOL),
  FIREBOLT(SqlDialect.DatabaseProduct.FIREBOLT),
  HIVE(SqlDialect.DatabaseProduct.HIVE),
  HIVE_2_0(
      new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
          .withDatabaseMajorVersion(2)
          .withDatabaseMinorVersion(0)
          .withNullCollation(NullCollation.LOW))),
  HIVE_2_1(
      new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
          .withDatabaseMajorVersion(2)
          .withDatabaseMinorVersion(1)
          .withNullCollation(NullCollation.LOW))),
  HIVE_2_2(
      new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
          .withDatabaseMajorVersion(2)
          .withDatabaseMinorVersion(2)
          .withNullCollation(NullCollation.LOW))),
  HSQLDB(SqlDialect.DatabaseProduct.HSQLDB),
  INFORMIX(SqlDialect.DatabaseProduct.INFORMIX),
  JETHRO(JETHRO_DIALECT_SUPPLIER.get()),
  MOCK(new MockSqlDialect()),
  MSSQL_2008(DialectTestConfigs.mssqlDialect(10)),
  MSSQL_2012(DialectTestConfigs.mssqlDialect(11)),
  MSSQL_2017(DialectTestConfigs.mssqlDialect(14)),
  MYSQL(SqlDialect.DatabaseProduct.MYSQL),
  MYSQL_8(
      new SqlDialect(
          MysqlSqlDialect.DEFAULT_CONTEXT.withDatabaseMajorVersion(8)
              .withIdentifierQuoteString(
                  DialectTestConfigs.mysqlDialect(NullCollation.LOW)
                      .quoteIdentifier("").substring(0, 1))
              .withNullCollation(
                  DialectTestConfigs.mysqlDialect(NullCollation.LOW)
                      .getNullCollation()))),
  MYSQL_FIRST(DialectTestConfigs.mysqlDialect(NullCollation.FIRST)),
  MYSQL_HIGH(DialectTestConfigs.mysqlDialect(NullCollation.HIGH)),
  MYSQL_LAST(DialectTestConfigs.mysqlDialect(NullCollation.LAST)),
  NON_ORDINAL(DialectTestConfigs.nonOrdinalDialect()),
  ORACLE(SqlDialect.DatabaseProduct.ORACLE),
  /** Oracle dialect with max length for varchar set to 512. */
  ORACLE_MODIFIED(DialectTestConfigs.oracleDialect(512)),
  POSTGRESQL(SqlDialect.DatabaseProduct.POSTGRESQL),
  /** Postgresql dialect with max length for varchar set to 256. */
  POSTGRESQL_MODIFIED(DialectTestConfigs.postgresqlDialect(256)),
  PRESTO(SqlDialect.DatabaseProduct.PRESTO),
  REDSHIFT(SqlDialect.DatabaseProduct.REDSHIFT),
  SNOWFLAKE(SqlDialect.DatabaseProduct.SNOWFLAKE),
  SPARK(SqlDialect.DatabaseProduct.SPARK),
  SYBASE(SqlDialect.DatabaseProduct.SYBASE),
  VERTICA(SqlDialect.DatabaseProduct.VERTICA);

  private final DialectTestConfig.Dialect dialect;

  DialectCode(SqlDialect.DatabaseProduct databaseProduct) {
    dialect = DialectTestConfig.Dialect.of(this, databaseProduct);
  }

  DialectCode(SqlDialect sqlDialect) {
    dialect = DialectTestConfig.Dialect.of(this, sqlDialect);
  }

  DialectTestConfig.Dialect toDialect() {
    return dialect;
  }
}