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
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.calcite.rel.rel2sql.DialectTestConfig.Dialect.of;

/** Utilities for {@link DialectTestConfig}. */
class DialectTestConfigs {
  private DialectTestConfigs() {
  }

  static final Supplier<DialectTestConfig> INSTANCE_SUPPLIER =
      Suppliers.memoize(() -> {
        final ImmutableList.Builder<DialectTestConfig.Dialect> b =
            ImmutableList.builder();
        extracted(b::add);
        final ImmutableList<DialectTestConfig.Dialect> list = b.build();
        final Iterable<String> dialectNames =
            Util.transform(list, dialect -> dialect.name);
        if (!Ordering.natural().isOrdered(dialectNames)) {
          throw new AssertionError("not ordered: " + dialectNames);
        }
        return DialectTestConfig.of(list);
      })::get;

  private static void extracted(Consumer<DialectTestConfig.Dialect> list) {
    // The following list must be ordered by dialect name.
    list.accept(of("bigquery", SqlDialect.DatabaseProduct.BIG_QUERY));
    list.accept(of("calcite", SqlDialect.DatabaseProduct.CALCITE));
    list.accept(of("clickhouse", SqlDialect.DatabaseProduct.CLICKHOUSE));
    list.accept(of("db2", SqlDialect.DatabaseProduct.DB2));
    list.accept(of("exasol", SqlDialect.DatabaseProduct.EXASOL));
    list.accept(of("firebolt", SqlDialect.DatabaseProduct.FIREBOLT));
    list.accept(of("hive", SqlDialect.DatabaseProduct.HIVE));
    list.accept(of("hsqldb", SqlDialect.DatabaseProduct.HSQLDB));
    list.accept(of("informix", SqlDialect.DatabaseProduct.INFORMIX));
    list.accept(of("jethro", JETHRO_DIALECT_SUPPLIER.get()));
    // MSSQL 2008 = 10.0, 2012 = 11.0, 2017 = 14.0
    list.accept(of("mssql", mssqlDialect(14)));

    final SqlDialect mysqlDialect =
        SqlDialect.DatabaseProduct.MYSQL.getDialect();
    list.accept(of("mysql", mysqlDialect));
    final SqlDialect mysql8Dialect =
        new SqlDialect(
            MysqlSqlDialect.DEFAULT_CONTEXT.withDatabaseMajorVersion(8)
                .withIdentifierQuoteString(mysqlDialect.quoteIdentifier("")
                    .substring(0, 1))
                .withNullCollation(mysqlDialect.getNullCollation()));
    list.accept(of("mysql-8", mysql8Dialect));
    list.accept(of("mysql-first", mysqlDialect(NullCollation.FIRST)));
    list.accept(of("mysql-high", mysqlDialect(NullCollation.HIGH)));
    list.accept(of("mysql-last", mysqlDialect(NullCollation.LAST)));

    list.accept(of("oracle", SqlDialect.DatabaseProduct.ORACLE));
    list.accept(of("oracle-modified", oracleDialect(512)));
    list.accept(of("postgresql", SqlDialect.DatabaseProduct.POSTGRESQL));
    list.accept(of("postgresql-modified", postgresqlDialect(256)));
    list.accept(of("presto", SqlDialect.DatabaseProduct.PRESTO));
    list.accept(of("redshift", SqlDialect.DatabaseProduct.REDSHIFT));
    list.accept(of("snowflake", SqlDialect.DatabaseProduct.SNOWFLAKE));
    list.accept(of("spark", SqlDialect.DatabaseProduct.SPARK));
    list.accept(of("sybase", SqlDialect.DatabaseProduct.SYBASE));
    list.accept(of("vertica", SqlDialect.DatabaseProduct.VERTICA));
  }

  static MysqlSqlDialect mysqlDialect(NullCollation nullCollation) {
    return new MysqlSqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT
        .withNullCollation(nullCollation));
  }

  static SqlDialect oracleDialect(final int maxVarcharLength) {
    return new OracleSqlDialect(OracleSqlDialect.DEFAULT_CONTEXT
        .withDataTypeSystem(new RelDataTypeSystemImpl() {
          @Override public int getMaxPrecision(SqlTypeName typeName) {
            switch (typeName) {
            case VARCHAR:
              return maxVarcharLength;
            default:
              return super.getMaxPrecision(typeName);
            }
          }
        }));
  }

  static SqlDialect postgresqlDialect(final int maxVarcharLength) {
    return new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT
        .withDataTypeSystem(new RelDataTypeSystemImpl() {
          @Override public int getMaxPrecision(SqlTypeName typeName) {
            switch (typeName) {
            case VARCHAR:
              return maxVarcharLength;
            default:
              return super.getMaxPrecision(typeName);
            }
          }
        }));
  }

  static SqlDialect mssqlDialect(int majorVersion) {
    final SqlDialect mssqlDialect =
        SqlDialect.DatabaseProduct.MSSQL.getDialect();
    return new MssqlSqlDialect(MssqlSqlDialect.DEFAULT_CONTEXT
        .withDatabaseMajorVersion(majorVersion)
        .withIdentifierQuoteString(mssqlDialect.quoteIdentifier("")
            .substring(0, 1))
        .withNullCollation(mssqlDialect.getNullCollation()));
  }

  static final Supplier<SqlDialect> JETHRO_DIALECT_SUPPLIER =
      Suppliers.memoize(() ->
          new JethroDataSqlDialect(
              SqlDialect.EMPTY_CONTEXT
                  .withDatabaseProduct(SqlDialect.DatabaseProduct.JETHRO)
                  .withDatabaseMajorVersion(1)
                  .withDatabaseMinorVersion(0)
                  .withDatabaseVersion("1.0")
                  .withIdentifierQuoteString("\"")
                  .withNullCollation(NullCollation.HIGH)
                  .withJethroInfo(JethroDataSqlDialect.JethroInfo.EMPTY)));
}
